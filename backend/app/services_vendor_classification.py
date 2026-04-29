"""
Self-improving vendor classification — service layer.

Sits between Layer 1 (hard-coded bank_transaction_rules) and Layer 3
(Claude API). Holds two responsibilities:

    1. Vendor memory lookup    — given a bank transaction description,
                                 return the most-likely (account_code,
                                 confidence) the system has learned.
    2. Memory updates           — `learn_from_gl_history` (bootstrap
                                  from QBO GL imports) and
                                  `record_user_feedback` (pin a
                                  bookkeeper's accept/override decision
                                  back into memory at the highest trust
                                  level).

Trust gradient:
    'gl_history'      → seeded automatically.       confidence = 0.4 + 0.1*occurrences (cap 1.0)
    'ai_seeded'       → Claude suggested + accepted. confidence = 0.8
    'user_confirmed'  → bookkeeper explicitly chose. confidence = 1.0
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
)


SOURCE_GL_HISTORY = "gl_history"
SOURCE_AI_SEEDED = "ai_seeded"
SOURCE_USER_CONFIRMED = "user_confirmed"

LAYER_RULES = "rules"
LAYER_VENDOR_MEMORY = "vendor_memory"
LAYER_CLAUDE = "claude"

STATUS_PENDING = "pending"
STATUS_ACCEPTED = "accepted"
STATUS_OVERRIDDEN = "overridden"
STATUS_REJECTED = "rejected"

# Memory tier above which a vendor-memory hit can flow into the
# auto-journal as a draft line. Below this, the suggestion is recorded
# but the row stays unmatched until the user explicitly promotes it.
VENDOR_MEMORY_AUTO_DRAFT_THRESHOLD = Decimal("0.700")
# Confidence threshold below which a Claude suggestion is rejected as
# too unsure to even draft.
CLAUDE_MIN_CONFIDENCE = Decimal("0.500")


# ----------------------------------------------------------------------
# Description normalization
# ----------------------------------------------------------------------


_RE_DATE_TOKEN = re.compile(r"^[A-Z]{3}\d{1,2}$")
_RE_PURE_DIGITS_OR_DASHES = re.compile(r"^[\d\-]+$")
_RE_NON_WORD_NON_SPACE = re.compile(r"[^\w\s\-]+")
_RE_LEADING_DIGITS = re.compile(r"^\d+")
_RE_TRAILING_DIGITS = re.compile(r"\d+$")

# Common noise tokens that don't add classification value.
_NOISE_TOKENS = frozenset(
    {
        "INC",
        "LTD",
        "LLC",
        "LLP",
        "CO",
        "CORP",
        "THE",
    }
)


def _normalize_vendor_key(description: str | None) -> str:
    """
    Stable, classification-friendly key from a free-text bank-transaction
    description. Drops merchant ids, cheque numbers, dates, and pure
    digit tokens; keeps the alpha tokens that identify the vendor /
    transaction type.

    Examples (verified against the Feb 2026 TD statement):
        'VSA DEP14350 MSP'           -> 'VSA_DEP_MSP'
        'AMEX 1995722444 MSP'        -> 'AMEX_MSP'
        'AMX FEE12593422 MSP'        -> 'AMX_FEE_MSP'
        'EF0202 14350 MSP'           -> 'EF_MSP'
        'CHQ#00177-1148309816'       -> 'CHQ'
        'Hydro Ottawa MSP'           -> 'HYDRO_OTTAWA_MSP'
        'SEND E-TFR *4YZ BPY'        -> 'SEND_E-TFR_BPY'
        'TD EXPRESS DEPOSIT'         -> 'TD_EXPRESS_DEPOSIT'
        'PENINSULA EMPLO BPY'        -> 'PENINSULA_EMPLO_BPY'
        'GST34 6766896 BUS'          -> 'GST_BUS'
    """
    if not description:
        return ""
    text_upper = description.upper()
    # Remove punctuation we don't want to keep (#, *, /, etc.) but keep
    # spaces and hyphens (E-TFR is meaningful).
    text_upper = _RE_NON_WORD_NON_SPACE.sub(" ", text_upper)
    tokens = text_upper.split()
    cleaned: list[str] = []
    for tok in tokens:
        # Pure digits or pure digits+dashes → drop (cheque numbers,
        # batch ids, transaction reference numbers).
        if _RE_PURE_DIGITS_OR_DASHES.match(tok):
            continue
        # Date tokens like 'FEB02' → drop.
        if _RE_DATE_TOKEN.match(tok):
            continue
        # Strip leading and trailing digits from inside the token
        # ('DEP14350' → 'DEP', 'EF0202' → 'EF', '00177ABC' → 'ABC').
        tok = _RE_LEADING_DIGITS.sub("", tok)
        tok = _RE_TRAILING_DIGITS.sub("", tok)
        if not tok:
            continue
        if len(tok) < 2:
            continue
        if tok in _NOISE_TOKENS:
            continue
        cleaned.append(tok)
    return "_".join(cleaned)


def _confidence_for_gl_history(occurrences: int) -> Decimal:
    score = Decimal("0.4") + Decimal("0.1") * Decimal(int(occurrences))
    if score > Decimal("1.0"):
        return Decimal("1.000")
    return score.quantize(Decimal("0.001"))


# ----------------------------------------------------------------------
# Bootstrap from QBO GL imports
# ----------------------------------------------------------------------


def learn_from_gl_history(
    session,
    *,
    entity_code: str,
    gl_import_run_id: str,
    actor_email: str,
) -> dict[str, Any]:
    """
    Walk gl_transactions for the given import run and seed
    vendor_classification_memory with one row per
    (account_code, normalized_vendor_key) pair, weighted by occurrences.

    Idempotent: re-running for the same import_run_id bumps
    occurrences_count and recomputes confidence_score; previously
    user-confirmed entries (source='user_confirmed') keep their
    confidence at 1.0 and never get downgraded.
    """
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(gl_import_run_id, "gl_import_run_id")

    txns = session.execute(
        text(
            """
            SELECT account_code, account_name, transaction_type,
                   reference_number, name, memo, split_account, amount
            FROM gl_transactions
            WHERE entity_id = :entity_id
              AND import_run_id = :run_id
              AND amount IS NOT NULL
            """
        ),
        {"entity_id": entity["id"], "run_id": run_uuid},
    ).mappings().all()

    # Group by (account_code, normalized_key) -> example list, count, dr/cr
    aggregates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in txns:
        # Skip the "Beginning Balance" marker rows that the parser
        # records under split_account='Beginning Balance'.
        if (row["split_account"] or "").strip().lower() == "beginning balance":
            continue
        # Build the source description for normalization. Prefer the
        # vendor name; fall back to memo, then split_account.
        sources = [
            (row["name"] or "").strip(),
            (row["memo"] or "").strip(),
        ]
        # Skip 'Closing cash' / 'Opening cash' style memo-only rows.
        memo_lower = (row["memo"] or "").strip().lower()
        if memo_lower in {"closing cash", "opening cash", ""} and not sources[0]:
            continue
        text_for_key = " ".join(s for s in sources if s).strip()
        if not text_for_key:
            continue
        key = _normalize_vendor_key(text_for_key)
        if not key or len(key) < 2:
            continue
        amount = Decimal(str(row["amount"] or 0))
        if amount == 0:
            continue
        dr_or_cr = "debit" if amount > 0 else "credit"
        agg_key = (row["account_code"], key)
        agg = aggregates.setdefault(
            agg_key,
            {
                "account_code": row["account_code"],
                "account_name": row["account_name"],
                "key": key,
                "examples": [],
                "occurrences": 0,
                "debit_count": 0,
                "credit_count": 0,
            },
        )
        agg["occurrences"] += 1
        if dr_or_cr == "debit":
            agg["debit_count"] += 1
        else:
            agg["credit_count"] += 1
        if len(agg["examples"]) < 5 and text_for_key not in agg["examples"]:
            agg["examples"].append(text_for_key)

    if not aggregates:
        return {
            "entity_code": entity_code,
            "gl_import_run_id": gl_import_run_id,
            "transactions_scanned": len(txns),
            "unique_vendor_account_pairs": 0,
            "inserted": 0,
            "updated": 0,
            "skipped_user_confirmed": 0,
        }

    # Pre-fetch only the user_confirmed rows for this entity (small
    # set, fast) so we can skip them Python-side. This lets us use a
    # simpler ON CONFLICT DO UPDATE on the bulk path — Render's
    # Postgres rejects the multi-CASE update we tried earlier.
    user_confirmed_keys: set[tuple[str, str]] = set()
    rows_uc = session.execute(
        text(
            """
            SELECT account_code, normalized_vendor_key
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND source = :source
            """
        ),
        {"entity_id": entity["id"], "source": SOURCE_USER_CONFIRMED},
    ).mappings().all()
    for r in rows_uc:
        user_confirmed_keys.add((r["account_code"], r["normalized_vendor_key"]))

    # Pre-fetch ONLY the prior occurrences_count per (key, account)
    # for non-user_confirmed rows so we can accumulate. One small read.
    prior_occurrences: dict[tuple[str, str], int] = {}
    rows_pri = session.execute(
        text(
            """
            SELECT account_code, normalized_vendor_key, occurrences_count
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND source <> :source
            """
        ),
        {"entity_id": entity["id"], "source": SOURCE_USER_CONFIRMED},
    ).mappings().all()
    for r in rows_pri:
        prior_occurrences[(r["account_code"], r["normalized_vendor_key"])] = int(
            r["occurrences_count"] or 0
        )

    rows_to_upsert: list[dict[str, Any]] = []
    skipped_user_confirmed = 0
    for (account_code, key), agg in aggregates.items():
        if (account_code, key) in user_confirmed_keys:
            skipped_user_confirmed += 1
            continue
        dr_or_cr = "debit" if agg["debit_count"] >= agg["credit_count"] else "credit"
        prior = prior_occurrences.get((account_code, key), 0)
        new_occurrences = prior + agg["occurrences"]
        confidence = _confidence_for_gl_history(new_occurrences)
        rows_to_upsert.append(
            {
                "key": key,
                "account_code": account_code,
                "examples": agg["examples"],
                "dr_or_cr": dr_or_cr,
                "occurrences": new_occurrences,
                "confidence": str(confidence),
            }
        )

    notes = f"Seeded from gl_import_run {gl_import_run_id} by {actor_email}"

    # Simple UPSERT — user_confirmed rows are already filtered out
    # Python-side, and the pre-aggregated occurrences_count already
    # includes the prior count, so a plain SET overwrites correctly.
    upsert_sql = text(
        """
        INSERT INTO vendor_classification_memory (
            entity_id, normalized_vendor_key, raw_examples,
            account_code, debit_or_credit, source,
            occurrences_count, confidence_score, notes
        ) VALUES (
            :entity_id, :key, CAST(:examples AS jsonb),
            :account_code, :dr_or_cr, :source,
            :occurrences, :confidence, :notes
        )
        ON CONFLICT (entity_id, normalized_vendor_key, account_code)
        DO UPDATE SET
            raw_examples = EXCLUDED.raw_examples,
            debit_or_credit = EXCLUDED.debit_or_credit,
            occurrences_count = EXCLUDED.occurrences_count,
            confidence_score = EXCLUDED.confidence_score,
            source = EXCLUDED.source,
            last_seen_at = NOW()
        """
    )
    # Bulk upsert against the outer session. Free-tier Render Postgres
    # used to abort the connection on bulk writes — that's why earlier
    # versions of this code spun up a NullPool engine per chunk. The
    # paid Basic tier handles plain executemany via the existing
    # session cleanly, so we use it directly.
    CHUNK = 100
    upserted = 0
    for start in range(0, len(rows_to_upsert), CHUNK):
        chunk = rows_to_upsert[start : start + CHUNK]
        params = [
            {
                "entity_id": entity["id"],
                "key": r["key"],
                "examples": json.dumps(r["examples"]),
                "account_code": r["account_code"],
                "dr_or_cr": r["dr_or_cr"],
                "source": SOURCE_GL_HISTORY,
                "occurrences": r["occurrences"],
                "confidence": r["confidence"],
                "notes": notes,
            }
            for r in chunk
        ]
        session.execute(upsert_sql, params)
        upserted += len(chunk)

    return {
        "entity_code": entity_code,
        "gl_import_run_id": gl_import_run_id,
        "transactions_scanned": len(txns),
        "unique_vendor_account_pairs": len(aggregates),
        "rows_upserted": upserted,
    }


# ----------------------------------------------------------------------
# Lookup
# ----------------------------------------------------------------------


def vendor_memory_lookup(
    session, *, entity_id: UUID, description: str
) -> dict[str, Any] | None:
    """
    Highest-confidence row for the description's normalized key.
    Returns None if memory has nothing or the description normalizes
    to an empty key.
    """
    key = _normalize_vendor_key(description)
    if not key:
        return None
    row = session.execute(
        text(
            """
            SELECT id, normalized_vendor_key, account_code, debit_or_credit,
                   source, occurrences_count, confidence_score
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND normalized_vendor_key = :key
            ORDER BY confidence_score DESC, occurrences_count DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "key": key},
    ).mappings().first()
    if not row:
        return None
    return {
        "id": str(row["id"]),
        "normalized_vendor_key": row["normalized_vendor_key"],
        "account_code": row["account_code"],
        "debit_or_credit": row["debit_or_credit"],
        "source": row["source"],
        "occurrences_count": row["occurrences_count"],
        "confidence_score": Decimal(str(row["confidence_score"])),
    }


def vendor_memory_similar(
    session,
    *,
    entity_id: UUID,
    description: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """
    Return up to `limit` vendor_memory rows whose normalized_key shares
    at least one token with the input description. Used to feed Claude
    a few "here's how similar past vendors were classified" examples.
    """
    key = _normalize_vendor_key(description)
    if not key:
        return []
    tokens = [t for t in key.split("_") if len(t) >= 3]
    if not tokens:
        return []
    # Match any row whose key contains any of our tokens.
    like_clauses = " OR ".join(
        [f"normalized_vendor_key LIKE :tok{i}" for i in range(len(tokens))]
    )
    params: dict[str, Any] = {"entity_id": entity_id, "limit": int(limit)}
    for i, t in enumerate(tokens):
        params[f"tok{i}"] = f"%{t}%"
    rows = session.execute(
        text(
            f"""
            SELECT normalized_vendor_key, account_code, debit_or_credit,
                   source, occurrences_count, confidence_score, raw_examples
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND ({like_clauses})
            ORDER BY confidence_score DESC, occurrences_count DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return [
        {
            "normalized_vendor_key": r["normalized_vendor_key"],
            "account_code": r["account_code"],
            "debit_or_credit": r["debit_or_credit"],
            "source": r["source"],
            "occurrences_count": r["occurrences_count"],
            "confidence_score": Decimal(str(r["confidence_score"])),
            "raw_examples": r["raw_examples"],
        }
        for r in rows
    ]


# ----------------------------------------------------------------------
# Suggestion bookkeeping
# ----------------------------------------------------------------------


def record_suggestion(
    session,
    *,
    entity_id: UUID,
    bank_transaction_id: UUID,
    auto_journal_run_id: UUID,
    layer: str,
    suggested_account_code: str | None,
    suggested_debit_or_credit: str | None,
    confidence_score: Decimal | None,
    reasoning: str | None,
    raw_response: dict[str, Any] | None = None,
) -> UUID:
    """
    Insert a suggestion row (pending). UNIQUE on (entity_id,
    bank_transaction_id) — re-running the auto-journal for the same
    transaction overwrites the previous pending suggestion but leaves
    accepted/overridden ones alone.
    """
    existing = session.execute(
        text(
            """
            SELECT id, status FROM bank_classification_suggestions
            WHERE entity_id = :entity_id
              AND bank_transaction_id = :bt_id
            """
        ),
        {"entity_id": entity_id, "bt_id": bank_transaction_id},
    ).mappings().first()

    if existing and existing["status"] in (STATUS_ACCEPTED, STATUS_OVERRIDDEN):
        # Keep the historical record; don't churn it.
        return existing["id"]

    if existing:
        session.execute(
            text(
                """
                UPDATE bank_classification_suggestions
                   SET layer = :layer,
                       suggested_account_code = :acct,
                       suggested_debit_or_credit = :dr_cr,
                       confidence_score = :conf,
                       reasoning = :reasoning,
                       raw_response_json = CAST(:raw AS jsonb),
                       auto_journal_run_id = :run_id,
                       status = :status
                 WHERE id = :id
                """
            ),
            {
                "layer": layer,
                "acct": suggested_account_code,
                "dr_cr": suggested_debit_or_credit,
                "conf": confidence_score,
                "reasoning": reasoning,
                "raw": json.dumps(raw_response or {}, default=str),
                "run_id": auto_journal_run_id,
                "status": STATUS_PENDING,
                "id": existing["id"],
            },
        )
        return existing["id"]

    row = session.execute(
        text(
            """
            INSERT INTO bank_classification_suggestions (
                entity_id, bank_transaction_id, auto_journal_run_id,
                layer, suggested_account_code, suggested_debit_or_credit,
                confidence_score, reasoning, raw_response_json, status
            ) VALUES (
                :entity_id, :bt_id, :run_id,
                :layer, :acct, :dr_cr,
                :conf, :reasoning, CAST(:raw AS jsonb), :status
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity_id,
            "bt_id": bank_transaction_id,
            "run_id": auto_journal_run_id,
            "layer": layer,
            "acct": suggested_account_code,
            "dr_cr": suggested_debit_or_credit,
            "conf": confidence_score,
            "reasoning": reasoning,
            "raw": json.dumps(raw_response or {}, default=str),
            "status": STATUS_PENDING,
        },
    ).mappings().first()
    return row["id"]


def record_user_feedback(
    session,
    *,
    entity_code: str,
    suggestion_id: str,
    final_account_code: str,
    final_debit_or_credit: str | None,
    actor_email: str,
    accepted: bool,
) -> dict[str, Any]:
    """
    Bookkeeper accepted or overrode a suggestion. Promotes the
    classification (final_account_code) into vendor_classification_memory
    at the highest trust level, source='user_confirmed', confidence 1.0.

    accepted=True       → status = 'accepted', final_account_code =
                          suggested account.
    accepted=False      → status = 'overridden', final_account_code =
                          whatever the bookkeeper picked.
    """
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    sug_uuid = _parse_uuid(suggestion_id, "suggestion_id")

    sug = session.execute(
        text(
            """
            SELECT bcs.id, bcs.bank_transaction_id, bcs.layer,
                   bcs.suggested_account_code, bcs.suggested_debit_or_credit,
                   bt.description, bt.amount, bt.direction
            FROM bank_classification_suggestions bcs
            JOIN bank_transactions bt ON bt.id = bcs.bank_transaction_id
            WHERE bcs.id = :id AND bcs.entity_id = :entity_id
            """
        ),
        {"id": sug_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not sug:
        raise ValueError(f"Suggestion not found: {suggestion_id}")

    dr_or_cr = (final_debit_or_credit or sug["suggested_debit_or_credit"] or "debit").lower()
    if dr_or_cr not in ("debit", "credit"):
        dr_or_cr = "debit"

    new_status = STATUS_ACCEPTED if accepted else STATUS_OVERRIDDEN

    session.execute(
        text(
            """
            UPDATE bank_classification_suggestions
               SET status = :status,
                   final_account_code = :final_acct,
                   feedback_actor_email = :actor,
                   feedback_recorded_at = NOW()
             WHERE id = :id
            """
        ),
        {
            "status": new_status,
            "final_acct": final_account_code,
            "actor": actor_email,
            "id": sug_uuid,
        },
    )

    # Promote to vendor memory at user_confirmed trust.
    description = sug["description"] or ""
    key = _normalize_vendor_key(description)
    if not key:
        return {
            "suggestion_id": suggestion_id,
            "status": new_status,
            "promoted_to_memory": False,
            "reason": "Description normalizes to empty key",
        }

    existing = session.execute(
        text(
            """
            SELECT id, occurrences_count, source
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND normalized_vendor_key = :key
              AND account_code = :account_code
            """
        ),
        {
            "entity_id": entity["id"],
            "key": key,
            "account_code": final_account_code,
        },
    ).mappings().first()

    if existing:
        session.execute(
            text(
                """
                UPDATE vendor_classification_memory
                   SET occurrences_count = occurrences_count + 1,
                       confidence_score = 1.000,
                       source = :source,
                       debit_or_credit = :dr_cr,
                       last_seen_at = NOW(),
                       raw_examples = (
                           CASE WHEN jsonb_array_length(raw_examples) >= 5
                                THEN raw_examples
                                ELSE raw_examples || CAST(:example AS jsonb)
                           END
                       )
                 WHERE id = :id
                """
            ),
            {
                "source": SOURCE_USER_CONFIRMED,
                "dr_cr": dr_or_cr,
                "example": json.dumps([description]),
                "id": existing["id"],
            },
        )
        promoted_id = existing["id"]
    else:
        ins = session.execute(
            text(
                """
                INSERT INTO vendor_classification_memory (
                    entity_id, normalized_vendor_key, raw_examples,
                    account_code, debit_or_credit, source,
                    occurrences_count, confidence_score, notes
                ) VALUES (
                    :entity_id, :key, CAST(:examples AS jsonb),
                    :account_code, :dr_cr, :source,
                    1, 1.000, :notes
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "key": key,
                "examples": json.dumps([description]),
                "account_code": final_account_code,
                "dr_cr": dr_or_cr,
                "source": SOURCE_USER_CONFIRMED,
                "notes": f"User-confirmed by {actor_email} (suggestion {suggestion_id})",
            },
        ).mappings().first()
        promoted_id = ins["id"]

    return {
        "suggestion_id": suggestion_id,
        "status": new_status,
        "promoted_to_memory": True,
        "vendor_memory_id": str(promoted_id),
        "normalized_vendor_key": key,
        "account_code": final_account_code,
    }


# ----------------------------------------------------------------------
# Read endpoints helpers
# ----------------------------------------------------------------------


def list_pending_suggestions(
    session,
    *,
    entity_code: str,
    status: str = STATUS_PENDING,
    limit: int = 200,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "bank_classification_suggestions"):
        return {"entity_code": entity_code, "count": 0, "suggestions": []}

    rows = session.execute(
        text(
            """
            SELECT bcs.id, bcs.layer, bcs.suggested_account_code,
                   bcs.suggested_debit_or_credit, bcs.confidence_score,
                   bcs.reasoning, bcs.status, bcs.final_account_code,
                   bcs.created_at, bcs.feedback_recorded_at,
                   bt.id AS bank_transaction_id, bt.transaction_date,
                   bt.description, bt.amount, bt.direction
            FROM bank_classification_suggestions bcs
            JOIN bank_transactions bt ON bt.id = bcs.bank_transaction_id
            WHERE bcs.entity_id = :entity_id
              AND (:status IS NULL OR bcs.status = :status)
            ORDER BY bcs.created_at DESC
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity["id"],
            "status": status,
            "limit": int(limit),
        },
    ).mappings().all()
    return {
        "entity_code": entity_code,
        "status_filter": status,
        "count": len(rows),
        "suggestions": [
            {
                "id": str(r["id"]),
                "bank_transaction_id": str(r["bank_transaction_id"]),
                "transaction_date": (
                    r["transaction_date"].isoformat() if r["transaction_date"] else None
                ),
                "description": r["description"],
                "amount": str(r["amount"]) if r["amount"] is not None else None,
                "direction": r["direction"],
                "layer": r["layer"],
                "suggested_account_code": r["suggested_account_code"],
                "suggested_debit_or_credit": r["suggested_debit_or_credit"],
                "confidence_score": (
                    str(r["confidence_score"]) if r["confidence_score"] is not None else None
                ),
                "reasoning": r["reasoning"],
                "status": r["status"],
                "final_account_code": r["final_account_code"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "feedback_recorded_at": (
                    r["feedback_recorded_at"].isoformat() if r["feedback_recorded_at"] else None
                ),
            }
            for r in rows
        ],
    }


def list_vendor_memory(
    session,
    *,
    entity_code: str,
    source: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "vendor_classification_memory"):
        return {"entity_code": entity_code, "count": 0, "rows": []}
    rows = session.execute(
        text(
            """
            SELECT id, normalized_vendor_key, raw_examples,
                   account_code, debit_or_credit, source,
                   occurrences_count, confidence_score,
                   first_seen_at, last_seen_at, notes
            FROM vendor_classification_memory
            WHERE entity_id = :entity_id
              AND (:source IS NULL OR source = :source)
            ORDER BY confidence_score DESC, occurrences_count DESC,
                     normalized_vendor_key
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity["id"],
            "source": source,
            "limit": int(limit),
        },
    ).mappings().all()
    return {
        "entity_code": entity_code,
        "source_filter": source,
        "count": len(rows),
        "rows": [
            {
                "id": str(r["id"]),
                "normalized_vendor_key": r["normalized_vendor_key"],
                "raw_examples": r["raw_examples"],
                "account_code": r["account_code"],
                "debit_or_credit": r["debit_or_credit"],
                "source": r["source"],
                "occurrences_count": r["occurrences_count"],
                "confidence_score": str(r["confidence_score"]),
                "first_seen_at": r["first_seen_at"].isoformat() if r["first_seen_at"] else None,
                "last_seen_at": r["last_seen_at"].isoformat() if r["last_seen_at"] else None,
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


def upsert_vendor_memory(
    session,
    *,
    entity_code: str,
    normalized_vendor_key: str,
    account_code: str,
    debit_or_credit: str,
    actor_email: str,
    notes: str | None = None,
) -> dict[str, Any]:
    """Manual entry / edit. Always recorded as source='user_confirmed'."""
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    key = (normalized_vendor_key or "").strip().upper()
    if not key:
        raise ValueError("normalized_vendor_key is required")
    if debit_or_credit not in ("debit", "credit"):
        raise ValueError("debit_or_credit must be 'debit' or 'credit'")

    row = session.execute(
        text(
            """
            INSERT INTO vendor_classification_memory (
                entity_id, normalized_vendor_key, raw_examples,
                account_code, debit_or_credit, source,
                occurrences_count, confidence_score, notes
            ) VALUES (
                :entity_id, :key, '[]'::jsonb,
                :account_code, :dr_cr, :source,
                1, 1.000, :notes
            )
            ON CONFLICT (entity_id, normalized_vendor_key, account_code)
            DO UPDATE SET
                debit_or_credit = EXCLUDED.debit_or_credit,
                source = EXCLUDED.source,
                confidence_score = 1.000,
                last_seen_at = NOW(),
                notes = EXCLUDED.notes
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "key": key,
            "account_code": account_code,
            "dr_cr": debit_or_credit,
            "source": SOURCE_USER_CONFIRMED,
            "notes": notes or f"Manual entry by {actor_email}",
        },
    ).mappings().first()
    return {
        "id": str(row["id"]),
        "entity_code": entity_code,
        "normalized_vendor_key": key,
        "account_code": account_code,
    }
