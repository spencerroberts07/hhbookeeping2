"""
Invoice-document → source matching.

Two invoice streams produce different match strategies:

    invoice_type='hh_ap'
        Each PDF represents one HH AP invoice. Match candidates live in
        hh_ap_invoices (rows the statement parser has already created).
        Primary key is invoice_number; amount is the tiebreaker.

    invoice_type='outside_vendor'
        Each PDF represents a vendor invoice that should land in bank
        outflows. Match candidates live in bank_transactions, scored by
        amount + date proximity.

Confidence is a 0-100 score. >=95 auto-creates the invoice_journal_links
row; lower scores are surfaced as suggestions in the unmatched queue.

All functions are *additive* — they never raise out of the calling
transaction. Callers wrap in try/except already (see route + journal-hook
sites) but we belt-and-brace by catching internally too, so a parsing
error or schema drift can't take down an existing journal post.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import text

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Result types
# --------------------------------------------------------------------------


@dataclass
class MatchResult:
    """One candidate match for an invoice document."""

    match_type: Literal['bank', 'journal', 'hh_ap']
    target_id: str
    amount: Decimal
    when: date | None
    description: str
    confidence: float  # 0-100
    auto_linked: bool  # True if we created the link row, False if it's only a suggestion


@dataclass
class SweepSummary:
    auto_matched: int
    suggested: int
    unmatched: int
    invoices_examined: int


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def auto_match_invoice(
    session,
    *,
    invoice: dict[str, Any],
    entity_code: str,
) -> list[MatchResult]:
    """
    Find candidate matches for one invoice_documents row. Auto-create the
    link row when confidence >= 95; otherwise return the candidates as
    suggestions. Always returns a list — empty if nothing scored above the
    floor (or the invoice is in a state we can't match).

    Never raises. Logs and returns [] on failure.
    """
    try:
        if invoice.get('status') in {'matched', 'posted_to_ap', 'deleted'}:
            # Already linked or deleted — nothing to do.
            return []

        invoice_type = invoice.get('invoice_type')
        if invoice_type == 'hh_ap':
            results = _match_hh_ap(session, invoice=invoice, entity_code=entity_code)
        elif invoice_type == 'outside_vendor':
            results = _match_outside_vendor(session, invoice=invoice, entity_code=entity_code)
        else:
            logger.warning(
                "auto_match_invoice: unknown invoice_type=%r for invoice %s",
                invoice_type, invoice.get('id'),
            )
            return []

        # Auto-link the top result if confidence >= 95.
        if results and results[0].confidence >= 95:
            top = results[0]
            try:
                _create_link(
                    session,
                    invoice_id=invoice['id'],
                    entity_code=entity_code,
                    match_type=top.match_type,
                    target_id=top.target_id,
                    confidence=top.confidence,
                    linked_by='auto',
                )
                _mark_matched(
                    session,
                    invoice_id=invoice['id'],
                    confidence=top.confidence,
                )
                top.auto_linked = True
            except Exception:
                logger.exception(
                    "auto_match_invoice: failed to create link for invoice %s",
                    invoice.get('id'),
                )

        return results
    except Exception:
        logger.exception(
            "auto_match_invoice: unexpected failure for invoice %s",
            invoice.get('id'),
        )
        return []


def auto_match_for_journal_line(
    session,
    *,
    entity_code: str,
    line_amount: Decimal,
    account_code: str,
    journal_batch_id: UUID | str,
    journal_line_id: UUID | str | None = None,
) -> int:
    """
    Hook called after a new journal_line is created. If there is an
    unmatched invoice_documents row whose amount matches `line_amount`
    exactly and whose ap_account matches `account_code`, link them.

    Returns the number of invoice rows linked (0 or 1 in practice).

    Never raises. Catches and logs.
    """
    try:
        if account_code not in {'2020', '2030'}:
            return 0
        rows = session.execute(
            text(
                """
                SELECT id, status, ap_account
                  FROM invoice_documents
                 WHERE entity_code = :ec
                   AND status = 'unmatched'
                   AND amount = :amt
                   AND (ap_account IS NULL OR ap_account = :acct)
                 ORDER BY uploaded_at DESC
                 LIMIT 1
                """
            ),
            {"ec": entity_code, "amt": line_amount, "acct": account_code},
        ).mappings().all()
        if not rows:
            return 0
        target = rows[0]
        _create_link(
            session,
            invoice_id=target['id'],
            entity_code=entity_code,
            match_type='journal',
            target_id=str(journal_batch_id),
            confidence=95.0,
            linked_by='auto',
            journal_line_id=journal_line_id,
        )
        _mark_matched(
            session, invoice_id=target['id'], confidence=95.0
        )
        return 1
    except Exception:
        logger.exception(
            "auto_match_for_journal_line: hook failed (entity=%s amount=%s acct=%s) — "
            "journal creation NOT affected",
            entity_code, line_amount, account_code,
        )
        return 0


def run_period_match_sweep(
    session,
    *,
    entity_code: str,
    period_end: date | None = None,
) -> SweepSummary:
    """
    Re-run auto_match_invoice on every unmatched invoice for the entity.
    Optional period_end filters to invoices whose invoice_date falls in the
    same calendar month or earlier — useful when a dealer uploaded
    invoices first and the matching candidates (bank txns, HH statement)
    arrived later.
    """
    where = ["entity_code = :ec", "status = 'unmatched'"]
    params: dict[str, Any] = {"ec": entity_code}
    if period_end is not None:
        where.append("(invoice_date IS NULL OR invoice_date <= :pe)")
        params["pe"] = period_end

    rows = session.execute(
        text(
            f"""
            SELECT id, entity_code, invoice_type, invoice_number, vendor_name,
                   invoice_date, due_date, amount, status, ap_account
              FROM invoice_documents
             WHERE {' AND '.join(where)}
             ORDER BY uploaded_at DESC
            """
        ),
        params,
    ).mappings().all()

    auto_matched = 0
    suggested = 0
    unmatched = 0
    for inv in rows:
        results = auto_match_invoice(
            session, invoice=dict(inv), entity_code=entity_code,
        )
        if any(r.auto_linked for r in results):
            auto_matched += 1
        elif results:
            suggested += 1
        else:
            unmatched += 1
    return SweepSummary(
        auto_matched=auto_matched,
        suggested=suggested,
        unmatched=unmatched,
        invoices_examined=len(rows),
    )


# --------------------------------------------------------------------------
# Internal matchers
# --------------------------------------------------------------------------


def _match_hh_ap(
    session, *, invoice: dict[str, Any], entity_code: str
) -> list[MatchResult]:
    """HH AP: match against rows in hh_ap_invoices."""
    invoice_number = (invoice.get('invoice_number') or '').strip()
    amount = Decimal(str(invoice.get('amount') or 0))

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return []

    candidates: list[MatchResult] = []
    seen: set[str] = set()

    # Exact invoice-number match — confidence 100.
    if invoice_number:
        rows = session.execute(
            text(
                """
                SELECT id, invoice_number, vendor_name, invoice_date,
                       total_amount
                  FROM hh_ap_invoices
                 WHERE entity_id = :eid
                   AND invoice_number = :inum
                """
            ),
            {"eid": entity['id'], "inum": invoice_number},
        ).mappings().all()
        for r in rows:
            rid = str(r['id'])
            if rid in seen:
                continue
            seen.add(rid)
            candidates.append(
                MatchResult(
                    match_type='hh_ap',
                    target_id=rid,
                    amount=Decimal(str(r['total_amount'] or 0)),
                    when=r['invoice_date'],
                    description=f"HH AP invoice #{r['invoice_number']}"
                    + (f" · {r['vendor_name']}" if r['vendor_name'] else ''),
                    confidence=100.0,
                    auto_linked=False,
                )
            )

    # Amount-only match — confidence 70.
    if amount and amount > 0:
        rows = session.execute(
            text(
                """
                SELECT id, invoice_number, vendor_name, invoice_date,
                       total_amount
                  FROM hh_ap_invoices
                 WHERE entity_id = :eid
                   AND total_amount = :amt
                 LIMIT 20
                """
            ),
            {"eid": entity['id'], "amt": amount},
        ).mappings().all()
        for r in rows:
            rid = str(r['id'])
            if rid in seen:
                continue
            seen.add(rid)
            candidates.append(
                MatchResult(
                    match_type='hh_ap',
                    target_id=rid,
                    amount=Decimal(str(r['total_amount'] or 0)),
                    when=r['invoice_date'],
                    description=f"HH AP #{r['invoice_number']} · amount-only match"
                    + (f" · {r['vendor_name']}" if r['vendor_name'] else ''),
                    confidence=70.0,
                    auto_linked=False,
                )
            )

    return candidates


def _match_outside_vendor(
    session, *, invoice: dict[str, Any], entity_code: str
) -> list[MatchResult]:
    """Outside vendor: match against bank_transactions by amount + date."""
    amount = invoice.get('amount')
    if amount is None:
        return []
    amount = Decimal(str(amount))
    if amount <= 0:
        return []

    inv_date: date | None = invoice.get('invoice_date')
    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return []

    # Pull every bank transaction within ±30 days whose amount is within $1.
    where = [
        "entity_id = :eid",
        "ABS(amount) BETWEEN :amt_lo AND :amt_hi",
    ]
    params: dict[str, Any] = {
        "eid": entity['id'],
        "amt_lo": amount - Decimal('1.00'),
        "amt_hi": amount + Decimal('1.00'),
    }
    if inv_date is not None:
        where.append("transaction_date BETWEEN :d_lo AND :d_hi")
        params["d_lo"] = inv_date - timedelta(days=30)
        params["d_hi"] = inv_date + timedelta(days=30)

    rows = session.execute(
        text(
            f"""
            SELECT id, transaction_date, amount, description, direction
              FROM bank_transactions
             WHERE {' AND '.join(where)}
             ORDER BY transaction_date DESC
             LIMIT 50
            """
        ),
        params,
    ).mappings().all()

    candidates: list[MatchResult] = []
    for r in rows:
        bank_amt = Decimal(str(r['amount'])).copy_abs()
        confidence = _score_outside(
            invoice_amount=amount,
            invoice_date=inv_date,
            bank_amount=bank_amt,
            bank_date=r['transaction_date'],
        )
        if confidence < 50:
            continue
        candidates.append(
            MatchResult(
                match_type='bank',
                target_id=str(r['id']),
                amount=bank_amt,
                when=r['transaction_date'],
                description=r['description'] or '',
                confidence=confidence,
                auto_linked=False,
            )
        )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates


def _score_outside(
    *,
    invoice_amount: Decimal,
    invoice_date: date | None,
    bank_amount: Decimal,
    bank_date: date | None,
) -> float:
    """Confidence ladder from the spec — outside-vendor branch."""
    amt_exact = bank_amount == invoice_amount
    amt_within_dollar = abs(bank_amount - invoice_amount) <= Decimal('1.00')

    if invoice_date is None or bank_date is None:
        # No date — exact amount only.
        return 60.0 if amt_exact else (40.0 if amt_within_dollar else 0.0)

    days = abs((bank_date - invoice_date).days)
    if amt_exact and days <= 7:
        return 95.0
    if amt_exact and days <= 15:
        return 80.0
    if amt_exact and days <= 30:
        return 65.0
    if amt_within_dollar and days <= 30:
        return 50.0
    return 0.0


# --------------------------------------------------------------------------
# DB writers
# --------------------------------------------------------------------------


def _create_link(
    session,
    *,
    invoice_id: UUID | str,
    entity_code: str,
    match_type: str,
    target_id: str,
    confidence: float,
    linked_by: str,
    journal_line_id: UUID | str | None = None,
) -> None:
    """Insert one invoice_journal_links row. Idempotent on (invoice_id,
    target_id, link_type) — duplicate inserts no-op."""
    params: dict[str, Any] = {
        "invoice_id": invoice_id,
        "entity_code": entity_code,
        "link_type": match_type,
        "linked_by": linked_by,
        "confidence": confidence,
        "journal_batch_id": None,
        "journal_line_id": journal_line_id,
        "bank_transaction_id": None,
        "hh_ap_invoice_id": None,
    }
    if match_type == 'journal':
        params["journal_batch_id"] = target_id
    elif match_type == 'bank':
        params["bank_transaction_id"] = target_id
    elif match_type == 'hh_ap':
        params["hh_ap_invoice_id"] = target_id
    else:
        raise ValueError(f"Unknown match_type {match_type!r}")

    # Idempotency: skip if a link with the same triple already exists.
    existing = session.execute(
        text(
            """
            SELECT id FROM invoice_journal_links
             WHERE invoice_document_id = :invoice_id
               AND link_type = :link_type
               AND COALESCE(journal_batch_id::text, '') = COALESCE(:journal_batch_id::text, '')
               AND COALESCE(bank_transaction_id::text, '') = COALESCE(:bank_transaction_id::text, '')
               AND COALESCE(hh_ap_invoice_id::text, '') = COALESCE(:hh_ap_invoice_id::text, '')
             LIMIT 1
            """
        ),
        params,
    ).mappings().first()
    if existing:
        return

    session.execute(
        text(
            """
            INSERT INTO invoice_journal_links (
                invoice_document_id, entity_code, link_type,
                journal_batch_id, journal_line_id,
                bank_transaction_id, hh_ap_invoice_id,
                linked_by, confidence
            ) VALUES (
                :invoice_id, :entity_code, :link_type,
                :journal_batch_id, :journal_line_id,
                :bank_transaction_id, :hh_ap_invoice_id,
                :linked_by, :confidence
            )
            """
        ),
        params,
    )


def _mark_matched(
    session,
    *,
    invoice_id: UUID | str,
    confidence: float,
    actor: str | None = None,
) -> None:
    session.execute(
        text(
            """
            UPDATE invoice_documents
               SET status = 'matched',
                   matched_at = NOW(),
                   matched_by_clerk_user_id = COALESCE(:actor, matched_by_clerk_user_id),
                   match_confidence = :confidence,
                   updated_at = NOW()
             WHERE id = :id
               AND status = 'unmatched'
            """
        ),
        {"id": invoice_id, "confidence": confidence, "actor": actor},
    )
