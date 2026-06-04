"""
Journal edit / reclassify / correct — the Slice 2 WRITE path for the report
drill-down. Isolated in its own module because every endpoint here mutates
accounting data (journal_lines / journal_batches).

Rules (locked by the approved plan):
  * Admin only (require_role("admin")); admin edits post immediately.
  * OPEN / draft / reopened period  -> in-place UPDATE of journal_lines.
  * LOCKED period (approved_to_close / closed_locked) -> HARD BLOCK on any
    in-place write; the only path is a CORRECTING ENTRY: a full REVERSAL of
    the original batch plus a corrected RE-ENTRY, both posted into the
    current open period, both linked back via correction_of_batch_id.
  * edit-amount supports ONLY genuine 2-line entries. >2 lines -> reject and
    direct the caller to the correcting-entry flow (never guess the line
    that absorbs the offset).
  * Every action writes a journal_line_change_events row (audit trail).
  * Balance guard: a batch must have debits == credits before the request
    returns. The guard raises HTTPException, which rolls back the session
    (db_session commits only on clean return) — nothing unbalanced commits.

Requires migration 045 (journal_line_change_events + correction_of_batch_id).
"""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_period_close import LOCKED_STATUSES, effective_period_status

router = APIRouter(prefix="/api/journal-edits", tags=["journal-edits"])

_CORRECTION_SOURCE = "correction"
_CENT = Decimal("0.01")


# --------------------------------------------------------------------------
# Request models
# --------------------------------------------------------------------------


class ReclassifyRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    journal_line_id: str
    to_account_code: str = Field(..., examples=["6510"])
    reason: str = Field(..., min_length=1)


class EditAmountRequest(BaseModel):
    entity_code: str
    journal_line_id: str
    new_debit: float = Field(..., ge=0)
    new_credit: float = Field(..., ge=0)
    reason: str = Field(..., min_length=1)


class CorrectRequest(BaseModel):
    entity_code: str
    journal_batch_id: str
    action: str = Field(..., examples=["reclassify", "edit_amount"])
    journal_line_id: str
    # reclassify:
    to_account_code: str | None = None
    # edit_amount (2-line only):
    new_debit: float | None = Field(default=None, ge=0)
    new_credit: float | None = Field(default=None, ge=0)
    reason: str = Field(..., min_length=1)


class NoteRequest(BaseModel):
    entity_code: str
    journal_batch_id: str
    journal_line_id: str | None = None
    note: str = Field(..., min_length=1)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _resolve_entity(session, entity_code: str) -> dict[str, Any]:
    row = session.execute(
        text("SELECT id, entity_code FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        raise HTTPException(404, f"Entity {entity_code!r} not found")
    return dict(row)


def _actor_email(user: Any) -> str | None:
    try:
        return user.get("email")
    except AttributeError:
        return getattr(user, "email", None)


def _period_is_locked(period_status: str | None) -> bool:
    return effective_period_status({"status": period_status}) in LOCKED_STATUSES


def _load_line(session, entity_id: str, line_id: str) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT jl.id              AS line_id,
                   jl.journal_batch_id,
                   jl.account_code,
                   jl.debit_amount,
                   jl.credit_amount,
                   jb.entity_id,
                   jb.accounting_period_id,
                   jb.status          AS batch_status,
                   jb.batch_label,
                   ap.status          AS period_status,
                   ap.period_label,
                   ap.period_end
              FROM journal_lines jl
              JOIN journal_batches jb ON jb.id = jl.journal_batch_id
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jl.id = :lid AND jb.entity_id = :eid
            """
        ),
        {"lid": line_id, "eid": entity_id},
    ).mappings().first()
    if not row:
        raise HTTPException(404, "Journal line not found for this entity")
    return dict(row)


def _load_batch(session, entity_id: str, batch_id: str) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT jb.id, jb.batch_label, jb.source_module, jb.status,
                   jb.accounting_period_id, jb.total_debits, jb.total_credits,
                   ap.status AS period_status, ap.period_label, ap.period_end
              FROM journal_batches jb
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jb.id = :bid AND jb.entity_id = :eid
            """
        ),
        {"bid": batch_id, "eid": entity_id},
    ).mappings().first()
    if not row:
        raise HTTPException(404, "Journal batch not found for this entity")
    return dict(row)


def _batch_lines(session, batch_id: str) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT id, line_number, account_code, debit_amount, credit_amount,
                   memo, source_json
              FROM journal_lines
             WHERE journal_batch_id = :bid
          ORDER BY line_number
            """
        ),
        {"bid": batch_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _assert_account_exists(session, entity_id: str, code: str) -> None:
    row = session.execute(
        text(
            """
            SELECT 1 FROM accounts
             WHERE entity_id = :eid AND account_code = :code AND is_active = TRUE
             LIMIT 1
            """
        ),
        {"eid": entity_id, "code": code},
    ).first()
    if not row:
        raise HTTPException(409, f"Account {code!r} is not an active account for this entity")


def _refresh_batch_totals(session, batch_id: str) -> None:
    session.execute(
        text(
            """
            UPDATE journal_batches
               SET total_debits  = (SELECT COALESCE(SUM(debit_amount), 0)
                                      FROM journal_lines WHERE journal_batch_id = :bid),
                   total_credits = (SELECT COALESCE(SUM(credit_amount), 0)
                                      FROM journal_lines WHERE journal_batch_id = :bid),
                   updated_at = NOW()
             WHERE id = :bid
            """
        ),
        {"bid": batch_id},
    )


def _assert_balanced(session, batch_id: str) -> None:
    """Raise 409 (→ session rollback) if the batch is not balanced."""
    r = session.execute(
        text(
            """
            SELECT COALESCE(SUM(debit_amount), 0)  AS d,
                   COALESCE(SUM(credit_amount), 0) AS c
              FROM journal_lines WHERE journal_batch_id = :bid
            """
        ),
        {"bid": batch_id},
    ).mappings().first()
    d = Decimal(str(r["d"] or 0))
    c = Decimal(str(r["c"] or 0))
    if abs(d - c) >= _CENT:
        raise HTTPException(
            409, f"Refusing to post unbalanced batch: debits {d} != credits {c}"
        )


def _log_change(
    session,
    *,
    entity_id: str,
    journal_batch_id: str | None,
    journal_line_id: str | None,
    accounting_period_id: str | None,
    action: str,
    from_account_code: str | None = None,
    to_account_code: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    reason: str | None,
    actor_email: str | None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO journal_line_change_events (
                entity_id, journal_batch_id, journal_line_id, accounting_period_id,
                action, from_account_code, to_account_code,
                before_json, after_json, reason, actor_email
            ) VALUES (
                :eid, :bid, :lid, :pid,
                :action, :from_code, :to_code,
                CAST(:before AS jsonb), CAST(:after AS jsonb), :reason, :actor
            )
            """
        ),
        {
            "eid": entity_id,
            "bid": journal_batch_id,
            "lid": journal_line_id,
            "pid": accounting_period_id,
            "action": action,
            "from_code": from_account_code,
            "to_code": to_account_code,
            "before": json.dumps(before or {}),
            "after": json.dumps(after or {}),
            "reason": reason,
            "actor": actor_email,
        },
    )


def _resolve_current_open_period(session, entity_id: str) -> dict[str, Any]:
    """Current open period to post a correction into — same tiered logic as
    routes/period_close.get_current_period, but returns the period id and
    refuses to fall back to a locked period (corrections must never post
    into a closed period)."""
    for clause in (
        # Tier 1: oldest past non-closed with an approved_to_post batch.
        """AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           AND EXISTS (SELECT 1 FROM journal_batches jb
                        WHERE jb.accounting_period_id = ap.id
                          AND jb.status = 'approved_to_post')
           ORDER BY ap.period_end ASC""",
        # Tier 2: oldest past non-closed regardless of batches.
        """AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           ORDER BY ap.period_end ASC""",
    ):
        row = session.execute(
            text(
                f"""
                SELECT ap.id, ap.period_label, ap.period_end, ap.status
                  FROM accounting_periods ap
                 WHERE ap.entity_id = :eid
                   AND ap.period_end <= CURRENT_DATE
                   {clause}
                 LIMIT 1
                """
            ),
            {"eid": entity_id},
        ).mappings().first()
        if row:
            return dict(row)
    raise HTTPException(
        409,
        "No open accounting period is available to post the correction into. "
        "Reopen a period first.",
    )


# --------------------------------------------------------------------------
# In-place edits (OPEN periods only — locked periods are hard-blocked)
# --------------------------------------------------------------------------


@router.post("/reclassify")
def reclassify_line(
    payload: ReclassifyRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Move a line to a different account, in place. Amount unchanged, so the
    batch stays balanced. Blocked on locked periods (use /correct)."""
    enforce_entity_code(_user, payload.entity_code)
    with db_session() as session:
        entity = _resolve_entity(session, payload.entity_code)
        eid = entity["id"]
        line = _load_line(session, eid, payload.journal_line_id)

        if _period_is_locked(line["period_status"]):
            raise HTTPException(
                409,
                f"Period {line['period_label']} is locked. In-place edits are "
                f"blocked — use a correcting entry (/api/journal-edits/correct).",
            )
        _assert_account_exists(session, eid, payload.to_account_code)
        from_code = line["account_code"]
        if from_code == payload.to_account_code:
            raise HTTPException(400, "Line is already on that account.")

        session.execute(
            text(
                """
                UPDATE journal_lines
                   SET account_code = :to_code, updated_at = NOW()
                 WHERE id = :lid AND journal_batch_id = :bid
                """
            ),
            {"to_code": payload.to_account_code, "lid": line["line_id"], "bid": line["journal_batch_id"]},
        )
        _assert_balanced(session, line["journal_batch_id"])  # unchanged, but defensive
        _log_change(
            session,
            entity_id=eid,
            journal_batch_id=line["journal_batch_id"],
            journal_line_id=line["line_id"],
            accounting_period_id=line["accounting_period_id"],
            action="reclassify",
            from_account_code=from_code,
            to_account_code=payload.to_account_code,
            before={"account_code": from_code},
            after={"account_code": payload.to_account_code},
            reason=payload.reason,
            actor_email=_actor_email(_user),
        )
    return {"ok": True, "action": "reclassify", "journal_batch_id": line["journal_batch_id"]}


@router.post("/edit-amount")
def edit_amount(
    payload: EditAmountRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Edit the amount on a genuine 2-line entry, in place. The counter line
    is mirrored to preserve balance. Rejected for entries with more than two
    lines (use /correct). Blocked on locked periods (use /correct)."""
    enforce_entity_code(_user, payload.entity_code)
    if payload.new_debit > 0 and payload.new_credit > 0:
        raise HTTPException(400, "A line is one-sided: provide a debit OR a credit, not both.")
    if payload.new_debit == 0 and payload.new_credit == 0:
        raise HTTPException(400, "Provide a non-zero debit or credit.")

    with db_session() as session:
        entity = _resolve_entity(session, payload.entity_code)
        eid = entity["id"]
        line = _load_line(session, eid, payload.journal_line_id)

        if _period_is_locked(line["period_status"]):
            raise HTTPException(
                409,
                f"Period {line['period_label']} is locked. In-place edits are "
                f"blocked — use a correcting entry (/api/journal-edits/correct).",
            )

        lines = _batch_lines(session, line["journal_batch_id"])
        if len(lines) != 2:
            raise HTTPException(
                409,
                f"edit-amount supports only genuine 2-line entries; this batch has "
                f"{len(lines)} lines. Use a correcting entry instead.",
            )
        counter = next(l for l in lines if str(l["id"]) != str(line["line_id"]))

        before = {
            "edited": {"id": str(line["line_id"]), "debit": float(line["debit_amount"]), "credit": float(line["credit_amount"])},
            "counter": {"id": str(counter["id"]), "debit": float(counter["debit_amount"]), "credit": float(counter["credit_amount"])},
        }
        # Edited line takes the new amounts; the 2-line counter mirrors them
        # (debit<->credit) so debits == credits without guessing anything.
        session.execute(
            text(
                """
                UPDATE journal_lines
                   SET debit_amount = :d, credit_amount = :c, updated_at = NOW()
                 WHERE id = :lid AND journal_batch_id = :bid
                """
            ),
            {"d": payload.new_debit, "c": payload.new_credit, "lid": line["line_id"], "bid": line["journal_batch_id"]},
        )
        session.execute(
            text(
                """
                UPDATE journal_lines
                   SET debit_amount = :d, credit_amount = :c, updated_at = NOW()
                 WHERE id = :lid AND journal_batch_id = :bid
                """
            ),
            {"d": payload.new_credit, "c": payload.new_debit, "lid": counter["id"], "bid": line["journal_batch_id"]},
        )
        _refresh_batch_totals(session, line["journal_batch_id"])
        _assert_balanced(session, line["journal_batch_id"])
        _log_change(
            session,
            entity_id=eid,
            journal_batch_id=line["journal_batch_id"],
            journal_line_id=line["line_id"],
            accounting_period_id=line["accounting_period_id"],
            action="edit_amount",
            before=before,
            after={
                "edited": {"id": str(line["line_id"]), "debit": payload.new_debit, "credit": payload.new_credit},
                "counter": {"id": str(counter["id"]), "debit": payload.new_credit, "credit": payload.new_debit},
            },
            reason=payload.reason,
            actor_email=_actor_email(_user),
        )
    return {"ok": True, "action": "edit_amount", "journal_batch_id": line["journal_batch_id"]}


# --------------------------------------------------------------------------
# Correcting entry (locked OR open) — full reversal + re-entry
# --------------------------------------------------------------------------


@router.post("/correct")
def correct_entry(
    payload: CorrectRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Correct a batch by posting a FULL REVERSAL of the original plus a
    corrected RE-ENTRY into the current open period. Never touches the
    original (closed) batch. Both new batches carry correction_of_batch_id.

    `action` describes how the re-entry differs from the original:
      reclassify  -> the target line's account_code changes (amounts intact)
      edit_amount -> the target line's amount changes (2-line batches only;
                     the counter line mirrors)
    """
    if payload.action not in ("reclassify", "edit_amount"):
        raise HTTPException(400, "action must be 'reclassify' or 'edit_amount'")
    enforce_entity_code(_user, payload.entity_code)

    with db_session() as session:
        entity = _resolve_entity(session, payload.entity_code)
        eid = entity["id"]
        orig = _load_batch(session, eid, payload.journal_batch_id)
        orig_lines = _batch_lines(session, payload.journal_batch_id)
        if not orig_lines:
            raise HTTPException(400, "Original batch has no lines to correct.")

        # Guard: don't stack a second correction on the same original.
        dup = session.execute(
            text(
                """
                SELECT 1 FROM journal_batches
                 WHERE correction_of_batch_id = :orig
                   AND source_module = :src
                   AND status <> 'voided'
                 LIMIT 1
                """
            ),
            {"orig": payload.journal_batch_id, "src": _CORRECTION_SOURCE},
        ).first()
        if dup:
            raise HTTPException(
                409,
                "This batch already has a correction (reversal + re-entry). "
                "Void those before correcting again.",
            )

        # Build the re-entry lines from the original + the requested change.
        target_id = str(payload.journal_line_id)
        if target_id not in {str(l["id"]) for l in orig_lines}:
            raise HTTPException(400, "journal_line_id is not part of that batch.")

        reentry_lines: list[dict[str, Any]] = []
        if payload.action == "reclassify":
            if not payload.to_account_code:
                raise HTTPException(400, "to_account_code is required for reclassify.")
            _assert_account_exists(session, eid, payload.to_account_code)
            for l in orig_lines:
                code = payload.to_account_code if str(l["id"]) == target_id else l["account_code"]
                reentry_lines.append({
                    "account_code": code,
                    "debit": Decimal(str(l["debit_amount"] or 0)),
                    "credit": Decimal(str(l["credit_amount"] or 0)),
                    "memo": l["memo"],
                    "source_json": l["source_json"],
                })
        else:  # edit_amount
            if payload.new_debit is None or payload.new_credit is None:
                raise HTTPException(400, "new_debit and new_credit are required for edit_amount.")
            if payload.new_debit > 0 and payload.new_credit > 0:
                raise HTTPException(400, "A line is one-sided: provide a debit OR a credit, not both.")
            if len(orig_lines) != 2:
                raise HTTPException(
                    409,
                    f"edit_amount corrections support only 2-line entries; this batch "
                    f"has {len(orig_lines)} lines.",
                )
            nd = Decimal(str(payload.new_debit))
            nc = Decimal(str(payload.new_credit))
            for l in orig_lines:
                if str(l["id"]) == target_id:
                    reentry_lines.append({"account_code": l["account_code"], "debit": nd, "credit": nc, "memo": l["memo"], "source_json": l["source_json"]})
                else:
                    reentry_lines.append({"account_code": l["account_code"], "debit": nc, "credit": nd, "memo": l["memo"], "source_json": l["source_json"]})

        current = _resolve_current_open_period(session, eid)
        actor = _actor_email(_user)
        short = str(payload.journal_batch_id)[:8]

        # --- Reversal batch: full mirror of the original (swap dr/cr) ---
        rev_total_d = sum((Decimal(str(l["credit_amount"] or 0)) for l in orig_lines), Decimal("0"))
        rev_total_c = sum((Decimal(str(l["debit_amount"] or 0)) for l in orig_lines), Decimal("0"))
        rev_batch_id = _insert_correction_batch(
            session, entity_id=eid, period_id=current["id"],
            label=f"Reversal of {orig['batch_label']} [{short}]",
            total_debits=rev_total_d, total_credits=rev_total_c,
            correction_of=payload.journal_batch_id,
            summary={"kind": "reversal", "original_batch_id": str(payload.journal_batch_id), "reason": payload.reason},
        )
        for i, l in enumerate(orig_lines, start=1):
            _insert_correction_line(
                session, rev_batch_id, i, l["account_code"],
                Decimal(str(l["credit_amount"] or 0)), Decimal(str(l["debit_amount"] or 0)),
                f"Reversal: {l['memo']}" if l["memo"] else "Reversal", l["source_json"],
            )
        _assert_balanced(session, rev_batch_id)

        # --- Re-entry batch: corrected version ---
        re_total_d = sum((l["debit"] for l in reentry_lines), Decimal("0"))
        re_total_c = sum((l["credit"] for l in reentry_lines), Decimal("0"))
        re_batch_id = _insert_correction_batch(
            session, entity_id=eid, period_id=current["id"],
            label=f"Correction of {orig['batch_label']} [{short}]",
            total_debits=re_total_d, total_credits=re_total_c,
            correction_of=payload.journal_batch_id,
            summary={"kind": "re_entry", "original_batch_id": str(payload.journal_batch_id),
                     "action": payload.action, "reason": payload.reason},
        )
        for i, l in enumerate(reentry_lines, start=1):
            _insert_correction_line(
                session, re_batch_id, i, l["account_code"], l["debit"], l["credit"],
                l["memo"], l["source_json"],
            )
        _assert_balanced(session, re_batch_id)

        # --- Audit + workflow events for both new batches ---
        for bid, kind in ((rev_batch_id, "reversal"), (re_batch_id, "re_entry")):
            _log_change(
                session,
                entity_id=eid,
                journal_batch_id=bid,
                journal_line_id=None,
                accounting_period_id=current["id"],
                action="correcting_entry",
                before={"original_batch_id": str(payload.journal_batch_id), "kind": kind},
                after={"action": payload.action},
                reason=payload.reason,
                actor_email=actor,
            )
            _log_workflow_created(session, batch_id=bid, entity_id=eid, period_id=current["id"],
                                  label=orig["batch_label"], actor=actor, reason=payload.reason,
                                  original_batch_id=str(payload.journal_batch_id))

    return {
        "ok": True,
        "action": "correcting_entry",
        "original_batch_id": str(payload.journal_batch_id),
        "reversal_batch_id": str(rev_batch_id),
        "reentry_batch_id": str(re_batch_id),
        "posted_into_period": current["period_label"],
    }


def _insert_correction_batch(
    session, *, entity_id, period_id, label, total_debits, total_credits, correction_of, summary,
):
    row = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status, total_debits, total_credits,
                correction_of_batch_id, summary_json
            ) VALUES (
                :eid, :pid, :src, :label,
                'posted', 'posted', :td, :tc,
                :corr, CAST(:summary AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "eid": entity_id, "pid": period_id, "src": _CORRECTION_SOURCE, "label": label,
            "td": total_debits, "tc": total_credits, "corr": correction_of,
            "summary": json.dumps(summary),
        },
    ).mappings().first()
    return row["id"]


def _insert_correction_line(session, batch_id, line_number, account_code, debit, credit, memo, source_json):
    session.execute(
        text(
            """
            INSERT INTO journal_lines (
                journal_batch_id, line_number, account_code,
                debit_amount, credit_amount, memo, source_json
            ) VALUES (
                :bid, :n, :code, :d, :c, :memo, CAST(:sj AS jsonb)
            )
            """
        ),
        {
            "bid": batch_id, "n": line_number, "code": account_code,
            "d": debit, "c": credit, "memo": memo,
            "sj": json.dumps(source_json or {}),
        },
    )


def _log_workflow_created(session, *, batch_id, entity_id, period_id, label, actor, reason, original_batch_id):
    session.execute(
        text(
            """
            INSERT INTO journal_batch_workflow_events (
                journal_batch_id, entity_id, accounting_period_id, source_module,
                batch_label, action, from_workflow_status, to_workflow_status,
                actor_email, note, payload_json
            ) VALUES (
                :bid, :eid, :pid, :src,
                :label, 'created', NULL, 'posted',
                :actor, :note, CAST(:payload AS jsonb)
            )
            """
        ),
        {
            "bid": batch_id, "eid": entity_id, "pid": period_id, "src": _CORRECTION_SOURCE,
            "label": label, "actor": actor, "note": reason,
            "payload": json.dumps({"correction_of_batch_id": original_batch_id}),
        },
    )


# --------------------------------------------------------------------------
# Add note (annotation only — no financial mutation)
# --------------------------------------------------------------------------


@router.post("/note")
def add_note(
    payload: NoteRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Attach a note to a batch/line in the audit trail. Touches no journal
    data, so it is allowed regardless of period lock."""
    enforce_entity_code(_user, payload.entity_code)
    with db_session() as session:
        entity = _resolve_entity(session, payload.entity_code)
        eid = entity["id"]
        batch = _load_batch(session, eid, payload.journal_batch_id)
        if payload.journal_line_id:
            # Confirm the line belongs to this batch/entity.
            _load_line(session, eid, payload.journal_line_id)
        _log_change(
            session,
            entity_id=eid,
            journal_batch_id=payload.journal_batch_id,
            journal_line_id=payload.journal_line_id,
            accounting_period_id=batch["accounting_period_id"],
            action="add_note",
            before={},
            after={"note": payload.note},
            reason=payload.note,
            actor_email=_actor_email(_user),
        )
    return {"ok": True, "action": "add_note", "journal_batch_id": payload.journal_batch_id}
