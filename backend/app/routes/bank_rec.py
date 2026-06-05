"""
Bank reconciliation endpoints (Phase 3C/3D). 3A-3C are read/link only; 3D
(journal candidates for unbooked items) is ask-before-write and never auto-posts.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from ..db import db_session
from ..services_auth import require_role
from ..services_bank_rec import compute_reconciliation
from ..services_bank_rec_match import run_match

router = APIRouter(prefix="/api/bank-rec", tags=["bank-rec"])


class ComputeRequest(BaseModel):
    entity_code: str
    source_account_code: str = "1020"
    period_end: str                       # YYYY-MM-DD
    statement_date: str
    statement_closing_balance: float
    statement_opening_balance: float | None = None
    confirmed_deposits_in_transit: float | None = None


def _period_start(session, entity_id, period_end) -> date:
    row = session.execute(
        text("SELECT period_start FROM accounting_periods WHERE entity_id=:e AND period_end=:pe"),
        {"e": entity_id, "pe": period_end},
    ).scalar()
    if not row:
        raise HTTPException(404, f"No accounting period ending {period_end}")
    return row


@router.post("/compute")
def compute(payload: ComputeRequest, _user: Any = Depends(require_role("bookkeeper"))) -> dict[str, Any]:
    pe = date.fromisoformat(payload.period_end)
    with db_session() as session:
        eid = session.execute(text("SELECT id FROM entities WHERE entity_code=:ec"),
                              {"ec": payload.entity_code}).scalar()
        if not eid:
            raise HTTPException(404, "entity not found")
        ps = _period_start(session, eid, pe)
        rec = compute_reconciliation(
            session, entity_code=payload.entity_code, source_account_code=payload.source_account_code,
            period_start=ps, period_end=pe, statement_date=date.fromisoformat(payload.statement_date),
            statement_closing_balance=Decimal(str(payload.statement_closing_balance)),
            statement_opening_balance=(Decimal(str(payload.statement_opening_balance))
                                       if payload.statement_opening_balance is not None else None),
            confirmed_deposits_in_transit=(Decimal(str(payload.confirmed_deposits_in_transit))
                                           if payload.confirmed_deposits_in_transit is not None else None),
        )
    return rec


@router.get("")
def get_rec(entity_code: str = Query(...), source_account_code: str = Query("1020"),
            period_end: str = Query(...), _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    with db_session() as session:
        eid = session.execute(text("SELECT id FROM entities WHERE entity_code=:ec"),
                              {"ec": entity_code}).scalar()
        row = session.execute(
            text(
                """
                SELECT r.*, ap.period_label
                  FROM bank_reconciliations r
                  JOIN accounting_periods ap ON ap.id = r.accounting_period_id
                 WHERE r.entity_id=:e AND r.source_account_code=:ac AND ap.period_end=:pe
                """
            ),
            {"e": eid, "ac": source_account_code, "pe": period_end},
        ).mappings().first()
        if not row:
            raise HTTPException(404, "No reconciliation for that account/period")
        return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in dict(row).items()
                if k not in ("id", "entity_id", "accounting_period_id")} | {"id": str(row["id"])}


@router.post("/{rec_id}/lock")
def lock(rec_id: str, override_note: str | None = None,
         _user: Any = Depends(require_role("approver"))) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text("SELECT ties, status FROM bank_reconciliations WHERE id=:id"), {"id": rec_id}
        ).mappings().first()
        if not row:
            raise HTTPException(404, "reconciliation not found")
        if not row["ties"] and not override_note:
            raise HTTPException(409, "Reconciliation does not tie (variance > $0.01). Provide an override_note to lock anyway.")
        actor = _actor(_user)
        session.execute(
            text("UPDATE bank_reconciliations SET status='locked', locked_by=:a, locked_at=NOW(), updated_at=NOW() WHERE id=:id"),
            {"a": actor, "id": rec_id},
        )
    return {"ok": True, "status": "locked"}


@router.get("/{rec_id}/journal-candidates")
def journal_candidates(rec_id: str, _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    """3D: unbooked statement items that would need a journal entry. PENDING —
    never auto-posted; the user confirms each before booking into the open period."""
    with db_session() as session:
        row = session.execute(
            text("SELECT summary_json, source_account_code FROM bank_reconciliations WHERE id=:id"),
            {"id": rec_id},
        ).mappings().first()
        if not row:
            raise HTTPException(404, "reconciliation not found")
        summ = row["summary_json"] or {}
    candidates = []
    pd = float(summ.get("payroll_deduction_residual") or 0)
    if abs(pd) > 0.01:
        candidates.append({
            "kind": "payroll_cra_remittance",
            "description": "Payroll source deductions drawn from the bank (gross eNet draw) "
                           "but not credited to 1020 — book the CRA remittance.",
            "lines": [
                {"account_code": "2320", "debit": round(pd, 2), "credit": 0.0,
                 "account_name": "CRA Payroll Remittances Payable"},
                {"account_code": row["source_account_code"], "debit": 0.0, "credit": round(pd, 2),
                 "account_name": "TD Canada Trust"},
            ],
            "post_to": "current_open_period",
            "status": "pending",  # ask-before-write; never auto-posts
        })
    return {"rec_id": rec_id, "candidates": candidates, "note": "PENDING — confirm before booking; never auto-posted."}


def _actor(user: Any) -> str | None:
    try:
        return user.get("email")
    except AttributeError:
        return getattr(user, "email", None)
