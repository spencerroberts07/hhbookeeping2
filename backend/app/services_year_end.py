"""
Year-end soft-close lifecycle (Phase 5A). Fiscal year = Oct 1 -> Sep 30.

When the September (fiscal year-end) period closes normally, its year_end_status
becomes 'draft'. The accountant moves it to 'in_review' to post adjusting
journal entries (source_module='year_end_adjustment') into that September period,
then 'final_locked' to block ALL further JEs including adjustments.

Retained earnings is NEVER closed out with a JE (D5-3): _account_sums computes RE
dynamically, and the cutover model already handles it. This module only manages
the soft-close flag + the adjusting-entry write path; it posts no closing entries.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

FY_END_MONTH = 9
FY_END_DAY = 30
YE_DRAFT = "draft"
YE_IN_REVIEW = "in_review"
YE_FINAL_LOCKED = "final_locked"
_VALID_YE = {YE_DRAFT, YE_IN_REVIEW, YE_FINAL_LOCKED}
_ADJ_SOURCE = "year_end_adjustment"
_FORBIDDEN_ACCOUNTS = {"3900"}  # never post corrections to Opening Balance Equity


class YearEndError(Exception):
    pass


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


def fy_of(d: date) -> int:
    """Fiscal year a date falls in. Oct-Dec -> next calendar year; else same."""
    return d.year + 1 if d.month >= (FY_END_MONTH + 1) else d.year


def is_fiscal_year_end(period_end: date) -> bool:
    return period_end.month == FY_END_MONTH and period_end.day == FY_END_DAY


def _entity_id(session, entity_code: str) -> str:
    eid = session.execute(
        text("SELECT id FROM entities WHERE entity_code=:ec"), {"ec": entity_code}
    ).scalar()
    if not eid:
        raise YearEndError(f"entity {entity_code} not found")
    return str(eid)


def _september_period(session, entity_id: str, fy: int) -> dict[str, Any] | None:
    ye_date = date(fy, FY_END_MONTH, FY_END_DAY)
    row = session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, status, year_end_status
              FROM accounting_periods
             WHERE entity_id=:e AND period_end=:pe
            """
        ),
        {"e": entity_id, "pe": ye_date},
    ).mappings().first()
    return dict(row) if row else None


def trigger_year_end(session, *, entity_code: str, period_end: str) -> dict[str, Any]:
    """Called when a period closes. If it's the Sep year-end period, stamp
    year_end_status='draft' (only when currently NULL). No-op otherwise."""
    pe = date.fromisoformat(period_end)
    if not is_fiscal_year_end(pe):
        return {"triggered": False, "reason": "not a fiscal year-end period"}
    entity_id = _entity_id(session, entity_code)
    updated = session.execute(
        text(
            """
            UPDATE accounting_periods
               SET year_end_status = :ye
             WHERE entity_id=:e AND period_end=:pe AND year_end_status IS NULL
         RETURNING id
            """
        ),
        {"ye": YE_DRAFT, "e": entity_id, "pe": pe},
    ).first()
    return {"triggered": bool(updated), "fy": fy_of(pe), "year_end_status": YE_DRAFT}


def get_year_end_status(session, *, entity_code: str, fy: int) -> dict[str, Any]:
    entity_id = _entity_id(session, entity_code)
    sep = _september_period(session, entity_id, fy)
    fy_start = date(fy - 1, 10, 1)
    fy_end = date(fy, 9, 30)
    # all periods in the FY + how many are closed
    periods = session.execute(
        text(
            """
            SELECT period_end, status
              FROM accounting_periods
             WHERE entity_id=:e AND period_end BETWEEN :s AND :en
          ORDER BY period_end
            """
        ),
        {"e": entity_id, "s": fy_start, "en": fy_end},
    ).mappings().all()
    closed = [p for p in periods if (p["status"] or "") == "closed_locked"]
    adj_count = 0
    if sep:
        adj_count = session.execute(
            text(
                """SELECT COUNT(*) FROM journal_batches
                    WHERE entity_id=:e AND accounting_period_id=:pid AND source_module=:src
                      AND status NOT IN ('voided','rejected')"""
            ),
            {"e": entity_id, "pid": sep["id"], "src": _ADJ_SOURCE},
        ).scalar() or 0
    return {
        "entity_code": entity_code,
        "fy": fy,
        "fy_start": fy_start.isoformat(),
        "fy_end": fy_end.isoformat(),
        "year_end_status": sep["year_end_status"] if sep else None,
        "september_period_closed": bool(sep and (sep["status"] or "") == "closed_locked"),
        "periods_total": len(periods),
        "periods_closed": len(closed),
        "all_periods_closed": len(periods) > 0 and len(closed) == len(periods),
        "adjusting_entry_count": int(adj_count),
    }


def set_year_end_status(session, *, entity_code: str, fy: int, new_status: str,
                        actor: str | None = None) -> dict[str, Any]:
    if new_status not in _VALID_YE:
        raise YearEndError(f"invalid year_end_status {new_status!r}")
    entity_id = _entity_id(session, entity_code)
    sep = _september_period(session, entity_id, fy)
    if not sep:
        raise YearEndError(f"no September period for FY{fy}")
    current = sep["year_end_status"]
    if current is None:
        raise YearEndError("year-end not triggered yet (close the September period first)")
    # allowed transitions
    allowed = {
        YE_DRAFT: {YE_IN_REVIEW},
        YE_IN_REVIEW: {YE_DRAFT, YE_FINAL_LOCKED},
        YE_FINAL_LOCKED: set(),
    }
    if new_status != current and new_status not in allowed.get(current, set()):
        raise YearEndError(f"cannot move year-end from {current} to {new_status}")
    session.execute(
        text("UPDATE accounting_periods SET year_end_status=:ye WHERE id=:pid"),
        {"ye": new_status, "pid": sep["id"]},
    )
    return {"entity_code": entity_code, "fy": fy, "year_end_status": new_status,
            "previous": current, "actor": actor}


def post_adjusting_je(
    session, *, entity_code: str, fy: int, lines: list[dict[str, Any]],
    label: str, actor: str | None = None,
) -> dict[str, Any]:
    """Post a balanced year_end_adjustment batch into the September period.
    Allowed ONLY when year_end_status='in_review' (final_locked blocks; draft must
    move to in_review first). Never posts to 3900 equity. Posts directly (status
    'posted') into the closed September period — the soft-close flag is the gate,
    not the normal period lock."""
    entity_id = _entity_id(session, entity_code)
    sep = _september_period(session, entity_id, fy)
    if not sep:
        raise YearEndError(f"no September period for FY{fy}")
    if sep["year_end_status"] != YE_IN_REVIEW:
        raise YearEndError(
            f"year-end is '{sep['year_end_status']}'; adjusting entries require 'in_review'"
        )
    if not lines:
        raise YearEndError("no journal lines provided")

    total_d = sum(_D(l.get("debit")) for l in lines)
    total_c = sum(_D(l.get("credit")) for l in lines)
    if abs(total_d - total_c) > Decimal("0.01"):
        raise YearEndError(f"unbalanced entry: debits {total_d} != credits {total_c}")
    for l in lines:
        if str(l.get("account_code")) in _FORBIDDEN_ACCOUNTS:
            raise YearEndError("refusing to post to account 3900 (Opening Balance Equity)")

    batch_id = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status, total_debits, total_credits, summary_json
            ) VALUES (
                :e, :pid, :src, :label, 'posted', 'posted', :td, :tc, CAST(:sj AS jsonb)
            ) RETURNING id
            """
        ),
        {"e": entity_id, "pid": sep["id"], "src": _ADJ_SOURCE, "label": label,
         "td": total_d, "tc": total_c,
         "sj": json.dumps({"year_end_adjustment": True, "fy": fy, "actor": actor})},
    ).scalar()

    for i, l in enumerate(lines, start=1):
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code, debit_amount, credit_amount, memo, source_json
                ) VALUES (:bid, :n, :code, :d, :c, :memo, CAST(:sj AS jsonb))
                """
            ),
            {"bid": batch_id, "n": i, "code": str(l.get("account_code")),
             "d": _D(l.get("debit")), "c": _D(l.get("credit")), "memo": l.get("memo"),
             "sj": json.dumps({"year_end_adjustment": True})},
        )

    return {"batch_id": str(batch_id), "fy": fy, "total_debits": float(total_d),
            "total_credits": float(total_c), "line_count": len(lines)}
