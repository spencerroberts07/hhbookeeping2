from datetime import date as DateType, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..schemas import DashboardResponse
from ..services import get_entity_by_code
from ..services_auth import require_role
from ..services_period_close import LOCKED_STATUSES

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/quickbooks-status", response_model=DashboardResponse)
def quickbooks_status(entity_code: str = Query(default="1877-8")) -> DashboardResponse:
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(status_code=404, detail=f"Unknown entity code: {entity_code}")
            conn = session.execute(
                text(
                    """
                    SELECT realm_id, connected_at
                    FROM quickbooks_connections
                    WHERE entity_id = :entity_id AND is_active = TRUE
                    ORDER BY connected_at DESC
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"]},
            ).mappings().first()
            acct_count = session.execute(
                text("SELECT COUNT(*) AS c FROM accounts WHERE entity_id = :entity_id"),
                {"entity_id": entity["id"]},
            ).mappings().first()["c"]
            txn_count = session.execute(
                text("SELECT COUNT(*) AS c FROM quickbooks_transactions WHERE entity_id = :entity_id"),
                {"entity_id": entity["id"]},
            ).mappings().first()["c"]
            realm = conn["realm_id"] if conn else None
            last = conn["connected_at"] if conn else None
            return DashboardResponse(
                entity_code=entity_code,
                has_quickbooks_connection=bool(conn),
                company_realm_id=realm,
                imported_accounts=acct_count,
                imported_transactions=txn_count,
                last_sync_at=last,
                # Frontend-compatible aliases — same data, different keys.
                is_connected=bool(conn),
                realm_id=realm,
                company_name=entity.get("entity_name"),
                last_synced_at=last,
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# --------------------------------------------------------------------------
# Gross margin (current period)
#
# Computed from journal_lines on the canonical 4xxx (revenue) and 5xxx
# (COGS) prefixes for the current period. When no period is found or
# either side is zero, returns 0% — frontend shows that as a flat
# sparkline rather than blowing up.
# --------------------------------------------------------------------------


@router.get("/gross-margin")
def gross_margin(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")

        # Resolve current period via the same tiered logic the dashboard
        # uses for everything else.
        period = _current_period_for_entity(session, str(entity["id"]))
        if not period:
            return {
                "entity_code": entity_code,
                "period_end": None,
                "sales": 0,
                "cogs": 0,
                "margin_pct": 0,
            }

        totals = session.execute(
            text(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN LEFT(jl.account_code, 1) = '4'
                                    THEN jl.credit_amount - jl.debit_amount
                                    ELSE 0 END), 0) AS sales,
                  COALESCE(SUM(CASE WHEN LEFT(jl.account_code, 1) = '5'
                                    THEN jl.debit_amount - jl.credit_amount
                                    ELSE 0 END), 0) AS cogs
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                 WHERE jb.entity_id = :eid
                   AND jb.accounting_period_id = :pid
                   AND jb.status <> 'voided'
                """
            ),
            {"eid": entity["id"], "pid": period["id"]},
        ).mappings().first()
        sales = Decimal(str(totals["sales"] or 0))
        cogs = Decimal(str(totals["cogs"] or 0))
        margin_pct = float((sales - cogs) / sales * 100) if sales else 0.0
        return {
            "entity_code": entity_code,
            "period_end": period["period_end"].isoformat(),
            "period_label": period["period_label"],
            "sales": float(sales),
            "cogs": float(cogs),
            "margin_pct": round(margin_pct, 1),
        }


# --------------------------------------------------------------------------
# Real alerts — period-late, missing HH AP statement, draft journals.
# Replaces the static "Missing HH AP statement" / "Month-end reminder"
# placeholders that used to sit in alerts-feed.tsx.
# --------------------------------------------------------------------------


@router.get("/alerts")
def dashboard_alerts(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")

        alerts: list[dict[str, Any]] = []
        today = datetime.utcnow().date()

        period = _current_period_for_entity(session, str(entity["id"]))
        if period:
            period_end: DateType = period["period_end"]
            days_since = (today - period_end).days
            # Period is "late" once we're past month-end + a 10-day grace.
            # Suppress the alert if the period is already in a locked
            # status (closed_locked or mid-approval approved_to_close) —
            # those mean the user is already on it or finished.
            if days_since > 10 and period["status"] not in LOCKED_STATUSES:
                alerts.append({
                    "type": "period_late",
                    "severity": "warning",
                    "label": f"{period['period_label']} is overdue",
                    "detail": f"{days_since} days past month-end — close it from /month-end",
                    "href": "/month-end",
                })

            # Draft journal_batches blocking close.
            draft = session.execute(
                text(
                    """
                    SELECT COUNT(*) AS c FROM journal_batches
                     WHERE entity_id = :eid
                       AND accounting_period_id = :pid
                       AND status IN ('draft', 'draft_unbalanced', 'draft_exception')
                    """
                ),
                {"eid": entity["id"], "pid": period["id"]},
            ).mappings().first()
            if int(draft["c"]) > 0:
                alerts.append({
                    "type": "draft_journals",
                    "severity": "info",
                    "label": f"{draft['c']} journal{'s' if draft['c'] != 1 else ''} not yet approved",
                    "detail": f"In {period['period_label']}",
                    "href": "/month-end",
                })

        # Missing HH AP statement for the current calendar month.
        month_start = DateType(today.year, today.month, 1)
        stmt = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM hh_ap_statements
                 WHERE entity_id = :eid
                   AND COALESCE(statement_month_end, statement_date) >= :ms
                """
            ),
            {"eid": entity["id"], "ms": month_start},
        ).mappings().first()
        if int(stmt["c"]) == 0:
            alerts.append({
                "type": "missing_hh_ap_statement",
                "severity": "info",
                "label": "No HH AP statement uploaded this month",
                "detail": "Upload your latest monthly statement to keep AP in sync",
                "href": "/ap",
            })

        # Unmatched invoices (read from the existing endpoint's query).
        unmatched = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM invoice_documents
                 WHERE entity_code = :ec AND status = 'unmatched'
                """
            ),
            {"ec": entity_code},
        ).mappings().first()
        if int(unmatched["c"]) > 0:
            alerts.append({
                "type": "unmatched_invoices",
                "severity": "warning",
                "label": f"{unmatched['c']} invoice{'s' if unmatched['c'] != 1 else ''} unmatched",
                "detail": "Review before period close",
                "href": "/ap/unmatched",
            })

        # Unclassified bank transactions (review queue).
        unclassified = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM bank_transactions
                 WHERE entity_id = :eid
                   AND review_status IN ('new', 'needs_review')
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()
        if int(unclassified["c"]) > 0:
            alerts.append({
                "type": "unclassified_transactions",
                "severity": "info",
                "label": f"{unclassified['c']} bank transaction{'s' if unclassified['c'] != 1 else ''} unclassified",
                "detail": "Run the classifier or review individually",
                "href": "/bank",
            })

        return {
            "entity_code": entity_code,
            "alerts": alerts,
            "count": len(alerts),
        }


def _current_period_for_entity(session, entity_id: str) -> dict[str, Any] | None:
    """Tiered current-period lookup — matches routes/period_close.py:
    approved-to-post → any non-voided → no-batch past → most recent
    closed. Returns the full row (incl. id + period_end) so dashboard
    cards can scope by period.
    """
    sql_approved = text(
        """
        SELECT ap.id, ap.period_end, ap.period_label, ap.status
          FROM accounting_periods ap
         WHERE ap.entity_id = :eid
           AND ap.period_end <= CURRENT_DATE
           AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           AND EXISTS (
               SELECT 1 FROM journal_batches jb
                WHERE jb.accounting_period_id = ap.id
                  AND jb.status = 'approved_to_post'
           )
         ORDER BY ap.period_end DESC
         LIMIT 1
        """
    )
    row = session.execute(sql_approved, {"eid": entity_id}).mappings().first()
    if row:
        return dict(row)
    sql_with_batches = text(
        """
        SELECT ap.id, ap.period_end, ap.period_label, ap.status
          FROM accounting_periods ap
         WHERE ap.entity_id = :eid
           AND ap.period_end <= CURRENT_DATE
           AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           AND EXISTS (
               SELECT 1 FROM journal_batches jb
                WHERE jb.accounting_period_id = ap.id
                  AND jb.status <> 'voided'
           )
         ORDER BY ap.period_end DESC
         LIMIT 1
        """
    )
    row = session.execute(sql_with_batches, {"eid": entity_id}).mappings().first()
    if row:
        return dict(row)
    sql_no_batches = text(
        """
        SELECT id, period_end, period_label, status
          FROM accounting_periods
         WHERE entity_id = :eid
           AND period_end <= CURRENT_DATE
           AND status NOT IN ('closed_locked', 'approved_to_close')
         ORDER BY period_end DESC LIMIT 1
        """
    )
    row = session.execute(sql_no_batches, {"eid": entity_id}).mappings().first()
    if row:
        return dict(row)
    sql_closed = text(
        """
        SELECT id, period_end, period_label, status
          FROM accounting_periods
         WHERE entity_id = :eid
           AND period_end <= CURRENT_DATE
         ORDER BY period_end DESC LIMIT 1
        """
    )
    row = session.execute(sql_closed, {"eid": entity_id}).mappings().first()
    return dict(row) if row else None
