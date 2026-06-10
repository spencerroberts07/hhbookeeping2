from datetime import date as DateType, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import bindparam, text

from ..db import db_session
from ..schemas import DashboardResponse
from ..services import get_entity_by_code
from ..services_auth import require_role
from ..services_period_close import LOCKED_STATUSES
from .reports import _account_type, _find_opening_balance_cutover

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/quickbooks-status", response_model=DashboardResponse)
def quickbooks_status(entity_code: str | None = Query(default=None)) -> DashboardResponse:
    if not entity_code:
        raise HTTPException(status_code=400, detail="entity_code query parameter is required")
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

        # Filter to posted/approved batches only — draft batches (e.g.
        # in-flight cash_balancing rows for the current month) should
        # not feed the headline revenue or margin numbers.
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
                   AND jb.status IN :statuses
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {
                "eid": entity["id"],
                "pid": period["id"],
                "statuses": list(POSTED_BATCH_STATUSES),
            },
        ).mappings().first()
        sales = Decimal(str(totals["sales"] or 0))
        cogs = Decimal(str(totals["cogs"] or 0))
        margin_pct = float((sales - cogs) / sales * 100) if sales else 0.0

        # 12-month rolling totals — sum across the last 12 monthly
        # periods up to and including the chosen period_end. Used by
        # the "Gross margin (12 mo)" card.
        ttm_row = session.execute(
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
                  JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                 WHERE jb.entity_id = :eid
                   AND jb.status IN :statuses
                   AND ap.period_end <= :pend
                   AND ap.period_end > (CAST(:pend AS date) - INTERVAL '12 months')
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {
                "eid": entity["id"],
                "pend": period["period_end"],
                "statuses": list(POSTED_BATCH_STATUSES),
            },
        ).mappings().first()
        ttm_sales = Decimal(str((ttm_row or {}).get("sales") or 0))
        ttm_cogs = Decimal(str((ttm_row or {}).get("cogs") or 0))
        ttm_margin_pct = (
            float((ttm_sales - ttm_cogs) / ttm_sales * 100) if ttm_sales else 0.0
        )

        return {
            "entity_code": entity_code,
            "period_end": period["period_end"].isoformat(),
            "period_label": period["period_label"],
            "sales": float(sales),
            "cogs": float(cogs),
            "margin_pct": round(margin_pct, 1),
            "ttm_sales": float(ttm_sales),
            "ttm_cogs": float(ttm_cogs),
            "ttm_margin_pct": round(ttm_margin_pct, 1),
        }


# --------------------------------------------------------------------------
# Sales history — monthly sales + COGS + margin for the last N months.
#
# Powers the "Sales — this month vs last year" bar chart and the gross
# margin sparkline. Replaces the hardcoded mock arrays that used to
# live in dashboard/_components/sales-chart.tsx and
# dashboard/_components/gross-margin-sparkline.tsx.
# --------------------------------------------------------------------------


@router.get("/sales-history")
def sales_history(
    entity_code: str = Query(...),
    months: int = Query(default=24, ge=1, le=60),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")

        rows = session.execute(
            text(
                """
                SELECT
                  ap.id              AS period_id,
                  ap.period_end      AS period_end,
                  ap.period_label    AS period_label,
                  COALESCE(SUM(CASE WHEN LEFT(jl.account_code, 1) = '4'
                                    THEN jl.credit_amount - jl.debit_amount
                                    ELSE 0 END), 0) AS sales,
                  COALESCE(SUM(CASE WHEN LEFT(jl.account_code, 1) = '5'
                                    THEN jl.debit_amount - jl.credit_amount
                                    ELSE 0 END), 0) AS cogs
                  FROM accounting_periods ap
                  LEFT JOIN journal_batches jb
                    ON jb.accounting_period_id = ap.id
                   AND jb.status IN :statuses
                  LEFT JOIN journal_lines jl
                    ON jl.journal_batch_id = jb.id
                 WHERE ap.entity_id = :eid
                   AND ap.period_end <= CURRENT_DATE
                   AND ap.period_end > (CURRENT_DATE - make_interval(months => :months))
                 GROUP BY ap.id, ap.period_end, ap.period_label
                 ORDER BY ap.period_end ASC
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {
                "eid": entity["id"],
                "months": months,
                "statuses": list(POSTED_BATCH_STATUSES),
            },
        ).mappings().all()

        series = []
        for r in rows:
            sales = float(r["sales"] or 0)
            cogs = float(r["cogs"] or 0)
            margin_pct = round((sales - cogs) / sales * 100, 1) if sales else 0.0
            series.append({
                "period_end": r["period_end"].isoformat(),
                "period_label": r["period_label"],
                "sales": sales,
                "cogs": cogs,
                "margin_pct": margin_pct,
            })

        return {
            "entity_code": entity_code,
            "months": months,
            "series": series,
        }


# --------------------------------------------------------------------------
# GL cash balance — fallback for the cash card when QBO is disconnected.
# Reads account 1020 from journal_lines (posted/approved batches only).
# --------------------------------------------------------------------------


@router.get("/gl-cash-balance")
def gl_cash_balance(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")

        row = session.execute(
            text(
                """
                SELECT COALESCE(SUM(jl.debit_amount - jl.credit_amount), 0)
                       AS balance
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                 WHERE jb.entity_id = :eid
                   AND jl.account_code = '1020'
                   AND jb.status IN :statuses
                """
            ).bindparams(bindparam("statuses", expanding=True)),
            {
                "eid": entity["id"],
                "statuses": list(POSTED_BATCH_STATUSES),
            },
        ).mappings().first()

        return {
            "entity_code": entity_code,
            "account_code": "1020",
            "balance": float((row or {}).get("balance") or 0),
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

        # AP due-date alerts (outside-vendor only — HH AP excluded at query).
        # Shows the most-urgent unfired / recently-fired alert group.
        # Overdue = threshold_days=0, 3-day = threshold_days=3, 7-day = 7.
        ap_due_rows = session.execute(
            text(
                """
                SELECT COUNT(*) AS c,
                       MIN(al.due_date) AS earliest_due,
                       MAX(CASE WHEN al.threshold_days = 0 THEN 1 ELSE 0 END) AS has_overdue,
                       MAX(CASE WHEN al.threshold_days <= 3 THEN 1 ELSE 0 END) AS has_urgent
                FROM ap_alert_log al
                WHERE al.entity_id = :eid
                  AND al.due_date >= :today - INTERVAL '30 days'
                """
            ),
            {"eid": str(entity["id"]), "today": today},
        ).mappings().first()
        if ap_due_rows and int(ap_due_rows["c"] or 0) > 0:
            has_overdue = bool(ap_due_rows["has_overdue"])
            has_urgent = bool(ap_due_rows["has_urgent"])
            count = int(ap_due_rows["c"])
            alerts.append({
                "type": "ap_due",
                "severity": "error" if has_overdue else ("warning" if has_urgent else "info"),
                "label": (
                    f"{count} vendor invoice{'s' if count != 1 else ''} overdue or due soon"
                ),
                "detail": (
                    "Overdue invoices require payment" if has_overdue
                    else "Invoices due within 7 days"
                ),
                "href": "/ap/payments",
            })

        return {
            "entity_code": entity_code,
            "alerts": alerts,
            "count": len(alerts),
        }


# Journal-batch statuses that count as "posted" / live for dashboard
# aggregates. Excludes 'draft', 'draft_unbalanced', 'draft_exception',
# 'voided', 'rejected'. 'approved_to_post' is the locked-and-published
# state used today; 'posted'/'approved'/'closed_locked' are forward-
# compatible names other modules write.
POSTED_BATCH_STATUSES = (
    "posted",
    "approved_to_post",
    "approved",
    "closed_locked",
)


def _current_period_for_entity(session, entity_id: str) -> dict[str, Any] | None:
    """Tiered current-period lookup for dashboard scoping.

    Priority:
      1. Active close in progress: status IN (submitted_for_close,
         approved_to_close)
      2. Most recent closed_locked period — the last finished month
      3. Draft with at least one posted/approved batch (real work in
         flight)
      4. Most recent draft as last-resort fallback

    The previous order preferred any non-voided draft over closed
    periods, which made the dashboard jump to a half-empty month as
    soon as someone created a cash_balancing draft for it. Closed
    periods are now preferred so the headline numbers reflect a
    finalized month.
    """
    sql_active_close = text(
        """
        SELECT id, period_end, period_label, status
          FROM accounting_periods
         WHERE entity_id = :eid
           AND period_end <= CURRENT_DATE
           AND status IN ('submitted_for_close', 'approved_to_close')
         ORDER BY period_end DESC
         LIMIT 1
        """
    )
    row = session.execute(sql_active_close, {"eid": entity_id}).mappings().first()
    if row:
        return dict(row)

    sql_closed = text(
        """
        SELECT id, period_end, period_label, status
          FROM accounting_periods
         WHERE entity_id = :eid
           AND period_end <= CURRENT_DATE
           AND status = 'closed_locked'
         ORDER BY period_end DESC
         LIMIT 1
        """
    )
    row = session.execute(sql_closed, {"eid": entity_id}).mappings().first()
    if row:
        return dict(row)

    sql_draft_with_posted = text(
        """
        SELECT ap.id, ap.period_end, ap.period_label, ap.status
          FROM accounting_periods ap
         WHERE ap.entity_id = :eid
           AND ap.period_end <= CURRENT_DATE
           AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           AND EXISTS (
               SELECT 1 FROM journal_batches jb
                WHERE jb.accounting_period_id = ap.id
                  AND jb.status IN :statuses
           )
         ORDER BY ap.period_end DESC
         LIMIT 1
        """
    ).bindparams(
        bindparam("statuses", expanding=True),
    )
    row = session.execute(
        sql_draft_with_posted,
        {"eid": entity_id, "statuses": list(POSTED_BATCH_STATUSES)},
    ).mappings().first()
    if row:
        return dict(row)

    sql_any_draft = text(
        """
        SELECT id, period_end, period_label, status
          FROM accounting_periods
         WHERE entity_id = :eid
           AND period_end <= CURRENT_DATE
         ORDER BY period_end DESC
         LIMIT 1
        """
    )
    row = session.execute(sql_any_draft, {"eid": entity_id}).mappings().first()
    return dict(row) if row else None


# --------------------------------------------------------------------------
# Sales drill-down (Phase 2A)
#
# Monthly trend / rolling-12 / growth come from the GL (4xxx revenue, 5xxx
# COGS) — "GL net", reconciles to the income statement. Daily and MTD come
# from cash_balancing_days — "POS gross", the SAME source the Sales MTD card
# reads, so the card and the drill agree. The two sources do not tie exactly
# (POS gross sales vs GL net revenue); each response carries a `source` label
# so the UI can mark them.
# --------------------------------------------------------------------------


def _prev_month(key: tuple[int, int]) -> tuple[int, int]:
    y, m = key
    return (y - 1, 12) if m == 1 else (y, m - 1)


def _same_day_prior_year(d: DateType) -> DateType:
    try:
        return d.replace(year=d.year - 1)
    except ValueError:  # Feb 29 -> Feb 28 prior year
        return d.replace(year=d.year - 1, day=28)


# Period statuses that mean the month's P&L is final (matches the income
# statement's recognized-closed set). Months not in this set are "not yet
# closed" — their GL totals are incomplete and must not be plotted as $0.
_CLOSED_PERIOD_STATUSES = ("closed_locked", "approved_to_close")


def _monthly_sales_map(session, entity_id: str, months_back: int) -> dict[tuple[int, int], dict]:
    """{(year, month): {period_end, period_label, status, closed, sales, cogs,
    opex, other_income}} from the GL."""
    rows = session.execute(
        text(
            """
            SELECT ap.period_end, ap.period_label, ap.status,
                   COALESCE(SUM(CASE WHEN LEFT(jl.account_code,1)='4'
                                     THEN jl.credit_amount - jl.debit_amount ELSE 0 END),0) AS sales,
                   COALESCE(SUM(CASE WHEN LEFT(jl.account_code,1)='5'
                                     THEN jl.debit_amount - jl.credit_amount ELSE 0 END),0) AS cogs,
                   COALESCE(SUM(CASE WHEN LEFT(jl.account_code,1)='6'
                                     THEN jl.debit_amount - jl.credit_amount ELSE 0 END),0) AS opex,
                   COALESCE(SUM(CASE WHEN LEFT(jl.account_code,1) IN ('7','8','9')
                                     THEN jl.credit_amount - jl.debit_amount ELSE 0 END),0) AS other_income
              FROM accounting_periods ap
              LEFT JOIN journal_batches jb
                ON jb.accounting_period_id = ap.id AND jb.status IN :statuses
              LEFT JOIN journal_lines jl ON jl.journal_batch_id = jb.id
             WHERE ap.entity_id = :eid
               AND ap.period_end <= CURRENT_DATE
               AND ap.period_end > (CURRENT_DATE - make_interval(months => :months))
             GROUP BY ap.period_end, ap.period_label, ap.status
             ORDER BY ap.period_end ASC
            """
        ).bindparams(bindparam("statuses", expanding=True)),
        {"eid": entity_id, "months": months_back, "statuses": list(POSTED_BATCH_STATUSES)},
    ).mappings().all()
    out: dict[tuple[int, int], dict] = {}
    for r in rows:
        pe = r["period_end"]
        out[(pe.year, pe.month)] = {
            "period_end": pe.isoformat(),
            "period_label": r["period_label"],
            "status": r["status"],
            "closed": r["status"] in _CLOSED_PERIOD_STATUSES,
            "sales": float(r["sales"] or 0),
            "cogs": float(r["cogs"] or 0),
            "opex": float(r["opex"] or 0),
            "other_income": float(r["other_income"] or 0),
        }
    return out


def _pct_growth(cur: float, base: float | None) -> float | None:
    if base is None or base == 0:
        return None
    return round((cur - base) / base * 100, 1)


@router.get("/sales/monthly")
def sales_monthly(
    entity_code: str = Query(...),
    months: int = Query(default=24, ge=1, le=60),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Monthly sales + COGS + margin, this year vs last, with YoY & MoM growth.
    GL-sourced (reconciles to the income statement)."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        m = _monthly_sales_map(session, entity["id"], months + 12)

    keys = sorted(m.keys())
    recent = keys[-months:]
    series = []
    for k in recent:
        cur = m[k]
        py = m.get((k[0] - 1, k[1]))
        prev = m.get(_prev_month(k))
        sales, cogs = cur["sales"], cur["cogs"]
        series.append({
            "period_end": cur["period_end"],
            "period_label": cur["period_label"],
            "closed": cur["closed"],
            "sales": sales,
            "cogs": cogs,
            "margin_pct": round((sales - cogs) / sales * 100, 1) if sales else 0.0,
            "py_sales": py["sales"] if py else None,
            "py_cogs": py["cogs"] if py else None,
            "py_margin_pct": (round((py["sales"] - py["cogs"]) / py["sales"] * 100, 1)
                              if py and py["sales"] else None),
            "yoy_growth_pct": _pct_growth(sales, py["sales"] if py else None),
            "mom_growth_pct": _pct_growth(sales, prev["sales"] if prev else None),
        })
    return {"entity_code": entity_code, "source": "gl_net", "months": months, "series": series}


@router.get("/sales/rolling12")
def sales_rolling12(
    entity_code: str = Query(...),
    months: int = Query(default=24, ge=1, le=48),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Trailing-12-month sales at each month-end, current vs prior-year rolling-12."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        m = _monthly_sales_map(session, entity["id"], months + 24)

    def trailing12(end_key: tuple[int, int]) -> float | None:
        total = 0.0
        seen = 0
        y, mo = end_key
        for _ in range(12):
            cell = m.get((y, mo))
            if cell:
                total += cell["sales"]
                seen += 1
            mo -= 1
            if mo == 0:
                y, mo = y - 1, 12
        return round(total, 2) if seen else None

    keys = sorted(m.keys())
    recent = keys[-months:]
    series = []
    for k in recent:
        cur = trailing12(k)
        py = trailing12((k[0] - 1, k[1]))
        series.append({
            "period_end": m[k]["period_end"],
            "period_label": m[k]["period_label"],
            "rolling12_sales": cur,
            "py_rolling12_sales": py,
            "yoy_growth_pct": _pct_growth(cur, py) if cur is not None else None,
        })
    return {"entity_code": entity_code, "source": "gl_net", "months": months, "series": series}


@router.get("/sales/daily")
def sales_daily(
    entity_code: str = Query(...),
    days: int = Query(default=90, ge=1, le=400),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Daily sales (POS gross) + the same calendar day prior year, from
    cash_balancing_days."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        rows = session.execute(
            text(
                """
                SELECT business_date, total_sales
                  FROM cash_balancing_days
                 WHERE entity_id = :e
                   AND business_date <= CURRENT_DATE
                   AND business_date > (CURRENT_DATE - make_interval(days => :days))
              ORDER BY business_date ASC
                """
            ),
            {"e": entity["id"], "days": days},
        ).mappings().all()
        py_rows = session.execute(
            text(
                """
                SELECT business_date, total_sales
                  FROM cash_balancing_days
                 WHERE entity_id = :e
                   AND business_date <= (CURRENT_DATE - make_interval(years => 1))
                   AND business_date > (CURRENT_DATE - make_interval(years => 1)
                                        - make_interval(days => :days))
                """
            ),
            {"e": entity["id"], "days": days},
        ).mappings().all()

    py_map = {r["business_date"]: float(r["total_sales"] or 0) for r in py_rows}
    series = []
    for r in rows:
        d = r["business_date"]
        py_d = _same_day_prior_year(d)
        series.append({
            "date": d.isoformat(),
            "sales": float(r["total_sales"] or 0),
            "py_date": py_d.isoformat(),
            "py_sales": py_map.get(py_d),
        })
    return {"entity_code": entity_code, "source": "pos_gross", "days": days, "series": series}


@router.get("/sales/mtd")
def sales_mtd(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Month-to-date sales (POS gross) vs the SAME month-to-date prior year
    (partial-period aware). This is the figure the dashboard Sales MTD card
    shows, so card and drill agree."""
    today = DateType.today()
    month_start = today.replace(day=1)
    py_today = _same_day_prior_year(today)
    py_month_start = _same_day_prior_year(month_start)
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")

        def mtd_sum(start: DateType, end: DateType) -> float:
            row = session.execute(
                text(
                    """
                    SELECT COALESCE(SUM(total_sales), 0) AS s
                      FROM cash_balancing_days
                     WHERE entity_id = :e AND business_date BETWEEN :a AND :b
                    """
                ),
                {"e": entity["id"], "a": start, "b": end},
            ).mappings().first()
            return float((row or {}).get("s") or 0)

        cur = mtd_sum(month_start, today)
        pyv = mtd_sum(py_month_start, py_today)

    return {
        "entity_code": entity_code,
        "source": "pos_gross",
        "as_of": today.isoformat(),
        "py_as_of": py_today.isoformat(),
        "month_label": today.strftime("%b %Y"),
        "days_elapsed": today.day,
        "mtd_sales": round(cur, 2),
        "py_mtd_sales": round(pyv, 2),
        "yoy_growth_pct": _pct_growth(cur, pyv),
    }


# --------------------------------------------------------------------------
# Metric drill-downs (Phase 2B) — cash, inventory, AR balance trends + margin
# trend. All GL-net and cutover-aware: the per-account balance series
# reproduces _account_sums' cumulative+cutover logic for a single account, so
# each point reconciles to the balance sheet as-of that month-end.
# --------------------------------------------------------------------------


def _account_trend_series(session, entity_id, account_code, month_ends):
    """Cutover-aware cumulative balance for one account at each month-end.
    Mirrors _account_sums: opening_balance batch + post-cutover activity once
    the as-of date reaches the cutover; legacy cumulative before it. Signed to
    the account's natural balance (assets/COGS/opex debit-positive)."""
    rows = session.execute(
        text(
            """
            SELECT ap.period_end, ap.period_start, jb.source_module,
                   SUM(jl.debit_amount - jl.credit_amount) AS net
              FROM journal_lines jl
              JOIN journal_batches jb ON jb.id = jl.journal_batch_id
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jb.entity_id = :e AND jl.account_code = :code
               AND jb.status NOT IN ('draft', 'voided', 'rejected')
             GROUP BY ap.period_end, ap.period_start, jb.source_module
            """
        ),
        {"e": entity_id, "code": account_code},
    ).mappings().all()
    movements = [dict(r) for r in rows]
    cutover = _find_opening_balance_cutover(session, entity_id)
    t = _account_type(account_code)
    sign = 1 if t in ("asset", "cogs", "operating_expense") else -1

    def balance_asof(me) -> float:
        total = Decimal("0")
        use_cutover = cutover is not None and me >= cutover
        for m in movements:
            if m["period_end"] > me:
                continue
            if use_cutover:
                included = (
                    (m["source_module"] == "opening_balance" and m["period_end"] <= cutover)
                    or m["period_start"] > cutover
                )
            else:
                included = True
            if included:
                total += Decimal(str(m["net"] or 0))
        return float(total * sign)

    return [balance_asof(me) for me in month_ends]


@router.get("/metric/account-trend")
def account_trend(
    entity_code: str = Query(...),
    account_code: str = Query(...),
    months: int = Query(default=24, ge=1, le=60),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Month-end balance of one account over time, current vs prior year.
    Cutover-aware (reconciles to the balance sheet). For cash (1020),
    inventory (1120), AR (1090), etc."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        periods = session.execute(
            text(
                """
                SELECT period_end, period_label FROM accounting_periods
                 WHERE entity_id = :e AND period_end <= CURRENT_DATE
              ORDER BY period_end DESC LIMIT :n
                """
            ),
            {"e": entity["id"], "n": months},
        ).mappings().all()
        periods = list(reversed(periods))
        month_ends = [p["period_end"] for p in periods]
        if not month_ends:
            return {"entity_code": entity_code, "account_code": account_code,
                    "source": "gl_net", "series": []}
        cur = _account_trend_series(session, entity["id"], account_code, month_ends)
        py_ends = [_same_day_prior_year(me) for me in month_ends]
        pyv = _account_trend_series(session, entity["id"], account_code, py_ends)

    series = []
    for i, p in enumerate(periods):
        series.append({
            "period_end": p["period_end"].isoformat(),
            "period_label": p["period_label"],
            "balance": round(cur[i], 2),
            "py_balance": round(pyv[i], 2),
            "yoy_growth_pct": _pct_growth(cur[i], pyv[i]),
        })
    return {"entity_code": entity_code, "account_code": account_code,
            "source": "gl_net", "series": series}


@router.get("/metric/margin-trend")
def margin_trend(
    entity_code: str = Query(...),
    months: int = Query(default=24, ge=1, le=60),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Gross / operating / net margin % by month, current vs prior year.
    Unclosed months are returned null (not 0) so the chart doesn't mislead."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        m = _monthly_sales_map(session, entity["id"], months + 12)

    def margins(c):
        s = c["sales"]
        if not s or not c["closed"]:
            return (None, None, None)
        gp = s - c["cogs"]
        op = gp - c["opex"]
        ni = op + c["other_income"]
        return (round(gp / s * 100, 1), round(op / s * 100, 1), round(ni / s * 100, 1))

    keys = sorted(m.keys())
    recent = keys[-months:]
    series = []
    for k in recent:
        cur = m[k]
        py = m.get((k[0] - 1, k[1]))
        gm, om, nm = margins(cur)
        pgm, pom, pnm = margins(py) if py else (None, None, None)
        series.append({
            "period_end": cur["period_end"],
            "period_label": cur["period_label"],
            "closed": cur["closed"],
            "gross_margin_pct": gm,
            "operating_margin_pct": om,
            "net_margin_pct": nm,
            "py_gross_margin_pct": pgm,
            "py_operating_margin_pct": pom,
            "py_net_margin_pct": pnm,
        })
    return {"entity_code": entity_code, "source": "gl_net", "months": months, "series": series}
