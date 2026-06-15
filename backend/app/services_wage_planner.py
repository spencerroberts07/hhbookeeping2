"""
Wage Cost Planner — calculation engine.

Per-entity, per-fiscal-year planner that tracks payroll cost against a target
% of sales, compares to prior year, and produces forward-looking go-forward
HOUR TARGETS for remaining pay periods so a dealer can still land on their
annual wage-cost target.

Model overview (see CLAUDE.md plan section for full derivation):
  B            = 1 + benefits_pct
  avg_wage_wb  = avg_hourly_wage * B            (avg hourly wage WITH benefits)
  salaried_pp  = Σ((annual_salary + bonus) * B / 26) per salaried staff row
  target_hours = (target_wage$ - salaried_pp) / avg_wage_wb  (salaried out first)
  over_under   = actual_hours - target_hours_on_actual_sales  (rebase on actual)
  adjusted     = base_target[q] - cum_over_under / remaining_periods

Multi-tenancy: every function scoped by entity_id UUID or entity_code TEXT.
No mock data; all numeric values from real DB queries.
Files to R2 only (services_storage.py); R2 failures never block callers.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from uuid import UUID

from sqlalchemy import text

_log = logging.getLogger(__name__)

# Finalized payroll-run statuses (mirrors services_payroll_stats.py pattern)
_FINAL_STATUSES = ("approved_to_post", "approved", "posted", "paid")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_entity_by_code(session, entity_code: str) -> dict | None:
    return session.execute(
        text(
            """
            SELECT e.id, e.entity_code, e.entity_name,
                   COALESCE(es.fiscal_year_end_month, 9)  AS fiscal_year_end_month,
                   COALESCE(es.fiscal_year_end_day, 30)   AS fiscal_year_end_day
            FROM entities e
            LEFT JOIN entity_settings es ON es.entity_id = e.id
            WHERE e.entity_code = :ec
            """
        ),
        {"ec": entity_code},
    ).mappings().first()


def _get_entity_by_id(session, entity_id: UUID) -> dict | None:
    return session.execute(
        text(
            """
            SELECT e.id, e.entity_code, e.entity_name,
                   COALESCE(es.fiscal_year_end_month, 9)  AS fiscal_year_end_month,
                   COALESCE(es.fiscal_year_end_day, 30)   AS fiscal_year_end_day
            FROM entities e
            LEFT JOIN entity_settings es ON es.entity_id = e.id
            WHERE e.id = :eid
            """
        ),
        {"eid": entity_id},
    ).mappings().first()


def _fiscal_year_for_date(d: date, fy_end_month: int, fy_end_day: int) -> int:
    """Return the fiscal year integer that contains date d.
    FY2026 = Oct 1 2025 – Sep 30 2026; so any date after Sep 30 2025 is FY2026.
    The returned integer is the calendar year in which the fiscal year ENDS.
    """
    if (d.month, d.day) > (fy_end_month, fy_end_day):
        return d.year + 1
    return d.year


def _d(v) -> Decimal | None:
    """Convert None-safe value to Decimal."""
    if v is None:
        return None
    return Decimal(str(v))


def _safe_div(a: Decimal | None, b: Decimal | None) -> Decimal | None:
    if a is None or b is None or b == 0:
        return None
    return (a / b).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Pay-period calendar
# ---------------------------------------------------------------------------

def backfill_calendar_from_runs(
    session,
    *,
    entity_id: UUID,
    fy_end_month: int,
    fy_end_day: int,
) -> int:
    """Backfill payroll_pay_periods from existing payroll_runs for all fiscal
    years present in the runs table.  Returns count of rows inserted."""
    rows = session.execute(
        text(
            """
            SELECT period_number, period_start, period_end, pay_date
            FROM payroll_runs
            WHERE entity_id = :eid
            ORDER BY period_start
            """
        ),
        {"eid": entity_id},
    ).mappings().all()

    inserted = 0
    for r in rows:
        fy = _fiscal_year_for_date(r["period_start"], fy_end_month, fy_end_day)
        session.execute(
            text(
                """
                INSERT INTO payroll_pay_periods
                    (entity_id, fiscal_year, period_number, period_start,
                     period_end, pay_date, source)
                VALUES
                    (:eid, :fy, :pn, :ps, :pe, :pd, 'backfill')
                ON CONFLICT (entity_id, fiscal_year, period_number) DO NOTHING
                """
            ),
            {
                "eid": entity_id,
                "fy": fy,
                "pn": r["period_number"],
                "ps": r["period_start"],
                "pe": r["period_end"],
                "pd": r["pay_date"],
            },
        )
        inserted += 1
    return inserted


def get_pay_period_calendar(session, *, entity_id: UUID, fiscal_year: int) -> list[dict]:
    """Return the canonical pay-period calendar rows for one fiscal year."""
    rows = session.execute(
        text(
            """
            SELECT id, fiscal_year, period_number, period_start, period_end, pay_date, source
            FROM payroll_pay_periods
            WHERE entity_id = :eid AND fiscal_year = :fy
            ORDER BY period_number
            """
        ),
        {"eid": entity_id, "fy": fiscal_year},
    ).mappings().all()
    return [dict(r) for r in rows]


def upsert_pay_period(
    session,
    *,
    entity_id: UUID,
    fiscal_year: int,
    period_number: int,
    period_start: date,
    period_end: date,
    pay_date: date | None = None,
) -> dict:
    """Manually insert or update a single calendar row."""
    row = session.execute(
        text(
            """
            INSERT INTO payroll_pay_periods
                (entity_id, fiscal_year, period_number, period_start, period_end, pay_date, source)
            VALUES
                (:eid, :fy, :pn, :ps, :pe, :pd, 'manual')
            ON CONFLICT (entity_id, fiscal_year, period_number) DO UPDATE
                SET period_start = EXCLUDED.period_start,
                    period_end   = EXCLUDED.period_end,
                    pay_date     = EXCLUDED.pay_date,
                    source       = 'manual',
                    updated_at   = NOW()
            RETURNING id, fiscal_year, period_number, period_start, period_end, pay_date, source
            """
        ),
        {
            "eid": entity_id,
            "fy": fiscal_year,
            "pn": period_number,
            "ps": period_start,
            "pe": period_end,
            "pd": pay_date,
        },
    ).mappings().first()
    return dict(row)


# ---------------------------------------------------------------------------
# Settings CRUD
# ---------------------------------------------------------------------------

def get_settings(session, *, entity_id: UUID, fiscal_year: int) -> dict | None:
    """Return settings row + salaried staff list, or None if not yet configured."""
    s = session.execute(
        text(
            """
            SELECT id, entity_id, fiscal_year, target_wage_pct, forecast_sales_change,
                   avg_hourly_wage, benefits_pct, distribution_basis, notes,
                   created_at, updated_at
            FROM wage_planner_settings
            WHERE entity_id = :eid AND fiscal_year = :fy
            """
        ),
        {"eid": entity_id, "fy": fiscal_year},
    ).mappings().first()
    if not s:
        return None
    staff = session.execute(
        text(
            """
            SELECT id, employee_name, annual_salary, bonus, assumed_hours_per_period, sort_order
            FROM wage_planner_salaried_staff
            WHERE settings_id = :sid
            ORDER BY sort_order, id
            """
        ),
        {"sid": s["id"]},
    ).mappings().all()
    result = dict(s)
    result["salaried_staff"] = [dict(r) for r in staff]
    return result


def upsert_settings(
    session,
    *,
    entity_id: UUID,
    fiscal_year: int,
    target_wage_pct: float,
    forecast_sales_change: float,
    avg_hourly_wage: float,
    benefits_pct: float,
    distribution_basis: str = "prior_year",
    notes: str | None = None,
    salaried_staff: list[dict] | None = None,
) -> dict:
    """Create or replace the annual settings for one entity + fiscal year.
    salaried_staff: list of {employee_name, annual_salary, bonus, assumed_hours_per_period}
    Existing salaried_staff rows are REPLACED (delete + re-insert).
    """
    if distribution_basis not in ("prior_year", "national_average"):
        raise ValueError("distribution_basis must be 'prior_year' or 'national_average'")

    row = session.execute(
        text(
            """
            INSERT INTO wage_planner_settings
                (entity_id, fiscal_year, target_wage_pct, forecast_sales_change,
                 avg_hourly_wage, benefits_pct, distribution_basis, notes)
            VALUES
                (:eid, :fy, :twp, :fsc, :ahw, :bp, :db, :notes)
            ON CONFLICT (entity_id, fiscal_year) DO UPDATE
                SET target_wage_pct      = EXCLUDED.target_wage_pct,
                    forecast_sales_change = EXCLUDED.forecast_sales_change,
                    avg_hourly_wage       = EXCLUDED.avg_hourly_wage,
                    benefits_pct          = EXCLUDED.benefits_pct,
                    distribution_basis    = EXCLUDED.distribution_basis,
                    notes                 = EXCLUDED.notes,
                    updated_at            = NOW()
            RETURNING id
            """
        ),
        {
            "eid": entity_id,
            "fy": fiscal_year,
            "twp": target_wage_pct,
            "fsc": forecast_sales_change,
            "ahw": avg_hourly_wage,
            "bp": benefits_pct,
            "db": distribution_basis,
            "notes": notes,
        },
    ).mappings().first()
    settings_id = row["id"]

    # Replace salaried staff
    session.execute(
        text("DELETE FROM wage_planner_salaried_staff WHERE settings_id = :sid"),
        {"sid": settings_id},
    )
    for i, emp in enumerate(salaried_staff or []):
        session.execute(
            text(
                """
                INSERT INTO wage_planner_salaried_staff
                    (settings_id, employee_name, annual_salary, bonus,
                     assumed_hours_per_period, sort_order)
                VALUES
                    (:sid, :name, :sal, :bonus, :hrs, :order)
                """
            ),
            {
                "sid": settings_id,
                "name": emp.get("employee_name", ""),
                "sal": emp.get("annual_salary", 0),
                "bonus": emp.get("bonus", 0),
                "hrs": emp.get("assumed_hours_per_period", 80),
                "order": i,
            },
        )

    return get_settings(session, entity_id=entity_id, fiscal_year=fiscal_year)


# ---------------------------------------------------------------------------
# Sales helpers
# ---------------------------------------------------------------------------

def _sum_sales(session, entity_id: UUID, start: date, end: date) -> Decimal:
    """Sum cash_balancing_days.total_sales for the given date window."""
    row = session.execute(
        text(
            """
            SELECT COALESCE(SUM(total_sales), 0) AS total
            FROM cash_balancing_days
            WHERE entity_id = :eid
              AND business_date BETWEEN :start AND :end
            """
        ),
        {"eid": entity_id, "start": start, "end": end},
    ).mappings().first()
    return Decimal(str(row["total"]))


def _prior_year_window(start: date, end: date) -> tuple[date, date]:
    """Return (start, end) shifted back 364 days for weekday-alignment
    (matches the dashboard.py pattern)."""
    shift = timedelta(days=364)
    return start - shift, end - shift


def managed_wage_dollars(
    session, entity_id: UUID, start: date, end: date
) -> tuple[Decimal, str]:
    """Return (managed_wage_$, basis) where basis ∈ {"gl_6120","runline_gross","none"}.

    Prefers journal_lines account 6120 on source_module='payroll' batches when
    any such batches exist for the window (current-FY GL path).  Falls back to
    payroll_run_lines non-management gross_pay (FY2025 backfill path — those
    runs are draft_confirmed and never posted to GL).

    NOTE: The two bases are NOT equivalent:
      gl_6120       = non-mgmt gross + employer CPP/EI + vacation accrual
                      ("Wages & Benefits incl. employer CPP/EI & vacation accrual")
      runline_gross = non-mgmt gross_pay only
                      ("Gross wages, non-management — no statutory burden")
    Callers must surface the basis label so dealers understand the difference.
    """
    # Branch A — preferred: literal GL account 6120 on payroll batches.
    # Window on ap.period_end matches _account_sums() in reports.py:219-288.
    # source_module='payroll' filter + GL-import writing to separate tables
    # (gl_import_runs/gl_account_balances/gl_transactions, never journal_lines)
    # gives double-count immunity.
    row_a = session.execute(
        text(
            """
            SELECT COALESCE(SUM(jl.debit_amount), 0) AS amt,
                   COUNT(DISTINCT jb.id)              AS batch_cnt
            FROM journal_lines jl
            JOIN journal_batches jb    ON jb.id = jl.journal_batch_id
            JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
            WHERE jb.entity_id     = :eid
              AND jl.account_code  = '6120'
              AND jb.source_module = 'payroll'
              AND jb.status NOT IN ('draft', 'voided', 'rejected')
              AND ap.period_end BETWEEN :start AND :end
            """
        ),
        {"eid": entity_id, "start": start, "end": end},
    ).mappings().first()

    if row_a and int(row_a["batch_cnt"]) > 0:
        return Decimal(str(row_a["amt"])), "gl_6120"

    # Branch B — fallback: non-management gross from payroll_run_lines.
    # No _FINAL_STATUSES gate so FY2025 draft_confirmed runs are included.
    row_b = session.execute(
        text(
            """
            SELECT COALESCE(SUM(prl.gross_pay), 0) AS amt
            FROM payroll_run_lines prl
            JOIN payroll_runs pr           ON pr.id = prl.payroll_run_id
            LEFT JOIN payroll_employees pe ON pe.id = prl.employee_id
            WHERE pr.entity_id = :eid
              AND pr.period_end BETWEEN :start AND :end
              AND COALESCE(pe.is_management, FALSE) = FALSE
            """
        ),
        {"eid": entity_id, "start": start, "end": end},
    ).mappings().first()

    if row_b:
        amt = Decimal(str(row_b["amt"]))
        if amt > 0:
            return amt, "runline_gross"

    return Decimal("0"), "none"


# ---------------------------------------------------------------------------
# Core calculation
# ---------------------------------------------------------------------------

def _compute_salaried_totals(settings_row: dict) -> tuple[Decimal, int]:
    """Return (salaried_wage_per_period, salaried_hours_per_period).

    salaried_wage_pp = Σ (annual_salary + bonus) * (1 + benefits_pct) / 26
    salaried_hours   = Σ assumed_hours_per_period
    """
    bp = Decimal(str(settings_row["benefits_pct"]))
    B = Decimal("1") + bp
    salaried_wage = Decimal("0")
    salaried_hours = 0
    for emp in settings_row.get("salaried_staff") or []:
        sal = Decimal(str(emp["annual_salary"])) + Decimal(str(emp["bonus"]))
        salaried_wage += sal * B / Decimal("26")
        salaried_hours += int(emp.get("assumed_hours_per_period") or 80)
    return salaried_wage.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), salaried_hours


def compute_plan(session, *, entity_id: UUID, fiscal_year: int) -> dict:
    """Compute the full 26-period planner table for one entity + fiscal year.

    Returns:
        {
          "settings": {...},
          "periods": [
            {
              "period_number": 1,
              "period_start": date,
              "period_end": date,
              "py_sales": Decimal | None,
              "forecast_sales": Decimal | None,
              "target_wage_dollars": Decimal | None,
              "target_hours": Decimal | None,
              "salaried_hours_pp": int,
              "actual_sales": Decimal | None,
              "actual_gross_wages": Decimal | None,
              "actual_stat_pay": Decimal | None,
              "actual_hours": Decimal | None,
              "hours_over_under": Decimal | None,
              "adjusted_target_hours": Decimal | None,
              "actual_sales_per_hour": Decimal | None,
              "py_sales_per_hour": Decimal | None,
              "locked": bool,
            },
            ...
          ],
          "summary": {
            "forecast_annual_sales": Decimal | None,
            "target_annual_wage_dollars": Decimal | None,
            "cum_over_under": Decimal | None,
            "periods_locked": int,
            "periods_remaining": int,
          }
        }
    """
    settings_row = get_settings(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not settings_row:
        return {"settings": None, "periods": [], "summary": {}}

    calendar = get_pay_period_calendar(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not calendar:
        return {"settings": settings_row, "periods": [], "summary": {}}

    twp = Decimal(str(settings_row["target_wage_pct"]))
    fsc = Decimal(str(settings_row["forecast_sales_change"]))
    ahw = Decimal(str(settings_row["avg_hourly_wage"]))
    bp = Decimal(str(settings_row["benefits_pct"]))
    B = Decimal("1") + bp
    avg_wage_wb = ahw * B  # avg hourly wage WITH benefits

    salaried_pp, salaried_hrs_pp = _compute_salaried_totals(settings_row)

    # Load existing locked-period rows from DB
    locked_rows = session.execute(
        text(
            """
            SELECT period_number, actual_sales, actual_gross_wages, actual_stat_pay,
                   actual_hours, hours_over_under, actual_sales_per_hour,
                   py_sales_per_hour, locked, manual_override_json
            FROM wage_planner_periods
            WHERE entity_id = :eid AND fiscal_year = :fy
            ORDER BY period_number
            """
        ),
        {"eid": entity_id, "fy": fiscal_year},
    ).mappings().all()
    locked_map = {r["period_number"]: dict(r) for r in locked_rows}

    periods_out = []
    locked_count = 0
    cum_over_under = Decimal("0")

    for cal in calendar:
        pn = cal["period_number"]
        ps = cal["period_start"]
        pe = cal["period_end"]

        # Prior-year sales
        py_start, py_end = _prior_year_window(ps, pe)
        py_sales = _sum_sales(session, entity_id, py_start, py_end)
        if py_sales == 0:
            py_sales = None  # no data yet

        forecast_sales = (py_sales * (Decimal("1") + fsc)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        ) if py_sales is not None else None

        target_wage_d = (
            (forecast_sales * twp).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if forecast_sales is not None else None
        )
        if target_wage_d is not None and avg_wage_wb > 0:
            numerator = target_wage_d - salaried_pp
            target_hrs = (numerator / avg_wage_wb).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            target_hrs = None

        # Pull actuals from DB row (if any)
        db_row = locked_map.get(pn) or {}
        locked = bool(db_row.get("locked", False))
        overrides = db_row.get("manual_override_json") or {}

        actual_sales = _d(overrides.get("actual_sales") or db_row.get("actual_sales"))
        actual_gross = _d(overrides.get("actual_gross_wages") or db_row.get("actual_gross_wages"))
        actual_stat = _d(overrides.get("actual_stat_pay") or db_row.get("actual_stat_pay"))
        actual_hrs = _d(overrides.get("actual_hours") or db_row.get("actual_hours"))

        hours_ou = _d(db_row.get("hours_over_under")) if locked else None
        actual_sph = _d(db_row.get("actual_sales_per_hour")) if locked else None
        py_sph = _d(db_row.get("py_sales_per_hour")) if locked else None

        if locked and hours_ou is not None:
            locked_count += 1
            cum_over_under += hours_ou

        periods_out.append({
            "period_number": pn,
            "period_start": ps,
            "period_end": pe,
            "pay_date": cal.get("pay_date"),
            "py_sales": py_sales,
            "forecast_sales": forecast_sales,
            "target_wage_dollars": target_wage_d,
            "target_hours": target_hrs,
            "salaried_hours_pp": salaried_hrs_pp,
            "actual_sales": actual_sales,
            "actual_gross_wages": actual_gross,
            "actual_stat_pay": actual_stat,
            "actual_hours": actual_hrs,
            "hours_over_under": hours_ou,
            "adjusted_target_hours": None,  # filled in second pass below
            "actual_sales_per_hour": actual_sph,
            "py_sales_per_hour": py_sph,
            "locked": locked,
        })

    # Second pass: compute adjusted_target_hours for remaining (unlocked) periods
    remaining = len(periods_out) - locked_count
    for p in periods_out:
        if not p["locked"] and remaining > 0 and p["target_hours"] is not None:
            adjustment = (cum_over_under / Decimal(str(remaining))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            p["adjusted_target_hours"] = (p["target_hours"] - adjustment).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

    # Summary
    forecast_annual = sum(
        (p["forecast_sales"] for p in periods_out if p["forecast_sales"] is not None),
        Decimal("0"),
    ) or None
    target_annual = sum(
        (p["target_wage_dollars"] for p in periods_out if p["target_wage_dollars"] is not None),
        Decimal("0"),
    ) or None

    return {
        "settings": settings_row,
        "periods": periods_out,
        "summary": {
            "forecast_annual_sales": forecast_annual,
            "target_annual_wage_dollars": target_annual,
            "cum_over_under": cum_over_under if locked_count else None,
            "periods_locked": locked_count,
            "periods_remaining": remaining,
        },
    }


# ---------------------------------------------------------------------------
# Refresh period actuals (fired on payroll-run approval)
# ---------------------------------------------------------------------------

def refresh_period_actuals(
    session,
    *,
    entity_id: UUID,
    fiscal_year: int,
    period_number: int,
    payroll_run_id: str,
    actor_email: str | None = None,
) -> dict:
    """Auto-pull actual_sales, gross_wages, stat_pay, hours from the payroll
    run and cash_balancing_days, write the wage_planner_periods row, set
    locked=TRUE, and recompute adjusted_target_hours for all remaining periods.

    Returns the written period row dict.
    """
    settings_row = get_settings(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not settings_row:
        _log.warning(
            "wage_planner: no settings for entity %s FY%s — skip refresh p%s",
            entity_id, fiscal_year, period_number,
        )
        return {}

    calendar = get_pay_period_calendar(session, entity_id=entity_id, fiscal_year=fiscal_year)
    cal_map = {r["period_number"]: r for r in calendar}
    cal = cal_map.get(period_number)
    if not cal:
        _log.warning(
            "wage_planner: no calendar entry for entity %s FY%s p%s",
            entity_id, fiscal_year, period_number,
        )
        return {}

    ps = cal["period_start"]
    pe = cal["period_end"]

    # Auto-pull actual sales from cash_balancing_days
    actual_sales = _sum_sales(session, entity_id, ps, pe)
    if actual_sales == 0:
        actual_sales = None

    # Auto-pull gross wages, stat pay, and hours from payroll_run_lines
    wage_row = session.execute(
        text(
            """
            SELECT
                COALESCE(SUM(prl.gross_pay), 0)   AS gross_wages,
                COALESCE(SUM(prl.stat_pay), 0)    AS stat_pay,
                COALESCE(SUM(
                    CASE WHEN prl.employment_type <> 'salaried'
                         THEN prl.total_hours ELSE 0 END
                ), 0) AS hourly_hours
            FROM payroll_run_lines prl
            JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
            WHERE pr.id = :run_id
              AND pr.entity_id = :eid
              AND COALESCE(pr.workflow_status, pr.status) = ANY(:statuses)
            """
        ),
        {
            "run_id": payroll_run_id,
            "eid": entity_id,
            "statuses": list(_FINAL_STATUSES),
        },
    ).mappings().first()

    actual_gross = _d(wage_row["gross_wages"]) if wage_row else None
    actual_stat = _d(wage_row["stat_pay"]) if wage_row else None
    actual_hrs = _d(wage_row["hourly_hours"]) if wage_row else None

    # Hours over/(under) — rebase target on ACTUAL sales
    twp = Decimal(str(settings_row["target_wage_pct"]))
    ahw = Decimal(str(settings_row["avg_hourly_wage"]))
    bp = Decimal(str(settings_row["benefits_pct"]))
    B = Decimal("1") + bp
    avg_wage_wb = ahw * B
    salaried_pp, _ = _compute_salaried_totals(settings_row)

    hours_ou = None
    actual_sph = None
    if actual_sales is not None and avg_wage_wb > 0 and actual_hrs is not None:
        target_hrs_on_actual = (actual_sales * twp - salaried_pp) / avg_wage_wb
        hours_ou = (actual_hrs - target_hrs_on_actual).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        actual_sph = _safe_div(actual_sales, actual_hrs)

    # Prior-year SPH
    py_start, py_end = _prior_year_window(ps, pe)
    py_sales = _sum_sales(session, entity_id, py_start, py_end) or None
    py_hrs_row = session.execute(
        text(
            """
            SELECT COALESCE(SUM(prl.total_hours), 0) AS hrs
            FROM payroll_run_lines prl
            JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
            WHERE pr.entity_id = :eid
              AND pr.period_start = :ps
              AND pr.period_end   = :pe
              AND COALESCE(pr.workflow_status, pr.status) = ANY(:statuses)
              AND prl.employment_type <> 'salaried'
            """
        ),
        {
            "eid": entity_id,
            "ps": py_start,
            "pe": py_end,
            "statuses": list(_FINAL_STATUSES),
        },
    ).mappings().first()
    py_hrs = _d(py_hrs_row["hrs"]) if py_hrs_row else None
    py_sph = _safe_div(py_sales, py_hrs) if py_sales else None

    # Check for manual_override_json so we honour existing overrides
    existing = session.execute(
        text(
            """
            SELECT manual_override_json
            FROM wage_planner_periods
            WHERE entity_id = :eid AND fiscal_year = :fy AND period_number = :pn
            """
        ),
        {"eid": entity_id, "fy": fiscal_year, "pn": period_number},
    ).mappings().first()
    manual_override = (existing["manual_override_json"] if existing else None) or {}

    # If manual overrides exist for actuals, apply them
    if manual_override.get("actual_sales") is not None:
        actual_sales = Decimal(str(manual_override["actual_sales"]))
    if manual_override.get("actual_gross_wages") is not None:
        actual_gross = Decimal(str(manual_override["actual_gross_wages"]))
    if manual_override.get("actual_stat_pay") is not None:
        actual_stat = Decimal(str(manual_override["actual_stat_pay"]))
    if manual_override.get("actual_hours") is not None:
        actual_hrs = Decimal(str(manual_override["actual_hours"]))
        # Recompute over/under with overridden hours
        if actual_sales is not None and avg_wage_wb > 0:
            target_hrs_on_actual = (actual_sales * twp - salaried_pp) / avg_wage_wb
            hours_ou = (actual_hrs - target_hrs_on_actual).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            actual_sph = _safe_div(actual_sales, actual_hrs)

    session.execute(
        text(
            """
            INSERT INTO wage_planner_periods
                (entity_id, fiscal_year, period_number,
                 actual_sales, actual_gross_wages, actual_stat_pay, actual_hours,
                 hours_over_under, actual_sales_per_hour, py_sales_per_hour,
                 locked, locked_at, source_payroll_run_id, computed_at)
            VALUES
                (:eid, :fy, :pn,
                 :as_, :ag, :asp, :ah,
                 :hou, :asph, :pysph,
                 TRUE, NOW(), :run_id, NOW())
            ON CONFLICT (entity_id, fiscal_year, period_number) DO UPDATE
                SET actual_sales          = EXCLUDED.actual_sales,
                    actual_gross_wages     = EXCLUDED.actual_gross_wages,
                    actual_stat_pay        = EXCLUDED.actual_stat_pay,
                    actual_hours           = EXCLUDED.actual_hours,
                    hours_over_under       = EXCLUDED.hours_over_under,
                    actual_sales_per_hour  = EXCLUDED.actual_sales_per_hour,
                    py_sales_per_hour      = EXCLUDED.py_sales_per_hour,
                    locked                 = TRUE,
                    locked_at              = COALESCE(wage_planner_periods.locked_at, NOW()),
                    source_payroll_run_id  = EXCLUDED.source_payroll_run_id,
                    computed_at            = NOW(),
                    updated_at             = NOW()
            """
        ),
        {
            "eid": entity_id,
            "fy": fiscal_year,
            "pn": period_number,
            "as_": actual_sales,
            "ag": actual_gross,
            "asp": actual_stat,
            "ah": actual_hrs,
            "hou": hours_ou,
            "asph": actual_sph,
            "pysph": py_sph,
            "run_id": payroll_run_id,
        },
    )
    _log.info(
        "wage_planner: locked p%s FY%s entity=%s over_under=%s",
        period_number, fiscal_year, entity_id, hours_ou,
    )

    # Recompute adjusted_target_hours for all remaining (unlocked) periods
    _recompute_adjusted(session, entity_id=entity_id, fiscal_year=fiscal_year)

    return {
        "period_number": period_number,
        "fiscal_year": fiscal_year,
        "locked": True,
        "actual_sales": str(actual_sales) if actual_sales else None,
        "actual_gross_wages": str(actual_gross) if actual_gross else None,
        "actual_stat_pay": str(actual_stat) if actual_stat else None,
        "actual_hours": str(actual_hrs) if actual_hrs else None,
        "hours_over_under": str(hours_ou) if hours_ou is not None else None,
        "actual_sales_per_hour": str(actual_sph) if actual_sph else None,
    }


def _recompute_adjusted(session, *, entity_id: UUID, fiscal_year: int) -> None:
    """Recompute adjusted_target_hours for all UNLOCKED periods in the fiscal year.

    Logic:
      cum_over_under  = SUM(hours_over_under) WHERE locked = TRUE
      remaining       = COUNT(*) WHERE locked = FALSE (or not yet a DB row)
      For each unlocked period q:
        base_target = (forecast_sales[q] * twp - salaried_pp) / avg_wage_wb
        adjusted    = base_target - cum_over_under / remaining
    """
    settings_row = get_settings(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not settings_row:
        return

    calendar = get_pay_period_calendar(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not calendar:
        return

    twp = Decimal(str(settings_row["target_wage_pct"]))
    fsc = Decimal(str(settings_row["forecast_sales_change"]))
    ahw = Decimal(str(settings_row["avg_hourly_wage"]))
    bp = Decimal(str(settings_row["benefits_pct"]))
    B = Decimal("1") + bp
    avg_wage_wb = ahw * B
    salaried_pp, _ = _compute_salaried_totals(settings_row)

    # Cumulative over/under from locked periods
    cum_row = session.execute(
        text(
            """
            SELECT COALESCE(SUM(hours_over_under), 0) AS cum_ou,
                   COUNT(*) FILTER (WHERE locked = TRUE) AS locked_cnt,
                   COUNT(*) AS total_cnt
            FROM wage_planner_periods
            WHERE entity_id = :eid AND fiscal_year = :fy
            """
        ),
        {"eid": entity_id, "fy": fiscal_year},
    ).mappings().first()

    cum_ou = Decimal(str(cum_row["cum_ou"])) if cum_row else Decimal("0")
    locked_cnt = int(cum_row["locked_cnt"]) if cum_row else 0
    remaining = len(calendar) - locked_cnt
    if remaining <= 0:
        return

    # Get locked period numbers so we can skip them
    locked_pns = set(
        r["period_number"] for r in session.execute(
            text(
                "SELECT period_number FROM wage_planner_periods "
                "WHERE entity_id = :eid AND fiscal_year = :fy AND locked = TRUE"
            ),
            {"eid": entity_id, "fy": fiscal_year},
        ).mappings().all()
    )

    for cal in calendar:
        pn = cal["period_number"]
        if pn in locked_pns:
            continue
        ps = cal["period_start"]
        pe = cal["period_end"]
        py_start, py_end = _prior_year_window(ps, pe)
        py_sales = _sum_sales(session, entity_id, py_start, py_end)
        if py_sales == 0 or avg_wage_wb == 0:
            continue
        forecast = py_sales * (Decimal("1") + fsc)
        target_wage_d = forecast * twp
        base_target = (target_wage_d - salaried_pp) / avg_wage_wb
        adjusted = (base_target - cum_ou / Decimal(str(remaining))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        session.execute(
            text(
                """
                INSERT INTO wage_planner_periods
                    (entity_id, fiscal_year, period_number,
                     py_sales, forecast_sales, target_wage_dollars, target_hours,
                     adjusted_target_hours, computed_at)
                VALUES
                    (:eid, :fy, :pn, :py, :fs, :twd, :th, :adj, NOW())
                ON CONFLICT (entity_id, fiscal_year, period_number) DO UPDATE
                    SET py_sales              = EXCLUDED.py_sales,
                        forecast_sales        = EXCLUDED.forecast_sales,
                        target_wage_dollars   = EXCLUDED.target_wage_dollars,
                        target_hours          = EXCLUDED.target_hours,
                        adjusted_target_hours = EXCLUDED.adjusted_target_hours,
                        computed_at           = NOW(),
                        updated_at            = NOW()
                """
            ),
            {
                "eid": entity_id,
                "fy": fiscal_year,
                "pn": pn,
                "py": py_sales,
                "fs": forecast.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "twd": target_wage_d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "th": base_target.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                "adj": adjusted,
            },
        )


# ---------------------------------------------------------------------------
# Manual override
# ---------------------------------------------------------------------------

def apply_manual_override(
    session,
    *,
    entity_id: UUID,
    fiscal_year: int,
    period_number: int,
    overrides: dict,
) -> dict:
    """Apply manual override fields (actual_sales, actual_hours, etc.) for a
    period and recompute over_under accordingly.  The override is stored in
    manual_override_json and takes precedence over auto-pulled values.
    """
    import json as _json
    # Merge with any existing overrides
    existing = session.execute(
        text(
            """
            SELECT manual_override_json, actual_sales, actual_gross_wages,
                   actual_stat_pay, actual_hours, source_payroll_run_id
            FROM wage_planner_periods
            WHERE entity_id = :eid AND fiscal_year = :fy AND period_number = :pn
            """
        ),
        {"eid": entity_id, "fy": fiscal_year, "pn": period_number},
    ).mappings().first()

    existing_overrides = {}
    if existing and existing["manual_override_json"]:
        existing_overrides = dict(existing["manual_override_json"])
    existing_overrides.update(
        {k: v for k, v in overrides.items() if v is not None}
    )

    session.execute(
        text(
            """
            INSERT INTO wage_planner_periods
                (entity_id, fiscal_year, period_number, manual_override_json, computed_at)
            VALUES
                (:eid, :fy, :pn, :oj::jsonb, NOW())
            ON CONFLICT (entity_id, fiscal_year, period_number) DO UPDATE
                SET manual_override_json = :oj::jsonb,
                    computed_at = NOW(),
                    updated_at  = NOW()
            """
        ),
        {
            "eid": entity_id,
            "fy": fiscal_year,
            "pn": period_number,
            "oj": _json.dumps(existing_overrides),
        },
    )

    # Refresh the period using current run_id (if any)
    run_id = str(existing["source_payroll_run_id"]) if existing else None
    if run_id:
        return refresh_period_actuals(
            session,
            entity_id=entity_id,
            fiscal_year=fiscal_year,
            period_number=period_number,
            payroll_run_id=run_id,
        )
    _recompute_adjusted(session, entity_id=entity_id, fiscal_year=fiscal_year)
    return {"period_number": period_number, "overrides_applied": existing_overrides}


# ---------------------------------------------------------------------------
# Minimum-wage impact calculator
# ---------------------------------------------------------------------------

def min_wage_impact(
    session,
    *,
    entity_id: UUID,
    new_min_wage: float,
) -> dict:
    """Compute the delta in gross wages if all hourly employees below
    new_min_wage are brought up to that rate.

    Returns a list of affected employees + total current vs projected cost
    per bi-weekly period.
    """
    employees = session.execute(
        text(
            """
            SELECT id, full_name, employment_type, hourly_rate
            FROM payroll_employees
            WHERE entity_id = :eid
              AND is_active = TRUE
              AND employment_type <> 'salaried'
              AND hourly_rate < :new_rate
            ORDER BY hourly_rate ASC
            """
        ),
        {"eid": entity_id, "new_rate": new_min_wage},
    ).mappings().all()

    affected = []
    total_current_biweekly = Decimal("0")
    total_projected_biweekly = Decimal("0")

    # Use 80 hrs / period as a standard estimate for each hourly employee
    HOURS_PP = Decimal("80")

    for emp in employees:
        current_rate = Decimal(str(emp["hourly_rate"]))
        new_rate = Decimal(str(new_min_wage))
        current_pp = (current_rate * HOURS_PP).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        new_pp = (new_rate * HOURS_PP).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        delta_pp = new_pp - current_pp
        total_current_biweekly += current_pp
        total_projected_biweekly += new_pp
        affected.append({
            "employee_id": str(emp["id"]),
            "full_name": emp["full_name"],
            "current_rate": str(current_rate),
            "new_rate": str(new_rate),
            "delta_rate": str(new_rate - current_rate),
            "current_biweekly_est": str(current_pp),
            "projected_biweekly_est": str(new_pp),
            "delta_biweekly_est": str(delta_pp),
        })

    return {
        "new_min_wage": str(new_min_wage),
        "affected_employees": len(affected),
        "employees": affected,
        "total_current_biweekly_est": str(total_current_biweekly),
        "total_projected_biweekly_est": str(total_projected_biweekly),
        "total_delta_biweekly_est": str(total_projected_biweekly - total_current_biweekly),
        "total_delta_annual_est": str(
            (total_projected_biweekly - total_current_biweekly) * Decimal("26")
        ),
    }


# ---------------------------------------------------------------------------
# Dashboard summary (all 5 cards + multi-FY trend)
# ---------------------------------------------------------------------------

# Ontario minimum-wage constants for Card 5 (min-wage alert).
# Update when Ontario raises the rate; $17.20 effective Oct 1 2024.
_ONTARIO_MIN_WAGE = Decimal("17.20")
_MIN_WAGE_ALERT_BAND = Decimal("2.00")   # alert if hourly_rate < min + $2.00
_HOURS_PP_STANDARD = Decimal("80")       # estimated hours per period for cost projection


def _safe_div_pct(a: Decimal | None, b: Decimal | None) -> Decimal | None:
    """Divide and return 6-decimal precision (matches NUMERIC(7,6) settings columns)."""
    if a is None or b is None or b == 0:
        return None
    return (a / b).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def compute_dashboard_summary(
    session,
    *,
    entity_id: UUID,
    fiscal_year: int,
    fy_end_month: int,
    fy_end_day: int,
) -> dict:
    """Aggregate all Wage Cost Planner dashboard cards + multi-FY trend series.

    Returns a dict covering:
      - card1_headline    : YTD managed wage % vs target and prior year
      - card2_forward_target : adjusted_target_hours for next unlocked period
      - card3_ytd_actuals : YTD wage $, sales $, vs targets
      - card4_salaried    : per-period + annual salaried cost breakdown
      - card5_min_wage    : near-min-wage employee alert (Ont. $17.20, $2 band)
      - trend             : multi-FY [{fy, period_number, label, actual_wage_pct,
                            target_pct, prior_year_pct, actual_wages, actual_sales,
                            basis}] for the trend chart
    """
    # ----- 1. Settings + plan ------------------------------------------------
    settings = get_settings(session, entity_id=entity_id, fiscal_year=fiscal_year)
    if not settings:
        return {
            "fiscal_year": fiscal_year,
            "settings_present": False,
            "ytd": None,
            "card1_headline": None,
            "card2_forward_target": None,
            "card3_ytd_actuals": None,
            "card4_salaried": None,
            "card5_min_wage": None,
            "trend": [],
        }

    plan = compute_plan(session, entity_id=entity_id, fiscal_year=fiscal_year)
    periods = plan["periods"]
    plan_summary = plan.get("summary", {})

    # ----- 2. Per-entity YTD window (no hardcoded fiscal-year-start month) ---
    today = date.today()
    fy_start = date(fiscal_year - 1, fy_end_month, fy_end_day) + timedelta(days=1)
    fy_end_cal = date(fiscal_year, fy_end_month, fy_end_day)
    ytd_end = min(today, fy_end_cal)

    periods_completed = int(plan_summary.get("periods_locked") or 0)
    periods_remaining = int(plan_summary.get("periods_remaining") or 0)

    # ----- 3. Current-FY YTD wages + sales -----------------------------------
    actual_wages_ytd, wage_basis = managed_wage_dollars(
        session, entity_id, fy_start, ytd_end
    )
    actual_sales_ytd = _sum_sales(session, entity_id, fy_start, ytd_end)

    # YTD targets derived from the plan rows (no extra SQL)
    target_wages_ytd = sum(
        (_d(p.get("target_wage_dollars")) or Decimal("0"))
        for p in periods
        if p.get("period_end") is not None and p["period_end"] <= ytd_end
    )
    forecast_sales_ytd = sum(
        (_d(p.get("forecast_sales")) or Decimal("0"))
        for p in periods
        if p.get("period_end") is not None and p["period_end"] <= ytd_end
    )

    # ----- 4. Card 1 — headline KPI ------------------------------------------
    twp = Decimal(str(settings["target_wage_pct"]))
    ytd_pct = _safe_div_pct(actual_wages_ytd, actual_sales_ytd)

    if ytd_pct is not None:
        if ytd_pct <= twp:
            health = "green"
        elif ytd_pct <= twp + Decimal("0.005"):
            health = "yellow"
        else:
            health = "red"
    else:
        health = "green"  # no data yet — neutral, not alarming

    # Prior-year same-period (364-day shift for weekday alignment)
    py_fy_start, py_ytd_end = _prior_year_window(fy_start, ytd_end)
    py_wages, py_wage_basis = managed_wage_dollars(
        session, entity_id, py_fy_start, py_ytd_end
    )
    py_sales = _sum_sales(session, entity_id, py_fy_start, py_ytd_end)
    prior_year_pct = _safe_div_pct(py_wages, py_sales) if py_sales else None

    # ----- 5. Card 2 — forward target ----------------------------------------
    next_unlocked = next(
        (p for p in periods if not p.get("locked")), None
    )
    next_period_number = next_unlocked["period_number"] if next_unlocked else None
    next_adj_hours = next_unlocked.get("adjusted_target_hours") if next_unlocked else None

    cum_ou = plan_summary.get("cum_over_under")
    if cum_ou is not None:
        c = Decimal(str(cum_ou))
        card2_color = "red" if c > 0 else ("emerald" if c < 0 else "muted")
    else:
        card2_color = "muted"

    # ----- 6. Card 3 — YTD actuals vs targets --------------------------------
    wages_variance = actual_wages_ytd - (target_wages_ytd or Decimal("0"))
    sales_variance = actual_sales_ytd - (forecast_sales_ytd or Decimal("0"))

    # ----- 7. Card 4 — salaried breakdown ------------------------------------
    bp = Decimal(str(settings["benefits_pct"]))
    B = Decimal("1") + bp
    salaried_pp, _ = _compute_salaried_totals(settings)
    salaried_annual = (salaried_pp * Decimal("26")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    target_annual = _d(plan_summary.get("target_annual_wage_dollars"))
    salaried_pct = _safe_div_pct(salaried_annual, target_annual)

    staff_detail = []
    for emp in (settings.get("salaried_staff") or []):
        sal = Decimal(str(emp.get("annual_salary", 0)))
        bonus = Decimal(str(emp.get("bonus", 0)))
        annual_cost = ((sal + bonus) * B).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        per_period = (annual_cost / Decimal("26")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        staff_detail.append({
            "employee_name": emp.get("employee_name", ""),
            "annual_salary": str(sal),
            "bonus": str(bonus),
            "annual_cost": str(annual_cost),
            "per_period": str(per_period),
        })

    # ----- 8. Card 5 — min-wage alert ----------------------------------------
    # Alert threshold: employees within $2.00 of Ontario minimum wage ($17.20)
    near_threshold = float(_ONTARIO_MIN_WAGE + _MIN_WAGE_ALERT_BAND)
    near_rows = session.execute(
        text(
            """
            SELECT id, full_name, hourly_rate
            FROM payroll_employees
            WHERE entity_id  = :eid
              AND is_active   = TRUE
              AND employment_type <> 'salaried'
              AND hourly_rate < :threshold
            ORDER BY hourly_rate ASC
            """
        ),
        {"eid": entity_id, "threshold": near_threshold},
    ).mappings().all()

    near_min_list = []
    total_delta_annual = Decimal("0")
    for emp in near_rows:
        current_rate = Decimal(str(emp["hourly_rate"]))
        gap = max(Decimal("0"), _ONTARIO_MIN_WAGE - current_rate)
        est_annual = (gap * _HOURS_PP_STANDARD * Decimal("26")).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        total_delta_annual += est_annual
        near_min_list.append({
            "employee_id": str(emp["id"]),
            "full_name": emp["full_name"],
            "current_rate": str(current_rate),
            "gap_to_min": str(gap),
            "est_annual_raise_cost": str(est_annual),
        })

    # ----- 9. Trend series (all imported fiscal years) -----------------------
    fy_rows = session.execute(
        text(
            """
            SELECT DISTINCT fiscal_year
            FROM wage_planner_periods
            WHERE entity_id = :eid
            UNION
            SELECT DISTINCT fiscal_year
            FROM payroll_pay_periods
            WHERE entity_id = :eid
            ORDER BY fiscal_year
            """
        ),
        {"eid": entity_id},
    ).mappings().all()
    all_fys = [r["fiscal_year"] for r in fy_rows]

    trend: list[dict] = []
    for fy in all_fys:
        fy_settings = get_settings(session, entity_id=entity_id, fiscal_year=fy)
        fy_target_pct = (
            Decimal(str(fy_settings["target_wage_pct"])) if fy_settings else None
        )
        cal = get_pay_period_calendar(session, entity_id=entity_id, fiscal_year=fy)

        for c in cal:
            ps: date = c["period_start"]
            pe: date = c["period_end"]

            # Only emit periods that are fully in the past (or up to today)
            if ps > today:
                break

            wage, basis = managed_wage_dollars(session, entity_id, ps, pe)
            sales = _sum_sales(session, entity_id, ps, pe)
            if wage == 0 and sales == 0:
                continue  # no data yet, skip

            actual_pct = _safe_div_pct(wage, sales)

            # Prior-year comparison for this specific period slot
            py_ps, py_pe = _prior_year_window(ps, pe)
            py_w, _ = managed_wage_dollars(session, entity_id, py_ps, py_pe)
            py_s = _sum_sales(session, entity_id, py_ps, py_pe)
            py_pct = _safe_div_pct(py_w, py_s)

            pn = c["period_number"]
            # "P06 Mar 1–14" — %-d removes zero-padding (Linux); Render is Linux
            label = (
                f"P{pn:02d} "
                + ps.strftime("%-d %b")
                + "–"
                + pe.strftime("%-d %b")
            )

            trend.append({
                "fy": fy,
                "period_number": pn,
                "label": label,
                "actual_wage_pct": str(actual_pct) if actual_pct is not None else None,
                "target_pct": str(fy_target_pct) if fy_target_pct is not None else None,
                "prior_year_pct": str(py_pct) if py_pct is not None else None,
                "actual_wages": str(wage),
                "actual_sales": str(sales),
                "basis": basis,
            })

    # ----- 10. Assemble response ---------------------------------------------
    return {
        "fiscal_year": fiscal_year,
        "settings_present": True,
        "ytd": {
            "start": fy_start.isoformat(),
            "end": ytd_end.isoformat(),
            "periods_completed": periods_completed,
            "periods_remaining": periods_remaining,
        },
        "card1_headline": {
            "ytd_managed_wage_pct": str(ytd_pct) if ytd_pct is not None else None,
            "target_wage_pct": str(twp),
            "prior_year_same_period_pct": str(prior_year_pct) if prior_year_pct is not None else None,
            "wage_basis": wage_basis,
            "prior_year_basis": py_wage_basis,
            "health": health,
        },
        "card2_forward_target": {
            "next_unlocked_period_number": next_period_number,
            "adjusted_target_hours": (
                str(next_adj_hours) if next_adj_hours is not None else None
            ),
            "cum_over_under": str(cum_ou) if cum_ou is not None else None,
            "color": card2_color,
        },
        "card3_ytd_actuals": {
            "actual_wages_ytd": str(actual_wages_ytd),
            "target_wages_ytd": str(target_wages_ytd),
            "wages_variance": str(wages_variance),
            "actual_sales_ytd": str(actual_sales_ytd),
            "forecast_sales_ytd": str(forecast_sales_ytd),
            "sales_variance": str(sales_variance),
            "wage_basis": wage_basis,
        },
        "card4_salaried": {
            "per_period": str(salaried_pp),
            "annual": str(salaried_annual),
            "pct_of_annual_target": str(salaried_pct) if salaried_pct is not None else None,
            "staff": staff_detail,
        },
        "card5_min_wage": {
            "ontario_min_wage": str(_ONTARIO_MIN_WAGE),
            "alert_band": str(_MIN_WAGE_ALERT_BAND),
            "near_min_employees": near_min_list,
            "affected_count": len(near_min_list),
            "total_delta_annual_est": str(total_delta_annual),
        },
        "trend": trend,
    }


# ---------------------------------------------------------------------------
# On-payroll-approval hook (called from services_payroll.py)
# ---------------------------------------------------------------------------

def on_payroll_run_finalized(
    session,
    *,
    entity_id: UUID,
    payroll_run_id: str,
    actor_email: str | None = None,
) -> None:
    """Called after a payroll run is approved (workflow_status = 'approved_to_post').

    1. Resolves the run's period_number and fiscal year.
    2. Backfills the pay-period calendar entry if missing.
    3. Calls refresh_period_actuals.
    4. Triggers Excel snapshot generation (best-effort, wraps errors).

    This function must NEVER raise — any failure is logged and swallowed so
    payroll approval is never blocked.
    """
    try:
        run = session.execute(
            text(
                """
                SELECT pr.id, pr.period_number, pr.period_start, pr.period_end, pr.pay_date,
                       e.entity_code,
                       COALESCE(es.fiscal_year_end_month, 9) AS fy_end_month,
                       COALESCE(es.fiscal_year_end_day, 30)  AS fy_end_day
                FROM payroll_runs pr
                JOIN entities e ON e.id = pr.entity_id
                LEFT JOIN entity_settings es ON es.entity_id = pr.entity_id
                WHERE pr.id = :run_id AND pr.entity_id = :eid
                """
            ),
            {"run_id": payroll_run_id, "eid": entity_id},
        ).mappings().first()

        if not run:
            _log.warning("wage_planner hook: run %s not found for entity %s", payroll_run_id, entity_id)
            return

        fy = _fiscal_year_for_date(run["period_start"], run["fy_end_month"], run["fy_end_day"])
        pn = run["period_number"]
        entity_code = run["entity_code"]

        # Ensure calendar entry exists
        existing_cal = session.execute(
            text(
                """
                SELECT id FROM payroll_pay_periods
                WHERE entity_id = :eid AND fiscal_year = :fy AND period_number = :pn
                """
            ),
            {"eid": entity_id, "fy": fy, "pn": pn},
        ).mappings().first()
        if not existing_cal:
            session.execute(
                text(
                    """
                    INSERT INTO payroll_pay_periods
                        (entity_id, fiscal_year, period_number, period_start,
                         period_end, pay_date, source)
                    VALUES (:eid, :fy, :pn, :ps, :pe, :pd, 'auto')
                    ON CONFLICT (entity_id, fiscal_year, period_number) DO NOTHING
                    """
                ),
                {
                    "eid": entity_id,
                    "fy": fy,
                    "pn": pn,
                    "ps": run["period_start"],
                    "pe": run["period_end"],
                    "pd": run["pay_date"],
                },
            )

        # Auto-forward: ensure period N+1 exists so the next run is never missing.
        # Guard pn < 26: period 26 rolls to the next calendar year's P01, which has
        # a different fiscal_year; that year's backfill covers it at onboarding.
        if pn < 26:
            next_ps = run["period_end"] + timedelta(days=1)
            next_pe = next_ps + timedelta(days=13)
            next_fy = _fiscal_year_for_date(next_ps, run["fy_end_month"], run["fy_end_day"])
            session.execute(
                text(
                    """
                    INSERT INTO payroll_pay_periods
                        (entity_id, fiscal_year, period_number, period_start,
                         period_end, source)
                    VALUES (:eid, :fy, :pn, :ps, :pe, 'auto')
                    ON CONFLICT (entity_id, fiscal_year, period_number) DO NOTHING
                    """
                ),
                {
                    "eid": entity_id,
                    "fy": next_fy,
                    "pn": pn + 1,
                    "ps": next_ps,
                    "pe": next_pe,
                },
            )
            _log.info(
                "wage_planner hook: ensured next calendar period FY%s P%s (%s - %s)",
                next_fy, pn + 1, next_ps, next_pe,
            )

        # Only refresh actuals if planner settings exist for this FY
        settings_row = get_settings(session, entity_id=entity_id, fiscal_year=fy)
        if not settings_row:
            _log.info(
                "wage_planner hook: no settings for entity %s FY%s — skip actuals refresh",
                entity_id, fy,
            )
            return

        refresh_period_actuals(
            session,
            entity_id=entity_id,
            fiscal_year=fy,
            period_number=pn,
            payroll_run_id=payroll_run_id,
            actor_email=actor_email,
        )

        # Excel snapshot — import lazily; must not block approval
        try:
            from .services_wage_planner_excel import generate_wage_planner_excel
            from .services_storage import content_type_for, storage_service
            import json as _json

            xlsx_bytes = generate_wage_planner_excel(
                session, entity_id=entity_id, fiscal_year=fy
            )
            filename = f"wage_planner_FY{fy}_p{pn:02d}_{entity_code}.xlsx"
            r2_key = storage_service.upload_file(
                file_bytes=xlsx_bytes,
                original_filename=filename,
                entity_code=entity_code,
                document_type="wage-planner",
                content_type=content_type_for(filename),
            )
            session.execute(
                text(
                    """
                    INSERT INTO wage_planner_snapshots
                        (entity_id, fiscal_year, pay_period_number,
                         r2_object_key, status, generated_at, generated_by)
                    VALUES (:eid, :fy, :pn, :key, 'ready', NOW(), :actor)
                    ON CONFLICT (entity_id, fiscal_year, pay_period_number) DO UPDATE
                        SET r2_object_key = EXCLUDED.r2_object_key,
                            status        = 'ready',
                            generated_at  = NOW(),
                            generated_by  = EXCLUDED.generated_by,
                            error_msg     = NULL,
                            updated_at    = NOW()
                    """
                ),
                {
                    "eid": entity_id,
                    "fy": fy,
                    "pn": pn,
                    "key": r2_key,
                    "actor": actor_email,
                },
            )
            _log.info(
                "wage_planner: snapshot archived FY%s p%s key=%s",
                fy, pn, r2_key,
            )
        except Exception as exc:
            _log.error(
                "wage_planner: Excel snapshot failed FY%s p%s: %r",
                fy, pn, exc,
            )
            try:
                session.execute(
                    text(
                        """
                        INSERT INTO wage_planner_snapshots
                            (entity_id, fiscal_year, pay_period_number,
                             status, error_msg, generated_at, generated_by)
                        VALUES (:eid, :fy, :pn, 'failed', :err, NOW(), :actor)
                        ON CONFLICT (entity_id, fiscal_year, pay_period_number) DO UPDATE
                            SET status     = 'failed',
                                error_msg  = EXCLUDED.error_msg,
                                updated_at = NOW()
                        """
                    ),
                    {
                        "eid": entity_id,
                        "fy": fy,
                        "pn": pn,
                        "err": str(exc)[:500],
                        "actor": actor_email,
                    },
                )
            except Exception:
                pass

    except Exception as exc:
        _log.error(
            "wage_planner hook: unhandled error for run %s entity %s: %r",
            payroll_run_id, entity_id, exc,
        )
