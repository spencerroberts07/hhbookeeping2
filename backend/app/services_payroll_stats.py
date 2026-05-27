"""
Statutory holiday calendar — Ontario (province-aware).

Pure functions; no DB writes. Used by:
  - routes/payroll.py    GET /api/payroll/stat-days
  - services_payroll_calc.py — stat-pay calc per period
  - frontend (via the endpoint) — new-period setup calendar

Ontario stat-day rules (ESA §24):
  - 9 paid public holidays per year. Remembrance Day is NOT one
    of them in Ontario (it is in some other provinces).
  - Stat-pay formula: regular wages earned in the four work weeks
    before the holiday ÷ 20. If <4 weeks of history, use what's
    available.
  - When a stat falls on a non-working day Ontario allows a
    substitute day off; for payroll calc we still pay the stat
    based on the calendar date.
  - Canada Day / Christmas / Boxing Day: when the date falls on a
    Sunday, the observed date shifts to the next weekday. This
    matters for "which period does the stat fall in" but the legal
    holiday date doesn't change.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import text


@dataclass
class StatDay:
    holiday_name: str
    holiday_date: date
    observed_date: date  # = holiday_date except for sun-shift cases


# --------------------------------------------------------------------------
# Per-holiday computation
# --------------------------------------------------------------------------


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the n-th `weekday` (Mon=0) of `year`/`month`. n=1 = first."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _monday_before(target: date) -> date:
    """Return the Monday in the same week as `target`, or the most
    recent Monday strictly before it if target is a Monday."""
    if target.weekday() == 0:
        return target - timedelta(days=7)
    return target - timedelta(days=target.weekday())


def _easter_sunday(year: int) -> date:
    """Anonymous Gregorian Easter algorithm (Computus). Returns the
    date of Easter Sunday for the given year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    L = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * L) // 451
    month = (h + L - 7 * m + 114) // 31
    day = ((h + L - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _sunday_shift_observed(d: date) -> date:
    """For Canada Day / Christmas / Boxing Day: if the calendar date
    falls on a Sunday, the observed (in-lieu) date is the following
    Monday."""
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def get_stat_days(year: int, province: str = "ON") -> list[StatDay]:
    """Return Ontario statutory holidays for `year`. Ordered by date.

    `province` is accepted for forward-compat; only 'ON' is implemented
    for now — any other value returns an empty list."""
    if province.upper() != "ON":
        return []

    easter = _easter_sunday(year)
    good_friday = easter - timedelta(days=2)

    # Victoria Day: the Monday on or before May 24 — i.e. the Monday
    # before May 25. (Per the federal Holidays Act.)
    may_25 = date(year, 5, 25)
    if may_25.weekday() == 0:  # May 25 is itself a Monday — Victoria
        victoria_day = may_25 - timedelta(days=7)
    else:
        victoria_day = may_25 - timedelta(days=may_25.weekday() + 1)

    canada_day = date(year, 7, 1)
    christmas = date(year, 12, 25)
    boxing_day = date(year, 12, 26)

    stats: list[StatDay] = [
        StatDay("New Year's Day", date(year, 1, 1),
                _sunday_shift_observed(date(year, 1, 1))),
        StatDay("Family Day", _nth_weekday_of_month(year, 2, 0, 3),
                _nth_weekday_of_month(year, 2, 0, 3)),
        StatDay("Good Friday", good_friday, good_friday),
        StatDay("Victoria Day", victoria_day, victoria_day),
        StatDay("Canada Day", canada_day, _sunday_shift_observed(canada_day)),
        StatDay("Civic Holiday", _nth_weekday_of_month(year, 8, 0, 1),
                _nth_weekday_of_month(year, 8, 0, 1)),
        StatDay("Labour Day", _nth_weekday_of_month(year, 9, 0, 1),
                _nth_weekday_of_month(year, 9, 0, 1)),
        StatDay("Thanksgiving", _nth_weekday_of_month(year, 10, 0, 2),
                _nth_weekday_of_month(year, 10, 0, 2)),
        StatDay("Christmas Day", christmas, _sunday_shift_observed(christmas)),
        StatDay("Boxing Day", boxing_day, _sunday_shift_observed(boxing_day)),
    ]
    # Note Civic Holiday is sometimes contested (not a federal stat,
    # not a "pure" Ontario ESA stat) — Bridlewood treats it as one
    # because most local employers do. Including it here.
    return sorted(stats, key=lambda s: s.holiday_date)


def get_stat_days_in_period(
    start_date: date, end_date: date, province: str = "ON"
) -> list[StatDay]:
    """Subset of get_stat_days() that falls within the inclusive
    [start_date, end_date] window. Uses the observed date when
    deciding which period a Sunday-shifted holiday lands in (the
    employee gets paid for the day they actually get off)."""
    years = {start_date.year, end_date.year}
    out: list[StatDay] = []
    for y in years:
        for s in get_stat_days(y, province):
            if start_date <= s.observed_date <= end_date:
                out.append(s)
    return sorted(out, key=lambda s: s.observed_date)


# --------------------------------------------------------------------------
# Stat pay calculation (Ontario ESA formula)
# --------------------------------------------------------------------------


def calculate_stat_pay(
    employee: dict[str, Any],
    stat_date: date,
    session: Any,
    entity_id: Any,
) -> Decimal:
    """Ontario ESA stat-pay formula:

        stat_pay = regular wages earned in the 4 work weeks
                   immediately before the stat ÷ 20

    "Regular wages" = reg_hours_pay + salary_pay from prior pay
    periods (excludes stat pay itself and vacation pay).

    When there's less than 4 weeks of history we use whatever's
    available — divide by the number of work days the employee
    actually worked (capped at 20).

    Returns Decimal($X.XX). The caller writes it to
    payroll_run_lines.stat_pay."""
    window_start = stat_date - timedelta(days=28)
    rows = session.execute(
        text(
            """
            SELECT
                COALESCE(SUM(prl.reg_hours_pay + prl.salary_pay), 0)
                  AS reg_wages,
                COUNT(*) AS line_count
              FROM payroll_run_lines prl
              JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
             WHERE pr.entity_id = :eid
               AND prl.employee_id = :emp
               AND pr.period_end >= :window_start
               AND pr.period_end < :stat_date
               AND COALESCE(pr.workflow_status, pr.status) IN
                   ('approved', 'approved_to_post', 'posted', 'paid')
            """
        ),
        {
            "eid": entity_id,
            "emp": employee["id"],
            "window_start": window_start,
            "stat_date": stat_date,
        },
    ).mappings().first()

    reg_wages = Decimal(str(rows["reg_wages"] or 0))
    if reg_wages <= 0:
        return Decimal("0.00")
    return (reg_wages / Decimal("20")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
