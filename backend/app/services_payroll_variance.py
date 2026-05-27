"""
Pre-approval variance analysis for payroll runs.

Compares the current draft run against the most recent approved/paid
run for the same entity and flags anything unusual. Surfaces in the
UI banner above the Approve button; 'block' severity stops the
approve endpoint until the variance is acknowledged.

Eight rules:
  gross_change       — gross pay differs by >20% from previous run
  hours_change       — total hours differ by >25%
  new_employee       — first time this employee appears in 3 runs
  missing_employee   — active employee paid last time but not this time
  cpp_max_reached    — YTD CPP ≥ annual max
  ei_max_reached     — YTD EI ≥ annual max
  zero_pay           — included with $0 gross
  large_bonus        — bonus > 50% of avg gross (placeholder until
                       payroll_run_lines has an explicit bonus column;
                       currently inferred from the gross_change rule)

Idempotent: re-running analyze drops prior non-acknowledged rows for
the run and re-inserts. Acknowledged rows are sticky — never deleted
by re-analysis.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .services_payroll_calc import (
    CPP_MAX_CONTRIB_ANNUAL,
    EI_MAX_CONTRIB_EE_ANNUAL,
)


# Tunable thresholds — at the top of the file so they're easy to
# review when calibrating against Bridlewood's real run data.
GROSS_CHANGE_WARN_PCT = Decimal("20")
GROSS_CHANGE_BLOCK_PCT = Decimal("50")
HOURS_CHANGE_WARN_PCT = Decimal("25")
LARGE_BONUS_PCT = Decimal("50")
HISTORY_LOOKBACK_RUNS = 3


@dataclass
class PayrollVariance:
    """In-memory shape used by analyze_run_variances + the route layer.
    Persisted to payroll_run_variances via INSERT in
    persist_variances()."""
    employee_id: str
    employee_name: str
    variance_type: str
    severity: str  # 'info' | 'warn' | 'block'
    message: str
    previous_value: Decimal | None = None
    current_value: Decimal | None = None
    change_pct: Decimal | None = None


def _previous_run(session, *, entity_id: Any, current_run_id: Any) -> dict[str, Any] | None:
    """The most recent payroll_runs row strictly before the current
    one, in approved/posted/paid state. Skips voided runs."""
    row = session.execute(
        text(
            """
            SELECT pr.id, pr.pay_run_number, pr.period_end
              FROM payroll_runs pr
             WHERE pr.entity_id = :eid
               AND pr.id <> :cur
               AND COALESCE(pr.workflow_status, pr.status) IN
                   ('approved', 'approved_to_post', 'posted', 'paid', 'eft_sent')
             ORDER BY pr.period_end DESC, pr.created_at DESC
             LIMIT 1
            """
        ),
        {"eid": entity_id, "cur": current_run_id},
    ).mappings().first()
    return dict(row) if row else None


def _recent_run_ids(session, *, entity_id: Any, before_run_id: Any, n: int) -> list[Any]:
    rows = session.execute(
        text(
            """
            SELECT id FROM payroll_runs
             WHERE entity_id = :eid
               AND id <> :cur
               AND COALESCE(workflow_status, status) IN
                   ('approved', 'approved_to_post', 'posted', 'paid', 'eft_sent')
             ORDER BY period_end DESC
             LIMIT :n
            """
        ),
        {"eid": entity_id, "cur": before_run_id, "n": n},
    ).mappings().all()
    return [r["id"] for r in rows]


def _pct_change(prev: Decimal, curr: Decimal) -> Decimal:
    if prev <= 0:
        return Decimal("0")
    return ((curr - prev) / prev * Decimal("100")).quantize(Decimal("0.01"))


def analyze_run_variances(
    session, *, payroll_run_id: str, entity_id: Any,
) -> list[PayrollVariance]:
    """Return the list of variances detected for the given run.
    Does not write to payroll_run_variances — persist_variances()
    does that. Splitting them lets the route layer dry-run."""
    # Pull the current run + lines + employee context.
    run = session.execute(
        text(
            """
            SELECT id, entity_id, pay_run_number, period_start, period_end,
                   pay_date, run_type, status, workflow_status
              FROM payroll_runs
             WHERE id = :rid AND entity_id = :eid
            """
        ),
        {"rid": payroll_run_id, "eid": entity_id},
    ).mappings().first()
    if not run:
        return []

    current_lines = session.execute(
        text(
            """
            SELECT prl.employee_id, prl.gross_pay, prl.total_hours,
                   pe.full_name, pe.is_active,
                   pe.ytd_cpp_employee, pe.ytd_ei_employee
              FROM payroll_run_lines prl
              JOIN payroll_employees pe ON pe.id = prl.employee_id
             WHERE prl.payroll_run_id = :rid
            """
        ),
        {"rid": payroll_run_id},
    ).mappings().all()
    current_by_emp: dict[Any, dict[str, Any]] = {
        l["employee_id"]: dict(l) for l in current_lines
    }

    prev_run = _previous_run(
        session, entity_id=entity_id, current_run_id=payroll_run_id
    )
    prev_by_emp: dict[Any, dict[str, Any]] = {}
    if prev_run:
        prev_lines = session.execute(
            text(
                """
                SELECT prl.employee_id, prl.gross_pay, prl.total_hours,
                       pe.full_name
                  FROM payroll_run_lines prl
                  JOIN payroll_employees pe ON pe.id = prl.employee_id
                 WHERE prl.payroll_run_id = :rid
                """
            ),
            {"rid": prev_run["id"]},
        ).mappings().all()
        prev_by_emp = {l["employee_id"]: dict(l) for l in prev_lines}

    # Set of employee_ids that appeared in any of the last N runs (for
    # the 'new_employee' rule).
    recent_run_ids = _recent_run_ids(
        session, entity_id=entity_id, before_run_id=payroll_run_id,
        n=HISTORY_LOOKBACK_RUNS,
    )
    seen_in_recent: set[Any] = set()
    if recent_run_ids:
        rows = session.execute(
            text(
                """
                SELECT DISTINCT employee_id
                  FROM payroll_run_lines
                 WHERE payroll_run_id = ANY(:ids)
                """
            ),
            {"ids": recent_run_ids},
        ).mappings().all()
        seen_in_recent = {r["employee_id"] for r in rows}

    # Run-type skip: corrections / bonus runs are expected to vary
    # heavily; skip the gross_change rule for those (caller intent
    # is already to apply an unusual amount).
    skip_gross = (run.get("run_type") or "regular") in ("bonus", "correction")

    out: list[PayrollVariance] = []

    # Walk every line in the current run.
    for emp_id, cur in current_by_emp.items():
        name = cur["full_name"]
        cur_gross = Decimal(str(cur["gross_pay"] or 0))
        cur_hours = Decimal(str(cur["total_hours"] or 0))

        # Zero pay
        if cur_gross == 0:
            out.append(PayrollVariance(
                employee_id=str(emp_id), employee_name=name,
                variance_type="zero_pay", severity="warn",
                message=f"{name}: included in run with $0 gross pay",
                current_value=Decimal("0"),
            ))

        # YTD CPP cap
        ytd_cpp = Decimal(str(cur["ytd_cpp_employee"] or 0))
        if ytd_cpp >= CPP_MAX_CONTRIB_ANNUAL:
            out.append(PayrollVariance(
                employee_id=str(emp_id), employee_name=name,
                variance_type="cpp_max_reached", severity="info",
                message=(
                    f"{name}: CPP annual maximum reached — no further "
                    "CPP deducted this fiscal year"
                ),
                current_value=ytd_cpp,
                previous_value=CPP_MAX_CONTRIB_ANNUAL,
            ))

        # YTD EI cap
        ytd_ei = Decimal(str(cur["ytd_ei_employee"] or 0))
        if ytd_ei >= EI_MAX_CONTRIB_EE_ANNUAL:
            out.append(PayrollVariance(
                employee_id=str(emp_id), employee_name=name,
                variance_type="ei_max_reached", severity="info",
                message=f"{name}: EI annual maximum reached",
                current_value=ytd_ei,
                previous_value=EI_MAX_CONTRIB_EE_ANNUAL,
            ))

        # New employee
        if emp_id not in seen_in_recent:
            out.append(PayrollVariance(
                employee_id=str(emp_id), employee_name=name,
                variance_type="new_employee", severity="info",
                message=f"{name}: first pay run on record",
                current_value=cur_gross,
            ))

        if not skip_gross and emp_id in prev_by_emp:
            prev = prev_by_emp[emp_id]
            prev_gross = Decimal(str(prev["gross_pay"] or 0))
            prev_hours = Decimal(str(prev["total_hours"] or 0))

            if prev_gross > 0:
                pct = _pct_change(prev_gross, cur_gross)
                abs_pct = abs(pct)
                if abs_pct > GROSS_CHANGE_BLOCK_PCT:
                    out.append(PayrollVariance(
                        employee_id=str(emp_id), employee_name=name,
                        variance_type="gross_change", severity="block",
                        message=(
                            f"{name}: gross pay ${cur_gross} differs by "
                            f"{pct}% from last period (${prev_gross}). "
                            "Acknowledge before approving."
                        ),
                        previous_value=prev_gross,
                        current_value=cur_gross,
                        change_pct=pct,
                    ))
                elif abs_pct > GROSS_CHANGE_WARN_PCT:
                    out.append(PayrollVariance(
                        employee_id=str(emp_id), employee_name=name,
                        variance_type="gross_change", severity="warn",
                        message=(
                            f"{name}: gross pay ${cur_gross} is {pct}% "
                            f"different from last period (${prev_gross})"
                        ),
                        previous_value=prev_gross,
                        current_value=cur_gross,
                        change_pct=pct,
                    ))

            if prev_hours > 0:
                pct_h = _pct_change(prev_hours, cur_hours)
                if abs(pct_h) > HOURS_CHANGE_WARN_PCT:
                    out.append(PayrollVariance(
                        employee_id=str(emp_id), employee_name=name,
                        variance_type="hours_change", severity="warn",
                        message=(
                            f"{name}: {cur_hours}h this period vs "
                            f"{prev_hours}h last period ({pct_h}%)"
                        ),
                        previous_value=prev_hours,
                        current_value=cur_hours,
                        change_pct=pct_h,
                    ))

    # Missing employees: paid last run, still active, not in this run
    if prev_by_emp:
        cur_ids = set(current_by_emp.keys())
        # Active flag for missing-employee detection
        missing_candidates = [
            emp_id for emp_id, prev in prev_by_emp.items()
            if emp_id not in cur_ids and Decimal(str(prev["gross_pay"] or 0)) > 0
        ]
        if missing_candidates:
            active_rows = session.execute(
                text(
                    """
                    SELECT id, full_name FROM payroll_employees
                     WHERE id = ANY(:ids) AND entity_id = :eid AND is_active = TRUE
                    """
                ),
                {"ids": missing_candidates, "eid": entity_id},
            ).mappings().all()
            for r in active_rows:
                out.append(PayrollVariance(
                    employee_id=str(r["id"]), employee_name=r["full_name"],
                    variance_type="missing_employee", severity="warn",
                    message=(
                        f"{r['full_name']}: was paid last period but not "
                        "included this period"
                    ),
                ))

    return out


def persist_variances(
    session, *, payroll_run_id: str, entity_id: Any,
    variances: list[PayrollVariance],
) -> int:
    """Replace non-acknowledged variances for the run; preserve any
    that have been acknowledged. Returns the count written."""
    session.execute(
        text(
            """
            DELETE FROM payroll_run_variances
             WHERE payroll_run_id = :rid AND acknowledged = FALSE
            """
        ),
        {"rid": payroll_run_id},
    )
    if not variances:
        return 0
    for v in variances:
        session.execute(
            text(
                """
                INSERT INTO payroll_run_variances (
                    entity_id, payroll_run_id, employee_id, variance_type,
                    severity, previous_value, current_value, change_pct,
                    message
                ) VALUES (
                    :eid, :rid, :emp, :vt, :sev, :prev, :cur, :pct, :msg
                )
                """
            ),
            {
                "eid": entity_id, "rid": payroll_run_id,
                "emp": v.employee_id, "vt": v.variance_type,
                "sev": v.severity, "prev": v.previous_value,
                "cur": v.current_value, "pct": v.change_pct,
                "msg": v.message,
            },
        )
    return len(variances)


def has_unacknowledged_blocks(session, *, payroll_run_id: str) -> bool:
    row = session.execute(
        text(
            """
            SELECT COUNT(*) AS n
              FROM payroll_run_variances
             WHERE payroll_run_id = :rid
               AND severity = 'block'
               AND acknowledged = FALSE
            """
        ),
        {"rid": payroll_run_id},
    ).mappings().first()
    return bool(row and int(row["n"]) > 0)
