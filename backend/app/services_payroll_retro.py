"""
Retroactive pay calculator + off-cycle correction-run builder.

Two public surfaces:

    calculate_retro_pay(...)            preview only, no DB writes.
                                        Walks approved runs since
                                        effective_date and computes
                                        the per-period rate delta.

    create_correction_run(...)          builds a payroll_runs row +
                                        payroll_run_lines for an
                                        off-cycle correction / bonus
                                        / retroactive payment.

Tax handling — non-periodic (bonus) method:
    The CRA bonus method withholds at the employee's projected
    marginal rate for the year. We approximate by annualizing
    (current YTD gross + bonus) against the bracket schedule, then
    diff against tax that would otherwise have been due. This is
    an estimate — see the TODO_CRA_2026_RATES block in
    services_payroll_calc.py. For Bridlewood the estimate is in
    the right ballpark; bookkeeper can override per-line.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from .services_payroll_calc import (
    BIWEEKLY_PERIODS,
    CPP_RATE_EE,
    EI_RATE_EE,
    _apply_brackets,
    FEDERAL_BRACKETS,
    ON_BRACKETS,
    FEDERAL_BPA_2026,
    ON_BPA_2026,
)


@dataclass
class RetroPeriod:
    payroll_run_id: str
    period_start: date
    period_end: date
    pay_date: date
    hours: Decimal
    old_gross: Decimal
    new_gross: Decimal
    delta: Decimal


@dataclass
class RetroCalculation:
    employee_id: str
    employee_name: str
    old_rate: Decimal
    new_rate: Decimal
    effective_date: date
    periods: list[RetroPeriod] = field(default_factory=list)
    retro_amount_gross: Decimal = Decimal("0.00")
    estimated_cpp: Decimal = Decimal("0.00")
    estimated_ei: Decimal = Decimal("0.00")
    estimated_fed_tax: Decimal = Decimal("0.00")
    estimated_net: Decimal = Decimal("0.00")
    note: str = ""


def _q(v: Any) -> Decimal:
    return Decimal(str(v or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _bonus_method_tax(
    annual_taxable_before: Decimal, bonus: Decimal
) -> Decimal:
    """Federal+ON bonus-method withholding on a one-off payment.

    CRA's method: compute annual tax on (taxable_before + bonus),
    subtract annual tax on (taxable_before), the diff is the
    withholding on the bonus. Uses our existing bracket helpers."""
    if bonus <= 0:
        return Decimal("0.00")
    tax_with = (
        _apply_brackets(annual_taxable_before + bonus, FEDERAL_BRACKETS)
        - FEDERAL_BPA_2026 * Decimal("0.15")
        + _apply_brackets(annual_taxable_before + bonus, ON_BRACKETS)
        - ON_BPA_2026 * Decimal("0.0505")
    )
    tax_without = (
        _apply_brackets(annual_taxable_before, FEDERAL_BRACKETS)
        - FEDERAL_BPA_2026 * Decimal("0.15")
        + _apply_brackets(annual_taxable_before, ON_BRACKETS)
        - ON_BPA_2026 * Decimal("0.0505")
    )
    delta = max(Decimal("0.00"), tax_with - tax_without)
    return delta.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calculate_retro_pay(
    session,
    *,
    entity_id: Any,
    employee_id: str,
    old_rate: Decimal,
    new_rate: Decimal,
    effective_date: date,
) -> RetroCalculation:
    """Compute the gross + estimated net retro owed since
    effective_date. Walks every approved/posted/paid run with
    period_end >= effective_date for this employee."""
    emp = session.execute(
        text(
            """
            SELECT id, full_name, ytd_gross, cpp_exempt, ei_exempt
              FROM payroll_employees
             WHERE id = :id AND entity_id = :eid
            """
        ),
        {"id": employee_id, "eid": entity_id},
    ).mappings().first()
    if not emp:
        raise ValueError("employee not found for this entity")

    rows = session.execute(
        text(
            """
            SELECT pr.id AS run_id, pr.period_start, pr.period_end,
                   pr.pay_date, prl.total_hours
              FROM payroll_run_lines prl
              JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
             WHERE pr.entity_id = :eid
               AND prl.employee_id = :emp
               AND pr.period_end >= :eff
               AND COALESCE(pr.workflow_status, pr.status) IN
                   ('approved', 'approved_to_post', 'posted', 'paid', 'eft_sent')
             ORDER BY pr.period_end ASC
            """
        ),
        {"eid": entity_id, "emp": employee_id, "eff": effective_date},
    ).mappings().all()

    rate_delta = (Decimal(str(new_rate)) - Decimal(str(old_rate)))
    calc = RetroCalculation(
        employee_id=str(employee_id),
        employee_name=emp["full_name"],
        old_rate=Decimal(str(old_rate)),
        new_rate=Decimal(str(new_rate)),
        effective_date=effective_date,
    )

    for r in rows:
        hours = Decimal(str(r["total_hours"] or 0))
        if hours <= 0:
            continue
        old_gross = (hours * Decimal(str(old_rate))).quantize(Decimal("0.01"))
        new_gross = (hours * Decimal(str(new_rate))).quantize(Decimal("0.01"))
        delta = (rate_delta * hours).quantize(Decimal("0.01"))
        calc.periods.append(RetroPeriod(
            payroll_run_id=str(r["run_id"]),
            period_start=r["period_start"],
            period_end=r["period_end"],
            pay_date=r["pay_date"],
            hours=hours,
            old_gross=old_gross,
            new_gross=new_gross,
            delta=delta,
        ))
        calc.retro_amount_gross += delta

    calc.retro_amount_gross = _q(calc.retro_amount_gross)

    if calc.retro_amount_gross <= 0:
        calc.note = (
            "No retro owed — either no approved runs since the effective "
            "date or no hours worked."
        )
        return calc

    # Bonus-method estimate
    bonus = calc.retro_amount_gross
    cpp = Decimal("0.00") if emp["cpp_exempt"] else (bonus * CPP_RATE_EE).quantize(Decimal("0.01"))
    ei = Decimal("0.00") if emp["ei_exempt"] else (bonus * EI_RATE_EE).quantize(Decimal("0.01"))
    ytd_gross = Decimal(str(emp["ytd_gross"] or 0))
    annualized_before = ytd_gross + (
        (ytd_gross / Decimal(BIWEEKLY_PERIODS)) * Decimal(BIWEEKLY_PERIODS)
        if BIWEEKLY_PERIODS > 0 else Decimal("0.00")
    )
    # Simpler/correct: annualize current pace then add the bonus
    if ytd_gross > 0:
        annualized_before = ytd_gross  # YTD already reflects YTD income
    fed_tax = _bonus_method_tax(annualized_before, bonus)

    calc.estimated_cpp = cpp
    calc.estimated_ei = ei
    calc.estimated_fed_tax = fed_tax
    calc.estimated_net = (bonus - cpp - ei - fed_tax).quantize(Decimal("0.01"))
    calc.note = (
        "Tax estimated using CRA bonus method (federal + ON combined). "
        "Actual withholding may differ by a few dollars."
    )
    return calc


def _resolve_accounting_period(
    session, *, entity_id: Any, pay_date: date
) -> Any:
    """Return the accounting_periods.id whose window contains pay_date
    and is not closed_locked. Raises ValueError on miss."""
    row = session.execute(
        text(
            """
            SELECT id, status FROM accounting_periods
             WHERE entity_id = :eid
               AND period_start <= :pd AND period_end >= :pd
             ORDER BY status, period_start
             LIMIT 1
            """
        ),
        {"eid": entity_id, "pd": pay_date},
    ).mappings().first()
    if not row:
        raise ValueError(
            f"No accounting period found for pay date {pay_date}. "
            "Please create or open a period before creating this run."
        )
    if row["status"] == "closed_locked":
        raise ValueError(
            f"The accounting period containing pay date {pay_date} is "
            "closed_locked. Reopen it or pick a different pay_date."
        )
    return row["id"]


def create_correction_run(
    session,
    *,
    entity_id: Any,
    entity_code: str,
    run_type: str,
    description: str,
    period_start: date,
    period_end: date,
    pay_date: date,
    employees: list[dict[str, Any]],
    parent_run_id: str | None,
    actor_email: str,
) -> dict[str, Any]:
    """Build a draft payroll_runs + payroll_run_lines for an off-cycle
    correction, bonus, or retroactive payment.

    `run_type` ∈ {'correction', 'bonus', 'retroactive', 'offcycle'}.
    `employees` is a list of dicts; each must have employee_id and
    one of: override_gross (correction/bonus), or
    retro_old_rate/retro_new_rate/hours_per_period/retro_periods
    (retroactive).
    """
    if run_type not in ("correction", "bonus", "retroactive", "offcycle"):
        raise ValueError(f"invalid run_type: {run_type!r}")
    if not description:
        raise ValueError("description is required for off-cycle runs")
    if not employees:
        raise ValueError("at least one employee row is required")

    accounting_period_id = _resolve_accounting_period(
        session, entity_id=entity_id, pay_date=pay_date
    )

    # pay_run_number: e.g. CORR-2026-05-27-001
    seq_row = session.execute(
        text(
            """
            SELECT COUNT(*) AS n FROM payroll_runs
             WHERE entity_id = :eid
               AND run_type = :rt
               AND DATE(created_at) = CURRENT_DATE
            """
        ),
        {"eid": entity_id, "rt": run_type},
    ).mappings().first()
    seq = int((seq_row or {}).get("n", 0) or 0) + 1
    pay_run_number = (
        f"{run_type.upper()[:4]}-{pay_date.strftime('%Y-%m-%d')}-{seq:03d}"
    )

    # Insert the run header
    run_row = session.execute(
        text(
            """
            INSERT INTO payroll_runs (
                entity_id, accounting_period_id, pay_run_number,
                period_number, period_start, period_end, pay_date,
                pay_type, status, workflow_status,
                run_type, run_description, parent_run_id,
                active_employees, actor_email
            ) VALUES (
                :eid, :apid, :prn, 0, :ps, :pe, :pd,
                :pt, 'draft', 'draft',
                :rt, :desc, :parent, :ae, :actor
            )
            RETURNING id
            """
        ),
        {
            "eid": entity_id,
            "apid": accounting_period_id,
            "prn": pay_run_number,
            "ps": period_start, "pe": period_end, "pd": pay_date,
            "pt": "Bonus" if run_type == "bonus" else "Special",
            "rt": run_type, "desc": description, "parent": parent_run_id,
            "ae": len(employees), "actor": actor_email,
        },
    ).mappings().first()
    run_id = run_row["id"]

    total_gross = Decimal("0.00")
    total_cpp_ee = Decimal("0.00")
    total_ei_ee = Decimal("0.00")
    total_fed_tax = Decimal("0.00")
    total_net = Decimal("0.00")
    paid_count = 0
    line_results: list[dict[str, Any]] = []

    for spec in employees:
        emp_id = spec.get("employee_id")
        if not emp_id:
            raise ValueError("each employee row needs employee_id")

        emp = session.execute(
            text(
                """
                SELECT id, full_name, hourly_rate, biweekly_salary,
                       vacation_rate, ytd_gross, cpp_exempt, ei_exempt
                  FROM payroll_employees
                 WHERE id = :id AND entity_id = :eid
                """
            ),
            {"id": emp_id, "eid": entity_id},
        ).mappings().first()
        if not emp:
            raise ValueError(
                f"employee {emp_id} not found for this entity (cross-entity refused)"
            )

        # Determine gross for this employee
        if run_type == "retroactive":
            old_rate = Decimal(str(spec.get("retro_old_rate") or 0))
            new_rate = Decimal(str(spec.get("retro_new_rate") or 0))
            hours_per = Decimal(str(spec.get("hours_per_period") or 0))
            periods = int(spec.get("retro_periods") or 0)
            if old_rate <= 0 or new_rate <= 0 or hours_per <= 0 or periods <= 0:
                raise ValueError(
                    "retroactive row requires retro_old_rate, retro_new_rate, "
                    "hours_per_period, retro_periods (all > 0)"
                )
            gross = ((new_rate - old_rate) * hours_per * Decimal(periods)).quantize(Decimal("0.01"))
            total_hours = (hours_per * Decimal(periods)).quantize(Decimal("0.01"))
        else:
            gross = Decimal(str(spec.get("override_gross") or 0)).quantize(Decimal("0.01"))
            total_hours = Decimal("0.00")
            if gross <= 0:
                raise ValueError(
                    f"correction/bonus row for {emp['full_name']} needs override_gross > 0"
                )

        # Bonus-method withholding
        cpp = Decimal("0.00") if emp["cpp_exempt"] else (gross * CPP_RATE_EE).quantize(Decimal("0.01"))
        ei = Decimal("0.00") if emp["ei_exempt"] else (gross * EI_RATE_EE).quantize(Decimal("0.01"))
        annualized = Decimal(str(emp["ytd_gross"] or 0))
        fed_tax = _bonus_method_tax(annualized, gross)
        net = (gross - cpp - ei - fed_tax).quantize(Decimal("0.01"))
        vacation_earned = (
            gross * Decimal(str(emp["vacation_rate"] or "0.04"))
        ).quantize(Decimal("0.01"))

        # Insert line — taxable_gross == gross for these one-off
        # payments (no benefit-in-kind on corrections).
        line = session.execute(
            text(
                """
                INSERT INTO payroll_run_lines (
                    payroll_run_id, employee_id, employment_type,
                    total_hours, hourly_rate, reg_hours_pay, gross_pay,
                    taxable_gross, fed_tax, cpp_ee, cpp_er, ei_ee, ei_er,
                    vacation_earned, net_pay, notes
                ) VALUES (
                    :rid, :emp, :et,
                    :hrs, :rate, :reg, :gross,
                    :tg, :fed, :cpp, :cpp_er, :ei, :ei_er,
                    :vac, :net, :note
                )
                RETURNING id
                """
            ),
            {
                "rid": run_id, "emp": emp_id,
                "et": "hourly" if total_hours > 0 else "salary",
                "hrs": total_hours,
                "rate": new_rate if run_type == "retroactive" else None,
                "reg": gross if run_type == "retroactive" else 0,
                "gross": gross,
                "tg": gross,
                "fed": fed_tax,
                "cpp": cpp, "cpp_er": cpp,  # employer matches
                "ei": ei, "ei_er": (ei * Decimal("1.4")).quantize(Decimal("0.01")),
                "vac": vacation_earned,
                "net": net,
                "note": f"{run_type}: {description[:200]}",
            },
        ).mappings().first()

        total_gross += gross
        total_cpp_ee += cpp
        total_ei_ee += ei
        total_fed_tax += fed_tax
        total_net += net
        paid_count += 1
        line_results.append({
            "line_id": str(line["id"]),
            "employee_id": str(emp_id),
            "employee_name": emp["full_name"],
            "gross_pay": float(gross),
            "net_pay": float(net),
            "fed_tax": float(fed_tax),
            "cpp_ee": float(cpp),
            "ei_ee": float(ei),
        })

    # Update run totals
    session.execute(
        text(
            """
            UPDATE payroll_runs
               SET total_gross = :g, total_net_pay = :n,
                   total_fed_tax = :ft, total_cpp_ee = :ce,
                   total_ei_ee = :ee, paid_employees = :pc,
                   cra_remittance_amount = :cra,
                   updated_at = NOW()
             WHERE id = :id
            """
        ),
        {
            "g": _q(total_gross), "n": _q(total_net),
            "ft": _q(total_fed_tax), "ce": _q(total_cpp_ee),
            "ee": _q(total_ei_ee), "pc": paid_count,
            "cra": _q(total_fed_tax + total_cpp_ee * 2 + total_ei_ee * Decimal("2.4")),
            "id": run_id,
        },
    )

    return {
        "ok": True,
        "payroll_run_id": str(run_id),
        "pay_run_number": pay_run_number,
        "run_type": run_type,
        "accounting_period_id": str(accounting_period_id),
        "lines": line_results,
        "total_gross": float(_q(total_gross)),
        "total_net": float(_q(total_net)),
    }
