"""
Canadian payroll tax calculation engine — 2026 rates, ON province.

Implements a simplified periodic-method approximation of the CRA Payroll
Deductions Tables (T4032/T4127). The formula:

    annualize biweekly gross  -> compute annual federal + provincial tax
    using bracket method, subtract TD1 personal-amount credit, divide
    by pay periods per year (26 biweekly).

This is NOT a bit-for-bit clone of CRA's PDOC — it does not implement
K1/K2/K3 factors, surtax stacking nuances, or the YTD-based proration
that PDOC uses. Expect $1-$10 of variance per employee per period vs.
the actual CRA tables. For Bridlewood the simplified method is good
enough to drive the journal; the bookkeeper can override fed_tax on
the run line if a specific employee's actual differs materially.

CPP and EI are exact — those are flat-rate calculations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


# ----------------------------------------------------------------------
# 2026 rates (hardcode for now; will move to config table when we
# need multi-year support)
# ----------------------------------------------------------------------

CPP_RATE_EE = Decimal("0.0595")
CPP_RATE_ER = Decimal("0.0595")
CPP_EXEMPTION_ANNUAL = Decimal("3500.00")
CPP_MAX_EARNINGS_ANNUAL = Decimal("71300.00")
CPP_MAX_CONTRIB_ANNUAL = (
    (CPP_MAX_EARNINGS_ANNUAL - CPP_EXEMPTION_ANNUAL) * CPP_RATE_EE
).quantize(Decimal("0.01"))  # = 4,034.10

EI_RATE_EE = Decimal("0.01657")
EI_RATE_ER_MULTIPLIER = Decimal("1.4")
EI_MAX_INSURABLE_ANNUAL = Decimal("65700.00")
EI_MAX_CONTRIB_EE_ANNUAL = (
    EI_MAX_INSURABLE_ANNUAL * EI_RATE_EE
).quantize(Decimal("0.01"))  # = 1,088.65

VACATION_RATE_DEFAULT = Decimal("0.04")
BIWEEKLY_PERIODS = 26

# Federal 2026 brackets (annual)
FEDERAL_BPA_2026 = Decimal("16129.00")
FEDERAL_BRACKETS = [
    (Decimal("57375.00"), Decimal("0.15")),
    (Decimal("114750.00"), Decimal("0.205")),
    (Decimal("158519.00"), Decimal("0.26")),
    (Decimal("220000.00"), Decimal("0.29")),
    (None, Decimal("0.33")),
]

# Ontario 2026 brackets (annual)
ON_BPA_2026 = Decimal("11865.00")
ON_BRACKETS = [
    (Decimal("51446.00"), Decimal("0.0505")),
    (Decimal("102894.00"), Decimal("0.0915")),
    (Decimal("150000.00"), Decimal("0.1116")),
    (Decimal("220000.00"), Decimal("0.1216")),
    (None, Decimal("0.1316")),
]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _apply_brackets(
    annual_taxable: Decimal, brackets: list[tuple[Decimal | None, Decimal]]
) -> Decimal:
    if annual_taxable <= 0:
        return Decimal("0.00")
    tax = Decimal("0.00")
    prior_ceiling = Decimal("0.00")
    remaining = annual_taxable
    for ceiling, rate in brackets:
        if ceiling is None:
            tax += remaining * rate
            return tax
        bracket_width = ceiling - prior_ceiling
        if remaining <= bracket_width:
            tax += remaining * rate
            return tax
        tax += bracket_width * rate
        remaining -= bracket_width
        prior_ceiling = ceiling
    return tax


def _claim_amount(
    bpa: Decimal, claim_code: int
) -> Decimal:
    """
    The TD1 form's claim code maps a personal-amount credit. Code 1 =
    Basic personal amount only. Higher codes add spouse/dependant/age
    amounts. Without the per-code table we approximate: code 1 = BPA,
    code N = BPA * N (very rough but reasonable for the bookkeeper's
    use case where most employees are code 1).
    """
    if claim_code <= 1:
        return bpa
    return bpa * Decimal(str(claim_code))


# ----------------------------------------------------------------------
# CPP
# ----------------------------------------------------------------------


def calculate_cpp(
    gross_pay: Decimal,
    *,
    ytd_cpp_ee: Decimal | None = None,
    province: str = "ON",
    cpp_exempt: bool = False,
    pay_periods: int = BIWEEKLY_PERIODS,
) -> dict[str, Decimal]:
    """
    Per-period CPP. Each period grants a per-employee exemption
    (annual_exemption / pay_periods). Pensionable = max(0, gross -
    period_exemption). CPP_EE = pensionable * 0.0595, capped at the
    year's remaining contribution allowance.
    """
    gross = _money(gross_pay)
    ytd = _money(ytd_cpp_ee or 0)

    if cpp_exempt or gross <= 0:
        return {"cpp_ee": Decimal("0.00"), "cpp_er": Decimal("0.00")}

    period_exemption = (CPP_EXEMPTION_ANNUAL / Decimal(pay_periods)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    pensionable = max(Decimal("0.00"), gross - period_exemption)
    cpp_ee = (pensionable * CPP_RATE_EE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    remaining_cap = CPP_MAX_CONTRIB_ANNUAL - ytd
    if remaining_cap < 0:
        remaining_cap = Decimal("0.00")
    if cpp_ee > remaining_cap:
        cpp_ee = remaining_cap

    cpp_er = (cpp_ee * (CPP_RATE_ER / CPP_RATE_EE)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    return {"cpp_ee": cpp_ee, "cpp_er": cpp_er}


# ----------------------------------------------------------------------
# EI
# ----------------------------------------------------------------------


def calculate_ei(
    gross_pay: Decimal,
    *,
    ytd_ei_ee: Decimal | None = None,
    ei_exempt: bool = False,
) -> dict[str, Decimal]:
    """
    Per-period EI. EI = gross * 0.01657 (no per-period exemption).
    Capped at annual max contribution. Employer = 1.4 × EE.
    """
    gross = _money(gross_pay)
    ytd = _money(ytd_ei_ee or 0)

    if ei_exempt or gross <= 0:
        return {"ei_ee": Decimal("0.00"), "ei_er": Decimal("0.00")}

    ei_ee = (gross * EI_RATE_EE).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    remaining_cap = EI_MAX_CONTRIB_EE_ANNUAL - ytd
    if remaining_cap < 0:
        remaining_cap = Decimal("0.00")
    if ei_ee > remaining_cap:
        ei_ee = remaining_cap

    ei_er = (ei_ee * EI_RATE_ER_MULTIPLIER).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    return {"ei_ee": ei_ee, "ei_er": ei_er}


# ----------------------------------------------------------------------
# Federal + provincial tax
# ----------------------------------------------------------------------


def calculate_federal_tax(
    taxable_gross: Decimal,
    *,
    federal_td1_claim_code: int = 1,
    provincial_td1_claim_code: int = 1,
    pay_periods: int = BIWEEKLY_PERIODS,
    province: str = "ON",
) -> dict[str, Decimal]:
    """
    Combined federal + provincial tax for a biweekly period.

    Annualize the biweekly taxable_gross by multiplying by pay_periods,
    apply the bracket method to compute annual federal and provincial
    tax, subtract the TD1 personal-amount credit valued at the lowest-
    bracket rate (CRA convention), then divide by pay_periods.

    Returns a dict with fed_only and provincial_only for transparency,
    plus the combined fed_tax that the journal posts.
    """
    biweekly = _money(taxable_gross)
    if biweekly <= 0:
        return {
            "fed_tax": Decimal("0.00"),
            "federal_only": Decimal("0.00"),
            "provincial_only": Decimal("0.00"),
        }

    annual_taxable = biweekly * Decimal(pay_periods)

    # Federal
    fed_gross_tax = _apply_brackets(annual_taxable, FEDERAL_BRACKETS)
    fed_credit = _claim_amount(FEDERAL_BPA_2026, federal_td1_claim_code) * Decimal("0.15")
    fed_annual = max(Decimal("0.00"), fed_gross_tax - fed_credit)
    fed_periodic = (fed_annual / Decimal(pay_periods)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )

    # Provincial (Ontario only for Bridlewood)
    if province.upper() == "ON":
        prov_gross_tax = _apply_brackets(annual_taxable, ON_BRACKETS)
        prov_credit = (
            _claim_amount(ON_BPA_2026, provincial_td1_claim_code) * Decimal("0.0505")
        )
        prov_annual = max(Decimal("0.00"), prov_gross_tax - prov_credit)
        prov_periodic = (prov_annual / Decimal(pay_periods)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    else:
        prov_periodic = Decimal("0.00")

    return {
        "fed_tax": (fed_periodic + prov_periodic).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        ),
        "federal_only": fed_periodic,
        "provincial_only": prov_periodic,
    }


# ----------------------------------------------------------------------
# Vacation + stat pay
# ----------------------------------------------------------------------


def calculate_vacation_earned(
    gross_pay: Decimal, vacation_rate: Decimal | None = None
) -> Decimal:
    rate = (
        Decimal(str(vacation_rate))
        if vacation_rate is not None
        else VACATION_RATE_DEFAULT
    )
    return (_money(gross_pay) * rate).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def calculate_stat_pay(
    employee: dict[str, Any] | None,
    period_start: date | None,
    period_end: date | None,
    manual_override: Decimal | None = None,
) -> dict[str, Any]:
    """
    Stat pay placeholder. The proper Ontario formula is "wages in 4
    weeks before holiday / 20", which requires prior-period gross
    history. For now we accept a manual_override per employee. Future
    work: pull prior 4 weeks from payroll_run_lines and auto-compute.
    """
    return {
        "stat_pay": _money(manual_override or 0),
        "holiday_names": [],
        "auto_calculated": False,
        "note": (
            "Stat pay requires prior 4 weeks of gross history; "
            "supply via stat_pay_overrides until that lookup is wired."
        ),
    }


# ----------------------------------------------------------------------
# Per-employee orchestration
# ----------------------------------------------------------------------


@dataclass
class PayrollLineResult:
    employee_id: str
    employment_type: str
    week1_hours: Decimal = Decimal("0.00")
    week2_hours: Decimal = Decimal("0.00")
    total_hours: Decimal = Decimal("0.00")
    hourly_rate: Decimal | None = None
    reg_hours_pay: Decimal = Decimal("0.00")
    overtime_pay: Decimal = Decimal("0.00")
    salary_pay: Decimal = Decimal("0.00")
    stat_pay: Decimal = Decimal("0.00")
    vacation_paid: Decimal = Decimal("0.00")
    gross_pay: Decimal = Decimal("0.00")
    taxable_gross: Decimal = Decimal("0.00")
    fed_tax: Decimal = Decimal("0.00")
    cpp_ee: Decimal = Decimal("0.00")
    cpp_er: Decimal = Decimal("0.00")
    ei_ee: Decimal = Decimal("0.00")
    ei_er: Decimal = Decimal("0.00")
    life_taxable_benefit: Decimal = Decimal("0.00")
    vacation_earned: Decimal = Decimal("0.00")
    net_pay: Decimal = Decimal("0.00")
    is_on_vacation: bool = False
    notes: str | None = None
    warnings: list[str] = field(default_factory=list)


def calculate_employee_payroll(
    employee: dict[str, Any],
    *,
    week1_hours: Decimal | float | int | None = None,
    week2_hours: Decimal | float | int | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    ytd_cpp_ee: Decimal | None = None,
    ytd_ei_ee: Decimal | None = None,
    vacation_paid_override: Decimal | None = None,
    stat_pay_override: Decimal | None = None,
    is_on_vacation: bool = False,
    pay_periods: int = BIWEEKLY_PERIODS,
) -> PayrollLineResult:
    """
    Compute a single employee's biweekly payroll.

    employee is a dict with keys: id, employment_type, hourly_rate,
    biweekly_salary, vacation_rate, federal_td1_claim_code,
    provincial_td1_claim_code, cpp_exempt, ei_exempt,
    has_life_insurance, life_insurance_biweekly, province.
    """
    line = PayrollLineResult(
        employee_id=str(employee["id"]),
        employment_type=str(employee.get("employment_type") or "hourly"),
        is_on_vacation=is_on_vacation,
    )
    line.week1_hours = _money(week1_hours)
    line.week2_hours = _money(week2_hours)
    line.total_hours = line.week1_hours + line.week2_hours

    hourly_rate = employee.get("hourly_rate")
    line.hourly_rate = (
        Decimal(str(hourly_rate)) if hourly_rate is not None else None
    )
    biweekly_salary = employee.get("biweekly_salary")
    employment_type = (employee.get("employment_type") or "").lower()
    is_salary = employment_type == "salary"

    # ------------------------------------------------------------------
    # Gross composition
    # ------------------------------------------------------------------
    if is_on_vacation:
        # Hours represent vacation hours paid out from the bank, NOT
        # regular hours. Vacation_paid uses the employee's hourly rate.
        rate = line.hourly_rate or Decimal("0.00")
        line.vacation_paid = (line.total_hours * rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        if vacation_paid_override is not None:
            line.vacation_paid = _money(vacation_paid_override)
        line.reg_hours_pay = Decimal("0.00")
        line.salary_pay = Decimal("0.00")
    elif is_salary:
        line.salary_pay = _money(biweekly_salary)
        line.reg_hours_pay = Decimal("0.00")
    else:
        if line.hourly_rate is None:
            line.warnings.append(
                f"Hourly rate not set for employee — gross will be zero. "
                "Set rate via /api/payroll/employees/upsert."
            )
            line.reg_hours_pay = Decimal("0.00")
        else:
            line.reg_hours_pay = (line.total_hours * line.hourly_rate).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

    line.stat_pay = _money(stat_pay_override or 0)

    line.gross_pay = (
        line.reg_hours_pay
        + line.overtime_pay
        + line.salary_pay
        + line.stat_pay
        + line.vacation_paid
    )

    # Taxable benefit for group life insurance (added to taxable gross
    # but not to net cash). Bridlewood treats Cynthia's life premium
    # this way per the Feb register.
    if employee.get("has_life_insurance"):
        line.life_taxable_benefit = _money(employee.get("life_insurance_biweekly") or 0)

    line.taxable_gross = line.gross_pay + line.life_taxable_benefit

    # ------------------------------------------------------------------
    # Statutory deductions
    # ------------------------------------------------------------------
    cpp = calculate_cpp(
        line.taxable_gross,
        ytd_cpp_ee=ytd_cpp_ee,
        province=str(employee.get("province") or "ON"),
        cpp_exempt=bool(employee.get("cpp_exempt")),
        pay_periods=pay_periods,
    )
    line.cpp_ee = cpp["cpp_ee"]
    line.cpp_er = cpp["cpp_er"]

    ei = calculate_ei(
        line.taxable_gross,
        ytd_ei_ee=ytd_ei_ee,
        ei_exempt=bool(employee.get("ei_exempt")),
    )
    line.ei_ee = ei["ei_ee"]
    line.ei_er = ei["ei_er"]

    tax = calculate_federal_tax(
        line.taxable_gross,
        federal_td1_claim_code=int(employee.get("federal_td1_claim_code") or 1),
        provincial_td1_claim_code=int(
            employee.get("provincial_td1_claim_code") or 1
        ),
        pay_periods=pay_periods,
        province=str(employee.get("province") or "ON"),
    )
    line.fed_tax = tax["fed_tax"]

    # ------------------------------------------------------------------
    # Vacation accrual + net pay
    # ------------------------------------------------------------------
    vacation_rate = employee.get("vacation_rate")
    line.vacation_earned = calculate_vacation_earned(
        line.gross_pay - line.vacation_paid,
        vacation_rate=vacation_rate,
    )

    line.net_pay = (
        line.gross_pay - line.fed_tax - line.cpp_ee - line.ei_ee
    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return line
