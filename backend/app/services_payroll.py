"""
Payroll module — services layer.

Pipeline:
    1. seed_employees(entity_code) — one-time seed of payroll_employees
       from Bridlewood's Feb 2026 register.
    2. parse_hours_ods(file_bytes) — parse the bookkeeper's ODS hours
       sheet into per-employee biweekly hours.
    3. build_payroll_run(...) — match ODS rows to employees, run
       calculate_employee_payroll for each, persist payroll_runs +
       payroll_run_lines.
    4. build_payroll_journal(payroll_run_id, ...) — produce the
       Dr 6120/6130/2220 + Cr 2300/1020 journal_batch.
    5. submit/approve through the existing journal_batch_workflow
       endpoints; bank withdrawals tracked in payroll_bank_withdrawals.
"""
from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
    get_or_create_accounting_period,
)
from .services_payroll_calc import (
    BIWEEKLY_PERIODS,
    PayrollLineResult,
    calculate_employee_payroll,
)


SOURCE_MODULE_PAYROLL = "payroll"
BATCH_LABEL_PAYROLL = "biweekly_payroll"

ACCOUNT_WAGES = "6120"
ACCOUNT_GROUP_INSURANCE = "6130"
ACCOUNT_VACATION_PAYABLE = "2220"
ACCOUNT_BANK = "1020"
# 2320 CRA Payroll Remittances Payable — added 2026-05-04 to separate
# payroll deductions (FED TAX + CPP + EI) from 2300 HST Payable. Note:
# user spec asked for 2310 but that code was already in use in
# Bridlewood's GL for "Income Tax Payable" (corporate income tax), so
# we use 2320 instead.
ACCOUNT_CRA_PAYABLE = "2320"


def _money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------------
# Seed — Bridlewood employees
# ----------------------------------------------------------------------


_BRIDLEWOOD_EMPLOYEE_SEED: list[dict[str, Any]] = [
    # Salaried
    {
        "employee_number": 5,
        "first_name": "Cynthia",
        "last_name": "Zullo",
        "employment_type": "salary",
        "biweekly_salary": Decimal("1923.08"),  # 50,000/yr
        "has_life_insurance": True,
        "life_insurance_biweekly": Decimal("16.93"),
        "ods_name_key": "Cynthia Zullo",
    },
    # Full-time hourly (rates NULL until Spencer confirms)
    {
        "employee_number": 25,
        "first_name": "Noah",
        "last_name": "Abraham",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Noah Abraham",
    },
    {
        "employee_number": 15,
        "first_name": "Elisabeth",
        "last_name": "Boult",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Elisabeth Boult",
    },
    {
        "employee_number": 20,
        "first_name": "Bill",
        "last_name": "Bramfield",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Bill Bramfield",
    },
    {
        "employee_number": 50,
        "first_name": "Neal",
        "last_name": "Freitag",
        "employment_type": "hourly_fulltime",
        "is_active": False,
        "ods_name_key": "Neal Freitag",
        "notes": "No Feb 2026 hours; flagged inactive at seed time.",
    },
    {
        "employee_number": 16,
        "first_name": "Alan",
        "last_name": "Moore",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Alan Moore",
    },
    {
        "employee_number": 40,
        "first_name": "Janice",
        "last_name": "Vaux",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Janice Vaux",
    },
    {
        "employee_number": 30,
        "first_name": "Vitold",
        "last_name": "Wisniewski",
        "employment_type": "hourly_fulltime",
        "ods_name_key": "Vito Wisniewski",  # ODS uses short form
    },
    # Part-time hourly
    {
        "employee_number": 14,
        "first_name": "Gail",
        "last_name": "Crocker",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Gail Crocker",
    },
    {
        "employee_number": 33,
        "first_name": "Leo",
        "last_name": "Buklin",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Leo Buklin",
    },
    {
        "employee_number": 45,
        "first_name": "Kiana",
        "last_name": "Ivanova",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Kiana Ivanova",
    },
    {
        "employee_number": 31,
        "first_name": "Samantha",
        "last_name": "Kreissl",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Samantha Kreissl",
    },
    {
        "employee_number": 22,
        "first_name": "Aven",
        "last_name": "Macdonald",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Aven Macdonald",
    },
    {
        "employee_number": 21,
        "first_name": "Eduard-Andres",
        "last_name": "Misca",
        "employment_type": "hourly_parttime",
        "ods_name_key": "Andy Misca",
    },
    {
        "employee_number": 43,
        "first_name": "Jonathan-Michael",
        "last_name": "Power",
        "employment_type": "hourly_parttime",
        "ods_name_key": "micky Power",
    },
]


def seed_employees(
    session, *, entity_code: str, actor_email: str
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    inserted = 0
    skipped = 0
    rows_out: list[dict[str, Any]] = []
    for cfg in _BRIDLEWOOD_EMPLOYEE_SEED:
        existing = session.execute(
            text(
                """
                SELECT id FROM payroll_employees
                WHERE entity_id = :entity_id
                  AND employee_number = :employee_number
                """
            ),
            {
                "entity_id": entity["id"],
                "employee_number": cfg["employee_number"],
            },
        ).mappings().first()
        if existing:
            skipped += 1
            rows_out.append(
                {
                    "employee_number": cfg["employee_number"],
                    "status": "exists",
                    "id": str(existing["id"]),
                }
            )
            continue
        full_name = f"{cfg['first_name']} {cfg['last_name']}"
        inserted_row = session.execute(
            text(
                """
                INSERT INTO payroll_employees (
                    entity_id, employee_number, first_name, last_name,
                    full_name, employment_type, hourly_rate, biweekly_salary,
                    vacation_rate, has_life_insurance, life_insurance_biweekly,
                    is_active, ods_name_key, notes
                ) VALUES (
                    :entity_id, :employee_number, :first_name, :last_name,
                    :full_name, :employment_type, :hourly_rate, :biweekly_salary,
                    0.0400, :has_life_insurance, :life_insurance_biweekly,
                    :is_active, :ods_name_key, :notes
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "employee_number": cfg["employee_number"],
                "first_name": cfg["first_name"],
                "last_name": cfg["last_name"],
                "full_name": full_name,
                "employment_type": cfg["employment_type"],
                "hourly_rate": cfg.get("hourly_rate"),
                "biweekly_salary": cfg.get("biweekly_salary"),
                "has_life_insurance": bool(cfg.get("has_life_insurance", False)),
                "life_insurance_biweekly": cfg.get("life_insurance_biweekly", Decimal("0")),
                "is_active": bool(cfg.get("is_active", True)),
                "ods_name_key": cfg.get("ods_name_key"),
                "notes": cfg.get("notes"),
            },
        ).mappings().first()
        inserted += 1
        rows_out.append(
            {
                "employee_number": cfg["employee_number"],
                "id": str(inserted_row["id"]),
                "full_name": full_name,
                "status": "inserted",
            }
        )

    return {
        "entity_code": entity_code,
        "inserted": inserted,
        "skipped": skipped,
        "employees": rows_out,
    }


def list_employees(session, *, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    rows = session.execute(
        text(
            """
            SELECT id, employee_number, full_name, employment_type,
                   hourly_rate, biweekly_salary, vacation_rate,
                   has_life_insurance, life_insurance_biweekly,
                   is_active, ods_name_key, notes
            FROM payroll_employees
            WHERE entity_id = :entity_id
            ORDER BY is_active DESC, full_name
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()
    return {
        "entity_code": entity_code,
        "count": len(rows),
        "employees": [
            {
                "id": str(r["id"]),
                "employee_number": r["employee_number"],
                "full_name": r["full_name"],
                "employment_type": r["employment_type"],
                "hourly_rate": str(r["hourly_rate"]) if r["hourly_rate"] is not None else None,
                "biweekly_salary": (
                    str(r["biweekly_salary"]) if r["biweekly_salary"] is not None else None
                ),
                "vacation_rate": str(r["vacation_rate"]),
                "has_life_insurance": r["has_life_insurance"],
                "life_insurance_biweekly": str(r["life_insurance_biweekly"]),
                "is_active": r["is_active"],
                "ods_name_key": r["ods_name_key"],
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


def upsert_employee(
    session,
    *,
    entity_code: str,
    employee_number: int,
    data: dict[str, Any],
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    first_name = (data.get("first_name") or "").strip()
    last_name = (data.get("last_name") or "").strip()
    if not first_name or not last_name:
        existing = session.execute(
            text(
                "SELECT first_name, last_name FROM payroll_employees "
                "WHERE entity_id=:e AND employee_number=:n"
            ),
            {"e": entity["id"], "n": employee_number},
        ).mappings().first()
        if existing:
            first_name = first_name or existing["first_name"]
            last_name = last_name or existing["last_name"]
        else:
            raise ValueError("first_name and last_name are required for a new employee")
    full_name = f"{first_name} {last_name}"

    row = session.execute(
        text(
            """
            INSERT INTO payroll_employees (
                entity_id, employee_number, first_name, last_name, full_name,
                employment_type, hourly_rate, biweekly_salary, vacation_rate,
                has_life_insurance, life_insurance_biweekly,
                is_active, ods_name_key, notes,
                bank_transit, bank_institution, bank_account
            ) VALUES (
                :entity_id, :employee_number, :first_name, :last_name, :full_name,
                :employment_type, :hourly_rate, :biweekly_salary,
                COALESCE(:vacation_rate, 0.0400),
                COALESCE(:has_life_insurance, FALSE),
                COALESCE(:life_insurance_biweekly, 0),
                COALESCE(:is_active, TRUE),
                :ods_name_key, :notes,
                :bank_transit, :bank_institution, :bank_account
            )
            ON CONFLICT (entity_id, employee_number) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                full_name = EXCLUDED.full_name,
                employment_type = COALESCE(EXCLUDED.employment_type, payroll_employees.employment_type),
                hourly_rate = COALESCE(EXCLUDED.hourly_rate, payroll_employees.hourly_rate),
                biweekly_salary = COALESCE(EXCLUDED.biweekly_salary, payroll_employees.biweekly_salary),
                vacation_rate = COALESCE(EXCLUDED.vacation_rate, payroll_employees.vacation_rate),
                has_life_insurance = COALESCE(EXCLUDED.has_life_insurance, payroll_employees.has_life_insurance),
                life_insurance_biweekly = COALESCE(EXCLUDED.life_insurance_biweekly, payroll_employees.life_insurance_biweekly),
                is_active = COALESCE(EXCLUDED.is_active, payroll_employees.is_active),
                ods_name_key = COALESCE(EXCLUDED.ods_name_key, payroll_employees.ods_name_key),
                notes = COALESCE(EXCLUDED.notes, payroll_employees.notes),
                bank_transit = COALESCE(EXCLUDED.bank_transit, payroll_employees.bank_transit),
                bank_institution = COALESCE(EXCLUDED.bank_institution, payroll_employees.bank_institution),
                bank_account = COALESCE(EXCLUDED.bank_account, payroll_employees.bank_account),
                updated_at = NOW()
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "employee_number": employee_number,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": full_name,
            "employment_type": data.get("employment_type"),
            "hourly_rate": data.get("hourly_rate"),
            "biweekly_salary": data.get("biweekly_salary"),
            "vacation_rate": data.get("vacation_rate"),
            "has_life_insurance": data.get("has_life_insurance"),
            "life_insurance_biweekly": data.get("life_insurance_biweekly"),
            "is_active": data.get("is_active"),
            "ods_name_key": data.get("ods_name_key"),
            "notes": data.get("notes"),
            "bank_transit": data.get("bank_transit"),
            "bank_institution": data.get("bank_institution"),
            "bank_account": data.get("bank_account"),
        },
    ).mappings().first()
    return {"id": str(row["id"]), "employee_number": employee_number}


# ----------------------------------------------------------------------
# ODS parser
# ----------------------------------------------------------------------


@dataclass
class ParsedHoursRow:
    name: str
    week1_hours: Decimal | None
    week2_hours: Decimal | None
    total_hours: Decimal | None
    is_on_vacation: bool
    is_salary_reg: bool


@dataclass
class ParsedHoursResult:
    period_end: date | None
    rows: list[ParsedHoursRow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    raw_period_label: str | None = None


_SKIP_LABELS = frozenset(
    {"", "EMPLOYEE", "FULL TIME", "PART TIME", "TOTALS"}
)


def _ods_cell_text(cell) -> str:
    from odf.text import P  # noqa: WPS433

    parts: list[str] = []
    for p in cell.getElementsByType(P):
        def walk(node):
            if node.nodeType == 3:  # TEXT_NODE
                parts.append(node.data)
            for child in node.childNodes:
                walk(child)

        walk(p)
    return "".join(parts)


def _to_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if not s:
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


def _parse_ods_period_end(value: str | None) -> date | None:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_hours_ods(file_bytes: bytes) -> ParsedHoursResult:
    from odf.opendocument import load  # noqa: WPS433
    from odf.table import Table, TableRow, TableCell  # noqa: WPS433

    doc = load(io.BytesIO(file_bytes))
    sheets = doc.spreadsheet.getElementsByType(Table)
    if not sheets:
        return ParsedHoursResult(period_end=None, warnings=["No sheets in ODS"])

    sheet = sheets[0]
    rows = sheet.getElementsByType(TableRow)

    raw_rows: list[list[str]] = []
    for r in rows:
        cells = r.getElementsByType(TableCell)
        row_vals: list[str] = []
        for c in cells:
            repeat = int(c.getAttribute("numbercolumnsrepeated") or 1)
            txt = _ods_cell_text(c)
            for _ in range(repeat):
                row_vals.append(txt)
        # Trim trailing empties
        while row_vals and row_vals[-1] == "":
            row_vals.pop()
        raw_rows.append(row_vals)

    result = ParsedHoursResult(period_end=None)

    # Header — period ending date typically in row 1, col 3
    if len(raw_rows) > 1 and len(raw_rows[1]) >= 4:
        result.raw_period_label = raw_rows[1][3] if raw_rows[1][3] else None
        result.period_end = _parse_ods_period_end(result.raw_period_label)

    if result.period_end is None:
        result.warnings.append(
            "Could not parse period_end from ODS row 1 col 3 — "
            "supply period_end via the upload form."
        )

    # Walk data rows
    for raw in raw_rows:
        if not raw:
            continue
        first = (raw[0] or "").strip()
        if first.upper() in _SKIP_LABELS:
            continue
        if first.startswith("`") or first == "`":
            continue

        week1_raw = raw[1].strip() if len(raw) > 1 else ""
        week2_raw = raw[2].strip() if len(raw) > 2 else ""
        total_raw = raw[3].strip() if len(raw) > 3 else ""

        is_on_vacation = week1_raw.lower() == "vacation"
        is_salary_reg = week1_raw.upper() == "REG"

        if is_salary_reg:
            row = ParsedHoursRow(
                name=first,
                week1_hours=None,
                week2_hours=None,
                total_hours=None,
                is_on_vacation=False,
                is_salary_reg=True,
            )
        elif is_on_vacation:
            # Vacation row: week2 holds the hours paid out
            hours = _to_decimal(week2_raw) or _to_decimal(total_raw)
            row = ParsedHoursRow(
                name=first,
                week1_hours=Decimal("0.00"),
                week2_hours=hours,
                total_hours=hours,
                is_on_vacation=True,
                is_salary_reg=False,
            )
        else:
            w1 = _to_decimal(week1_raw)
            w2 = _to_decimal(week2_raw)
            total = _to_decimal(total_raw)
            if w1 is None and w2 is None and total is None:
                # Not a data row
                continue
            row = ParsedHoursRow(
                name=first,
                week1_hours=w1 or Decimal("0.00"),
                week2_hours=w2 or Decimal("0.00"),
                total_hours=total
                or ((w1 or Decimal("0.00")) + (w2 or Decimal("0.00"))),
                is_on_vacation=False,
                is_salary_reg=False,
            )
        result.rows.append(row)

    return result


# ----------------------------------------------------------------------
# Build payroll run
# ----------------------------------------------------------------------


def _match_employees(
    session, *, entity_id: UUID, parsed_rows: list[ParsedHoursRow]
) -> tuple[list[tuple[ParsedHoursRow, dict[str, Any]]], list[str]]:
    matched: list[tuple[ParsedHoursRow, dict[str, Any]]] = []
    unmatched: list[str] = []
    if not parsed_rows:
        return matched, unmatched

    employees = session.execute(
        text(
            """
            SELECT id, employee_number, full_name, employment_type, hourly_rate,
                   biweekly_salary, vacation_rate, federal_td1_claim_code,
                   provincial_td1_claim_code, cpp_exempt, ei_exempt,
                   has_life_insurance, life_insurance_biweekly, province,
                   is_active, ods_name_key
            FROM payroll_employees
            WHERE entity_id = :entity_id
            """
        ),
        {"entity_id": entity_id},
    ).mappings().all()

    by_key: dict[str, dict[str, Any]] = {}
    for emp in employees:
        key = (emp["ods_name_key"] or emp["full_name"] or "").strip().lower()
        if key:
            by_key[key] = dict(emp)

    for parsed in parsed_rows:
        key = parsed.name.strip().lower()
        emp = by_key.get(key)
        if emp is None:
            unmatched.append(parsed.name)
            continue
        matched.append((parsed, emp))

    return matched, unmatched


def build_payroll_run_from_manual_hours(
    session,
    *,
    entity_code: str,
    pay_run_number: str,
    period_number: int,
    period_start: date,
    period_end: date,
    pay_date: date,
    hours: list[dict[str, Any]],
    stat_pay_overrides: dict[str, str | float | int | Decimal] | None = None,
    vacation_paid_overrides: dict[str, str | float | int | Decimal] | None = None,
    actor_email: str,
) -> dict[str, Any]:
    """
    Build a payroll run from a JSON hours array instead of an ODS file.
    Each row in `hours` must reference an existing employee by either
    employee_number (preferred) or employee_id, and supply EITHER
    total_hours OR (week1_hours + week2_hours), OR is_salary_reg=True
    for salaried employees, OR is_on_vacation=True with hours paid out.
    """
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    # Resolve each input row to a ParsedHoursRow keyed by employee name
    # so we can reuse the same downstream pipeline.
    if not hours:
        raise ValueError("hours array is required and must be non-empty")

    employees_by_number: dict[int, dict[str, Any]] = {}
    employees_by_id: dict[str, dict[str, Any]] = {}
    for emp in session.execute(
        text(
            "SELECT id, employee_number, full_name, ods_name_key "
            "FROM payroll_employees WHERE entity_id = :e"
        ),
        {"e": entity["id"]},
    ).mappings():
        if emp["employee_number"] is not None:
            employees_by_number[emp["employee_number"]] = dict(emp)
        employees_by_id[str(emp["id"])] = dict(emp)

    parsed_rows: list[ParsedHoursRow] = []
    unresolved: list[dict[str, Any]] = []
    for row in hours:
        emp = None
        if row.get("employee_number") is not None:
            emp = employees_by_number.get(int(row["employee_number"]))
        if emp is None and row.get("employee_id"):
            emp = employees_by_id.get(str(row["employee_id"]))
        if emp is None:
            unresolved.append(row)
            continue
        is_salary_reg = bool(row.get("is_salary_reg"))
        is_on_vacation = bool(row.get("is_on_vacation"))
        if is_salary_reg:
            parsed_rows.append(
                ParsedHoursRow(
                    name=emp["ods_name_key"] or emp["full_name"],
                    week1_hours=None,
                    week2_hours=None,
                    total_hours=None,
                    is_on_vacation=False,
                    is_salary_reg=True,
                )
            )
            continue
        total = _to_decimal(row.get("total_hours"))
        w1 = _to_decimal(row.get("week1_hours"))
        w2 = _to_decimal(row.get("week2_hours"))
        if total is None and w1 is None and w2 is None:
            unresolved.append(row)
            continue
        if total is None:
            total = (w1 or Decimal("0")) + (w2 or Decimal("0"))
        if w1 is None and w2 is None:
            half = (total / Decimal("2")).quantize(Decimal("0.01"))
            w1 = half
            w2 = total - half
        parsed_rows.append(
            ParsedHoursRow(
                name=emp["ods_name_key"] or emp["full_name"],
                week1_hours=w1 or Decimal("0"),
                week2_hours=w2 or Decimal("0"),
                total_hours=total,
                is_on_vacation=is_on_vacation,
                is_salary_reg=False,
            )
        )

    if unresolved:
        raise ValueError(
            "Could not resolve these hours rows (need employee_number or "
            f"employee_id, plus hours/total/salary): {unresolved}"
        )

    parsed = ParsedHoursResult(
        period_end=period_end,
        rows=parsed_rows,
        warnings=[],
        raw_period_label="manual",
    )

    return _build_payroll_run_from_parsed(
        session,
        entity=dict(entity),
        parsed=parsed,
        file_name="manual_hours",
        pay_run_number=pay_run_number,
        period_number=period_number,
        period_start=period_start,
        period_end=period_end,
        pay_date=pay_date,
        stat_pay_overrides=stat_pay_overrides,
        vacation_paid_overrides=vacation_paid_overrides,
        actor_email=actor_email,
    )


def build_payroll_run(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    pay_run_number: str,
    period_number: int,
    period_start: date,
    period_end: date,
    pay_date: date,
    stat_pay_overrides: dict[str, str | float | int | Decimal] | None = None,
    vacation_paid_overrides: dict[str, str | float | int | Decimal] | None = None,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    parsed = parse_hours_ods(file_bytes)
    return _build_payroll_run_from_parsed(
        session,
        entity=dict(entity),
        parsed=parsed,
        file_name=file_name,
        pay_run_number=pay_run_number,
        period_number=period_number,
        period_start=period_start,
        period_end=period_end,
        pay_date=pay_date,
        stat_pay_overrides=stat_pay_overrides,
        vacation_paid_overrides=vacation_paid_overrides,
        actor_email=actor_email,
    )


def _build_payroll_run_from_parsed(
    session,
    *,
    entity: dict[str, Any],
    parsed: ParsedHoursResult,
    file_name: str,
    pay_run_number: str,
    period_number: int,
    period_start: date,
    period_end: date,
    pay_date: date,
    stat_pay_overrides: dict[str, str | float | int | Decimal] | None = None,
    vacation_paid_overrides: dict[str, str | float | int | Decimal] | None = None,
    actor_email: str,
) -> dict[str, Any]:
    matched, unmatched = _match_employees(
        session, entity_id=entity["id"], parsed_rows=parsed.rows
    )
    if unmatched:
        raise ValueError(
            "Could not match these ODS names to payroll_employees "
            "(set ods_name_key on the matching employee row): "
            f"{unmatched}"
        )

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], period_end
    )

    stat_overrides = {
        str(k): _money(v) for k, v in (stat_pay_overrides or {}).items()
    }
    vac_paid_overrides = {
        str(k): _money(v) for k, v in (vacation_paid_overrides or {}).items()
    }

    # ------------------------------------------------------------------
    # Calculate per-employee
    # ------------------------------------------------------------------
    line_results: list[tuple[ParsedHoursRow, dict[str, Any], PayrollLineResult]] = []
    warnings: list[str] = list(parsed.warnings)

    for parsed_row, emp in matched:
        if not emp["is_active"]:
            warnings.append(
                f"Skipping inactive employee {emp['full_name']}; remove from "
                "ODS or mark them active to include."
            )
            continue
        result = calculate_employee_payroll(
            emp,
            week1_hours=parsed_row.week1_hours or Decimal("0"),
            week2_hours=parsed_row.week2_hours or Decimal("0"),
            period_start=period_start,
            period_end=period_end,
            stat_pay_override=stat_overrides.get(str(emp["id"])),
            vacation_paid_override=vac_paid_overrides.get(str(emp["id"])),
            is_on_vacation=parsed_row.is_on_vacation,
            pay_periods=BIWEEKLY_PERIODS,
        )
        warnings.extend(result.warnings)
        line_results.append((parsed_row, emp, result))

    # ------------------------------------------------------------------
    # Totals
    # ------------------------------------------------------------------
    totals = {
        "gross": Decimal("0.00"),
        "net_pay": Decimal("0.00"),
        "fed_tax": Decimal("0.00"),
        "cpp_ee": Decimal("0.00"),
        "cpp_er": Decimal("0.00"),
        "ei_ee": Decimal("0.00"),
        "ei_er": Decimal("0.00"),
        "life_taxable": Decimal("0.00"),
        "vacation_earned": Decimal("0.00"),
        "vacation_paid": Decimal("0.00"),
        "stat_pay": Decimal("0.00"),
        "reg_hours_pay": Decimal("0.00"),
        "salary_pay": Decimal("0.00"),
    }
    paid_count = 0
    for _, _, r in line_results:
        if r.gross_pay > 0 or r.net_pay > 0:
            paid_count += 1
        totals["gross"] += r.gross_pay
        totals["net_pay"] += r.net_pay
        totals["fed_tax"] += r.fed_tax
        totals["cpp_ee"] += r.cpp_ee
        totals["cpp_er"] += r.cpp_er
        totals["ei_ee"] += r.ei_ee
        totals["ei_er"] += r.ei_er
        totals["life_taxable"] += r.life_taxable_benefit
        totals["vacation_earned"] += r.vacation_earned
        totals["vacation_paid"] += r.vacation_paid
        totals["stat_pay"] += r.stat_pay
        totals["reg_hours_pay"] += r.reg_hours_pay
        totals["salary_pay"] += r.salary_pay

    cra_remittance = (
        totals["fed_tax"]
        + totals["cpp_ee"]
        + totals["cpp_er"]
        + totals["ei_ee"]
        + totals["ei_er"]
    )

    # ------------------------------------------------------------------
    # Persist run + lines
    # ------------------------------------------------------------------
    summary = {
        "pay_run_number": pay_run_number,
        "period_number": period_number,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "pay_date": pay_date.isoformat(),
        "active_employees": len(line_results),
        "paid_employees": paid_count,
        "totals": {k: str(v) for k, v in totals.items()},
        "cra_remittance_amount": str(cra_remittance),
        "warnings": warnings,
        "hours_import_file": file_name,
        "ods_period_label": parsed.raw_period_label,
    }

    run_row = session.execute(
        text(
            """
            INSERT INTO payroll_runs (
                entity_id, accounting_period_id, pay_run_number, period_number,
                period_start, period_end, pay_date, pay_type,
                status, workflow_status,
                active_employees, paid_employees,
                total_gross, total_net_pay, total_fed_tax,
                total_cpp_ee, total_cpp_er, total_ei_ee, total_ei_er,
                total_life_taxable, total_vacation_earned, total_vacation_paid,
                total_stat_pay, cra_remittance_amount,
                hours_import_file, summary_json, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id, :pay_run_number, :period_number,
                :period_start, :period_end, :pay_date, 'Normal',
                'calculated', 'draft_ready',
                :active_employees, :paid_employees,
                :total_gross, :total_net_pay, :total_fed_tax,
                :total_cpp_ee, :total_cpp_er, :total_ei_ee, :total_ei_er,
                :total_life_taxable, :total_vacation_earned, :total_vacation_paid,
                :total_stat_pay, :cra_remittance_amount,
                :hours_import_file, CAST(:summary_json AS jsonb), :actor_email
            )
            ON CONFLICT (entity_id, pay_run_number) DO UPDATE SET
                accounting_period_id = EXCLUDED.accounting_period_id,
                period_number = EXCLUDED.period_number,
                period_start = EXCLUDED.period_start,
                period_end = EXCLUDED.period_end,
                pay_date = EXCLUDED.pay_date,
                status = 'calculated',
                workflow_status = 'draft_ready',
                active_employees = EXCLUDED.active_employees,
                paid_employees = EXCLUDED.paid_employees,
                total_gross = EXCLUDED.total_gross,
                total_net_pay = EXCLUDED.total_net_pay,
                total_fed_tax = EXCLUDED.total_fed_tax,
                total_cpp_ee = EXCLUDED.total_cpp_ee,
                total_cpp_er = EXCLUDED.total_cpp_er,
                total_ei_ee = EXCLUDED.total_ei_ee,
                total_ei_er = EXCLUDED.total_ei_er,
                total_life_taxable = EXCLUDED.total_life_taxable,
                total_vacation_earned = EXCLUDED.total_vacation_earned,
                total_vacation_paid = EXCLUDED.total_vacation_paid,
                total_stat_pay = EXCLUDED.total_stat_pay,
                cra_remittance_amount = EXCLUDED.cra_remittance_amount,
                hours_import_file = EXCLUDED.hours_import_file,
                summary_json = EXCLUDED.summary_json,
                actor_email = EXCLUDED.actor_email,
                journal_batch_id = NULL,
                submitted_by = NULL, submitted_at = NULL,
                approved_by = NULL, approved_at = NULL,
                updated_at = NOW()
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
            "pay_run_number": pay_run_number,
            "period_number": int(period_number),
            "period_start": period_start,
            "period_end": period_end,
            "pay_date": pay_date,
            "active_employees": len(line_results),
            "paid_employees": paid_count,
            "total_gross": totals["gross"],
            "total_net_pay": totals["net_pay"],
            "total_fed_tax": totals["fed_tax"],
            "total_cpp_ee": totals["cpp_ee"],
            "total_cpp_er": totals["cpp_er"],
            "total_ei_ee": totals["ei_ee"],
            "total_ei_er": totals["ei_er"],
            "total_life_taxable": totals["life_taxable"],
            "total_vacation_earned": totals["vacation_earned"],
            "total_vacation_paid": totals["vacation_paid"],
            "total_stat_pay": totals["stat_pay"],
            "cra_remittance_amount": cra_remittance,
            "hours_import_file": file_name,
            "summary_json": json.dumps(summary),
            "actor_email": actor_email,
        },
    ).mappings().first()
    payroll_run_id = run_row["id"]

    session.execute(
        text(
            "DELETE FROM payroll_run_lines WHERE payroll_run_id = :id"
        ),
        {"id": payroll_run_id},
    )
    line_records_out: list[dict[str, Any]] = []
    for parsed_row, emp, r in line_results:
        session.execute(
            text(
                """
                INSERT INTO payroll_run_lines (
                    payroll_run_id, employee_id, employment_type,
                    week1_hours, week2_hours, total_hours, hourly_rate,
                    reg_hours_pay, overtime_pay, salary_pay, stat_pay,
                    vacation_paid, gross_pay, taxable_gross,
                    fed_tax, cpp_ee, cpp_er, ei_ee, ei_er,
                    life_taxable_benefit, vacation_earned, net_pay,
                    is_on_vacation, notes
                ) VALUES (
                    :payroll_run_id, :employee_id, :employment_type,
                    :week1_hours, :week2_hours, :total_hours, :hourly_rate,
                    :reg_hours_pay, :overtime_pay, :salary_pay, :stat_pay,
                    :vacation_paid, :gross_pay, :taxable_gross,
                    :fed_tax, :cpp_ee, :cpp_er, :ei_ee, :ei_er,
                    :life_taxable_benefit, :vacation_earned, :net_pay,
                    :is_on_vacation, :notes
                )
                """
            ),
            {
                "payroll_run_id": payroll_run_id,
                "employee_id": emp["id"],
                "employment_type": r.employment_type,
                "week1_hours": r.week1_hours,
                "week2_hours": r.week2_hours,
                "total_hours": r.total_hours,
                "hourly_rate": r.hourly_rate,
                "reg_hours_pay": r.reg_hours_pay,
                "overtime_pay": r.overtime_pay,
                "salary_pay": r.salary_pay,
                "stat_pay": r.stat_pay,
                "vacation_paid": r.vacation_paid,
                "gross_pay": r.gross_pay,
                "taxable_gross": r.taxable_gross,
                "fed_tax": r.fed_tax,
                "cpp_ee": r.cpp_ee,
                "cpp_er": r.cpp_er,
                "ei_ee": r.ei_ee,
                "ei_er": r.ei_er,
                "life_taxable_benefit": r.life_taxable_benefit,
                "vacation_earned": r.vacation_earned,
                "net_pay": r.net_pay,
                "is_on_vacation": r.is_on_vacation,
                "notes": "; ".join(r.warnings) if r.warnings else None,
            },
        )
        line_records_out.append(
            {
                "employee_id": str(emp["id"]),
                "employee_number": emp["employee_number"],
                "full_name": emp["full_name"],
                "employment_type": r.employment_type,
                "is_on_vacation": r.is_on_vacation,
                "week1_hours": str(r.week1_hours),
                "week2_hours": str(r.week2_hours),
                "total_hours": str(r.total_hours),
                "hourly_rate": str(r.hourly_rate) if r.hourly_rate is not None else None,
                "gross_pay": str(r.gross_pay),
                "taxable_gross": str(r.taxable_gross),
                "fed_tax": str(r.fed_tax),
                "cpp_ee": str(r.cpp_ee),
                "cpp_er": str(r.cpp_er),
                "ei_ee": str(r.ei_ee),
                "ei_er": str(r.ei_er),
                "life_taxable_benefit": str(r.life_taxable_benefit),
                "vacation_earned": str(r.vacation_earned),
                "vacation_paid": str(r.vacation_paid),
                "stat_pay": str(r.stat_pay),
                "net_pay": str(r.net_pay),
                "warnings": r.warnings,
            }
        )

    return {
        "payroll_run_id": str(payroll_run_id),
        "entity_code": entity["entity_code"],
        "pay_run_number": pay_run_number,
        "period_number": period_number,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "pay_date": pay_date.isoformat(),
        "active_employees": len(line_results),
        "paid_employees": paid_count,
        "totals": {k: str(v) for k, v in totals.items()},
        "cra_remittance_amount": str(cra_remittance),
        "lines": line_records_out,
        "warnings": warnings,
    }


# ----------------------------------------------------------------------
# Journal builder
# ----------------------------------------------------------------------


def build_payroll_journal(
    session,
    *,
    entity_code: str,
    payroll_run_id: str,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    run_uuid = _parse_uuid(payroll_run_id, "payroll_run_id")
    run = session.execute(
        text(
            """
            SELECT id, entity_id, accounting_period_id, pay_run_number,
                   period_start, period_end, pay_date,
                   total_gross, total_net_pay, total_fed_tax,
                   total_cpp_ee, total_cpp_er, total_ei_ee, total_ei_er,
                   total_life_taxable, total_vacation_earned,
                   total_vacation_paid, total_stat_pay, cra_remittance_amount
            FROM payroll_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"payroll_run not found: {payroll_run_id}")
    if run["accounting_period_id"] is None:
        raise ValueError(
            f"payroll_run has no accounting_period_id (period_end={run['period_end']})"
        )

    # Pull per-line data so we can split the wages debit into reg / salary / etc.
    lines = session.execute(
        text(
            """
            SELECT reg_hours_pay, overtime_pay, salary_pay, stat_pay,
                   vacation_paid
            FROM payroll_run_lines
            WHERE payroll_run_id = :id
            """
        ),
        {"id": run_uuid},
    ).mappings().all()
    sum_reg = sum((Decimal(str(l["reg_hours_pay"])) for l in lines), Decimal("0"))
    sum_overtime = sum((Decimal(str(l["overtime_pay"])) for l in lines), Decimal("0"))
    sum_salary = sum((Decimal(str(l["salary_pay"])) for l in lines), Decimal("0"))
    sum_stat = sum((Decimal(str(l["stat_pay"])) for l in lines), Decimal("0"))
    sum_vac_paid = sum(
        (Decimal(str(l["vacation_paid"])) for l in lines), Decimal("0")
    )

    cpp_ee = _money(run["total_cpp_ee"])
    cpp_er = _money(run["total_cpp_er"])
    ei_ee = _money(run["total_ei_ee"])
    ei_er = _money(run["total_ei_er"])
    fed_tax = _money(run["total_fed_tax"])
    life_taxable = _money(run["total_life_taxable"])
    vacation_earned = _money(run["total_vacation_earned"])
    net_pay = _money(run["total_net_pay"])

    # ------------------------------------------------------------------
    # Build journal lines
    # ------------------------------------------------------------------
    journal_lines: list[dict[str, Any]] = []

    def _add(account: str, dr: Decimal, cr: Decimal, memo: str, component: str):
        if dr == 0 and cr == 0:
            return
        journal_lines.append(
            {
                "account_code": account,
                "debit_amount": dr,
                "credit_amount": cr,
                "memo": memo,
                "component": component,
            }
        )

    pay_run_label = run["pay_run_number"]

    # Wages & Benefits (6120) — multiple debit lines for transparency
    _add(ACCOUNT_WAGES, _money(sum_reg), Decimal("0.00"),
         f"Reg hours — {pay_run_label}", "wages_reg")
    if sum_overtime > 0:
        _add(ACCOUNT_WAGES, _money(sum_overtime), Decimal("0.00"),
             f"Overtime — {pay_run_label}", "wages_overtime")
    _add(ACCOUNT_WAGES, _money(sum_salary), Decimal("0.00"),
         f"Salary — {pay_run_label}", "wages_salary")
    if sum_stat > 0:
        _add(ACCOUNT_WAGES, _money(sum_stat), Decimal("0.00"),
             f"Stat pay — {pay_run_label}", "wages_stat")
    if sum_vac_paid > 0:
        _add(ACCOUNT_WAGES, _money(sum_vac_paid), Decimal("0.00"),
             f"Vacation paid — {pay_run_label}", "wages_vacation_paid")
    _add(ACCOUNT_WAGES, cpp_er, Decimal("0.00"),
         f"Employer CPP — {pay_run_label}", "cpp_er_expense")
    _add(ACCOUNT_WAGES, ei_er, Decimal("0.00"),
         f"Employer EI — {pay_run_label}", "ei_er_expense")

    # Group insurance taxable benefit — Dr 6130, Cr 6130 (wash, but
    # surfaces the benefit value on the GL)
    if life_taxable > 0:
        _add(ACCOUNT_GROUP_INSURANCE, life_taxable, Decimal("0.00"),
             f"Group life taxable benefit — {pay_run_label}", "life_dr")
        _add(ACCOUNT_GROUP_INSURANCE, Decimal("0.00"), life_taxable,
             f"Group life taxable benefit offset — {pay_run_label}", "life_cr")

    # Vacation accrual: Dr 6120 / Cr 2220 — but spec wants Dr 2220 + Cr 2220
    # to track payable. Our composition: Dr 6120 already includes the
    # gross above. Spec separately shows Dr 2220 / Cr 2220 (net zero on
    # 2220 but the entry surfaces the accrual). Match the spec by adding
    # both sides on 2220.
    if vacation_earned > 0:
        _add(ACCOUNT_VACATION_PAYABLE, vacation_earned, Decimal("0.00"),
             f"Vacation earned — {pay_run_label}", "vacation_earned_dr")
        _add(ACCOUNT_VACATION_PAYABLE, Decimal("0.00"), vacation_earned,
             f"Vacation earned — {pay_run_label}", "vacation_earned_cr")

    # CRA remittance liability (uses 2300 placeholder until Spencer
    # confirms a dedicated CRA payroll-payable account)
    cra_total = fed_tax + cpp_ee + cpp_er + ei_ee + ei_er
    _add(ACCOUNT_CRA_PAYABLE, Decimal("0.00"), fed_tax,
         f"Federal+ON tax withheld — {pay_run_label}", "cra_fed_tax")
    _add(ACCOUNT_CRA_PAYABLE, Decimal("0.00"), cpp_ee + cpp_er,
         f"CPP (EE+ER) — {pay_run_label}", "cra_cpp")
    _add(ACCOUNT_CRA_PAYABLE, Decimal("0.00"), ei_ee + ei_er,
         f"EI (EE+ER) — {pay_run_label}", "cra_ei")

    # Net pay clearing 1020
    _add(ACCOUNT_BANK, Decimal("0.00"), net_pay,
         f"Net pay EFT — {pay_run_label}", "net_pay")

    total_debits = sum(l["debit_amount"] for l in journal_lines)
    total_credits = sum(l["credit_amount"] for l in journal_lines)
    if total_debits != total_credits:
        # Should never happen given our inputs are consistent, but
        # surface clearly if it does.
        raise ValueError(
            f"Payroll journal not balanced: Dr {total_debits} vs Cr {total_credits}"
        )

    summary = {
        "pay_run_number": pay_run_label,
        "period_start": run["period_start"].isoformat(),
        "period_end": run["period_end"].isoformat(),
        "pay_date": run["pay_date"].isoformat(),
        "totals": {
            "gross": str(run["total_gross"]),
            "net_pay": str(net_pay),
            "fed_tax": str(fed_tax),
            "cpp_ee": str(cpp_ee),
            "cpp_er": str(cpp_er),
            "ei_ee": str(ei_ee),
            "ei_er": str(ei_er),
            "vacation_earned": str(vacation_earned),
            "cra_remittance": str(cra_total),
        },
        "cra_payable_account_note": (
            f"Posted CRA remittance liabilities to {ACCOUNT_CRA_PAYABLE} "
            "as a placeholder — confirm dedicated CRA payroll payable "
            "account in the chart and update if needed."
        ),
    }

    batch = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status,
                total_debits, total_credits, summary_json
            ) VALUES (
                :entity_id, :accounting_period_id, :source_module, :batch_label,
                'draft', 'draft_ready',
                :total_debits, :total_credits, CAST(:summary_json AS jsonb)
            )
            ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label)
            DO UPDATE SET
                status = 'draft',
                workflow_status = 'draft_ready',
                total_debits = EXCLUDED.total_debits,
                total_credits = EXCLUDED.total_credits,
                summary_json = EXCLUDED.summary_json,
                submitted_by = NULL, submitted_at = NULL,
                reviewed_by = NULL, reviewed_at = NULL,
                approved_by = NULL, approved_at = NULL,
                approval_note = NULL, rejection_note = NULL,
                locked_by = NULL, locked_at = NULL,
                updated_at = NOW()
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": run["accounting_period_id"],
            "source_module": SOURCE_MODULE_PAYROLL,
            "batch_label": f"{BATCH_LABEL_PAYROLL}_{pay_run_label}",
            "total_debits": total_debits,
            "total_credits": total_credits,
            "summary_json": json.dumps(summary),
        },
    ).mappings().first()
    journal_batch_id = batch["id"]

    session.execute(
        text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
        {"id": journal_batch_id},
    )
    line_number = 0
    for l in journal_lines:
        line_number += 1
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :id, :line_number, :account_code,
                    :debit_amount, :credit_amount, :memo, CAST(:src AS jsonb)
                )
                """
            ),
            {
                "id": journal_batch_id,
                "line_number": line_number,
                "account_code": l["account_code"],
                "debit_amount": l["debit_amount"],
                "credit_amount": l["credit_amount"],
                "memo": l["memo"],
                "src": json.dumps(
                    {
                        "source_module": SOURCE_MODULE_PAYROLL,
                        "payroll_run_id": str(run_uuid),
                        "component": l["component"],
                    }
                ),
            },
        )

    # Link the run to the batch
    session.execute(
        text(
            "UPDATE payroll_runs SET journal_batch_id = :batch_id, "
            "status = 'submitted', workflow_status = 'submitted_for_review', "
            "submitted_by = :actor, submitted_at = NOW(), updated_at = NOW() "
            "WHERE id = :id"
        ),
        {
            "batch_id": journal_batch_id,
            "actor": actor_email,
            "id": run_uuid,
        },
    )

    return {
        "payroll_run_id": str(run_uuid),
        "journal_batch_id": str(journal_batch_id),
        "pay_run_number": pay_run_label,
        "total_debits": str(total_debits),
        "total_credits": str(total_credits),
        "lines": [
            {
                "account_code": l["account_code"],
                "debit_amount": str(l["debit_amount"]),
                "credit_amount": str(l["credit_amount"]),
                "memo": l["memo"],
                "component": l["component"],
            }
            for l in journal_lines
        ],
        "summary": summary,
    }


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------


def list_payroll_runs(
    session, *, entity_code: str, period_end: date | None = None, limit: int = 50
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    where = ["entity_id = :entity_id"]
    params: dict[str, Any] = {"entity_id": entity["id"], "limit": int(limit)}
    if period_end is not None:
        where.append("period_end = :period_end")
        params["period_end"] = period_end
    rows = session.execute(
        text(
            f"""
            SELECT id, pay_run_number, period_number,
                   period_start, period_end, pay_date,
                   status, workflow_status, active_employees, paid_employees,
                   total_gross, total_net_pay, cra_remittance_amount,
                   journal_batch_id
            FROM payroll_runs
            WHERE {" AND ".join(where)}
            ORDER BY period_end DESC, pay_run_number DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return {
        "entity_code": entity_code,
        "count": len(rows),
        "runs": [
            {
                "id": str(r["id"]),
                "pay_run_number": r["pay_run_number"],
                "period_number": r["period_number"],
                "period_start": r["period_start"].isoformat(),
                "period_end": r["period_end"].isoformat(),
                "pay_date": r["pay_date"].isoformat(),
                "status": r["status"],
                "workflow_status": r["workflow_status"],
                "active_employees": r["active_employees"],
                "paid_employees": r["paid_employees"],
                "total_gross": str(r["total_gross"]),
                "total_net_pay": str(r["total_net_pay"]),
                "cra_remittance_amount": str(r["cra_remittance_amount"]),
                "journal_batch_id": (
                    str(r["journal_batch_id"]) if r["journal_batch_id"] else None
                ),
            }
            for r in rows
        ],
    }


def get_payroll_run_detail(
    session, *, entity_code: str, payroll_run_id: str
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(payroll_run_id, "payroll_run_id")
    run = session.execute(
        text(
            """
            SELECT * FROM payroll_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"payroll_run not found: {payroll_run_id}")

    lines = session.execute(
        text(
            """
            SELECT prl.*, pe.full_name, pe.employee_number
            FROM payroll_run_lines prl
            JOIN payroll_employees pe ON pe.id = prl.employee_id
            WHERE prl.payroll_run_id = :id
            ORDER BY pe.full_name
            """
        ),
        {"id": run_uuid},
    ).mappings().all()

    summary_json = run["summary_json"]
    if isinstance(summary_json, str):
        try:
            summary_json = json.loads(summary_json)
        except Exception:
            summary_json = {}

    return {
        "entity_code": entity_code,
        "run": {
            "id": str(run["id"]),
            "pay_run_number": run["pay_run_number"],
            "period_number": run["period_number"],
            "period_start": run["period_start"].isoformat(),
            "period_end": run["period_end"].isoformat(),
            "pay_date": run["pay_date"].isoformat(),
            "pay_type": run["pay_type"],
            "status": run["status"],
            "workflow_status": run["workflow_status"],
            "active_employees": run["active_employees"],
            "paid_employees": run["paid_employees"],
            "total_gross": str(run["total_gross"]),
            "total_net_pay": str(run["total_net_pay"]),
            "total_fed_tax": str(run["total_fed_tax"]),
            "total_cpp_ee": str(run["total_cpp_ee"]),
            "total_cpp_er": str(run["total_cpp_er"]),
            "total_ei_ee": str(run["total_ei_ee"]),
            "total_ei_er": str(run["total_ei_er"]),
            "total_life_taxable": str(run["total_life_taxable"]),
            "total_vacation_earned": str(run["total_vacation_earned"]),
            "total_vacation_paid": str(run["total_vacation_paid"]),
            "total_stat_pay": str(run["total_stat_pay"]),
            "cra_remittance_amount": str(run["cra_remittance_amount"]),
            "journal_batch_id": (
                str(run["journal_batch_id"]) if run["journal_batch_id"] else None
            ),
            "summary": summary_json,
        },
        "lines": [
            {
                "id": str(l["id"]),
                "employee_id": str(l["employee_id"]),
                "employee_number": l["employee_number"],
                "full_name": l["full_name"],
                "employment_type": l["employment_type"],
                "is_on_vacation": l["is_on_vacation"],
                "week1_hours": str(l["week1_hours"]),
                "week2_hours": str(l["week2_hours"]),
                "total_hours": str(l["total_hours"]),
                "hourly_rate": str(l["hourly_rate"]) if l["hourly_rate"] is not None else None,
                "reg_hours_pay": str(l["reg_hours_pay"]),
                "salary_pay": str(l["salary_pay"]),
                "stat_pay": str(l["stat_pay"]),
                "vacation_paid": str(l["vacation_paid"]),
                "gross_pay": str(l["gross_pay"]),
                "taxable_gross": str(l["taxable_gross"]),
                "fed_tax": str(l["fed_tax"]),
                "cpp_ee": str(l["cpp_ee"]),
                "cpp_er": str(l["cpp_er"]),
                "ei_ee": str(l["ei_ee"]),
                "ei_er": str(l["ei_er"]),
                "life_taxable_benefit": str(l["life_taxable_benefit"]),
                "vacation_earned": str(l["vacation_earned"]),
                "net_pay": str(l["net_pay"]),
                "notes": l["notes"],
            }
            for l in lines
        ],
    }


def get_payroll_summary(
    session, *, entity_code: str, payroll_run_id: str
) -> dict[str, Any]:
    detail = get_payroll_run_detail(
        session, entity_code=entity_code, payroll_run_id=payroll_run_id
    )
    run = detail["run"]
    return {
        "entity_code": entity_code,
        "pay_run_number": run["pay_run_number"],
        "period_end": run["period_end"],
        "active_employees": run["active_employees"],
        "paid_employees": run["paid_employees"],
        "total_gross": run["total_gross"],
        "total_net_pay": run["total_net_pay"],
        "cra_remittance_amount": run["cra_remittance_amount"],
        "per_employee": [
            {
                "full_name": l["full_name"],
                "gross_pay": l["gross_pay"],
                "fed_tax": l["fed_tax"],
                "cpp_ee": l["cpp_ee"],
                "ei_ee": l["ei_ee"],
                "net_pay": l["net_pay"],
            }
            for l in detail["lines"]
        ],
    }


# ----------------------------------------------------------------------
# Submit / approve workflow
# ----------------------------------------------------------------------


def submit_payroll_run(
    session, *, entity_code: str, payroll_run_id: str, actor_email: str
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(payroll_run_id, "payroll_run_id")
    row = session.execute(
        text(
            """
            UPDATE payroll_runs
            SET status = 'submitted',
                workflow_status = 'submitted_for_review',
                submitted_by = :actor, submitted_at = NOW(),
                updated_at = NOW()
            WHERE id = :id AND entity_id = :entity_id
            RETURNING id, workflow_status
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"], "actor": actor_email},
    ).mappings().first()
    if not row:
        raise ValueError(f"payroll_run not found: {payroll_run_id}")
    return {"id": str(row["id"]), "workflow_status": row["workflow_status"]}


def approve_payroll_run(
    session, *, entity_code: str, payroll_run_id: str, actor_email: str
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(payroll_run_id, "payroll_run_id")
    row = session.execute(
        text(
            """
            UPDATE payroll_runs
            SET status = 'approved',
                workflow_status = 'approved_to_post',
                approved_by = :actor, approved_at = NOW(),
                updated_at = NOW()
            WHERE id = :id AND entity_id = :entity_id
              AND workflow_status = 'submitted_for_review'
            RETURNING id, workflow_status
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"], "actor": actor_email},
    ).mappings().first()
    if not row:
        raise ValueError(
            f"payroll_run {payroll_run_id} not found OR not in "
            "'submitted_for_review' state"
        )
    return {"id": str(row["id"]), "workflow_status": row["workflow_status"]}


# ----------------------------------------------------------------------
# Bank withdrawal stub (Phase 2)
# ----------------------------------------------------------------------


def schedule_withdrawals(
    session, *, entity_code: str, payroll_run_id: str, actor_email: str
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(payroll_run_id, "payroll_run_id")

    run = session.execute(
        text(
            """
            SELECT id, pay_date, cra_remittance_amount, total_net_pay
            FROM payroll_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"payroll_run not found: {payroll_run_id}")

    lines = session.execute(
        text(
            """
            SELECT prl.employee_id, prl.net_pay,
                   pe.full_name, pe.bank_transit, pe.bank_institution,
                   pe.bank_account
            FROM payroll_run_lines prl
            JOIN payroll_employees pe ON pe.id = prl.employee_id
            WHERE prl.payroll_run_id = :id AND prl.net_pay > 0
            """
        ),
        {"id": run_uuid},
    ).mappings().all()

    # Wipe + write fresh withdrawals for this run
    session.execute(
        text(
            "DELETE FROM payroll_bank_withdrawals WHERE payroll_run_id = :id"
        ),
        {"id": run_uuid},
    )
    out: list[dict[str, Any]] = []
    for l in lines:
        bank_complete = bool(
            l["bank_transit"] and l["bank_institution"] and l["bank_account"]
        )
        notes = (
            None if bank_complete
            else "Missing bank details on payroll_employees row"
        )
        session.execute(
            text(
                """
                INSERT INTO payroll_bank_withdrawals (
                    payroll_run_id, entity_id, employee_id,
                    withdrawal_type, amount, scheduled_date, status, notes
                ) VALUES (
                    :run_id, :entity_id, :employee_id,
                    'net_pay_eft', :amount, :scheduled_date, 'pending', :notes
                )
                """
            ),
            {
                "run_id": run_uuid,
                "entity_id": entity["id"],
                "employee_id": l["employee_id"],
                "amount": l["net_pay"],
                "scheduled_date": run["pay_date"],
                "notes": notes,
            },
        )
        out.append(
            {
                "employee_full_name": l["full_name"],
                "amount": str(l["net_pay"]),
                "scheduled_date": run["pay_date"].isoformat(),
                "bank_complete": bank_complete,
            }
        )

    session.execute(
        text(
            """
            INSERT INTO payroll_bank_withdrawals (
                payroll_run_id, entity_id, employee_id,
                withdrawal_type, amount, scheduled_date, status, notes
            ) VALUES (
                :run_id, :entity_id, NULL,
                'cra_remittance', :amount, :scheduled_date, 'pending',
                'Stub — manual remittance until TD Bank API integration ships'
            )
            """
        ),
        {
            "run_id": run_uuid,
            "entity_id": entity["id"],
            "amount": run["cra_remittance_amount"],
            "scheduled_date": run["pay_date"],
        },
    )

    return {
        "payroll_run_id": str(run_uuid),
        "scheduled_net_pays": out,
        "scheduled_cra_remittance": {
            "amount": str(run["cra_remittance_amount"]),
            "scheduled_date": run["pay_date"].isoformat(),
        },
        "implementation_status": (
            "stub — withdrawal records created but no actual EFT "
            "initiation. Wire TD Bank API or manual approval workflow "
            "to flip status to 'initiated' / 'confirmed'."
        ),
    }


# ----------------------------------------------------------------------
# Close-control-center section
# ----------------------------------------------------------------------


def section_payroll(
    session, *, entity_id: UUID, period_start: date, period_end: date
) -> dict[str, Any]:
    if not _has_table(session, "payroll_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "payroll_runs table not present",
        }

    rows = session.execute(
        text(
            """
            SELECT id, pay_run_number, workflow_status, total_gross,
                   total_net_pay, cra_remittance_amount, journal_batch_id
            FROM payroll_runs
            WHERE entity_id = :entity_id
              AND period_start >= :period_start
              AND period_end <= :period_end
            ORDER BY period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().all()

    if not rows:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"No payroll_runs covering {period_start.isoformat()}.."
                f"{period_end.isoformat()}. Upload hours via "
                "/api/payroll/runs/upload-hours."
            ),
        }

    not_approved = [
        r for r in rows if r["workflow_status"] != "approved_to_post"
    ]
    if not_approved:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"{len(not_approved)} of {len(rows)} payroll runs not yet "
                "approved_to_post."
            ),
            "runs": [
                {
                    "pay_run_number": r["pay_run_number"],
                    "workflow_status": r["workflow_status"],
                }
                for r in rows
            ],
        }

    return {
        "status": "ready",
        "module_present": True,
        "summary": (
            f"{len(rows)} payroll runs approved. Total net pay "
            f"${sum(_money(r['total_net_pay']) for r in rows)}."
        ),
        "runs": [
            {
                "pay_run_number": r["pay_run_number"],
                "total_gross": str(r["total_gross"]),
                "total_net_pay": str(r["total_net_pay"]),
                "cra_remittance_amount": str(r["cra_remittance_amount"]),
                "journal_batch_id": (
                    str(r["journal_batch_id"]) if r["journal_batch_id"] else None
                ),
            }
            for r in rows
        ],
    }
