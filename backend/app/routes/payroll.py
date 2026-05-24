"""
Payroll module — HTTP routes.
"""
from __future__ import annotations

import json
from datetime import date as DateType, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_payroll import (
    approve_payroll_run,
    build_payroll_journal,
    build_payroll_run,
    build_payroll_run_from_manual_hours,
    build_payroll_run_from_register,
    get_payroll_run_detail,
    get_payroll_summary,
    list_employees,
    list_payroll_runs,
    schedule_withdrawals,
    section_payroll as section_payroll_impl,
    seed_employees,
    submit_payroll_run,
    upsert_employee,
)
from ..services_payroll_calc import (
    BIWEEKLY_PERIODS,
    calculate_employee_payroll,
)


router = APIRouter(prefix="/api/payroll", tags=["payroll"])


# --------------------------------------------------------------------------
# CRA remittance summary
#
# Reads journal_lines on the canonical CRA-payable account (2320) for the
# requested calendar year, grouped by month, and shows per-period owings
# plus a running total. "Remitted" status currently inferred from the
# month being closed (period.status='closed' → remitted). We can swap
# in a real remittance-tracking table later without changing the
# response shape.
# --------------------------------------------------------------------------


CRA_ACCOUNT_CODE = "2320"


@router.get("/cra-remittance")
def cra_remittance(
    entity_code: str = Query(...),
    year: int = Query(..., ge=2000, le=2100),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Calendar-year CRA remittance ledger. `year` is the calendar year
    the dealer is filing for (matches CRA filing-year semantics)."""
    from sqlalchemy import text as _text
    from ..db import db_session as _db_session
    from ..services import get_entity_by_code as _get_entity

    with _db_session() as session:
        entity = _get_entity(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        rows = session.execute(
            _text(
                """
                SELECT ap.period_end,
                       ap.period_label,
                       ap.status AS period_status,
                       COALESCE(SUM(jl.credit_amount - jl.debit_amount), 0)
                           AS net_credit
                  FROM accounting_periods ap
                  LEFT JOIN journal_batches jb
                         ON jb.accounting_period_id = ap.id
                        AND jb.status <> 'voided'
                  LEFT JOIN journal_lines jl
                         ON jl.journal_batch_id = jb.id
                        AND jl.account_code = :acct
                 WHERE ap.entity_id = :eid
                   AND EXTRACT(YEAR FROM ap.period_end) = :yr
                 GROUP BY ap.period_end, ap.period_label, ap.status
                 ORDER BY ap.period_end
                """
            ),
            {"acct": CRA_ACCOUNT_CODE, "eid": entity["id"], "yr": year},
        ).mappings().all()

        remittances: list[dict[str, Any]] = []
        total_outstanding = 0.0
        for r in rows:
            total_owing = float(r["net_credit"])
            status = "remitted" if r["period_status"] == "closed" else "owing"
            if status == "owing":
                total_outstanding += total_owing
            remittances.append({
                "period_end": r["period_end"].isoformat(),
                "period_label": r["period_label"],
                # Detail rollup — the calc engine doesn't split CRA by
                # CPP/EI/tax in the GL today, so we surface only the
                # net liability. Frontend renders Gross/CPP/EI/Tax cells
                # as "—" until the calc engine writes those breakdowns
                # to a side table.
                "gross_payroll": None,
                "cpp_employer": None,
                "cpp_employee": None,
                "ei_employer": None,
                "ei_remittable": None,
                "income_tax": None,
                "total_owing": round(total_owing, 2),
                "status": status,
                "remitted_date": None,
            })

        return {
            "entity_code": entity_code,
            "year": year,
            "remittances": remittances,
            "total_outstanding": round(total_outstanding, 2),
            "cra_account_code": CRA_ACCOUNT_CODE,
        }


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"{name} must be YYYY-MM-DD, got {value!r}"
        ) from exc


# ----------------------------------------------------------------------
# Employees
# ----------------------------------------------------------------------


class SeedEmployeesRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/employees/seed")
def post_seed_employees(
    body: SeedEmployeesRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_employees(
                session,
                entity_code=body.entity_code,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/employees")
def get_employees(entity_code: str = Query(...)) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_employees(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpsertEmployeeRequest(BaseModel):
    entity_code: str
    employee_number: int
    actor_email: str
    first_name: str | None = None
    last_name: str | None = None
    employment_type: str | None = None
    hourly_rate: Decimal | None = None
    biweekly_salary: Decimal | None = None
    vacation_rate: Decimal | None = None
    has_life_insurance: bool | None = None
    life_insurance_biweekly: Decimal | None = None
    is_active: bool | None = None
    ods_name_key: str | None = None
    notes: str | None = None
    bank_transit: str | None = None
    bank_institution: str | None = None
    bank_account: str | None = None


@router.post("/employees/upsert")
def post_upsert_employee(
    body: UpsertEmployeeRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return upsert_employee(
                session,
                entity_code=body.entity_code,
                employee_number=body.employee_number,
                actor_email=body.actor_email,
                data=body.model_dump(
                    exclude={"entity_code", "employee_number", "actor_email"}
                ),
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ----------------------------------------------------------------------
# Payroll runs
# ----------------------------------------------------------------------


@router.post("/runs/upload-hours")
async def post_upload_hours(
    entity_code: str = Form(...),
    pay_run_number: str = Form(...),
    period_number: int = Form(...),
    period_start: str = Form(...),
    period_end: str = Form(...),
    pay_date: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    stat_pay_overrides: str | None = Form(default=None),
    vacation_paid_overrides: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    period_start_d = _parse_date("period_start", period_start)
    period_end_d = _parse_date("period_end", period_end)
    pay_date_d = _parse_date("pay_date", pay_date)

    def _parse_override(name: str, raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400, detail=f"{name} must be valid JSON: {exc}"
            ) from exc

    stat_dict = _parse_override("stat_pay_overrides", stat_pay_overrides)
    vac_dict = _parse_override("vacation_paid_overrides", vacation_paid_overrides)

    file_bytes = await file.read()
    try:
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="payroll_hours",
            ), None)
            return build_payroll_run(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "hours.ods",
                pay_run_number=pay_run_number,
                period_number=period_number,
                period_start=period_start_d,
                period_end=period_end_d,
                pay_date=pay_date_d,
                stat_pay_overrides=stat_dict,
                vacation_paid_overrides=vac_dict,
                actor_email=actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class ManualHoursRow(BaseModel):
    employee_number: int | None = None
    employee_id: str | None = None
    week1_hours: Decimal | None = None
    week2_hours: Decimal | None = None
    total_hours: Decimal | None = None
    is_on_vacation: bool = False
    is_salary_reg: bool = False


class ManualHoursRequest(BaseModel):
    entity_code: str
    pay_run_number: str
    period_number: int
    period_start: str
    period_end: str
    pay_date: str
    actor_email: str
    hours: list[ManualHoursRow]
    stat_pay_overrides: dict[str, Decimal] | None = None
    vacation_paid_overrides: dict[str, Decimal] | None = None


@router.post("/runs/manual-hours")
def post_manual_hours(
    body: ManualHoursRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_start_d = _parse_date("period_start", body.period_start)
    period_end_d = _parse_date("period_end", body.period_end)
    pay_date_d = _parse_date("pay_date", body.pay_date)
    try:
        with db_session() as session:
            return build_payroll_run_from_manual_hours(
                session,
                entity_code=body.entity_code,
                pay_run_number=body.pay_run_number,
                period_number=body.period_number,
                period_start=period_start_d,
                period_end=period_end_d,
                pay_date=pay_date_d,
                hours=[h.model_dump() for h in body.hours],
                stat_pay_overrides=(
                    dict(body.stat_pay_overrides) if body.stat_pay_overrides else None
                ),
                vacation_paid_overrides=(
                    dict(body.vacation_paid_overrides)
                    if body.vacation_paid_overrides else None
                ),
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/upload-register")
async def post_upload_register(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    pay_run_number: str | None = Form(default=None),
    period_number: int | None = Form(default=None),
    pay_date: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    PRIMARY entry point: upload an ENetEmployer payroll register PDF.
    Parses the exact per-employee deductions and totals, persists the
    payroll_run + payroll_run_lines at status='draft_confirmed'. Use
    this in place of upload-hours for any run that will hit the GL.
    """
    enforce_entity_code(_user, entity_code)
    pay_date_d = _parse_date("pay_date", pay_date) if pay_date else None
    file_bytes = await file.read()
    try:
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="payroll_register",
            ), None)
            return build_payroll_run_from_register(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "payroll_register.pdf",
                actor_email=actor_email,
                pay_run_number_override=pay_run_number,
                period_number_override=period_number,
                pay_date_override=pay_date_d,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildJournalRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/runs/{payroll_run_id}/build-journal")
def post_build_journal(
    body: BuildJournalRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return build_payroll_journal(
                session,
                entity_code=body.entity_code,
                payroll_run_id=payroll_run_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def get_runs(
    entity_code: str = Query(...),
    period_end: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end) if period_end else None
    try:
        with db_session() as session:
            return list_payroll_runs(
                session, entity_code=entity_code,
                period_end=period_end_d, limit=limit,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{payroll_run_id}")
def get_run(
    payroll_run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_payroll_run_detail(
                session, entity_code=entity_code, payroll_run_id=payroll_run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{payroll_run_id}/summary")
def get_run_summary(
    payroll_run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_payroll_summary(
                session, entity_code=entity_code, payroll_run_id=payroll_run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


class WorkflowRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/runs/{payroll_run_id}/submit")
def post_submit(
    body: WorkflowRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return submit_payroll_run(
                session,
                entity_code=body.entity_code,
                payroll_run_id=payroll_run_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{payroll_run_id}/approve")
def post_approve(
    body: WorkflowRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return approve_payroll_run(
                session,
                entity_code=body.entity_code,
                payroll_run_id=payroll_run_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{payroll_run_id}/schedule-withdrawals")
def post_schedule_withdrawals(
    body: WorkflowRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return schedule_withdrawals(
                session,
                entity_code=body.entity_code,
                payroll_run_id=payroll_run_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ----------------------------------------------------------------------
# Validation endpoint — Period 5 actuals from Feb 2026 register
# ----------------------------------------------------------------------


_FEB_PERIOD_4_TARGETS = {
    "gross": Decimal("10465.43"),
    "net_pay": Decimal("8872.68"),
    "fed_tax": Decimal("924.96"),
    "cpp_ee": Decimal("497.18"),
    "cpp_er": Decimal("497.18"),
    "ei_ee": Decimal("170.61"),
    "ei_er": Decimal("238.84"),
    "life_taxable": Decimal("16.93"),
    "vacation_earned": Decimal("644.89"),
    "cra_remittance": Decimal("2328.77"),
}

_FEB_PERIOD_5_TARGETS = {
    "gross": Decimal("11260.95"),
    "net_pay": Decimal("9534.00"),
    "fed_tax": Decimal("1005.64"),
    "cpp_ee": Decimal("537.77"),
    "cpp_er": Decimal("537.77"),
    "ei_ee": Decimal("183.54"),
    "ei_er": Decimal("256.96"),
    "life_taxable": Decimal("16.93"),
    "vacation_earned_net": Decimal("107.76"),
    "stat_pay": Decimal("1027.27"),
    "vacation_paid": Decimal("528.00"),
    "cra_remittance": Decimal("2521.68"),
}


@router.get("/validate-feb-2026")
def get_validate_feb_2026(entity_code: str = Query(default="1877-8")) -> dict[str, Any]:
    """
    Compare the latest stored payroll_runs in Feb 2026 against the
    known register actuals. Reports per-line variance for the
    bookkeeper.
    """
    try:
        with db_session() as session:
            runs = list_payroll_runs(
                session, entity_code=entity_code, limit=10
            )["runs"]
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    feb_runs = [
        r for r in runs
        if r["period_end"].startswith("2026-02")
    ]

    variances: list[dict[str, Any]] = []
    for r in feb_runs:
        period_num = r["period_number"]
        targets = (
            _FEB_PERIOD_4_TARGETS if period_num == 4
            else _FEB_PERIOD_5_TARGETS if period_num == 5
            else None
        )
        if targets is None:
            continue
        with db_session() as session:
            detail = get_payroll_run_detail(
                session, entity_code=entity_code, payroll_run_id=r["id"]
            )
        run = detail["run"]
        line_variance = []
        for k, target in targets.items():
            calc_key_map = {
                "gross": "total_gross",
                "net_pay": "total_net_pay",
                "fed_tax": "total_fed_tax",
                "cpp_ee": "total_cpp_ee",
                "cpp_er": "total_cpp_er",
                "ei_ee": "total_ei_ee",
                "ei_er": "total_ei_er",
                "life_taxable": "total_life_taxable",
                "vacation_earned": "total_vacation_earned",
                "vacation_earned_net": "total_vacation_earned",
                "stat_pay": "total_stat_pay",
                "vacation_paid": "total_vacation_paid",
                "cra_remittance": "cra_remittance_amount",
            }
            calc_field = calc_key_map.get(k)
            if calc_field is None:
                continue
            calculated = Decimal(run.get(calc_field, "0"))
            variance = (calculated - target).quantize(Decimal("0.01"))
            line_variance.append(
                {
                    "metric": k,
                    "actual": str(target),
                    "calculated": str(calculated),
                    "variance": str(variance),
                }
            )
        variances.append(
            {
                "pay_run_number": r["pay_run_number"],
                "period_number": period_num,
                "comparison": line_variance,
            }
        )

    return {
        "entity_code": entity_code,
        "feb_runs_found": [r["pay_run_number"] for r in feb_runs],
        "validation": variances,
        "note": (
            "Tax-engine variance is expected: this is a simplified "
            "annualize-and-bracket implementation, not a full PDOC clone. "
            "If a metric is off by > $5 per employee, override fed_tax on "
            "the payroll_run_lines row with the bookkeeper's actual."
        ),
    }


# ----------------------------------------------------------------------
# Close-control-center wrapper (used via services_month_end_close)
# ----------------------------------------------------------------------


def section_payroll(session, entity_code, period_start, period_end):  # noqa: ARG001
    # Backward-compatible wrapper for the close-center.
    from ..services import get_entity_by_code  # noqa: WPS433
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        return {"status": "no_data", "module_present": False, "summary": "Entity not found"}
    return section_payroll_impl(
        session, entity_id=entity["id"],
        period_start=period_start, period_end=period_end,
    )
