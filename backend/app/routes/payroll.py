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
from ..services_auth import require_role
from ..services_payroll import (
    approve_payroll_run,
    build_payroll_journal,
    build_payroll_run,
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
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    period_start_d = _parse_date("period_start", period_start)
    period_end_d = _parse_date("period_end", period_end)
    pay_date_d = _parse_date("pay_date", pay_date)
    overrides_dict: dict[str, Any] = {}
    if stat_pay_overrides:
        try:
            overrides_dict = json.loads(stat_pay_overrides)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"stat_pay_overrides must be valid JSON: {exc}",
            ) from exc

    file_bytes = await file.read()
    try:
        with db_session() as session:
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
                stat_pay_overrides=overrides_dict,
                actor_email=actor_email,
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
