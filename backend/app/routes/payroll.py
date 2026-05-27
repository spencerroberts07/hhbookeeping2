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


_HOURS_ACCEPTED_EXTS = (".ods", ".xlsx", ".xlsm", ".xls")


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

    # File-type gate — check by extension, not Content-Type. Browsers
    # send inconsistent Content-Type for .ods (sometimes
    # application/octet-stream, sometimes the proper OASIS type) and
    # we'd rather be explicit about what we parse.
    filename = (file.filename or "").lower()
    if not filename.endswith(_HOURS_ACCEPTED_EXTS):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported hours file type: {file.filename!r}. "
                f"Accepted: {', '.join(_HOURS_ACCEPTED_EXTS)}."
            ),
        )

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
        from ..services_storage import (
            content_type_for as _ct_for,
            storage_service as _r2,
        )
        from sqlalchemy import text as _text
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="payroll_hours",
            ), None)
            r2_key = _r2.upload_file(
                file_bytes=file_bytes,
                original_filename=file.filename or "hours.ods",
                entity_code=entity_code,
                document_type="payroll-hours",
                content_type=_ct_for(file.filename or "hours.ods"),
            )
            result = build_payroll_run(
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
            # Persist R2 key onto the run row that was just created.
            run_id = (
                result.get("payroll_run_id") or result.get("run_id")
                if isinstance(result, dict) else None
            )
            if r2_key and run_id:
                session.execute(
                    _text("UPDATE payroll_runs SET file_path = :p WHERE id = :id"),
                    {"p": r2_key, "id": run_id},
                )
            return result
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
        from ..services_storage import (
            content_type_for as _ct_for,
            storage_service as _r2,
        )
        from sqlalchemy import text as _text
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="payroll_register",
            ), None)
            r2_key = _r2.upload_file(
                file_bytes=file_bytes,
                original_filename=file.filename or "payroll_register.pdf",
                entity_code=entity_code,
                document_type="payroll-register",
                content_type=_ct_for(file.filename or "payroll_register.pdf"),
            )
            result = build_payroll_run_from_register(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "payroll_register.pdf",
                actor_email=actor_email,
                pay_run_number_override=pay_run_number,
                period_number_override=period_number,
                pay_date_override=pay_date_d,
            )
            run_id = (
                result.get("payroll_run_id") or result.get("run_id")
                if isinstance(result, dict) else None
            )
            if r2_key and run_id:
                session.execute(
                    _text("UPDATE payroll_runs SET file_path = :p WHERE id = :id"),
                    {"p": r2_key, "id": run_id},
                )
            return result
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
            result = build_payroll_journal(
                session,
                entity_code=body.entity_code,
                payroll_run_id=payroll_run_id,
                actor_email=body.actor_email,
            )
            # Backfill payroll_cra_breakdowns. Idempotent via UNIQUE
            # constraint on payroll_run_id; we ON CONFLICT update so
            # rebuilds after edits stay consistent.
            _backfill_cra_breakdown(session, payroll_run_id=payroll_run_id,
                                    entity_code=body.entity_code)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _backfill_cra_breakdown(session, *, payroll_run_id: str, entity_code: str) -> None:
    """Write/refresh the payroll_cra_breakdowns row for this run. Called
    after build_payroll_journal so the snapshot lines up with the
    journal totals. Best-effort: a failure here logs but does not roll
    back the journal."""
    from sqlalchemy import text as _text
    import logging as _l
    try:
        run = session.execute(
            _text(
                """
                SELECT pr.id, pr.entity_id, pr.period_start, pr.period_end,
                       pr.pay_date, pr.total_gross, pr.total_fed_tax,
                       pr.total_cpp_ee, pr.total_cpp_er, pr.total_ei_ee,
                       pr.total_ei_er, pr.cra_remittance_amount
                  FROM payroll_runs pr
                  JOIN entities e ON e.id = pr.entity_id
                 WHERE pr.id = :rid AND e.entity_code = :ec
                """
            ),
            {"rid": payroll_run_id, "ec": entity_code},
        ).mappings().first()
        if not run:
            return
        session.execute(
            _text(
                """
                INSERT INTO payroll_cra_breakdowns (
                    entity_id, payroll_run_id, business_number,
                    period_start, period_end, pay_date, gross_taxable,
                    fed_tax, cpp_employee, cpp_employer, ei_employee,
                    ei_employer, total_remittance
                ) VALUES (
                    :eid, :rid, :bn, :ps, :pe, :pd, :gross, :fed,
                    :cppee, :cpper, :eiee, :eier, :total
                )
                ON CONFLICT (payroll_run_id) DO UPDATE SET
                    business_number = EXCLUDED.business_number,
                    period_start = EXCLUDED.period_start,
                    period_end = EXCLUDED.period_end,
                    pay_date = EXCLUDED.pay_date,
                    gross_taxable = EXCLUDED.gross_taxable,
                    fed_tax = EXCLUDED.fed_tax,
                    cpp_employee = EXCLUDED.cpp_employee,
                    cpp_employer = EXCLUDED.cpp_employer,
                    ei_employee = EXCLUDED.ei_employee,
                    ei_employer = EXCLUDED.ei_employer,
                    total_remittance = EXCLUDED.total_remittance
                """
            ),
            {
                "eid": run["entity_id"], "rid": payroll_run_id,
                "bn": PAYROLL_BUSINESS_NUMBER,
                "ps": run["period_start"], "pe": run["period_end"],
                "pd": run["pay_date"],
                "gross": run["total_gross"] or 0,
                "fed": run["total_fed_tax"] or 0,
                "cppee": run["total_cpp_ee"] or 0,
                "cpper": run["total_cpp_er"] or 0,
                "eiee": run["total_ei_ee"] or 0,
                "eier": run["total_ei_er"] or 0,
                "total": run["cra_remittance_amount"] or 0,
            },
        )
    except Exception:
        _l.getLogger(__name__).exception(
            "cra_breakdowns backfill failed for run %s — non-fatal", payroll_run_id,
        )


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
# Employee edit endpoint (named PUT for the frontend editor drawer)
# ----------------------------------------------------------------------


class UpdateEmployeeRequest(BaseModel):
    entity_code: str
    actor_email: str
    first_name: str | None = None
    last_name: str | None = None
    employment_type: str | None = None
    hourly_rate: float | None = None
    biweekly_salary: float | None = None
    vacation_rate: float | None = None
    province: str | None = None
    federal_td1_claim_code: int | None = None
    provincial_td1_claim_code: int | None = None
    cpp_exempt: bool | None = None
    ei_exempt: bool | None = None
    has_life_insurance: bool | None = None
    life_insurance_biweekly: float | None = None
    is_active: bool | None = None
    start_date: str | None = None
    address: str | None = None
    bank_transit: str | None = None
    bank_institution: str | None = None
    bank_account: str | None = None
    notes: str | None = None
    # Feature 1 — additional withholding
    additional_fed_tax: float | None = None
    additional_prov_tax: float | None = None
    additional_tax_effective_date: str | None = None
    additional_tax_td1_on_file: bool | None = None


@router.get("/employees/{employee_id}")
def get_employee_detail(
    employee_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Full employee row for the editor drawer. The /employees list
    endpoint returns a slimmer projection; this one returns everything
    the edit form needs.

    Role-gated to viewer-or-above. The earlier-shipped version removed
    this gate to work around a Clerk token-loading race that 401'd
    authenticated users; that race is now fixed in clerk-token-bridge
    (deferring setTokenResolver until Clerk is fully loaded), so the
    gate is back."""
    from sqlalchemy import text as _text
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        row = session.execute(
            _text(
                """
                SELECT id, entity_id, employee_number, first_name, last_name,
                       full_name, employment_type, hourly_rate, biweekly_salary,
                       vacation_rate, province, federal_td1_claim_code,
                       provincial_td1_claim_code, cpp_exempt, ei_exempt,
                       has_life_insurance, life_insurance_biweekly, is_active,
                       start_date, address, bank_transit, bank_institution,
                       bank_account, ods_name_key, notes,
                       additional_fed_tax, additional_prov_tax,
                       additional_tax_effective_date, additional_tax_td1_on_file,
                       vacation_hours_balance, vacation_dollars_balance,
                       ytd_gross, ytd_cpp_employee, ytd_cpp2_employee,
                       ytd_ei_employee, ytd_fed_tax, ytd_reset_date,
                       created_at, updated_at
                  FROM payroll_employees
                 WHERE id = :id AND entity_id = :eid
                """
            ),
            {"id": employee_id, "eid": entity["id"]},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Employee {employee_id} not found")
        return {
            k: (v.isoformat() if hasattr(v, "isoformat") else (str(v) if isinstance(v, Decimal) else v))
            for k, v in dict(row).items()
        }


@router.put("/employees/{employee_id}")
def put_update_employee(
    body: UpdateEmployeeRequest,
    employee_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Partial-update an existing payroll_employees row. Same fields as
    /employees/upsert but addressed by UUID (the editor drawer has the
    id in hand). Strictly scoped — refuses to update an employee under
    a different entity."""
    from sqlalchemy import text as _text
    enforce_entity_code(_user, body.entity_code)

    payload = body.model_dump(exclude_none=True)
    payload.pop("entity_code", None)
    payload.pop("actor_email", None)
    if not payload:
        raise HTTPException(400, "No fields to update")

    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")

        # Cross-entity safety: must match.
        existing = session.execute(
            _text(
                "SELECT id, first_name, last_name FROM payroll_employees "
                "WHERE id = :id AND entity_id = :eid"
            ),
            {"id": employee_id, "eid": entity["id"]},
        ).mappings().first()
        if not existing:
            raise HTTPException(
                404, f"Employee {employee_id} not found for entity {body.entity_code}"
            )

        # Maintain full_name if first/last changed.
        new_first = payload.get("first_name", existing["first_name"])
        new_last = payload.get("last_name", existing["last_name"])
        if "first_name" in payload or "last_name" in payload:
            payload["full_name"] = f"{(new_first or '').strip()} {(new_last or '').strip()}".strip()

        set_clauses = ", ".join(f"{k} = :{k}" for k in payload.keys())
        params = {**payload, "id": employee_id}
        params["updated_at"] = datetime.utcnow()
        session.execute(
            _text(
                f"UPDATE payroll_employees SET {set_clauses}, updated_at = :updated_at "
                "WHERE id = :id"
            ),
            params,
        )
        row = session.execute(
            _text(
                "SELECT id, entity_id, employee_number, first_name, last_name, "
                "full_name, employment_type, hourly_rate, biweekly_salary, "
                "vacation_rate, province, federal_td1_claim_code, "
                "provincial_td1_claim_code, cpp_exempt, ei_exempt, "
                "has_life_insurance, life_insurance_biweekly, is_active, "
                "start_date, address, bank_transit, bank_institution, "
                "bank_account, notes, updated_at "
                "FROM payroll_employees WHERE id = :id"
            ),
            {"id": employee_id},
        ).mappings().first()
        return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(row).items()}


# ----------------------------------------------------------------------
# EFT (CPA 005) file generation — direct-deposit payroll
#
# Spec: see services_payroll_eft.py. Generates a 1464-char text file
# accepted by TD Business Banking. File is uploaded to R2 (fail-tolerant);
# audit row written to payroll_eft_files; presigned URL surfaced via
# the matching GET endpoint.
#
# Bridlewood's payroll BN is the only one configured for now. When
# additional entities go live, move this to a per-entity setting.
# ----------------------------------------------------------------------


# TODO: Move to entities table when additional dealers come online.
PAYROLL_BUSINESS_NUMBER = "753391010RP0001"
# TD-issued 10-character originator ID. Provided by Spencer / TD's
# EFT origination setup. Must be exactly 10 chars for CPA-005.
EFT_ORIGINATOR_ID = "TPBHC10203"
EFT_SHORT_NAME = "BRIDLEWOOD HH"
EFT_LONG_NAME = "BRIDLEWOOD HOME HARDWARE"
# Return-credit routing — where TD credits funds back if an EFT is
# undeliverable. Bridlewood's operating account at TD.
EFT_RETURN_INSTITUTION = "0004"
EFT_RETURN_TRANSIT = "10202"
EFT_RETURN_ACCOUNT = "06905660371"


class GenerateEftRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/runs/{payroll_run_id}/generate-eft")
def post_generate_eft(
    body: GenerateEftRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Generate a CPA-005 EFT file for an approved payroll run.

    Validates:
      - run exists for this entity and is in approved/posted state
      - every paid employee has bank_transit + bank_institution + bank_account

    On success: file uploaded to R2, row written to payroll_eft_files,
    response includes object key + summary totals. R2 failure is
    fail-tolerant — DB row still written, file_path=NULL, frontend
    surfaces a warning on the download endpoint."""
    from sqlalchemy import text as _text
    from .. import services_payroll_eft as _eft
    from ..services_storage import storage_service as _r2

    enforce_entity_code(_user, body.entity_code)

    with db_session() as session:
        entity = session.execute(
            _text("SELECT id, entity_code FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")

        run = session.execute(
            _text(
                """
                SELECT id, pay_run_number, period_start, period_end, pay_date,
                       status, workflow_status, total_net_pay
                  FROM payroll_runs
                 WHERE id = :id AND entity_id = :eid
                """
            ),
            {"id": payroll_run_id, "eid": entity["id"]},
        ).mappings().first()
        if not run:
            raise HTTPException(404, f"Payroll run {payroll_run_id} not found")

        wf = run["workflow_status"] or run["status"]
        if wf not in {"approved", "approved_to_post", "posted"}:
            raise HTTPException(
                409,
                f"Payroll run must be approved before EFT generation "
                f"(workflow_status={wf!r})",
            )

        # Pull every paid employee with their bank info. net_pay > 0.
        rows = session.execute(
            _text(
                """
                SELECT pe.id, pe.full_name, pe.bank_transit, pe.bank_institution,
                       pe.bank_account, prl.net_pay
                  FROM payroll_run_lines prl
                  JOIN payroll_employees pe ON pe.id = prl.employee_id
                 WHERE prl.payroll_run_id = :rid AND prl.net_pay > 0
                 ORDER BY pe.employee_number
                """
            ),
            {"rid": payroll_run_id},
        ).mappings().all()
        if not rows:
            raise HTTPException(400, "No paid employees on this run.")

        # Block if anyone is missing bank info.
        missing: list[str] = []
        for r in rows:
            if not (r["bank_transit"] and r["bank_institution"] and r["bank_account"]):
                missing.append(r["full_name"])
        if missing:
            raise HTTPException(
                400,
                "Cannot generate EFT — missing bank info for: " + ", ".join(missing),
            )

        # Monotonic file_creation_number per entity.
        next_num_row = session.execute(
            _text(
                """
                SELECT COALESCE(MAX(file_creation_number), 0) + 1 AS n
                  FROM payroll_eft_files
                 WHERE entity_id = :eid
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()
        file_creation_number = int(next_num_row["n"])

        header = _eft.EFTHeader(
            originator_id=EFT_ORIGINATOR_ID,
            file_creation_number=file_creation_number,
            creation_date=DateType.today(),
            originator_short_name=EFT_SHORT_NAME,
            originator_long_name=EFT_LONG_NAME,
            return_institution=EFT_RETURN_INSTITUTION,
            return_transit=EFT_RETURN_TRANSIT,
            return_account=EFT_RETURN_ACCOUNT,
        )
        employees = [
            _eft.EFTEmployee(
                name=r["full_name"],
                transit=r["bank_transit"],
                institution=r["bank_institution"],
                account=r["bank_account"],
                amount=Decimal(str(r["net_pay"])),
            )
            for r in rows
        ]

        try:
            built = _eft.build_eft_file(
                header=header,
                employees=employees,
                payment_date=run["pay_date"],
                cross_reference=f"PAYROLL-{run['pay_run_number']}",
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

        # R2 archive (fail-tolerant). file_path stays NULL on failure;
        # the DB row records the generation event regardless so we have
        # an audit trail of what was sent to TD.
        filename = f"payroll-eft-{run['pay_run_number']}-{file_creation_number:04d}.txt"
        r2_key = _r2.upload_file(
            file_bytes=built.text.encode("ascii", errors="replace"),
            original_filename=filename,
            entity_code=entity["entity_code"],
            document_type="payroll-eft",
            content_type="text/plain",
        )

        eft_row = session.execute(
            _text(
                """
                INSERT INTO payroll_eft_files (
                    entity_id, payroll_run_id, file_name, file_path,
                    record_count, total_amount, file_creation_number,
                    summary_json, actor_email
                ) VALUES (
                    :eid, :rid, :fn, :fp, :rc, :ta, :fcn,
                    CAST(:sj AS jsonb), :ae
                )
                RETURNING id, generated_at
                """
            ),
            {
                "eid": entity["id"],
                "rid": payroll_run_id,
                "fn": filename,
                "fp": r2_key,
                "rc": built.record_count,
                "ta": built.total_amount,
                "fcn": file_creation_number,
                "sj": json.dumps({
                    "credit_count": built.credit_count,
                    "originator_id": EFT_ORIGINATOR_ID,
                    "business_number": PAYROLL_BUSINESS_NUMBER,
                    "pay_run_number": run["pay_run_number"],
                }),
                "ae": body.actor_email,
            },
        ).mappings().first()

        return {
            "id": str(eft_row["id"]),
            "payroll_run_id": payroll_run_id,
            "file_name": filename,
            "file_path": r2_key,
            "r2_uploaded": bool(r2_key),
            "record_count": built.record_count,
            "credit_count": built.credit_count,
            "total_amount": float(built.total_amount),
            "file_creation_number": file_creation_number,
            "generated_at": eft_row["generated_at"].isoformat(),
        }


@router.get("/runs/{payroll_run_id}/eft/download")
def get_eft_download(
    payroll_run_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Return a presigned R2 URL for the most-recent EFT file on this
    run. 404 if no file exists; 409 if R2 didn't store the file (e.g.
    R2 was down when the file was generated). The TXT is available
    again by re-running generate-eft.

    Role-gated to viewer-or-above. The earlier-shipped version removed
    this gate to work around a Clerk token-loading race that 401'd
    authenticated users; that race is now fixed in clerk-token-bridge
    (deferring setTokenResolver until Clerk is fully loaded), so the
    gate is back."""
    from sqlalchemy import text as _text
    from ..services_storage import storage_service as _r2

    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        eft = session.execute(
            _text(
                """
                SELECT id, file_name, file_path, record_count, total_amount,
                       file_creation_number, generated_at
                  FROM payroll_eft_files
                 WHERE payroll_run_id = :rid AND entity_id = :eid
                 ORDER BY generated_at DESC LIMIT 1
                """
            ),
            {"rid": payroll_run_id, "eid": entity["id"]},
        ).mappings().first()

    if not eft:
        raise HTTPException(404, "No EFT file generated for this run yet.")
    if not eft["file_path"]:
        raise HTTPException(
            409,
            "EFT file metadata exists but the file isn't in R2 — re-run "
            "generate-eft to produce a downloadable copy.",
        )

    presigned = _r2.get_presigned_url(eft["file_path"], expires_in=3600)
    if not presigned:
        raise HTTPException(503, "R2 presign failed; try again in a moment.")
    return {
        "file_name": eft["file_name"],
        "download_url": presigned,
        "expires_in_seconds": 3600,
        "record_count": eft["record_count"],
        "total_amount": float(eft["total_amount"]),
        "file_creation_number": eft["file_creation_number"],
        "generated_at": eft["generated_at"].isoformat(),
    }


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


# ======================================================================
# Tier-1 additions — Features 2, 3, 4, 5
# ======================================================================


# ----------------------------------------------------------------------
# Vacation ledger (Feature 2)
# ----------------------------------------------------------------------


@router.get("/employees/{employee_id}/vacation-ledger")
def get_vacation_ledger(
    employee_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Full ledger history for an employee + current denormalized balances.

    Scoped by entity_id — refuses cross-entity reads."""
    from sqlalchemy import text as _text
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        emp = session.execute(
            _text(
                "SELECT id, full_name, vacation_rate, "
                "       vacation_hours_balance, vacation_dollars_balance "
                "  FROM payroll_employees "
                " WHERE id = :id AND entity_id = :eid"
            ),
            {"id": employee_id, "eid": entity["id"]},
        ).mappings().first()
        if not emp:
            raise HTTPException(404, "Employee not found for this entity")
        entries = session.execute(
            _text(
                """
                SELECT pvl.id, pvl.payroll_run_id, pvl.entry_type,
                       pvl.hours_delta, pvl.dollars_delta,
                       pvl.balance_hours_after, pvl.balance_dollars_after,
                       pvl.notes, pvl.created_at, pvl.created_by,
                       pr.pay_run_number, pr.period_end
                  FROM payroll_vacation_ledger pvl
                  LEFT JOIN payroll_runs pr ON pr.id = pvl.payroll_run_id
                 WHERE pvl.employee_id = :emp
                 ORDER BY pvl.created_at DESC
                """
            ),
            {"emp": employee_id},
        ).mappings().all()
    return {
        "employee_id": str(emp["id"]),
        "employee_name": emp["full_name"],
        "vacation_rate": float(emp["vacation_rate"] or 0),
        "balance_hours": float(emp["vacation_hours_balance"] or 0),
        "balance_dollars": float(emp["vacation_dollars_balance"] or 0),
        "entries": [
            {
                "id": str(e["id"]),
                "payroll_run_id": str(e["payroll_run_id"]) if e["payroll_run_id"] else None,
                "pay_run_number": e["pay_run_number"],
                "period_end": e["period_end"].isoformat() if e["period_end"] else None,
                "entry_type": e["entry_type"],
                "hours_delta": float(e["hours_delta"] or 0),
                "dollars_delta": float(e["dollars_delta"] or 0),
                "balance_hours_after": float(e["balance_hours_after"] or 0),
                "balance_dollars_after": float(e["balance_dollars_after"] or 0),
                "notes": e["notes"],
                "created_at": e["created_at"].isoformat() if e["created_at"] else None,
                "created_by": e["created_by"],
            }
            for e in entries
        ],
    }


# ----------------------------------------------------------------------
# YTD reset (Feature 3) — admin trigger, runs once a fiscal year
# ----------------------------------------------------------------------


class YtdResetRequest(BaseModel):
    entity_code: str
    actor_email: str
    confirm: bool = False


@router.post("/ytd/reset")
def post_ytd_reset(
    body: YtdResetRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Zero every employee's YTD totals for the entity. Admin-only —
    this is the start-of-fiscal-year reset.

    Requires `confirm: true` in the body — a stray double-click on the
    UI won't blow away YTD by accident."""
    from sqlalchemy import text as _text
    enforce_entity_code(_user, body.entity_code)
    if not body.confirm:
        raise HTTPException(
            400,
            "YTD reset is irreversible — set confirm=true to proceed.",
        )
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")
        res = session.execute(
            _text(
                """
                UPDATE payroll_employees
                   SET ytd_gross = 0,
                       ytd_cpp_employee = 0,
                       ytd_cpp2_employee = 0,
                       ytd_ei_employee = 0,
                       ytd_fed_tax = 0,
                       ytd_reset_date = CURRENT_DATE,
                       updated_at = NOW()
                 WHERE entity_id = :eid
                """
            ),
            {"eid": entity["id"]},
        )
    return {
        "ok": True,
        "entity_code": body.entity_code,
        "employees_reset": res.rowcount,
        "reset_date": DateType.today().isoformat(),
    }


# ----------------------------------------------------------------------
# Stat-day calendar (Feature 4)
# ----------------------------------------------------------------------


@router.get("/stat-days")
def get_stat_days_endpoint(
    year: int = Query(...),
    province: str = Query(default="ON"),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Return statutory holidays for a year + province. Pure
    calculation — no DB read, no entity scoping needed (stat dates
    are the same for every employer in the province)."""
    from ..services_payroll_stats import get_stat_days
    days = get_stat_days(year, province)
    return {
        "year": year,
        "province": province.upper(),
        "stat_days": [
            {
                "holiday_name": s.holiday_name,
                "holiday_date": s.holiday_date.isoformat(),
                "observed_date": s.observed_date.isoformat(),
            }
            for s in days
        ],
        "count": len(days),
    }


# ----------------------------------------------------------------------
# Pay stubs (Feature 5)
# ----------------------------------------------------------------------


class GeneratePaystubsRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/runs/{payroll_run_id}/generate-paystubs")
def post_generate_paystubs(
    body: GeneratePaystubsRequest,
    payroll_run_id: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Generate a PDF pay stub for every employee on the run.

    Only valid for runs in workflow_status in
    ('approved','approved_to_post','posted','paid'). R2 is
    fail-tolerant — payroll_paystubs rows are written even if the
    upload fails, with r2_object_key NULL."""
    from sqlalchemy import text as _text
    from ..services_payroll_paystub import generate_pay_stub
    from ..services_storage import storage_service as _r2

    enforce_entity_code(_user, body.entity_code)

    with db_session() as session:
        entity = session.execute(
            _text(
                "SELECT id, entity_code, entity_name "
                "  FROM entities WHERE entity_code = :ec"
            ),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")

        run = session.execute(
            _text(
                """
                SELECT id, pay_run_number, period_start, period_end, pay_date,
                       status, workflow_status
                  FROM payroll_runs
                 WHERE id = :rid AND entity_id = :eid
                """
            ),
            {"rid": payroll_run_id, "eid": entity["id"]},
        ).mappings().first()
        if not run:
            raise HTTPException(404, "Payroll run not found")
        wf = (run["workflow_status"] or run["status"] or "").lower()
        if wf not in {"approved", "approved_to_post", "posted", "paid"}:
            raise HTTPException(
                409,
                f"Pay stubs can only be generated on approved runs (current: {wf!r}).",
            )

        lines = session.execute(
            _text(
                """
                SELECT prl.*, pe.full_name, pe.employee_number,
                       pe.employment_type AS emp_employment_type,
                       pe.province AS emp_province,
                       pe.bank_account, pe.vacation_dollars_balance,
                       pe.ytd_gross, pe.ytd_fed_tax, pe.ytd_cpp_employee,
                       pe.ytd_ei_employee
                  FROM payroll_run_lines prl
                  JOIN payroll_employees pe ON pe.id = prl.employee_id
                 WHERE prl.payroll_run_id = :rid
                 ORDER BY pe.employee_number
                """
            ),
            {"rid": payroll_run_id},
        ).mappings().all()
        if not lines:
            raise HTTPException(400, "No employees on this run — nothing to stub.")

        generated = 0
        r2_failed = 0
        results: list[dict[str, Any]] = []
        for line in lines:
            employee = {
                "id": line["employee_id"],
                "full_name": line["full_name"],
                "employee_number": line["employee_number"],
                "employment_type": line["emp_employment_type"],
                "province": line["emp_province"],
                "bank_account": line["bank_account"],
                "vacation_dollars_balance": line["vacation_dollars_balance"],
            }
            ytd_snapshot = {
                "gross": line["ytd_gross"],
                "fed_tax": line["ytd_fed_tax"],
                "cpp_employee": line["ytd_cpp_employee"],
                "ei_employee": line["ytd_ei_employee"],
            }
            try:
                pdf_bytes = generate_pay_stub(
                    run_line=dict(line),
                    employee=employee,
                    run=dict(run),
                    entity=dict(entity),
                    ytd=ytd_snapshot,
                )
            except Exception as exc:
                logging.getLogger(__name__).exception(
                    "pay stub gen failed for emp %s", line["employee_id"]
                )
                results.append({
                    "employee_name": line["full_name"],
                    "ok": False,
                    "error": str(exc)[:120],
                })
                continue

            file_name = (
                f"paystub-{run['pay_run_number']}-"
                f"{line['employee_number']:03d}-{(line['full_name'] or '').replace(' ','_')}.pdf"
            )
            r2_key = _r2.upload_file(
                file_bytes=pdf_bytes,
                original_filename=file_name,
                entity_code=entity["entity_code"],
                document_type=f"paystubs/{run['period_end'].year}/{payroll_run_id}",
                content_type="application/pdf",
            )
            if not r2_key:
                r2_failed += 1

            paystub_row = session.execute(
                _text(
                    """
                    INSERT INTO payroll_paystubs (
                        entity_id, employee_id, payroll_run_id,
                        r2_object_key, file_name, generated_by
                    ) VALUES (
                        :eid, :emp, :rid, :key, :fn, :who
                    )
                    ON CONFLICT (payroll_run_id, employee_id) DO UPDATE
                       SET r2_object_key = EXCLUDED.r2_object_key,
                           file_name = EXCLUDED.file_name,
                           generated_at = NOW(),
                           generated_by = EXCLUDED.generated_by
                    RETURNING id
                    """
                ),
                {
                    "eid": entity["id"],
                    "emp": line["employee_id"],
                    "rid": payroll_run_id,
                    "key": r2_key,
                    "fn": file_name,
                    "who": body.actor_email,
                },
            ).mappings().first()
            generated += 1
            results.append({
                "paystub_id": str(paystub_row["id"]) if paystub_row else None,
                "employee_name": line["full_name"],
                "file_name": file_name,
                "r2_uploaded": bool(r2_key),
                "ok": True,
            })

    return {
        "ok": True,
        "payroll_run_id": payroll_run_id,
        "generated": generated,
        "r2_upload_failures": r2_failed,
        "results": results,
    }


@router.get("/runs/{payroll_run_id}/paystubs")
def list_run_paystubs(
    payroll_run_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    from sqlalchemy import text as _text
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        rows = session.execute(
            _text(
                """
                SELECT ps.id, ps.employee_id, ps.file_name, ps.r2_object_key,
                       ps.generated_at, ps.generated_by,
                       pe.full_name, pe.employee_number
                  FROM payroll_paystubs ps
                  JOIN payroll_employees pe ON pe.id = ps.employee_id
                 WHERE ps.payroll_run_id = :rid AND ps.entity_id = :eid
                 ORDER BY pe.employee_number
                """
            ),
            {"rid": payroll_run_id, "eid": entity["id"]},
        ).mappings().all()
    return {
        "payroll_run_id": payroll_run_id,
        "paystubs": [
            {
                "id": str(r["id"]),
                "employee_id": str(r["employee_id"]),
                "employee_name": r["full_name"],
                "employee_number": r["employee_number"],
                "file_name": r["file_name"],
                "r2_uploaded": bool(r["r2_object_key"]),
                "generated_at": r["generated_at"].isoformat() if r["generated_at"] else None,
                "generated_by": r["generated_by"],
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/employees/{employee_id}/paystubs")
def list_employee_paystubs(
    employee_id: str = Path(...),
    entity_code: str = Query(...),
    limit: int = Query(default=12, ge=1, le=100),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    from sqlalchemy import text as _text
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        rows = session.execute(
            _text(
                """
                SELECT ps.id, ps.payroll_run_id, ps.file_name,
                       ps.r2_object_key, ps.generated_at,
                       pr.pay_run_number, pr.period_start, pr.period_end,
                       pr.pay_date
                  FROM payroll_paystubs ps
                  JOIN payroll_runs pr ON pr.id = ps.payroll_run_id
                 WHERE ps.employee_id = :emp AND ps.entity_id = :eid
                 ORDER BY pr.pay_date DESC
                 LIMIT :limit
                """
            ),
            {"emp": employee_id, "eid": entity["id"], "limit": limit},
        ).mappings().all()
    return {
        "employee_id": employee_id,
        "paystubs": [
            {
                "id": str(r["id"]),
                "payroll_run_id": str(r["payroll_run_id"]),
                "pay_run_number": r["pay_run_number"],
                "period_start": r["period_start"].isoformat() if r["period_start"] else None,
                "period_end": r["period_end"].isoformat() if r["period_end"] else None,
                "pay_date": r["pay_date"].isoformat() if r["pay_date"] else None,
                "file_name": r["file_name"],
                "r2_uploaded": bool(r["r2_object_key"]),
                "generated_at": r["generated_at"].isoformat() if r["generated_at"] else None,
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/paystubs/{paystub_id}/download")
def get_paystub_download(
    paystub_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("viewer")),
) -> dict[str, Any]:
    from sqlalchemy import text as _text
    from ..services_storage import storage_service as _r2
    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        row = session.execute(
            _text(
                """
                SELECT id, file_name, r2_object_key, generated_at
                  FROM payroll_paystubs
                 WHERE id = :id AND entity_id = :eid
                """
            ),
            {"id": paystub_id, "eid": entity["id"]},
        ).mappings().first()
    if not row:
        raise HTTPException(404, "Paystub not found")
    if not row["r2_object_key"]:
        raise HTTPException(
            409,
            "Paystub metadata exists but the PDF isn't in R2 — re-run generate-paystubs.",
        )
    url = _r2.get_presigned_url(row["r2_object_key"], expires_in=3600)
    if not url:
        raise HTTPException(503, "R2 presign failed; try again in a moment.")
    return {
        "file_name": row["file_name"],
        "download_url": url,
        "expires_in_seconds": 3600,
        "generated_at": row["generated_at"].isoformat() if row["generated_at"] else None,
    }
