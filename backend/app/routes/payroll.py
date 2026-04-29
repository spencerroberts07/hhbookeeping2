"""
Payroll control — HTTP routes.

Endpoints:
    POST /api/payroll/runs/upsert
    GET  /api/payroll/runs?entity_code=...&period_start=...&period_end=...
    GET  /api/payroll/runs/{payroll_reference}?entity_code=...
    POST /api/payroll/runs/{payroll_reference}/submit
    POST /api/payroll/runs/{payroll_reference}/approve
    POST /api/payroll/runs/{payroll_reference}/mark-bank-cleared
    POST /api/payroll/runs/{payroll_reference}/mark-remittance-cleared
    GET  /api/payroll/summary?entity_code=...&period_start=...&period_end=...
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_payroll import (
    approve_payroll_run,
    get_payroll_run,
    get_payroll_summary,
    list_payroll_runs,
    mark_bank_cleared,
    mark_remittance_cleared,
    submit_payroll_run,
    upsert_payroll_run,
)


router = APIRouter(prefix="/api/payroll", tags=["payroll"])


class UpsertPayrollRunRequest(BaseModel):
    entity_code: str
    payroll_reference: str
    pay_period_start: DateType
    pay_period_end: DateType
    pay_date: DateType
    actor_email: str
    processor: str | None = None
    gross_wages: Decimal | None = None
    employer_cpp: Decimal | None = None
    employer_ei: Decimal | None = None
    employer_benefits: Decimal | None = None
    employee_cpp: Decimal | None = None
    employee_ei: Decimal | None = None
    employee_tax: Decimal | None = None
    employee_benefits: Decimal | None = None
    net_pay: Decimal | None = None
    remittance_amount: Decimal | None = None
    total_employer_cost: Decimal | None = None
    notes: str | None = None
    raw_import_json: dict[str, Any] = Field(default_factory=dict)


class WorkflowRequest(BaseModel):
    entity_code: str
    actor_email: str
    notes: str | None = None


class MarkClearedRequest(BaseModel):
    entity_code: str
    bank_transaction_id: str
    actor_email: str


@router.post("/runs/upsert")
def post_upsert(
    body: UpsertPayrollRunRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return upsert_payroll_run(
                session,
                entity_code=body.entity_code,
                payroll_reference=body.payroll_reference,
                pay_period_start=body.pay_period_start,
                pay_period_end=body.pay_period_end,
                pay_date=body.pay_date,
                processor=body.processor,
                gross_wages=body.gross_wages,
                employer_cpp=body.employer_cpp,
                employer_ei=body.employer_ei,
                employer_benefits=body.employer_benefits,
                employee_cpp=body.employee_cpp,
                employee_ei=body.employee_ei,
                employee_tax=body.employee_tax,
                employee_benefits=body.employee_benefits,
                net_pay=body.net_pay,
                remittance_amount=body.remittance_amount,
                total_employer_cost=body.total_employer_cost,
                notes=body.notes,
                raw_import_json=body.raw_import_json,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def get_runs(
    entity_code: str = Query(...),
    period_start: DateType | None = Query(default=None),
    period_end: DateType | None = Query(default=None),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_payroll_runs(
                session,
                entity_code=entity_code,
                period_start=period_start,
                period_end=period_end,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{payroll_reference}")
def get_one_run(
    payroll_reference: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_payroll_run(
                session, entity_code=entity_code, payroll_reference=payroll_reference
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/runs/{payroll_reference}/submit")
def post_submit(
    body: WorkflowRequest,
    payroll_reference: str = Path(...),
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return submit_payroll_run(
                session,
                entity_code=body.entity_code,
                payroll_reference=payroll_reference,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{payroll_reference}/approve")
def post_approve(
    body: WorkflowRequest,
    payroll_reference: str = Path(...),
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return approve_payroll_run(
                session,
                entity_code=body.entity_code,
                payroll_reference=payroll_reference,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{payroll_reference}/mark-bank-cleared")
def post_mark_bank_cleared(
    body: MarkClearedRequest,
    payroll_reference: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return mark_bank_cleared(
                session,
                entity_code=body.entity_code,
                payroll_reference=payroll_reference,
                bank_transaction_id=body.bank_transaction_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{payroll_reference}/mark-remittance-cleared")
def post_mark_remittance_cleared(
    body: MarkClearedRequest,
    payroll_reference: str = Path(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return mark_remittance_cleared(
                session,
                entity_code=body.entity_code,
                payroll_reference=payroll_reference,
                bank_transaction_id=body.bank_transaction_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary")
def get_summary(
    entity_code: str = Query(...),
    period_start: DateType = Query(...),
    period_end: DateType = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_payroll_summary(
                session,
                entity_code=entity_code,
                period_start=period_start,
                period_end=period_end,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
