"""
Year-end workflow endpoints (Phase 5A/5B/5C). Soft-close lifecycle + (5B/5C)
year-end statements and tax package. Fiscal year = Oct 1 -> Sep 30; {fy} is the
ending calendar year (e.g. 2026 = Oct 2025 - Sep 2026).
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ..db import db_session
from ..services_auth import require_role
from ..services_year_end import (
    YearEndError,
    get_year_end_status,
    post_adjusting_je,
    set_year_end_status,
)

router = APIRouter(prefix="/api/year-end", tags=["year-end"])


@router.get("/{fy}")
def get_status(fy: int, entity_code: str = Query(...),
               _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    with db_session() as session:
        try:
            return get_year_end_status(session, entity_code=entity_code, fy=fy)
        except YearEndError as exc:
            raise HTTPException(400, str(exc)) from exc


@router.get("/{fy}/statements")
def get_statements(fy: int, entity_code: str = Query(...),
                   _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    from ..services_year_end_reports import get_year_end_statements
    with db_session() as session:
        try:
            return get_year_end_statements(session, entity_code=entity_code, fy=fy)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc


@router.get("/{fy}/tax-package")
def get_tax_package(fy: int, entity_code: str = Query(...),
                    _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    from ..services_year_end_tax import generate_tax_package
    with db_session() as session:
        try:
            res = generate_tax_package(session, entity_code=entity_code, fy=fy)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    res.pop("pdf_bytes", None)  # don't serialize raw bytes to the API
    return res


class StatusRequest(BaseModel):
    entity_code: str
    status: str
    actor_email: str | None = None


@router.post("/{fy}/status")
def post_status(fy: int, body: StatusRequest,
                _user: Any = Depends(require_role("approver"))) -> dict[str, Any]:
    with db_session() as session:
        try:
            return set_year_end_status(session, entity_code=body.entity_code, fy=fy,
                                       new_status=body.status, actor=body.actor_email)
        except YearEndError as exc:
            raise HTTPException(409, str(exc)) from exc


class AdjustingLine(BaseModel):
    account_code: str
    debit: float = 0.0
    credit: float = 0.0
    memo: str | None = None


class AdjustingRequest(BaseModel):
    entity_code: str
    label: str
    lines: list[AdjustingLine]
    actor_email: str | None = None


@router.post("/{fy}/adjusting-entry")
def post_adjusting(fy: int, body: AdjustingRequest,
                   _user: Any = Depends(require_role("approver"))) -> dict[str, Any]:
    with db_session() as session:
        try:
            return post_adjusting_je(
                session, entity_code=body.entity_code, fy=fy,
                lines=[l.model_dump() for l in body.lines],
                label=body.label, actor=body.actor_email)
        except YearEndError as exc:
            raise HTTPException(409, str(exc)) from exc
