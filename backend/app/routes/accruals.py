"""
Monthly accruals — HTTP routes.
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_accruals import (
    build_accrual_journal,
    list_accrual_journals,
    list_accrual_templates,
    seed_accrual_templates,
    upsert_accrual_template,
)


router = APIRouter(prefix="/api/accruals", tags=["accruals"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


class SeedTemplatesRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])


@router.post("/seed-templates")
def post_seed_templates(
    body: SeedTemplatesRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return seed_accrual_templates(
                session,
                entity_code=body.entity_code,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/templates")
def get_templates(entity_code: str = Query(...)) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_accrual_templates(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpsertTemplateRequest(BaseModel):
    entity_code: str
    actor_email: str
    accrual_code: str
    description: str | None = None
    debit_account: str
    credit_account: str
    default_amount: float | None = None
    frequency: str | None = "monthly"
    is_active: bool = True
    notes: str | None = None


@router.post("/templates/upsert")
def post_upsert_template(
    body: UpsertTemplateRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return upsert_accrual_template(
                session,
                entity_code=body.entity_code,
                data=body.model_dump(exclude={"entity_code", "actor_email"}),
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildJournalRequest(BaseModel):
    entity_code: str
    period_end: str = Field(..., examples=["2026-02-28"])
    accrual_codes: list[str]
    amounts_override: dict[str, float] | None = None
    actor_email: str


@router.post("/build-journal")
def post_build_journal(
    body: BuildJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return build_accrual_journal(
                session,
                entity_code=body.entity_code,
                period_end=period_end,
                accrual_codes=body.accrual_codes,
                amounts_override=body.amounts_override,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/journals")
def get_journals(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return list_accrual_journals(
                session, entity_code=entity_code, period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
