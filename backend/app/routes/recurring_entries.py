"""
Recurring journal-entry engine — HTTP routes (Module A).

Prefix: /api/recurring-entries

Endpoints:
    GET    /                      list templates + last posting info
    POST   /                      create new template
    PUT    /{id}                  update template
    PATCH  /{id}/toggle           toggle is_active
    DELETE /{id}                  deactivate (soft delete)
    POST   /{id}/post             post a template for a period
    POST   /post-all              post all due templates for a period
    POST   /seed                  seed standard templates (admin)
    GET    /month-end-status      status for close-readiness panel
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_period_close import PeriodLockedError
from ..services_recurring_entries import (
    get_month_end_status,
    list_templates,
    post_due_templates,
    post_template,
    seed_standard_templates,
    set_template_active,
    upsert_template,
)
from ..services import get_entity_by_code

router = APIRouter(prefix="/api/recurring-entries", tags=["recurring-entries"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


# =========================================================================
# LIST
# =========================================================================

@router.get("")
def get_templates(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_templates(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# CREATE
# =========================================================================

class LineModel(BaseModel):
    line_number: int
    account_code: str
    direction: str  # 'debit' | 'credit'
    memo: str | None = None


class CreateTemplateRequest(BaseModel):
    entity_code: str
    name: str
    description: str | None = None
    calc_type: str  # 'fixed' | 'formula' | 'schedule'
    fixed_amount: float | None = None
    formula_expr: str | None = None
    schedule_source: str | None = None
    cadence: str = "monthly"
    posting_day: int = 1
    is_active: bool = False
    auto_post: bool | None = None
    notes: str | None = None
    lines: list[LineModel] = Field(default_factory=list)
    actor_email: str


@router.post("")
def post_create_template(
    body: CreateTemplateRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return upsert_template(
                session,
                entity_code=body.entity_code,
                name=body.name,
                description=body.description,
                calc_type=body.calc_type,
                fixed_amount=Decimal(str(body.fixed_amount)) if body.fixed_amount is not None else None,
                formula_expr=body.formula_expr,
                schedule_source=body.schedule_source,
                cadence=body.cadence,
                posting_day=body.posting_day,
                is_active=body.is_active,
                auto_post=body.auto_post,
                notes=body.notes,
                lines=[l.model_dump() for l in body.lines],
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# UPDATE
# =========================================================================

class UpdateTemplateRequest(BaseModel):
    entity_code: str
    name: str
    description: str | None = None
    calc_type: str
    fixed_amount: float | None = None
    formula_expr: str | None = None
    schedule_source: str | None = None
    cadence: str = "monthly"
    posting_day: int = 1
    is_active: bool = False
    auto_post: bool | None = None
    notes: str | None = None
    lines: list[LineModel] | None = None
    actor_email: str


@router.put("/{template_id}")
def put_update_template(
    template_id: str,
    body: UpdateTemplateRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return upsert_template(
                session,
                entity_code=body.entity_code,
                template_id=template_id,
                name=body.name,
                description=body.description,
                calc_type=body.calc_type,
                fixed_amount=Decimal(str(body.fixed_amount)) if body.fixed_amount is not None else None,
                formula_expr=body.formula_expr,
                schedule_source=body.schedule_source,
                cadence=body.cadence,
                posting_day=body.posting_day,
                is_active=body.is_active,
                auto_post=body.auto_post,
                notes=body.notes,
                lines=[l.model_dump() for l in body.lines] if body.lines is not None else None,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# TOGGLE active
# =========================================================================

class ToggleRequest(BaseModel):
    entity_code: str
    is_active: bool


@router.patch("/{template_id}/toggle")
def patch_toggle(
    template_id: str,
    body: ToggleRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return set_template_active(
                session,
                entity_code=body.entity_code,
                template_id=template_id,
                is_active=body.is_active,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# DELETE (soft deactivate)
# =========================================================================

class DeactivateRequest(BaseModel):
    entity_code: str


@router.delete("/{template_id}")
def delete_template(
    template_id: str,
    body: DeactivateRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return set_template_active(
                session,
                entity_code=body.entity_code,
                template_id=template_id,
                is_active=False,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# POST (trigger posting for a period)
# =========================================================================

class PostRequest(BaseModel):
    entity_code: str
    period_end: str = Field(..., examples=["2026-03-31"])
    actor_email: str
    dry_run: bool = False


@router.post("/{template_id}/post")
def post_trigger(
    template_id: str,
    body: PostRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return post_template(
                session,
                entity_code=body.entity_code,
                template_id=template_id,
                period_end=period_end,
                actor_email=body.actor_email,
                dry_run=body.dry_run,
            )
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        # Already-posted or balance-guard errors
        status = 409 if "already posted" in str(exc) or "unapproved draft" in str(exc) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


# =========================================================================
# POST-ALL (bulk trigger all due)
# =========================================================================

class PostAllRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str


@router.post("/post-all")
def post_all_due(
    body: PostAllRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return post_due_templates(
                session,
                entity_code=body.entity_code,
                period_end=period_end,
                actor_email=body.actor_email,
            )
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# SEED standard templates
# =========================================================================

class SeedRequest(BaseModel):
    entity_code: str


@router.post("/seed")
def post_seed(
    body: SeedRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_standard_templates(session, entity_code=body.entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MONTH-END STATUS
# =========================================================================

@router.get("/month-end-status")
def get_me_status(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(status_code=404, detail=f"Entity not found: {entity_code}")
            return get_month_end_status(
                session, entity_id=entity["id"], period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
