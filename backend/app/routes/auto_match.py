"""
Post-import auto-match runner — HTTP routes.

Endpoints:
    POST /api/auto-match/run
        body: {entity_code, period_start, period_end, actor_email,
               triggered_by? = "manual", date_window_days?,
               amount_tolerance?, max_to_apply?}
    GET  /api/auto-match/runs?entity_code=...&limit=...
    GET  /api/auto-match/runs/{id}?entity_code=...
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_auto_match import (
    TRIGGER_MANUAL,
    get_auto_match_run_detail,
    list_auto_match_runs,
    run_auto_match,
)


router = APIRouter(prefix="/api/auto-match", tags=["auto-match"])


class RunRequest(BaseModel):
    entity_code: str
    period_start: DateType
    period_end: DateType
    actor_email: str
    triggered_by: str = TRIGGER_MANUAL
    trigger_source_id: str | None = None
    date_window_days: int = Field(default=7, ge=0, le=31)
    amount_tolerance: Decimal = Field(default=Decimal("0.05"))
    max_to_apply: int = Field(default=100, ge=1, le=1000)


@router.post("/run")
def post_run(
    body: RunRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return run_auto_match(
                session,
                entity_code=body.entity_code,
                period_start=body.period_start,
                period_end=body.period_end,
                actor_email=body.actor_email,
                triggered_by=body.triggered_by,
                trigger_source_id=body.trigger_source_id,
                date_window_days=body.date_window_days,
                amount_tolerance=body.amount_tolerance,
                max_to_apply=body.max_to_apply,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def get_runs(
    entity_code: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_auto_match_runs(session, entity_code=entity_code, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
def get_run(
    run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_auto_match_run_detail(
                session, entity_code=entity_code, run_id=run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
