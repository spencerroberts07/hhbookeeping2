"""
Bank transaction auto-journal builder — HTTP routes.

Endpoints:
    POST /api/bank-auto-journal/seed-rules
    POST /api/bank-auto-journal/run
    GET  /api/bank-auto-journal/rules?entity_code=...
    GET  /api/bank-auto-journal/runs?entity_code=...
    GET  /api/bank-auto-journal/runs/{id}?entity_code=...
    GET  /api/bank-auto-journal/unmatched?entity_code=...&period_start=...&period_end=...
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_bank_auto_journal import (
    get_auto_journal_run_detail,
    list_auto_journal_runs,
    list_rules,
    list_unmatched_transactions,
    run_auto_journal,
    seed_rules,
)
from ..services_period_close import PeriodLockedError


router = APIRouter(prefix="/api/bank-auto-journal", tags=["bank-auto-journal"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


class SeedRulesRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])


@router.post("/seed-rules")
def post_seed_rules(
    body: SeedRulesRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_rules(
                session,
                entity_code=body.entity_code,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/rules")
def get_rules(entity_code: str = Query(...)) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_rules(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class RunRequest(BaseModel):
    entity_code: str
    period_start: str = Field(..., examples=["2026-02-01"])
    period_end: str = Field(..., examples=["2026-02-28"])
    actor_email: str


@router.post("/run")
def post_run(
    body: RunRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_start = _parse_date("period_start", body.period_start)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return run_auto_journal(
                session,
                entity_code=body.entity_code,
                period_start=period_start,
                period_end=period_end,
                actor_email=body.actor_email,
            )
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def get_runs(
    entity_code: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_auto_journal_runs(
                session, entity_code=entity_code, limit=limit
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
def get_run(
    run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_auto_journal_run_detail(
                session, entity_code=entity_code, run_id=run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/unmatched")
def get_unmatched(
    entity_code: str = Query(...),
    period_start: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_start_d = _parse_date("period_start", period_start)
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return list_unmatched_transactions(
                session,
                entity_code=entity_code,
                period_start=period_start_d,
                period_end=period_end_d,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
