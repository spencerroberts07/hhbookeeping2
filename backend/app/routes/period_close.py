"""
Period close lock workflow — HTTP routes.

Endpoints:
    POST /api/period-close/submit
    POST /api/period-close/approve
    POST /api/period-close/reopen
    GET  /api/period-close/status?entity_code=...&period_end=...
    GET  /api/period-close/history?entity_code=...&period_end=...
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_period_close import (
    BlockingItemsError,
    PeriodLockedError,
    approve_period_close,
    get_period_close_status_payload,
    list_period_close_history,
    reopen_period,
    submit_period_for_close,
)


router = APIRouter(prefix="/api/period-close", tags=["period-close"])


class SubmitRequest(BaseModel):
    entity_code: str
    period_end: str = Field(examples=["2026-02-28"])
    actor_email: str
    notes: str | None = None


class ApproveRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str
    notes: str | None = None


class ReopenRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str
    notes: str = Field(min_length=1)


def _blocking_to_http(exc: BlockingItemsError) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "message": str(exc),
            "blocking_items": exc.blocking_items,
            "warning_items": exc.warning_items,
        },
    )


@router.post("/submit")
def post_submit(
    body: SubmitRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return submit_period_for_close(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except BlockingItemsError as exc:
        raise _blocking_to_http(exc) from exc
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/approve")
def post_approve(
    body: ApproveRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return approve_period_close(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except BlockingItemsError as exc:
        raise _blocking_to_http(exc) from exc
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/reopen")
def post_reopen(
    body: ReopenRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return reopen_period(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/status")
def get_status(
    entity_code: str = Query(...),
    period_end: str = Query(..., examples=["2026-02-28"]),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_period_close_status_payload(
                session, entity_code=entity_code, period_end=period_end
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/history")
def get_history(
    entity_code: str = Query(...),
    period_end: str = Query(..., examples=["2026-02-28"]),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_period_close_history(
                session, entity_code=entity_code, period_end=period_end
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
