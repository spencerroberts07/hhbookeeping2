"""
GL Import + Trial Balance Comparison — HTTP routes.

Endpoints:
    POST /api/gl-import/upload
    POST /api/gl-import/runs/{id}/build-comparison
    GET  /api/gl-import/runs?entity_code=...
    GET  /api/gl-import/runs/{id}?entity_code=...
    GET  /api/gl-import/runs/{id}/trial-balance?entity_code=...&only_variance=...
    GET  /api/gl-import/runs/{id}/transactions?entity_code=...&account_code=...
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_gl_import import (
    build_trial_balance_comparison,
    get_gl_account_transactions,
    get_gl_import_run_detail,
    get_trial_balance_comparison,
    import_gl,
    list_gl_import_runs,
)


router = APIRouter(prefix="/api/gl-import", tags=["gl-import"])


def _parse_optional_date(name: str, value: str | None) -> DateType | None:
    if not value:
        return None
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


@router.post("/upload")
async def post_gl_upload(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    period_start: str | None = Form(default=None),
    period_end: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_bytes = await file.read()
    try:
        with db_session() as session:
            return import_gl(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "general_ledger.xlsx",
                period_start=_parse_optional_date("period_start", period_start),
                period_end=_parse_optional_date("period_end", period_end),
                actor_email=actor_email,
            )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildComparisonRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])


@router.post("/runs/{run_id}/build-comparison")
def post_build_comparison(
    run_id: str = Path(...),
    body: BuildComparisonRequest = ...,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return build_trial_balance_comparison(
                session,
                entity_code=body.entity_code,
                gl_import_run_id=run_id,
                actor_email=body.actor_email,
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
            return list_gl_import_runs(
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
            return get_gl_import_run_detail(
                session, entity_code=entity_code, run_id=run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs/{run_id}/trial-balance")
def get_trial_balance(
    run_id: str = Path(...),
    entity_code: str = Query(...),
    only_variance: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_trial_balance_comparison(
                session,
                entity_code=entity_code,
                gl_import_run_id=run_id,
                only_variance=only_variance,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{run_id}/transactions")
def get_run_transactions(
    run_id: str = Path(...),
    entity_code: str = Query(...),
    account_code: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=10000),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_gl_account_transactions(
                session,
                entity_code=entity_code,
                run_id=run_id,
                account_code=account_code,
                limit=limit,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
