"""
Fixed asset / depreciation — HTTP routes.
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_depreciation import (
    build_depreciation_journal,
    generate_depreciation_schedule,
    get_depreciation_schedule,
    get_depreciation_summary,
    list_fixed_assets,
    seed_fixed_assets,
)


router = APIRouter(prefix="/api/depreciation", tags=["depreciation"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


class SeedAssetsRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])


@router.post("/seed-assets")
def post_seed_assets(
    body: SeedAssetsRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_fixed_assets(
                session,
                entity_code=body.entity_code,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/assets")
def get_assets(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_fixed_assets(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class GenerateScheduleRequest(BaseModel):
    entity_code: str
    fiscal_year: int
    actor_email: str
    half_year_asset_codes: list[str] | None = None


@router.post("/generate-schedule")
def post_generate_schedule(
    body: GenerateScheduleRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return generate_depreciation_schedule(
                session,
                entity_code=body.entity_code,
                fiscal_year=int(body.fiscal_year),
                actor_email=body.actor_email,
                half_year_asset_codes=body.half_year_asset_codes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/schedule")
def get_schedule(
    entity_code: str = Query(...),
    fiscal_year: int = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_depreciation_schedule(
                session,
                entity_code=entity_code,
                fiscal_year=int(fiscal_year),
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildJournalRequest(BaseModel):
    entity_code: str
    period_end: str = Field(..., examples=["2026-02-28"])
    actor_email: str


@router.post("/build-journal")
def post_build_journal(
    body: BuildJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return build_depreciation_journal(
                session,
                entity_code=body.entity_code,
                period_end=period_end,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary")
def get_summary(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return get_depreciation_summary(
                session, entity_code=entity_code, period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
