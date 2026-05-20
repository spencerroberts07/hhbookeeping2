"""
Monthly COGS journal — HTTP routes.
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_cogs import (
    build_cogs_journal,
    get_cogs_status,
    get_suggested_dating_reversal,
)


router = APIRouter(prefix="/api/cogs", tags=["cogs"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


class BuildCogsJournalRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    period_end: str = Field(..., examples=["2026-02-28"])
    actor_email: str
    dating_new_amount: Decimal
    dating_reversal_amount: Decimal | None = None
    other_adjustment_amount: Decimal | None = None
    other_adjustment_memo: str | None = None
    shrinkage_included: bool = True


@router.post("/build-journal")
def post_build_journal(
    body: BuildCogsJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return build_cogs_journal(
                session,
                entity_code=body.entity_code,
                period_end=period_end,
                dating_new_amount=body.dating_new_amount,
                dating_reversal_amount=body.dating_reversal_amount,
                other_adjustment_amount=body.other_adjustment_amount,
                other_adjustment_memo=body.other_adjustment_memo,
                actor_email=body.actor_email,
                shrinkage_included=body.shrinkage_included,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/status")
def get_status(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return get_cogs_status(
                session, entity_code=entity_code, period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/suggested-dating")
def get_suggested(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return get_suggested_dating_reversal(
                session, entity_code=entity_code, period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
