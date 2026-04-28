"""
Month-End Close Control Center — HTTP routes.

Single endpoint:
    GET /api/month-end-close/status?entity_code=...&period_end=YYYY-MM-DD

Returns a roll-up of every module's status for the period plus an
overall_close_readiness verdict and a list of blocking_items / warning_items
the user has to clear before close.

This module is read-only. The actual close lock workflow (which would write
period.status = 'closed_locked') is intentionally a separate module.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from ..db import db_session
from ..services_month_end_close import get_month_end_close_status


router = APIRouter(prefix="/api/month-end-close", tags=["month-end-close"])


@router.get("/status")
def month_end_close_status(
    entity_code: str = Query(...),
    period_end: str = Query(..., examples=["2026-02-28"]),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_month_end_close_status(
                session=session,
                entity_code=entity_code,
                period_end=period_end,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
