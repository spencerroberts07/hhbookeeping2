"""
AP Due-Date Alert cron route.

POST /api/ap-alerts/run-daily
  — guarded by X-Cron-Secret (reuses verify_sync_auth from cash_balancing).
  — fires for all active entities (or a single entity_id via query param).
  — idempotent: ap_alert_log ON CONFLICT DO NOTHING dedup prevents double-fire.

Render cron job: POST /api/ap-alerts/run-daily  (daily, 08:00 UTC)
  Header: X-Cron-Secret: <cron_secret from config>
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query, HTTPException

from ..database import db_session
from ..services_ap_alerts import run_ap_due_alerts, run_all_entities_ap_alerts
from .cash_balancing import verify_sync_auth

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ap-alerts", tags=["ap-alerts"])


@router.post("/run-daily")
async def run_daily_ap_alerts(
    entity_id: str | None = Query(default=None, description="Scope to one entity (UUID). Omit for all entities."),
    run_date: str | None = Query(default=None, description="Override today's date (YYYY-MM-DD). Omit for today."),
    _auth: Any = Depends(verify_sync_auth),
) -> dict[str, Any]:
    """Run the AP due-date alert engine.

    Fires 7-day and 3-day pre-due alerts (and overdue alerts) for
    outside-vendor invoices.  HH AP (2030) is excluded at the query level.
    Each alert fires at most once per invoice per threshold (dedup via
    ap_alert_log ON CONFLICT DO NOTHING).

    Requires X-Cron-Secret header matching config.cron_secret.
    """
    today: date | None = None
    if run_date:
        try:
            today = date.fromisoformat(run_date)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid run_date: {run_date!r}. Use YYYY-MM-DD.")

    with db_session() as session:
        if entity_id:
            from uuid import UUID
            try:
                eid = UUID(entity_id)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid entity_id UUID: {entity_id!r}")
            result = run_ap_due_alerts(session, entity_id=eid, today=today)
        else:
            result = run_all_entities_ap_alerts(session, today=today)

    return result
