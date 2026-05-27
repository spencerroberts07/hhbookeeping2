"""
Standalone data-import routes — sit outside the onboarding wizard but
drive the same import services. Used by /settings/data-import and the
QBO sync card on /settings/integrations.

Currently exposes a single endpoint:

    GET /api/data-import/chart-sync-status?entity_code=...
        Returns persisted chart-sync state so the integrations page
        can show "last synced" across browser sessions instead of
        relying on per-session state.

Future endpoints (preview, etc.) live on the existing onboarding
router because they're tied to the file-upload + job-polling
machinery there.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import enforce_entity_code, require_role

router = APIRouter(prefix="/api/data-import", tags=["data-import"])


@router.get("/chart-sync-status")
def chart_sync_status(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Latest chart-of-accounts sync metadata for the active entity.

    Looks at quickbooks_sync_runs for the most recent successful
    sync_type='chart_of_accounts' row; falls back to the bare account
    count when no sync record exists yet.
    """
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        last_sync = session.execute(
            text(
                """
                SELECT finished_at, status, summary_json
                  FROM quickbooks_sync_runs
                 WHERE entity_id = :eid
                   AND sync_type = 'chart_of_accounts'
                   AND status = 'complete'
                 ORDER BY finished_at DESC
                 LIMIT 1
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()

        counts = session.execute(
            text(
                """
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE quickbooks_account_id IS NOT NULL) AS qbo_mapped
                  FROM accounts
                 WHERE entity_id = :eid AND is_active = TRUE
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first() or {}

        qbo = session.execute(
            text(
                """
                SELECT realm_id, connected_at FROM quickbooks_connections
                 WHERE entity_id = :eid AND is_active = TRUE
                 ORDER BY connected_at DESC LIMIT 1
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()

        return {
            "entity_code": entity_code,
            "last_synced_at": (
                last_sync["finished_at"].isoformat() if last_sync else None
            ),
            "accounts_count": int(counts.get("total") or 0),
            "qbo_mapped_count": int(counts.get("qbo_mapped") or 0),
            "qbo_connected": qbo is not None,
            "qbo_realm_id": qbo["realm_id"] if qbo else None,
        }
