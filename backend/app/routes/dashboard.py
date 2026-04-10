from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..schemas import DashboardResponse
from ..services import get_entity_by_code

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/quickbooks-status", response_model=DashboardResponse)
def quickbooks_status(entity_code: str = Query(default="1877-8")) -> DashboardResponse:
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(status_code=404, detail=f"Unknown entity code: {entity_code}")
            conn = session.execute(
                text(
                    """
                    SELECT realm_id, connected_at
                    FROM quickbooks_connections
                    WHERE entity_id = :entity_id AND is_active = TRUE
                    ORDER BY connected_at DESC
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"]},
            ).mappings().first()
            acct_count = session.execute(
                text("SELECT COUNT(*) AS c FROM accounts WHERE entity_id = :entity_id"),
                {"entity_id": entity["id"]},
            ).mappings().first()["c"]
            txn_count = session.execute(
                text("SELECT COUNT(*) AS c FROM quickbooks_transactions WHERE entity_id = :entity_id"),
                {"entity_id": entity["id"]},
            ).mappings().first()["c"]
            return DashboardResponse(
                entity_code=entity_code,
                has_quickbooks_connection=bool(conn),
                company_realm_id=conn["realm_id"] if conn else None,
                imported_accounts=acct_count,
                imported_transactions=txn_count,
                last_sync_at=conn["connected_at"] if conn else None,
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
