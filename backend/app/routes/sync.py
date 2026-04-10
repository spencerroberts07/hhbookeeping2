from fastapi import APIRouter, HTTPException

from ..db import db_session
from ..schemas import SyncRequest, SyncResponse
from ..services import import_chart_of_accounts, import_transactions_cdc

router = APIRouter(prefix="/api/sync", tags=["quickbooks-sync"])


@router.post("/chart-of-accounts", response_model=SyncResponse)
async def sync_chart_of_accounts(request: SyncRequest) -> SyncResponse:
    try:
        with db_session() as session:
            result = await import_chart_of_accounts(session, request.entity_code)
            return SyncResponse(
                entity_code=request.entity_code,
                sync_type="chart_of_accounts",
                imported_count=result["imported_count"],
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/transactions", response_model=SyncResponse)
async def sync_transactions(request: SyncRequest) -> SyncResponse:
    try:
        with db_session() as session:
            result = await import_transactions_cdc(session, request.entity_code, request.date_from, request.date_to)
            return SyncResponse(
                entity_code=request.entity_code,
                sync_type="transactions_cdc",
                imported_count=result["imported_count"],
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
