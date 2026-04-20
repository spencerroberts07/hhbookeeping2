from fastapi import APIRouter, HTTPException, Query

from ..db import db_session
from ..schemas import BankSyncRequest, BankSyncResponse, BankTransactionListResponse
from ..services import list_bank_transactions, sync_qbo_bank_transactions

router = APIRouter(prefix="/api/qbo-bank-sync", tags=["qbo-bank-sync"])


@router.post("/sync", response_model=BankSyncResponse)
async def sync_qbo_bank_activity(request: BankSyncRequest) -> BankSyncResponse:
    try:
        with db_session() as session:
            result = await sync_qbo_bank_transactions(
                session=session,
                entity_code=request.entity_code,
                date_from=request.date_from,
                date_to=request.date_to,
            )
            return BankSyncResponse(
                entity_code=request.entity_code,
                sync_type="qbo_bank_activity",
                imported_count=result["inserted_count"],
                updated_count=result["updated_count"],
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/transactions", response_model=BankTransactionListResponse)
def get_qbo_bank_transactions(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    review_status: str | None = Query(default=None),
) -> BankTransactionListResponse:
    try:
        from datetime import date

        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_bank_transactions(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                review_status=review_status,
            )
            return BankTransactionListResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
