from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services import list_bank_transactions, sync_qbo_bank_transactions

router = APIRouter(prefix="/api/qbo-bank-sync", tags=["qbo-bank-sync"])


class BankSyncRequest(BaseModel):
    entity_code: str = Field(default="1877-8", examples=["1877-8"])
    date_from: date
    date_to: date


class BankSyncResponse(BaseModel):
    entity_code: str
    sync_type: str
    imported_count: int
    updated_count: int = 0
    summary: dict[str, Any]


class BankTransactionListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    review_status: str | None = None
    count: int
    transactions: list[dict[str, Any]]


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
