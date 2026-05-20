from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from ..db import db_session
from ..schemas import (
    BankSyncRequest,
    BankSyncResponse,
    BankTransactionActionResponse,
    BankTransactionDetailResponse,
    BankTransactionListResponse,
    BankTransactionMatchRequest,
    BankTransactionReviewStatusRequest,
    BankTransactionUnmatchRequest,
)
from ..services import (
    create_bank_transaction_match,
    get_bank_transaction_detail,
    list_bank_transactions,
    release_bank_transaction_match,
    set_bank_transaction_review_status,
    sync_qbo_bank_transactions,
)
from ..services_auth import enforce_entity_code, require_role

router = APIRouter(prefix="/api/qbo-bank-sync", tags=["qbo-bank-sync"])


@router.post("/sync", response_model=BankSyncResponse)
async def sync_qbo_bank_activity(
    request: BankSyncRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> BankSyncResponse:
    enforce_entity_code(_user, request.entity_code)
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


@router.get("/transactions/{transaction_id}", response_model=BankTransactionDetailResponse)
def get_qbo_bank_transaction_detail(
    transaction_id: str = Path(...),
    entity_code: str = Query(...),
) -> BankTransactionDetailResponse:
    try:
        with db_session() as session:
            result = get_bank_transaction_detail(
                session=session,
                entity_code=entity_code,
                transaction_id=transaction_id,
            )
            return BankTransactionDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/transactions/{transaction_id}/review-status",
    response_model=BankTransactionActionResponse,
)
def set_qbo_bank_transaction_review_status(
    request: BankTransactionReviewStatusRequest,
    transaction_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> BankTransactionActionResponse:
    enforce_entity_code(_user, entity_code)
    try:
        with db_session() as session:
            result = set_bank_transaction_review_status(
                session=session,
                entity_code=entity_code,
                transaction_id=transaction_id,
                review_status=request.review_status,
                actor_email=request.actor_email,
                note=request.note,
            )
            return BankTransactionActionResponse(
                transaction_id=transaction_id,
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/transactions/{transaction_id}/match", response_model=BankTransactionActionResponse)
def match_qbo_bank_transaction(
    request: BankTransactionMatchRequest,
    transaction_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> BankTransactionActionResponse:
    enforce_entity_code(_user, entity_code)
    try:
        with db_session() as session:
            result = create_bank_transaction_match(
                session=session,
                entity_code=entity_code,
                transaction_id=transaction_id,
                match_type=request.match_type,
                target_table=request.target_table,
                target_record_id=request.target_record_id,
                target_label=request.target_label,
                amount_matched=request.amount_matched,
                actor_email=request.actor_email,
                note=request.note,
                payload_json=request.payload_json,
            )
            return BankTransactionActionResponse(
                transaction_id=transaction_id,
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/transactions/{transaction_id}/unmatch/{match_id}",
    response_model=BankTransactionActionResponse,
)
def unmatch_qbo_bank_transaction(
    request: BankTransactionUnmatchRequest,
    transaction_id: str = Path(...),
    match_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> BankTransactionActionResponse:
    enforce_entity_code(_user, entity_code)
    try:
        with db_session() as session:
            result = release_bank_transaction_match(
                session=session,
                entity_code=entity_code,
                transaction_id=transaction_id,
                match_id=match_id,
                actor_email=request.actor_email,
                note=request.note,
            )
            return BankTransactionActionResponse(
                transaction_id=transaction_id,
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
