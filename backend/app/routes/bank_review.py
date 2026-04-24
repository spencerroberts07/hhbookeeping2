from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services import (
    get_bank_transaction_detail,
    list_bank_review_summary,
    list_bank_review_transactions,
    match_bank_transaction,
    set_bank_transaction_review_status,
    unmatch_bank_transaction,
)

router = APIRouter(prefix="/api/bank-review", tags=["bank-review"])


class BankReviewSummaryResponse(BaseModel):
    entity_code: str
    entity_name: str | None = None
    date_from: str
    date_to: str
    totals: dict[str, Any]
    summary_by_status: list[dict[str, Any]]
    summary_by_account: list[dict[str, Any]]
    summary_by_match_type: list[dict[str, Any]]


class BankReviewTransactionListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    review_status: str | None = None
    match_state: str | None = None
    count: int
    transactions: list[dict[str, Any]]


class BankTransactionDetailResponse(BaseModel):
    entity_code: str
    entity_name: str | None = None
    transaction: dict[str, Any]
    matches: list[dict[str, Any]]
    history: list[dict[str, Any]]


class BankTransactionReviewStatusRequest(BaseModel):
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    review_status: str = Field(..., examples=["needs_review"])
    note: str | None = Field(default=None, examples=["Reviewed and waiting for vendor support"])


class BankTransactionMatchRequest(BaseModel):
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    match_type: str = Field(..., examples=["manual_explanation"])
    note: str | None = Field(default=None, examples=["Temporary manual match while direct vendor workflow is being built"])
    matched_amount: float | None = Field(default=None, examples=[55.0])
    target_table_name: str | None = Field(default=None, examples=["hh_ap_remittances"])
    target_record_id: str | None = Field(default=None, examples=["248ca2ae-530c-4d7d-bd61-9b3c4ecb77c4"])
    raw_json: dict[str, Any] = Field(default_factory=dict)


class BankTransactionUnmatchRequest(BaseModel):
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    note: str | None = Field(default=None, examples=["Undoing earlier manual match"])


@router.get("/summary", response_model=BankReviewSummaryResponse)
def get_bank_review_summary(
    entity_code: str,
    date_from: str,
    date_to: str,
):
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_bank_review_summary(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
            )
            return BankReviewSummaryResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/transactions", response_model=BankReviewTransactionListResponse)
def get_bank_review_transactions(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    review_status: str | None = Query(default=None),
    match_state: str | None = Query(default=None),
):
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_bank_review_transactions(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                review_status=review_status,
                match_state=match_state,
            )
            return BankReviewTransactionListResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/transactions/{transaction_id}", response_model=BankTransactionDetailResponse)
def get_bank_review_transaction_detail(transaction_id: str):
    try:
        with db_session() as session:
            result = get_bank_transaction_detail(session=session, transaction_id=transaction_id)
            return BankTransactionDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/transactions/{transaction_id}/set-review-status", response_model=BankTransactionDetailResponse)
def set_review_status(transaction_id: str, payload: BankTransactionReviewStatusRequest):
    try:
        with db_session() as session:
            result = set_bank_transaction_review_status(
                session=session,
                transaction_id=transaction_id,
                actor_email=payload.actor_email,
                review_status=payload.review_status,
                note=payload.note,
            )
            return BankTransactionDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/transactions/{transaction_id}/match", response_model=BankTransactionDetailResponse)
def match_transaction(transaction_id: str, payload: BankTransactionMatchRequest):
    try:
        with db_session() as session:
            result = match_bank_transaction(
                session=session,
                transaction_id=transaction_id,
                actor_email=payload.actor_email,
                match_type=payload.match_type,
                note=payload.note,
                matched_amount=payload.matched_amount,
                target_table_name=payload.target_table_name,
                target_record_id=payload.target_record_id,
                raw_json=payload.raw_json,
            )
            return BankTransactionDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/transactions/{transaction_id}/unmatch", response_model=BankTransactionDetailResponse)
def unmatch_transaction(transaction_id: str, payload: BankTransactionUnmatchRequest):
    try:
        with db_session() as session:
            result = unmatch_bank_transaction(
                session=session,
                transaction_id=transaction_id,
                actor_email=payload.actor_email,
                note=payload.note,
            )
            return BankTransactionDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
