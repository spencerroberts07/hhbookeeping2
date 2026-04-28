from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Path, Query

from ..db import db_session
from ..schemas import (
    CardSettlementActionResponse,
    CardSettlementBatchUpsertRequest,
    CardSettlementDetailResponse,
    CardSettlementListResponse,
    CardSettlementMatchRequest,
    CardSettlementStatusRequest,
    CardSettlementSummaryResponse,
    CardSettlementUnmatchRequest,
)
from ..services import (
    create_card_settlement_bank_match,
    get_card_settlement_batch_detail,
    list_card_settlement_batches,
    release_card_settlement_bank_match,
    set_card_settlement_status,
    upsert_card_settlement_batch,
)

router = APIRouter(prefix="/api/card-settlement", tags=["card-settlement"])


@router.get("/summary", response_model=CardSettlementSummaryResponse)
def card_settlement_summary(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    reconciliation_status: str | None = Query(default=None),
    bank_match_state: str | None = Query(default=None),
) -> CardSettlementSummaryResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_card_settlement_batches(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                reconciliation_status=reconciliation_status,
                bank_match_state=bank_match_state,
            )
            return CardSettlementSummaryResponse(
                entity_code=result["entity_code"],
                date_from=result["date_from"],
                date_to=result["date_to"],
                summary=result["summary"],
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/batches", response_model=CardSettlementListResponse)
def card_settlement_list(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    reconciliation_status: str | None = Query(default=None),
    bank_match_state: str | None = Query(default=None),
) -> CardSettlementListResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_card_settlement_batches(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                reconciliation_status=reconciliation_status,
                bank_match_state=bank_match_state,
            )
            return CardSettlementListResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/batches/upsert", response_model=CardSettlementActionResponse)
def card_settlement_upsert(
    request: CardSettlementBatchUpsertRequest,
    entity_code: str = Query(...),
) -> CardSettlementActionResponse:
    try:
        with db_session() as session:
            result = upsert_card_settlement_batch(
                session=session,
                entity_code=entity_code,
                actor_email=request.actor_email,
                processor_name=request.processor_name,
                business_date=request.business_date,
                net_deposit_amount=request.net_deposit_amount,
                deposit_date=request.deposit_date,
                merchant_account=request.merchant_account,
                settlement_reference=request.settlement_reference,
                currency_code=request.currency_code,
                gross_sales_amount=request.gross_sales_amount,
                refunds_amount=request.refunds_amount,
                chargebacks_amount=request.chargebacks_amount,
                fees_amount=request.fees_amount,
                tax_on_fees_amount=request.tax_on_fees_amount,
                reconciliation_status=request.reconciliation_status,
                source_file_name=request.source_file_name,
                note=request.note,
                payload_json=request.payload_json,
            )
            return CardSettlementActionResponse(
                batch_id=str(result["batch"]["id"]),
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/batches/{batch_id}", response_model=CardSettlementDetailResponse)
def card_settlement_detail(
    batch_id: str = Path(...),
    entity_code: str = Query(...),
    suggestion_date_window_days: int = Query(default=7, ge=0, le=31),
    amount_tolerance: Decimal = Query(default=Decimal("0.05")),
) -> CardSettlementDetailResponse:
    try:
        with db_session() as session:
            result = get_card_settlement_batch_detail(
                session=session,
                entity_code=entity_code,
                batch_id=batch_id,
                suggestion_date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
            )
            return CardSettlementDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/set-status", response_model=CardSettlementActionResponse)
def card_settlement_set_status(
    request: CardSettlementStatusRequest,
    batch_id: str = Path(...),
    entity_code: str = Query(...),
) -> CardSettlementActionResponse:
    try:
        with db_session() as session:
            result = set_card_settlement_status(
                session=session,
                entity_code=entity_code,
                batch_id=batch_id,
                reconciliation_status=request.reconciliation_status,
                actor_email=request.actor_email,
                note=request.note,
            )
            return CardSettlementActionResponse(batch_id=batch_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/match", response_model=CardSettlementActionResponse)
def card_settlement_match(
    request: CardSettlementMatchRequest,
    batch_id: str = Path(...),
    entity_code: str = Query(...),
) -> CardSettlementActionResponse:
    try:
        with db_session() as session:
            result = create_card_settlement_bank_match(
                session=session,
                entity_code=entity_code,
                batch_id=batch_id,
                bank_transaction_id=request.bank_transaction_id,
                actor_email=request.actor_email,
                amount_matched=request.amount_matched,
                note=request.note,
            )
            return CardSettlementActionResponse(batch_id=batch_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/unmatch/{match_id}", response_model=CardSettlementActionResponse)
def card_settlement_unmatch(
    request: CardSettlementUnmatchRequest,
    batch_id: str = Path(...),
    match_id: str = Path(...),
    entity_code: str = Query(...),
) -> CardSettlementActionResponse:
    try:
        with db_session() as session:
            result = release_card_settlement_bank_match(
                session=session,
                entity_code=entity_code,
                batch_id=batch_id,
                match_id=match_id,
                actor_email=request.actor_email,
                note=request.note,
            )
            return CardSettlementActionResponse(batch_id=batch_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
