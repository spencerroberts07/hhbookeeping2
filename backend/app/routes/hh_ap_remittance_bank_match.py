from datetime import date
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Path, Query

from ..db import db_session
from ..schemas import (
    HHAPRemittanceBankActionResponse,
    HHAPRemittanceBankAutoMatchRequest,
    HHAPRemittanceBankMatchDetailResponse,
    HHAPRemittanceBankMatchListResponse,
    HHAPRemittanceBankMatchRequest,
    HHAPRemittanceBankSummaryResponse,
    HHAPRemittanceBankUnmatchRequest,
)
from ..services import (
    auto_match_hh_ap_remittances_to_bank,
    create_hh_ap_remittance_bank_match,
    get_hh_ap_remittance_bank_match_detail,
    list_hh_ap_remittances_for_bank_matching,
    release_hh_ap_remittance_bank_match,
)

router = APIRouter(
    prefix="/api/hh-ap/remittance-bank-match",
    tags=["hh-ap-remittance-bank-match"],
)


@router.get("/summary", response_model=HHAPRemittanceBankSummaryResponse)
def hh_ap_remittance_bank_match_summary(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    bank_match_status: str | None = Query(default=None),
    suggestion_date_window_days: int = Query(default=7, ge=0, le=31),
    amount_tolerance: Decimal = Query(default=Decimal("0.05")),
) -> HHAPRemittanceBankSummaryResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_hh_ap_remittances_for_bank_matching(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                bank_match_status=bank_match_status,
                suggestion_date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
            )
            return HHAPRemittanceBankSummaryResponse(
                entity_code=result["entity_code"],
                date_from=result["date_from"],
                date_to=result["date_to"],
                summary=result["summary"],
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/remittances", response_model=HHAPRemittanceBankMatchListResponse)
def hh_ap_remittance_bank_match_list(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    bank_match_status: str | None = Query(default=None),
    suggestion_date_window_days: int = Query(default=7, ge=0, le=31),
    amount_tolerance: Decimal = Query(default=Decimal("0.05")),
) -> HHAPRemittanceBankMatchListResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_hh_ap_remittances_for_bank_matching(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                bank_match_status=bank_match_status,
                suggestion_date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
            )
            return HHAPRemittanceBankMatchListResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/remittances/{remittance_id}", response_model=HHAPRemittanceBankMatchDetailResponse)
def hh_ap_remittance_bank_match_detail(
    remittance_id: str = Path(...),
    entity_code: str = Query(...),
    suggestion_date_window_days: int = Query(default=7, ge=0, le=31),
    amount_tolerance: Decimal = Query(default=Decimal("0.05")),
) -> HHAPRemittanceBankMatchDetailResponse:
    try:
        with db_session() as session:
            result = get_hh_ap_remittance_bank_match_detail(
                session=session,
                entity_code=entity_code,
                remittance_id=remittance_id,
                suggestion_date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
            )
            return HHAPRemittanceBankMatchDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/remittances/{remittance_id}/match", response_model=HHAPRemittanceBankActionResponse)
def hh_ap_remittance_bank_match_apply(
    request: HHAPRemittanceBankMatchRequest,
    remittance_id: str = Path(...),
    entity_code: str = Query(...),
) -> HHAPRemittanceBankActionResponse:
    try:
        with db_session() as session:
            result = create_hh_ap_remittance_bank_match(
                session=session,
                entity_code=entity_code,
                remittance_id=remittance_id,
                bank_transaction_id=request.bank_transaction_id,
                actor_email=request.actor_email,
                amount_matched=request.amount_matched,
                note=request.note,
            )
            return HHAPRemittanceBankActionResponse(
                remittance_id=remittance_id,
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/remittances/{remittance_id}/unmatch/{match_id}",
    response_model=HHAPRemittanceBankActionResponse,
)
def hh_ap_remittance_bank_match_release(
    request: HHAPRemittanceBankUnmatchRequest,
    remittance_id: str = Path(...),
    match_id: str = Path(...),
    entity_code: str = Query(...),
) -> HHAPRemittanceBankActionResponse:
    try:
        with db_session() as session:
            result = release_hh_ap_remittance_bank_match(
                session=session,
                entity_code=entity_code,
                remittance_id=remittance_id,
                match_id=match_id,
                actor_email=request.actor_email,
                note=request.note,
            )
            return HHAPRemittanceBankActionResponse(
                remittance_id=remittance_id,
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/auto-match", response_model=HHAPRemittanceBankActionResponse)
def hh_ap_remittance_bank_auto_match(
    request: HHAPRemittanceBankAutoMatchRequest,
) -> HHAPRemittanceBankActionResponse:
    try:
        with db_session() as session:
            result = auto_match_hh_ap_remittances_to_bank(
                session=session,
                entity_code=request.entity_code,
                date_from=request.date_from,
                date_to=request.date_to,
                actor_email=request.actor_email,
                date_window_days=request.date_window_days,
                amount_tolerance=request.amount_tolerance,
                max_to_apply=request.max_to_apply,
                note=request.note,
            )
            return HHAPRemittanceBankActionResponse(
                remittance_id="bulk_auto_match",
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
