from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from ..db import db_session
from ..services_auth import require_role
from ..schemas import (
    DirectVendorAPActionResponse,
    DirectVendorAPDetailResponse,
    DirectVendorAPInvoiceMatchRequest,
    DirectVendorAPInvoiceStatusRequest,
    DirectVendorAPInvoiceUnmatchRequest,
    DirectVendorAPInvoiceUpsertRequest,
    DirectVendorAPListResponse,
    DirectVendorAPSummaryResponse,
)
from ..services import (
    create_direct_vendor_ap_invoice_bank_match,
    get_direct_vendor_ap_invoice_detail,
    list_direct_vendor_ap_invoices,
    release_direct_vendor_ap_invoice_bank_match,
    set_direct_vendor_ap_invoice_status,
    upsert_direct_vendor_ap_invoice,
)

router = APIRouter(prefix="/api/direct-vendor-ap", tags=["direct-vendor-ap"])


@router.get("/summary", response_model=DirectVendorAPSummaryResponse)
def direct_vendor_ap_summary(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    status: str | None = Query(default=None),
    payment_status: str | None = Query(default=None),
    due_state: str | None = Query(default=None),
    match_state: str | None = Query(default=None),
) -> DirectVendorAPSummaryResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_direct_vendor_ap_invoices(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                status=status,
                payment_status=payment_status,
                due_state=due_state,
                match_state=match_state,
            )
            return DirectVendorAPSummaryResponse(
                entity_code=result["entity_code"],
                date_from=result["date_from"],
                date_to=result["date_to"],
                summary=result["summary"],
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/invoices", response_model=DirectVendorAPListResponse)
def direct_vendor_ap_list(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    status: str | None = Query(default=None),
    payment_status: str | None = Query(default=None),
    due_state: str | None = Query(default=None),
    match_state: str | None = Query(default=None),
) -> DirectVendorAPListResponse:
    try:
        parsed_from = date.fromisoformat(date_from)
        parsed_to = date.fromisoformat(date_to)
        with db_session() as session:
            result = list_direct_vendor_ap_invoices(
                session=session,
                entity_code=entity_code,
                date_from=parsed_from,
                date_to=parsed_to,
                status=status,
                payment_status=payment_status,
                due_state=due_state,
                match_state=match_state,
            )
            return DirectVendorAPListResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/invoices/upsert", response_model=DirectVendorAPActionResponse)
def direct_vendor_ap_upsert(
    request: DirectVendorAPInvoiceUpsertRequest,
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> DirectVendorAPActionResponse:
    try:
        with db_session() as session:
            result = upsert_direct_vendor_ap_invoice(
                session=session,
                entity_code=entity_code,
                actor_email=request.actor_email,
                vendor_name=request.vendor_name,
                invoice_number=request.invoice_number,
                invoice_date=request.invoice_date,
                total_amount=request.total_amount,
                due_date=request.due_date,
                received_date=request.received_date,
                vendor_code=request.vendor_code,
                subtotal_amount=request.subtotal_amount,
                tax_amount=request.tax_amount,
                currency_code=request.currency_code,
                priority=request.priority,
                status=request.status,
                payment_status=request.payment_status,
                source_document_name=request.source_document_name,
                note=request.note,
                payload_json=request.payload_json,
            )
            return DirectVendorAPActionResponse(
                invoice_id=str(result["invoice"]["id"]),
                summary=result,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/invoices/{invoice_id}", response_model=DirectVendorAPDetailResponse)
def direct_vendor_ap_detail(
    invoice_id: str = Path(...),
    entity_code: str = Query(...),
    suggestion_date_window_days: int = Query(default=14, ge=0, le=60),
    amount_tolerance: Decimal = Query(default=Decimal("0.05")),
) -> DirectVendorAPDetailResponse:
    try:
        with db_session() as session:
            result = get_direct_vendor_ap_invoice_detail(
                session=session,
                entity_code=entity_code,
                invoice_id=invoice_id,
                suggestion_date_window_days=suggestion_date_window_days,
                amount_tolerance=amount_tolerance,
            )
            return DirectVendorAPDetailResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/invoices/{invoice_id}/set-status", response_model=DirectVendorAPActionResponse)
def direct_vendor_ap_set_status(
    request: DirectVendorAPInvoiceStatusRequest,
    invoice_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> DirectVendorAPActionResponse:
    try:
        with db_session() as session:
            result = set_direct_vendor_ap_invoice_status(
                session=session,
                entity_code=entity_code,
                invoice_id=invoice_id,
                status=request.status,
                actor_email=request.actor_email,
                note=request.note,
            )
            return DirectVendorAPActionResponse(invoice_id=invoice_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/invoices/{invoice_id}/match", response_model=DirectVendorAPActionResponse)
def direct_vendor_ap_match(
    request: DirectVendorAPInvoiceMatchRequest,
    invoice_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> DirectVendorAPActionResponse:
    try:
        with db_session() as session:
            result = create_direct_vendor_ap_invoice_bank_match(
                session=session,
                entity_code=entity_code,
                invoice_id=invoice_id,
                bank_transaction_id=request.bank_transaction_id,
                actor_email=request.actor_email,
                amount_matched=request.amount_matched,
                note=request.note,
            )
            return DirectVendorAPActionResponse(invoice_id=invoice_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/invoices/{invoice_id}/unmatch/{match_id}", response_model=DirectVendorAPActionResponse)
def direct_vendor_ap_unmatch(
    request: DirectVendorAPInvoiceUnmatchRequest,
    invoice_id: str = Path(...),
    match_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> DirectVendorAPActionResponse:
    try:
        with db_session() as session:
            result = release_direct_vendor_ap_invoice_bank_match(
                session=session,
                entity_code=entity_code,
                invoice_id=invoice_id,
                match_id=match_id,
                actor_email=request.actor_email,
                note=request.note,
            )
            return DirectVendorAPActionResponse(invoice_id=invoice_id, summary=result)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
