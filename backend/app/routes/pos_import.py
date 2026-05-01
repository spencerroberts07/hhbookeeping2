"""
Inventory adjustment + month-end POS imports — HTTP routes.

Endpoints:
    POST /api/pos-import/inventory-adjustment   (multipart upload)
    POST /api/pos-import/pos-financial          (multipart upload)
    POST /api/pos-import/inventory-value        (multipart upload)
    POST /api/pos-import/aged-ar                (multipart upload)
    POST /api/pos-import/ar-adjustment          (multipart upload)
    POST /api/pos-import/build-store-use-journal
    POST /api/pos-import/build-donation-journal
    POST /api/pos-import/build-ar-adjustment-journal
    GET  /api/pos-import/runs?entity_code=...&period_start=...&period_end=...
    GET  /api/pos-import/runs/{run_id}?entity_code=...
    GET  /api/pos-import/inventory-value/latest?entity_code=...
    GET  /api/pos-import/aged-ar/latest?entity_code=...
    GET  /api/pos-import/pos-financial/latest?entity_code=...

All write endpoints require role 'bookkeeper'. Reads stay open.
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_pos_import import (
    build_ar_adjustment_journal,
    build_donation_journal,
    build_store_use_journal,
    extract_text_from_upload,
    get_latest_aged_ar_snapshot,
    get_latest_inventory_value_snapshot,
    get_latest_pos_financial_snapshot,
    get_pos_import_run_detail,
    import_aged_ar,
    import_ar_adjustment,
    import_inventory_adjustment,
    import_inventory_value,
    import_pos_financial,
    list_pos_import_runs,
    validate_pos_financial,
)


router = APIRouter(prefix="/api/pos-import", tags=["pos-import"])


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


async def _read_file_text(file: UploadFile) -> tuple[str, str]:
    """
    Returns (text, source) where source is 'text' / 'pdf_text' / 'pdf_ocr'.
    PDFs go through pypdf first; if the extraction looks corrupted (a
    non-standard font breaks glyph mappings), OCR fallback runs via
    pytesseract — provided Tesseract is installed on the host.
    """
    raw = await file.read()
    try:
        return extract_text_from_upload(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _parse_optional_date(name: str, value: str | None) -> DateType | None:
    if not value:
        return None
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


# --------------------------------------------------------------------------
# Request / response shapes for the journal builder routes
# --------------------------------------------------------------------------


class BuildJournalRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    import_run_id: str = Field(
        ..., examples=["a3c7e0d2-9c4f-4d87-9c8b-2f1a8eb1b001"]
    )
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    expense_account_code: str | None = Field(
        default=None,
        description=(
            "Override for the debit-side expense account. Defaults to "
            "6510 for store_use, 6695 for donation."
        ),
    )
    inventory_account_code: str | None = Field(
        default=None,
        description="Override for the credit-side inventory account. Defaults to 1120.",
    )
    override_total: float | None = Field(
        default=None,
        description=(
            "Force the journal magnitude to this value (e.g. 672.5581 from "
            "the printed Prism subtotal when the parser missed lines and "
            "line_sum drifts below the report header). Bypasses run.total_amount "
            "and the line-sum fallback."
        ),
    )


# --------------------------------------------------------------------------
# Imports — multipart uploads
# --------------------------------------------------------------------------


@router.post("/inventory-adjustment")
async def post_import_inventory_adjustment(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text, source = await _read_file_text(file)
    try:
        with db_session() as session:
            result = import_inventory_adjustment(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "inventory_adjustment.txt",
                actor_email=actor_email,
            )
            result["extraction_source"] = source
            return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/pos-financial")
async def post_import_pos_financial(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text, source = await _read_file_text(file)
    try:
        with db_session() as session:
            result = import_pos_financial(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "pos_financial.txt",
                actor_email=actor_email,
            )
            result["extraction_source"] = source
            return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/inventory-value")
async def post_import_inventory_value(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    snapshot_date: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text, source = await _read_file_text(file)
    snap_override = _parse_optional_date("snapshot_date", snapshot_date)
    try:
        with db_session() as session:
            result = import_inventory_value(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "inventory_value.txt",
                actor_email=actor_email,
                snapshot_date_override=snap_override,
            )
            result["extraction_source"] = source
            return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/aged-ar")
async def post_import_aged_ar(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    snapshot_date: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text, source = await _read_file_text(file)
    snap_override = _parse_optional_date("snapshot_date", snapshot_date)
    try:
        with db_session() as session:
            result = import_aged_ar(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "aged_ar.txt",
                actor_email=actor_email,
                snapshot_date_override=snap_override,
            )
            result["extraction_source"] = source
            return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ar-adjustment")
async def post_import_ar_adjustment(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text, source = await _read_file_text(file)
    try:
        with db_session() as session:
            result = import_ar_adjustment(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "ar_adjustment.txt",
                actor_email=actor_email,
            )
            result["extraction_source"] = source
            return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# --------------------------------------------------------------------------
# Journal builders
# --------------------------------------------------------------------------


@router.post("/build-store-use-journal")
def post_build_store_use_journal(
    body: BuildJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            kwargs: dict[str, Any] = {
                "entity_code": body.entity_code,
                "import_run_id": body.import_run_id,
                "actor_email": body.actor_email,
            }
            if body.expense_account_code:
                kwargs["expense_account_code"] = body.expense_account_code
            if body.inventory_account_code:
                kwargs["inventory_account_code"] = body.inventory_account_code
            if body.override_total is not None:
                from decimal import Decimal
                kwargs["override_total"] = Decimal(str(body.override_total))
            return build_store_use_journal(session, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/build-donation-journal")
def post_build_donation_journal(
    body: BuildJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            kwargs: dict[str, Any] = {
                "entity_code": body.entity_code,
                "import_run_id": body.import_run_id,
                "actor_email": body.actor_email,
            }
            if body.expense_account_code:
                kwargs["expense_account_code"] = body.expense_account_code
            if body.inventory_account_code:
                kwargs["inventory_account_code"] = body.inventory_account_code
            if body.override_total is not None:
                from decimal import Decimal
                kwargs["override_total"] = Decimal(str(body.override_total))
            return build_donation_journal(session, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildArAdjustmentJournalRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    import_run_id: str = Field(
        ..., examples=["a3c7e0d2-9c4f-4d87-9c8b-2f1a8eb1b001"]
    )
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])
    bad_debt_account_code: str | None = Field(
        default=None,
        description="Override for the debit-side bad-debt expense account. Defaults to 6550.",
    )
    ar_account_code: str | None = Field(
        default=None,
        description="Override for the credit-side AR account. Defaults to 1085.",
    )


@router.post("/build-ar-adjustment-journal")
def post_build_ar_adjustment_journal(
    body: BuildArAdjustmentJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            kwargs: dict[str, Any] = {
                "entity_code": body.entity_code,
                "import_run_id": body.import_run_id,
                "actor_email": body.actor_email,
            }
            if body.bad_debt_account_code:
                kwargs["bad_debt_account_code"] = body.bad_debt_account_code
            if body.ar_account_code:
                kwargs["ar_account_code"] = body.ar_account_code
            return build_ar_adjustment_journal(session, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class ValidatePosFinancialRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    import_run_id: str = Field(
        ..., examples=["9ba44c24-40c5-4e6d-acf2-005351ff97f3"]
    )


@router.post("/validate-pos-financial")
def post_validate_pos_financial(
    body: ValidatePosFinancialRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Compare a monthly POS Financial snapshot against the SUM of daily
    cash_balancing journals for the same period. Returns a per-GL-account
    variance report. **No journal entries are written** — this endpoint
    is read-only.

    Replaces the deprecated POST /api/pos-import/build-pos-financial-journal,
    which double-counted daily entries.
    """
    try:
        with db_session() as session:
            return validate_pos_financial(
                session,
                entity_code=body.entity_code,
                import_run_id=body.import_run_id,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------


@router.get("/runs")
def get_runs(
    entity_code: str = Query(...),
    period_start: str | None = Query(default=None),
    period_end: str | None = Query(default=None),
    report_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_pos_import_runs(
                session,
                entity_code=entity_code,
                period_start=_parse_optional_date("period_start", period_start),
                period_end=_parse_optional_date("period_end", period_end),
                report_type=report_type,
                limit=limit,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{run_id}")
def get_run_detail(
    run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_pos_import_run_detail(
                session, entity_code=entity_code, run_id=run_id
            )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/inventory-value/latest")
def get_inventory_value_latest(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            snapshot = get_latest_inventory_value_snapshot(
                session, entity_code=entity_code
            )
            return {
                "entity_code": entity_code,
                "snapshot": snapshot,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/aged-ar/latest")
def get_aged_ar_latest(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            snapshot = get_latest_aged_ar_snapshot(
                session, entity_code=entity_code
            )
            return {
                "entity_code": entity_code,
                "snapshot": snapshot,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/pos-financial/latest")
def get_pos_financial_latest(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            snapshot = get_latest_pos_financial_snapshot(
                session, entity_code=entity_code
            )
            return {
                "entity_code": entity_code,
                "snapshot": snapshot,
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
