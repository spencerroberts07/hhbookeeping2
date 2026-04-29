"""
Inventory adjustment + month-end POS imports — HTTP routes.

Endpoints:
    POST /api/pos-import/inventory-adjustment   (multipart upload)
    POST /api/pos-import/pos-financial          (multipart upload)
    POST /api/pos-import/inventory-value        (multipart upload)
    POST /api/pos-import/aged-ar                (multipart upload)
    POST /api/pos-import/build-store-use-journal
    POST /api/pos-import/build-donation-journal
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
    build_donation_journal,
    build_store_use_journal,
    get_latest_aged_ar_snapshot,
    get_latest_inventory_value_snapshot,
    get_latest_pos_financial_snapshot,
    get_pos_import_run_detail,
    import_aged_ar,
    import_inventory_adjustment,
    import_inventory_value,
    import_pos_financial,
    list_pos_import_runs,
)


router = APIRouter(prefix="/api/pos-import", tags=["pos-import"])


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


async def _read_file_text(file: UploadFile) -> str:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    # POS reports are usually plain ASCII / latin-1 fixed-width text.
    # Try utf-8 first, fall back to latin-1, and finally decode with
    # replacement so weird control bytes don't take the request down.
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


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
    file_text = await _read_file_text(file)
    try:
        with db_session() as session:
            return import_inventory_adjustment(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "inventory_adjustment.txt",
                actor_email=actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/pos-financial")
async def post_import_pos_financial(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    file_text = await _read_file_text(file)
    try:
        with db_session() as session:
            return import_pos_financial(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "pos_financial.txt",
                actor_email=actor_email,
            )
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
    file_text = await _read_file_text(file)
    snap_override = _parse_optional_date("snapshot_date", snapshot_date)
    try:
        with db_session() as session:
            return import_inventory_value(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "inventory_value.txt",
                actor_email=actor_email,
                snapshot_date_override=snap_override,
            )
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
    file_text = await _read_file_text(file)
    snap_override = _parse_optional_date("snapshot_date", snapshot_date)
    try:
        with db_session() as session:
            return import_aged_ar(
                session,
                entity_code=entity_code,
                file_text=file_text,
                file_name=file.filename or "aged_ar.txt",
                actor_email=actor_email,
                snapshot_date_override=snap_override,
            )
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
            return build_donation_journal(session, **kwargs)
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
