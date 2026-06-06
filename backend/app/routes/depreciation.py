"""
Fixed asset / depreciation — HTTP routes.

Module B additions:
    GET/POST/PUT /api/depreciation/classes          — asset class CRUD
    POST         /api/depreciation/seed-classes     — seed Bridlewood classes
    POST         /api/depreciation/link-classes     — link assets to classes
    POST         /api/depreciation/add-asset        — add a new asset
    GET          /api/depreciation/monthly-amounts  — per-class monthly amounts (Module A feed)
    POST         /api/depreciation/dispose          — disposal journal
    GET          /api/depreciation/schedule/excel   — download Excel schedule
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_depreciation import (
    MissingGainLossAccountError,
    add_fixed_asset,
    build_depreciation_journal,
    compute_monthly_depreciation_by_class,
    compute_disposal_nbv,
    generate_depreciation_schedule,
    generate_excel_schedule,
    get_depreciation_schedule,
    get_depreciation_summary,
    link_assets_to_classes,
    list_asset_classes,
    list_fixed_assets,
    post_disposal_journal,
    seed_bridlewood_classes,
    seed_fixed_assets,
    upsert_asset_class,
)
from ..services import get_entity_by_code


router = APIRouter(prefix="/api/depreciation", tags=["depreciation"])


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateType.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be YYYY-MM-DD, got {value!r}",
        ) from exc


class SeedAssetsRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    actor_email: str = Field(..., examples=["controller@bridlewood.ca"])


@router.post("/seed-assets")
def post_seed_assets(
    body: SeedAssetsRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_fixed_assets(
                session,
                entity_code=body.entity_code,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/assets")
def get_assets(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_fixed_assets(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class GenerateScheduleRequest(BaseModel):
    entity_code: str
    fiscal_year: int
    actor_email: str
    half_year_asset_codes: list[str] | None = None


@router.post("/generate-schedule")
def post_generate_schedule(
    body: GenerateScheduleRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return generate_depreciation_schedule(
                session,
                entity_code=body.entity_code,
                fiscal_year=int(body.fiscal_year),
                actor_email=body.actor_email,
                half_year_asset_codes=body.half_year_asset_codes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/schedule")
def get_schedule(
    entity_code: str = Query(...),
    fiscal_year: int = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_depreciation_schedule(
                session,
                entity_code=entity_code,
                fiscal_year=int(fiscal_year),
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class BuildJournalRequest(BaseModel):
    entity_code: str
    period_end: str = Field(..., examples=["2026-02-28"])
    actor_email: str


@router.post("/build-journal")
def post_build_journal(
    body: BuildJournalRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    period_end = _parse_date("period_end", body.period_end)
    try:
        with db_session() as session:
            return build_depreciation_journal(
                session,
                entity_code=body.entity_code,
                period_end=period_end,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary")
def get_summary(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            return get_depreciation_summary(
                session, entity_code=entity_code, period_end=period_end_d
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MODULE B — Asset class CRUD
# =========================================================================


@router.get("/classes")
def get_classes(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_asset_classes(session, entity_code=entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpsertClassRequest(BaseModel):
    entity_code: str
    class_code: str
    class_name: str
    cca_rate: float
    expense_account: str
    accum_account: str
    formula_expr: str | None = None
    is_active: bool = True
    display_order: int = 0


@router.post("/classes")
def post_upsert_class(
    body: UpsertClassRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return upsert_asset_class(
                session,
                entity_code=body.entity_code,
                class_code=body.class_code,
                class_name=body.class_name,
                cca_rate=Decimal(str(body.cca_rate)),
                expense_account=body.expense_account,
                accum_account=body.accum_account,
                formula_expr=body.formula_expr,
                is_active=body.is_active,
                display_order=body.display_order,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class SeedClassesRequest(BaseModel):
    entity_code: str


@router.post("/seed-classes")
def post_seed_classes(
    body: SeedClassesRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return seed_bridlewood_classes(session, entity_code=body.entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/link-classes")
def post_link_classes(
    body: SeedClassesRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Link existing fixed_assets rows to fixed_asset_classes via class_id FK.
    MUST be called after seed-classes. Safe to re-run."""
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return link_assets_to_classes(session, entity_code=body.entity_code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MODULE B — Add fixed asset
# =========================================================================


class AddAssetRequest(BaseModel):
    entity_code: str
    asset_code: str
    description: str
    fixed_asset_class_id: str
    acquisition_date: str
    cost: float
    opening_nbv: float | None = None
    opening_nbv_date: str | None = None
    notes: str | None = None
    actor_email: str


@router.post("/add-asset")
def post_add_asset(
    body: AddAssetRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return add_fixed_asset(
                session,
                entity_code=body.entity_code,
                asset_code=body.asset_code,
                description=body.description,
                fixed_asset_class_id=body.fixed_asset_class_id,
                acquisition_date=_parse_date("acquisition_date", body.acquisition_date),
                cost=Decimal(str(body.cost)),
                opening_nbv=Decimal(str(body.opening_nbv)) if body.opening_nbv is not None else None,
                opening_nbv_date=(
                    _parse_date("opening_nbv_date", body.opening_nbv_date)
                    if body.opening_nbv_date else None
                ),
                notes=body.notes,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MODULE B — Module A feed: per-class monthly amounts
# =========================================================================


@router.get("/monthly-amounts")
def get_monthly_amounts(
    entity_code: str = Query(...),
    period_end: str = Query(...),
) -> dict[str, Any]:
    """Return per-class monthly depreciation amounts for a given period.
    This is the primary feed for Module A (recurring entry engine).
    """
    period_end_d = _parse_date("period_end", period_end)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(status_code=404, detail=f"Entity not found: {entity_code}")
            items = compute_monthly_depreciation_by_class(
                session, entity_id=entity["id"], period_end=period_end_d
            )
            total = sum((i["amount"] for i in items), Decimal("0"))
            return {
                "entity_code": entity_code,
                "period_end": period_end_d.isoformat(),
                "class_count": len(items),
                "grand_total_monthly": str(total),
                "classes": [
                    {
                        "class_id": i["class_id"],
                        "class_code": i["class_code"],
                        "class_name": i["class_name"],
                        "expense_account": i["expense_account"],
                        "accum_account": i["accum_account"],
                        "amount": str(i["amount"]),
                    }
                    for i in items
                ],
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MODULE B — Disposal
# =========================================================================


class DisposeRequest(BaseModel):
    entity_code: str
    fixed_asset_id: str
    disposal_date: str
    proceeds: float = 0.0
    proceeds_account: str = ""
    gain_account: str = ""
    loss_account: str = ""
    actor_email: str
    dry_run: bool = False


@router.post("/dispose")
def post_dispose(
    body: DisposeRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Post a disposal journal entry.
    IMPORTANT: gain_account and loss_account must exist in the entity's
    chart of accounts. For Bridlewood, these accounts (4020/6950) do NOT
    exist as of 2026-06-06 — verify before use.
    """
    enforce_entity_code(_user, body.entity_code)
    disposal_date = _parse_date("disposal_date", body.disposal_date)
    try:
        with db_session() as session:
            return post_disposal_journal(
                session,
                entity_code=body.entity_code,
                fixed_asset_id=body.fixed_asset_id,
                disposal_date=disposal_date,
                proceeds=Decimal(str(body.proceeds)),
                proceeds_account=body.proceeds_account,
                gain_account=body.gain_account,
                loss_account=body.loss_account,
                actor_email=body.actor_email,
                dry_run=body.dry_run,
            )
    except MissingGainLossAccountError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# =========================================================================
# MODULE B — Excel schedule download
# =========================================================================


@router.get("/schedule/excel")
def get_schedule_excel(
    entity_code: str = Query(...),
    fiscal_year: int = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> Any:
    """Generate the fixed-asset Excel schedule and return a presigned R2 URL.
    Falls back to inline bytes if R2 is not configured."""
    enforce_entity_code(_user, entity_code)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(status_code=404, detail=f"Entity not found: {entity_code}")
            xlsx_bytes = generate_excel_schedule(
                session, entity_id=entity["id"], fiscal_year=fiscal_year
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Upload to R2 and return presigned URL
    from ..services_storage import content_type_for, storage_service
    filename = f"fixed_assets_FY{fiscal_year}_{entity_code}.xlsx"
    r2_key = storage_service.upload_file(
        file_bytes=xlsx_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="fixed-assets",
        content_type=content_type_for(filename),
    )
    if r2_key:
        url = storage_service.get_presigned_url(r2_key, expires_in=3600)
        return {"url": url, "r2_key": r2_key, "filename": filename}

    # Fallback: return bytes directly
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
