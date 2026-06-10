"""
Vendor EFT payment file routes.

POST /api/vendor-eft/preview   — dry-run: totals + missing-banking list
POST /api/vendor-eft/generate  — build CPA-005 file, flip to payment_pending
GET  /api/vendor-eft/{file_id} — file metadata
GET  /api/vendor-eft/{file_id}/download — presigned R2 URL

Vendor banking CRUD (on the vendor master):
GET  /api/vendor-eft/vendors            — list all vendors for entity
GET  /api/vendor-eft/vendors/{id}       — one vendor
PUT  /api/vendor-eft/vendors/{id}/banking   — set transit/institution/account
PUT  /api/vendor-eft/vendors/{id}/email     — set remittance email

All endpoints: bookkeeper+ role, entity-scoped, dry_run guard always on.
HH AP (2030) is excluded at the service layer.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel

from ..services_auth import CurrentUser, enforce_entity_code, require_role
from ..database import db_session
from ..services_vendor_eft import (
    build_vendor_payment_file,
    preview_vendor_payment_file,
)
from ..services_vendor_master import (
    compute_profile_confidence,
    get_vendor,
    list_vendors,
    set_vendor_banking,
    set_vendor_email,
)

_log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/vendor-eft", tags=["vendor-eft"])


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class PreviewRequest(BaseModel):
    entity_code: str
    invoice_ids: list[str]


class GenerateRequest(BaseModel):
    entity_code: str
    invoice_ids: list[str]
    payment_date: date


class SetBankingRequest(BaseModel):
    entity_code: str
    transit: str
    institution: str
    account: str
    eft_transaction_type: str | None = None


class SetEmailRequest(BaseModel):
    entity_code: str
    email: str | None


# ---------------------------------------------------------------------------
# Helper: resolve entity_id from entity_code
# ---------------------------------------------------------------------------

def _resolve_entity_id(session, entity_code: str) -> UUID:
    from sqlalchemy import text
    row = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_code}' not found")
    return UUID(str(row["id"]))


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

@router.post("/preview")
async def vendor_payment_preview(
    body: PreviewRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Dry-run preview: totals, vendor count, missing-banking list.

    The frontend uses this response to prompt the user to enter banking
    details for any vendor in missing_banking before calling /generate.
    """
    enforce_entity_code(_user, body.entity_code)
    if not body.invoice_ids:
        raise HTTPException(status_code=400, detail="invoice_ids must not be empty")

    with db_session() as session:
        entity_id = _resolve_entity_id(session, body.entity_code)
        try:
            result = preview_vendor_payment_file(
                session,
                entity_id=entity_id,
                invoice_ids=body.invoice_ids,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    return result


# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

@router.post("/generate")
async def vendor_payment_generate(
    body: GenerateRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Build the CPA-005 EFT credit file, upload to R2, and flip all
    included invoices to payment_pending.

    Returns the file metadata and a presigned download URL.
    Response always includes dry_run=True — file is NEVER auto-submitted.
    """
    enforce_entity_code(_user, body.entity_code)
    if not body.invoice_ids:
        raise HTTPException(status_code=400, detail="invoice_ids must not be empty")

    actor_email = (
        _user.email if isinstance(_user, CurrentUser) else str(_user)
    )

    with db_session() as session:
        entity_id = _resolve_entity_id(session, body.entity_code)
        try:
            result = build_vendor_payment_file(
                session,
                entity_id=entity_id,
                invoice_ids=body.invoice_ids,
                payment_date=body.payment_date,
                actor_email=actor_email,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    return result


# ---------------------------------------------------------------------------
# File metadata + download
# ---------------------------------------------------------------------------

@router.get("/files")
async def list_vendor_eft_files(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> list[dict[str, Any]]:
    """List EFT files generated for this entity, newest first."""
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text

    with db_session() as session:
        entity_id = _resolve_entity_id(session, entity_code)
        rows = session.execute(
            text("""
                SELECT id, file_name, file_path, record_count, total_amount,
                       file_creation_number, payment_date, vendor_count,
                       invoice_ids, status, actor_email, generated_at
                FROM vendor_eft_files
                WHERE entity_id = :eid
                ORDER BY generated_at DESC
                LIMIT 50
            """),
            {"eid": str(entity_id)},
        ).mappings().all()
    return [dict(r) for r in rows]


@router.get("/files/{file_id}")
async def get_vendor_eft_file(
    file_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Get metadata for one EFT file."""
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text

    with db_session() as session:
        entity_id = _resolve_entity_id(session, entity_code)
        row = session.execute(
            text("""
                SELECT id, entity_id, file_name, file_path, record_count, total_amount,
                       file_creation_number, payment_date, vendor_count,
                       invoice_ids, summary_json, status, actor_email, generated_at
                FROM vendor_eft_files
                WHERE id = :fid AND entity_id = :eid
            """),
            {"fid": file_id, "eid": str(entity_id)},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="EFT file not found")
    return dict(row)


@router.get("/files/{file_id}/download")
async def download_vendor_eft_file(
    file_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Return a presigned R2 download URL for the EFT file (1-hour TTL)."""
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text
    from ..services_storage import storage_service

    with db_session() as session:
        entity_id = _resolve_entity_id(session, entity_code)
        row = session.execute(
            text("""
                SELECT id, file_name, file_path
                FROM vendor_eft_files
                WHERE id = :fid AND entity_id = :eid
            """),
            {"fid": file_id, "eid": str(entity_id)},
        ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="EFT file not found")
    if not row["file_path"]:
        raise HTTPException(status_code=404, detail="File not available in R2 (upload may have failed)")

    url = storage_service.get_presigned_url(row["file_path"], expires_in=3600)
    return {
        "file_id": file_id,
        "file_name": row["file_name"],
        "download_url": url,
        "expires_in_seconds": 3600,
    }


# ---------------------------------------------------------------------------
# Vendor master CRUD
# ---------------------------------------------------------------------------

@router.get("/vendors")
async def list_entity_vendors(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> list[dict[str, Any]]:
    """List all vendor master records for this entity, sorted by name."""
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity_id = _resolve_entity_id(session, entity_code)
        vendors = list_vendors(session, entity_id=entity_id)
    return vendors


@router.get("/vendors/{vendor_id}")
async def get_entity_vendor(
    vendor_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Get one vendor master record."""
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity_id = _resolve_entity_id(session, entity_code)
        vendor = get_vendor(session, entity_id=entity_id, vendor_id=UUID(vendor_id))
    if not vendor:
        raise HTTPException(status_code=404, detail="Vendor not found")
    return vendor


@router.put("/vendors/{vendor_id}/banking")
async def set_vendor_banking_details(
    vendor_id: str = Path(...),
    body: SetBankingRequest = ...,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Set EFT banking details for a vendor (transit/institution/account).
    Stamps banking_confirmed_at. Saved once, auto-populated in all future
    payment files.
    """
    enforce_entity_code(_user, body.entity_code)
    if not body.transit or not body.institution or not body.account:
        raise HTTPException(status_code=400, detail="transit, institution, and account are all required")

    actor_email = (
        _user.email if isinstance(_user, CurrentUser) else str(_user)
    )

    with db_session() as session:
        entity_id = _resolve_entity_id(session, body.entity_code)
        vendor = get_vendor(session, entity_id=entity_id, vendor_id=UUID(vendor_id))
        if not vendor:
            raise HTTPException(status_code=404, detail="Vendor not found")
        updated = set_vendor_banking(
            session,
            vendor_id=UUID(vendor_id),
            transit=body.transit,
            institution=body.institution,
            account=body.account,
            eft_transaction_type=body.eft_transaction_type,
            actor_email=actor_email,
        )
    updated["profile_confidence_computed"] = float(compute_profile_confidence(updated))
    return updated


@router.put("/vendors/{vendor_id}/email")
async def set_vendor_remittance_email(
    vendor_id: str = Path(...),
    body: SetEmailRequest = ...,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Set the remittance advice email for a vendor."""
    enforce_entity_code(_user, body.entity_code)
    with db_session() as session:
        entity_id = _resolve_entity_id(session, body.entity_code)
        vendor = get_vendor(session, entity_id=entity_id, vendor_id=UUID(vendor_id))
        if not vendor:
            raise HTTPException(status_code=404, detail="Vendor not found")
        updated = set_vendor_email(session, vendor_id=UUID(vendor_id), email=body.email)
    updated["profile_confidence_computed"] = float(compute_profile_confidence(updated))
    return updated
