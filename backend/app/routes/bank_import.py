"""
Adapter-agnostic bank statement import (Phase 3A).

POST /api/bank-import/preview — parse only, return a summary + sample (no write).
POST /api/bank-import/upload  — parse, archive to R2, ingest into bank_transactions.

Auto-detects format (CSV/Excel/OFX/PDF) and falls back to the AI parser. The
existing /api/bank-pdf/* and /api/bank-csv/* routes stay for the current UI.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from ..db import db_session
from ..services_auth import require_role
from ..services_bank_ingest import dispatch, ingest

router = APIRouter(prefix="/api/bank-import", tags=["bank-import"])


def _summary(result, ingest_result: dict | None = None) -> dict[str, Any]:
    out = {
        "source_system": result.meta.source_system,
        "parser_confidence": result.parser_confidence,
        "parsed": len(result.transactions),
        "opening_balance": float(result.meta.opening_balance) if result.meta.opening_balance is not None else None,
        "closing_balance": float(result.meta.closing_balance) if result.meta.closing_balance is not None else None,
        "period_start": result.meta.period_start.isoformat() if result.meta.period_start else None,
        "period_end": result.meta.period_end.isoformat() if result.meta.period_end else None,
        "warnings": result.warnings,
        "sample": [
            {"date": t.transaction_date.isoformat() if t.transaction_date else None,
             "amount": float(t.amount), "direction": t.signed_direction(),
             "description": t.description}
            for t in result.transactions[:10]
        ],
    }
    if ingest_result:
        out.update({k: ingest_result[k] for k in ("inserted", "duplicate", "tie_out_ok", "tie_out_variance")})
    return out


@router.post("/preview")
async def preview(
    entity_code: str = Form(...),
    source_account_code: str = Form("1020"),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    data = await file.read()
    result = dispatch(file_bytes=data, filename=file.filename or "statement",
                      content_type=file.content_type or "", entity_code=entity_code,
                      hints={"source_account_code": source_account_code})
    ok, var = result.tie_out()
    s = _summary(result)
    s["tie_out_ok"] = ok
    s["tie_out_variance"] = float(var) if var is not None else None
    return s


@router.post("/upload")
async def upload(
    entity_code: str = Form(...),
    source_account_code: str = Form("1020"),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    data = await file.read()
    result = dispatch(file_bytes=data, filename=file.filename or "statement",
                      content_type=file.content_type or "", entity_code=entity_code,
                      hints={"source_account_code": source_account_code})
    if not result.transactions:
        raise HTTPException(422, f"No transactions parsed. {'; '.join(result.warnings) or 'Unknown format.'}")

    # archive to R2 (best-effort; never blocks the DB write)
    r2_key = None
    try:
        from ..services_storage import storage_service as _r2
        r2_key = _r2.upload_file(
            file_bytes=data, original_filename=file.filename or "statement",
            entity_code=entity_code, document_type="bank-statements",
            content_type=file.content_type or "application/octet-stream",
        )
    except Exception:
        r2_key = None

    with db_session() as session:
        ing = ingest(session, entity_code=entity_code, result=result,
                     source_account_code=source_account_code,
                     actor_email=_actor(_user))
    out = _summary(result, ing)
    out["r2_object_key"] = r2_key
    return out


def _actor(user: Any) -> str | None:
    try:
        return user.get("email")
    except AttributeError:
        return getattr(user, "email", None)
