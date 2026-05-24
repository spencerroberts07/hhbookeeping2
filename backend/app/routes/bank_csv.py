"""
Bank CSV upload fallback module — HTTP routes.

Endpoints:
    GET  /api/bank-csv/mapping-profiles
    POST /api/bank-csv/preview            (multipart upload)
    POST /api/bank-csv/upload             (multipart upload)
    GET  /api/bank-csv/import-runs
    GET  /api/bank-csv/import-runs/{id}

This module is the next-step fallback for the QBO bank sync, which only
imports QBO-posted bank activity. CSV-imported transactions land in the
same bank_transactions table (source_system='statement_csv') and feed
the existing matching modules (HH remittance, card settlement, direct
vendor AP, etc.) automatically.
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_bank_csv import (
    get_bank_csv_import_run_detail,
    list_bank_csv_import_runs,
    list_bank_csv_mapping_profiles,
    preview_bank_csv_import,
    run_bank_csv_import,
)
from ..services_period_close import PeriodLockedError


router = APIRouter(prefix="/api/bank-csv", tags=["bank-csv"])


def _parse_column_map_json(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"column_map_json is not valid JSON: {exc}",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail="column_map_json must be a JSON object",
        )
    return parsed


@router.get("/mapping-profiles")
def get_mapping_profiles() -> dict[str, Any]:
    """Return all built-in mapping profiles and how to use column_map_json."""
    return list_bank_csv_mapping_profiles()


@router.post("/preview")
async def bank_csv_preview(
    entity_code: str = Form(...),
    file: UploadFile = File(...),
    mapping_profile: str = Form("generic"),
    source_account_code: str | None = Form(default=None),
    source_account_name: str | None = Form(default=None),
    column_map_json: str | None = Form(default=None),
    sample_limit: int = Form(default=20),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Parse the uploaded CSV and report what WOULD happen on import,
    without inserting anything. Use this before /upload.
    """
    enforce_entity_code(_user, entity_code)
    try:
        file_bytes = await file.read()
        column_map_override = _parse_column_map_json(column_map_json)
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="bank_csv",
            ), None)
            return preview_bank_csv_import(
                session=session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "uploaded.csv",
                mapping_profile=mapping_profile,
                source_account_code=source_account_code,
                source_account_name=source_account_name,
                column_map_override=column_map_override,
                sample_limit=int(sample_limit),
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/upload")
async def bank_csv_upload(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    mapping_profile: str = Form("generic"),
    source_account_code: str | None = Form(default=None),
    source_account_name: str | None = Form(default=None),
    column_map_json: str | None = Form(default=None),
    note: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Import the uploaded CSV into bank_transactions.
    Idempotent: re-uploading the same file results in 0 inserts and
    counts the rows as duplicates instead.
    """
    enforce_entity_code(_user, entity_code)
    try:
        file_bytes = await file.read()
        column_map_override = _parse_column_map_json(column_map_json)
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        # R2 archive — best-effort.
        from ..services_storage import content_type_for, storage_service
        from sqlalchemy import text as _text
        object_key = storage_service.upload_file(
            file_bytes=file_bytes,
            original_filename=file.filename or "uploaded.csv",
            entity_code=entity_code,
            document_type="bank-csv",
            content_type=content_type_for(file.filename or ""),
        )
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="bank_csv",
            ), None)
            result = run_bank_csv_import(
                session=session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "uploaded.csv",
                mapping_profile=mapping_profile,
                source_account_code=source_account_code,
                source_account_name=source_account_name,
                column_map_override=column_map_override,
                actor_email=actor_email,
                note=note,
            )
            run_id = result.get("run_id") if isinstance(result, dict) else None
            if object_key and run_id:
                session.execute(
                    _text(
                        "UPDATE bank_csv_import_runs SET file_path = :p "
                        "WHERE id = :id"
                    ),
                    {"p": object_key, "id": run_id},
                )

            # Pending-intent matcher (D2). Tries to reconcile any
            # assistant notes ("I paid $X for Y on date Z") against the
            # bank transactions we just imported. Non-fatal on failure.
            try:
                from ..services_assistant import check_pending_intents as _cpi
                _cpi(session, entity_code)
            except Exception:
                import logging as _l
                _l.getLogger(__name__).exception(
                    "check_pending_intents failed for %s — non-fatal",
                    entity_code,
                )

            return result
    except HTTPException:
        raise
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/import-runs")
def get_bank_csv_import_runs(
    entity_code: str = Query(...),
    limit: int = Query(default=50, ge=1, le=500),
    source_account_code: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_bank_csv_import_runs(
                session=session,
                entity_code=entity_code,
                limit=limit,
                source_account_code=source_account_code,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/import-runs/{run_id}")
def get_bank_csv_import_run(
    run_id: str = Path(...),
    entity_code: str = Query(...),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_bank_csv_import_run_detail(
                session=session,
                entity_code=entity_code,
                run_id=run_id,
            )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
