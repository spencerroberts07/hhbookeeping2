"""
Bank PDF importer (TD Canada Trust) — HTTP routes.

Endpoints:
    POST /api/bank-pdf/preview
    POST /api/bank-pdf/upload
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_bank_pdf import (
    preview_bank_pdf_import,
    run_bank_pdf_import,
)
from ..services_period_close import PeriodLockedError


router = APIRouter(prefix="/api/bank-pdf", tags=["bank-pdf"])


@router.post("/preview")
async def post_preview(
    entity_code: str = Form(...),
    file: UploadFile = File(...),
    source_account_code: str | None = Form(default=None),
    source_account_name: str | None = Form(default=None),
    sample_limit: int = Form(default=25),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    file_bytes = await file.read()
    try:
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        with db_session() as session:
            # Entity gate — warn-only for bank statements (formats vary
            # too much to block confidently).
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="bank_pdf",
            ), None)
            return preview_bank_pdf_import(
                session=session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "statement.pdf",
                source_account_code=source_account_code,
                source_account_name=source_account_name,
                sample_limit=int(sample_limit),
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/upload")
async def post_upload(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    source_account_code: str | None = Form(default=None),
    source_account_name: str | None = Form(default=None),
    note: str | None = Form(default=None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    file_bytes = await file.read()
    try:
        from ..services_entity_validation import (
            raise_or_warn as _raise_or_warn,
            validate_document_entity as _validate_entity,
        )
        from ..services_storage import (
            content_type_for as _ct_for,
            storage_service as _r2,
        )
        from sqlalchemy import text as _text
        with db_session() as session:
            _raise_or_warn(_validate_entity(
                session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                filename=file.filename or "",
                document_type="bank_pdf",
            ), None)

            # R2 archive (best-effort) — failure doesn't block the parse.
            r2_key = _r2.upload_file(
                file_bytes=file_bytes,
                original_filename=file.filename or "statement.pdf",
                entity_code=entity_code,
                document_type="bank-pdf",
                content_type=_ct_for(file.filename or "statement.pdf"),
            )

            result = run_bank_pdf_import(
                session=session,
                entity_code=entity_code,
                file_bytes=file_bytes,
                file_name=file.filename or "statement.pdf",
                source_account_code=source_account_code,
                source_account_name=source_account_name,
                actor_email=actor_email,
                note=note,
            )

            # Write a bank_pdf_imports row so /documents (Section H)
            # can surface this upload. Bank PDFs previously had no
            # per-file row — they parsed straight into bank_transactions.
            try:
                entity_row = session.execute(
                    _text("SELECT id FROM entities WHERE entity_code = :ec"),
                    {"ec": entity_code},
                ).mappings().first()
                if entity_row:
                    inserted = (
                        result.get("inserted_count")
                        or result.get("transactions_inserted")
                        or 0
                    ) if isinstance(result, dict) else 0
                    duplicates = (
                        result.get("duplicate_count")
                        or result.get("transactions_duplicate")
                        or 0
                    ) if isinstance(result, dict) else 0
                    session.execute(
                        _text(
                            """
                            INSERT INTO bank_pdf_imports (
                                entity_id, source_account_code,
                                source_account_name, file_name,
                                file_path, transactions_parsed,
                                transactions_inserted,
                                transactions_duplicate,
                                status, actor_email
                            ) VALUES (
                                :eid, :sac, :san, :fn, :fp, :tp, :ti, :td,
                                'complete', :ae
                            )
                            """
                        ),
                        {
                            "eid": entity_row["id"],
                            "sac": source_account_code,
                            "san": source_account_name,
                            "fn": file.filename or "statement.pdf",
                            "fp": r2_key,
                            "tp": int(inserted) + int(duplicates),
                            "ti": int(inserted),
                            "td": int(duplicates),
                            "ae": actor_email,
                        },
                    )
            except Exception:
                import logging as _l
                _l.getLogger(__name__).exception(
                    "bank_pdf_imports insert failed for %s — non-fatal",
                    entity_code,
                )

            # Pending-intent matcher (D2) — see bank_csv.py for context.
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
