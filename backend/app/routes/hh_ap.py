import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/hh-ap", tags=["hh-ap"])


def money(value) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"))


def money_float(value: Decimal | None) -> float:
    if value is None:
        return 0.0
    return float(value.quantize(Decimal("0.01")))


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def normalize_invoice_number(value: str | None) -> str | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    return cleaned.upper()


def build_source_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


def try_extract_text(file_bytes: bytes, filename: str, content_type: str | None) -> str | None:
    suffix = Path(filename).suffix.lower()
    is_text_like = (
        suffix in {".txt", ".csv", ".json", ".xml"}
        or (content_type or "").startswith("text/")
    )

    if not is_text_like:
        return None

    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            text_value = file_bytes.decode(encoding)
            return text_value[:200000]
        except Exception:
            continue

    return None


def get_entity(session, entity_code: str):
    entity = session.execute(
        text(
            """
            SELECT id, entity_code, entity_name
            FROM entities
            WHERE entity_code = :entity_code
            """
        ),
        {"entity_code": entity_code},
    ).mappings().first()

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found")

    return entity


def get_statement_by_month_end(session, entity_id: str, statement_month_end: str | None):
    if statement_month_end:
        statement = session.execute(
            text(
                """
                SELECT
                    id,
                    statement_date,
                    statement_month_end,
                    total_open_balance,
                    raw_json,
                    created_at,
                    updated_at
                FROM hh_ap_statements
                WHERE entity_id = :entity_id
                  AND statement_month_end = :statement_month_end
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {
                "entity_id": entity_id,
                "statement_month_end": statement_month_end,
            },
        ).mappings().first()
    else:
        statement = session.execute(
            text(
                """
                SELECT
                    id,
                    statement_date,
                    statement_month_end,
                    total_open_balance,
                    raw_json,
                    created_at,
                    updated_at
                FROM hh_ap_statements
                WHERE entity_id = :entity_id
                ORDER BY statement_month_end DESC NULLS LAST, created_at DESC
                LIMIT 1
                """
            ),
            {"entity_id": entity_id},
        ).mappings().first()

    return statement


class HHAPInvoiceInput(BaseModel):
    invoice_number: str = Field(...)
    invoice_type: str = Field(...)
    vendor_name: str | None = None
    vendor_invoice_number: str | None = None
    po_number: str | None = None
    invoice_date: date | None = None
    due_date: date | None = None
    remittance_due_date: date | None = None
    currency_code: str = "CAD"
    subtotal: Decimal | None = None
    hst_amount: Decimal | None = None
    surcharge_amount: Decimal | None = None
    advertising_amount: Decimal | None = None
    subscribed_shares_amount: Decimal | None = None
    five_year_note_amount: Decimal | None = None
    total_amount: Decimal | None = None
    is_statement_only: bool = False
    notes: str | None = None
    raw_json: dict[str, Any] = Field(default_factory=dict)


class HHAPInvoiceUpsertRequest(BaseModel):
    entity_code: str
    document_id: str | None = None
    invoices: list[HHAPInvoiceInput] = Field(default_factory=list)


class HHAPRemittanceLineInput(BaseModel):
    invoice_number: str | None = None
    line_description: str | None = None
    due_date: date | None = None
    line_amount: Decimal
    raw_json: dict[str, Any] = Field(default_factory=dict)


class HHAPRemittanceUpsertRequest(BaseModel):
    entity_code: str
    document_id: str | None = None
    remittance_reference: str | None = None
    remittance_date: date | None = None
    withdrawal_date: date | None = None
    total_amount: Decimal | None = None
    raw_json: dict[str, Any] = Field(default_factory=dict)
    lines: list[HHAPRemittanceLineInput] = Field(default_factory=list)


class HHAPStatementLineInput(BaseModel):
    invoice_number: str | None = None
    invoice_type: str | None = None
    invoice_date: date | None = None
    due_date: date | None = None
    invoice_amount: Decimal | None = None
    open_amount: Decimal | None = None
    current_amount: Decimal | None = None
    past_due_amount: Decimal | None = None
    raw_json: dict[str, Any] = Field(default_factory=dict)


class HHAPStatementUpsertRequest(BaseModel):
    entity_code: str
    document_id: str | None = None
    statement_date: date | None = None
    statement_month_end: date
    total_open_balance: Decimal | None = None
    raw_json: dict[str, Any] = Field(default_factory=dict)
    lines: list[HHAPStatementLineInput] = Field(default_factory=list)


class HHAPMatchRunRequest(BaseModel):
    entity_code: str
    statement_month_end: date | None = None


@router.post("/upload-documents")
async def hh_ap_upload_documents(
    entity_code: str = Form(...),
    document_type: str = Form(...),
    document_date: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
):
    with db_session() as session:
        entity = get_entity(session, entity_code)

        inserted_documents: list[dict] = []
        duplicate_documents: list[dict] = []

        for upload in files:
            file_bytes = await upload.read()
            if not file_bytes:
                continue

            source_hash = build_source_hash(file_bytes)
            extracted_text = try_extract_text(
                file_bytes=file_bytes,
                filename=upload.filename or "unknown",
                content_type=upload.content_type,
            )

            existing = session.execute(
                text(
                    """
                    SELECT id, source_filename, document_type
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND document_type = :document_type
                      AND source_hash = :source_hash
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_type": document_type,
                    "source_hash": source_hash,
                },
            ).mappings().first()

            if existing:
                duplicate_documents.append(
                    {
                        "id": str(existing["id"]),
                        "source_filename": existing["source_filename"],
                        "document_type": existing["document_type"],
                    }
                )
                continue

            processing_status = (
                "uploaded_text_ready" if extracted_text else "uploaded_pending_parse"
            )

            doc_row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_documents (
                        entity_id,
                        document_type,
                        source_filename,
                        source_hash,
                        document_date,
                        upload_source,
                        processing_status,
                        extracted_text,
                        raw_json
                    ) VALUES (
                        :entity_id,
                        :document_type,
                        :source_filename,
                        :source_hash,
                        :document_date,
                        'manual_upload',
                        :processing_status,
                        :extracted_text,
                        CAST(:raw_json AS jsonb)
                    )
                    RETURNING id, created_at
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_type": document_type,
                    "source_filename": upload.filename or "unknown",
                    "source_hash": source_hash,
                    "document_date": document_date,
                    "processing_status": processing_status,
                    "extracted_text": extracted_text,
                    "raw_json": json.dumps(
                        {
                            "content_type": upload.content_type,
                            "file_size_bytes": len(file_bytes),
                        }
                    ),
                },
            ).mappings().first()

            inserted_documents.append(
                {
                    "id": str(doc_row["id"]),
                    "source_filename": upload.filename or "unknown",
                    "document_type": document_type,
                    "processing_status": processing_status,
                    "created_at": doc_row["created_at"].isoformat() if doc_row["created_at"] else None,
                }
            )

        return {
            "entity_code": entity["entity_code"],
            "document_type": document_type,
            "inserted_count": len(inserted_documents),
            "duplicate_count": len(duplicate_documents),
            "inserted_documents": inserted_documents,
            "duplicate_documents": duplicate_documents,
        }


@router.post("/invoices/upsert")
def hh_ap_invoices_upsert(payload: HHAPInvoiceUpsertRequest):
    if not payload.invoices:
        raise HTTPException(status_code=400, detail="At least one invoice is required")

    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        upserted: list[dict] = []

        for invoice in payload.invoices:
            invoice_number = normalize_invoice_number(invoice.invoice_number)
            invoice_type = normalize_text(invoice.invoice_type)

            if not invoice_number or not invoice_type:
                raise HTTPException(
                    status_code=400,
                    detail="invoice_number and invoice_type are required on every invoice",
                )

            row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_invoices (
                        entity_id,
                        document_id,
                        invoice_number,
                        invoice_type,
                        vendor_name,
                        vendor_invoice_number,
                        po_number,
                        invoice_date,
                        due_date,
                        remittance_due_date,
                        currency_code,
                        subtotal,
                        hst_amount,
                        surcharge_amount,
                        advertising_amount,
                        subscribed_shares_amount,
                        five_year_note_amount,
                        total_amount,
                        match_status,
                        is_statement_only,
                        notes,
                        raw_json
                    ) VALUES (
                        :entity_id,
                        :document_id,
                        :invoice_number,
                        :invoice_type,
                        :vendor_name,
                        :vendor_invoice_number,
                        :po_number,
                        :invoice_date,
                        :due_date,
                        :remittance_due_date,
                        :currency_code,
                        :subtotal,
                        :hst_amount,
                        :surcharge_amount,
                        :advertising_amount,
                        :subscribed_shares_amount,
                        :five_year_note_amount,
                        :total_amount,
                        :match_status,
                        :is_statement_only,
                        :notes,
                        CAST(:raw_json AS jsonb)
                    )
                    ON CONFLICT (entity_id, invoice_number, invoice_type)
                    DO UPDATE SET
                        document_id = COALESCE(EXCLUDED.document_id, hh_ap_invoices.document_id),
                        vendor_name = EXCLUDED.vendor_name,
                        vendor_invoice_number = EXCLUDED.vendor_invoice_number,
                        po_number = EXCLUDED.po_number,
                        invoice_date = EXCLUDED.invoice_date,
                        due_date = EXCLUDED.due_date,
                        remittance_due_date = EXCLUDED.remittance_due_date,
                        currency_code = EXCLUDED.currency_code,
                        subtotal = EXCLUDED.subtotal,
                        hst_amount = EXCLUDED.hst_amount,
                        surcharge_amount = EXCLUDED.surcharge_amount,
                        advertising_amount = EXCLUDED.advertising_amount,
                        subscribed_shares_amount = EXCLUDED.subscribed_shares_amount,
                        five_year_note_amount = EXCLUDED.five_year_note_amount,
                        total_amount = EXCLUDED.total_amount,
                        is_statement_only = EXCLUDED.is_statement_only,
                        notes = EXCLUDED.notes,
                        raw_json = EXCLUDED.raw_json,
                        updated_at = NOW()
                    RETURNING id, invoice_number, invoice_type, updated_at
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": payload.document_id,
                    "invoice_number": invoice_number,
                    "invoice_type": invoice_type,
                    "vendor_name": normalize_text(invoice.vendor_name),
                    "vendor_invoice_number": normalize_text(invoice.vendor_invoice_number),
                    "po_number": normalize_text(invoice.po_number),
                    "invoice_date": invoice.invoice_date,
                    "due_date": invoice.due_date,
                    "remittance_due_date": invoice.remittance_due_date,
                    "currency_code": normalize_text(invoice.currency_code) or "CAD",
                    "subtotal": invoice.subtotal,
                    "hst_amount": invoice.hst_amount,
                    "surcharge_amount": invoice.surcharge_amount,
                    "advertising_amount": invoice.advertising_amount,
                    "subscribed_shares_amount": invoice.subscribed_shares_amount,
                    "five_year_note_amount": invoice.five_year_note_amount,
                    "total_amount": invoice.total_amount,
                    "match_status": "unmatched",
                    "is_statement_only": invoice.is_statement_only,
                    "notes": normalize_text(invoice.notes),
                    "raw_json": json.dumps(invoice.raw_json or {}),
                },
            ).mappings().first()

            upserted.append(
                {
                    "id": str(row["id"]),
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                }
            )

        return {
            "entity_code": entity["entity_code"],
            "upserted_count": len(upserted),
            "upserted_invoices": upserted,
        }


@router.post("/remittances/upsert")
def hh_ap_remittances_upsert(payload: HHAPRemittanceUpsertRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        remittance = None

        if payload.document_id:
            remittance = session.execute(
                text(
                    """
                    SELECT id
                    FROM hh_ap_remittances
                    WHERE entity_id = :entity_id
                      AND document_id = :document_id
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": payload.document_id,
                },
            ).mappings().first()

        if not remittance and (payload.remittance_reference or payload.withdrawal_date):
            remittance = session.execute(
                text(
                    """
                    SELECT id
                    FROM hh_ap_remittances
                    WHERE entity_id = :entity_id
                      AND COALESCE(remittance_reference, '') = COALESCE(:remittance_reference, '')
                      AND withdrawal_date IS NOT DISTINCT FROM :withdrawal_date
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "remittance_reference": normalize_text(payload.remittance_reference),
                    "withdrawal_date": payload.withdrawal_date,
                },
            ).mappings().first()

        if remittance:
            remittance_row = session.execute(
                text(
                    """
                    UPDATE hh_ap_remittances
                    SET document_id = COALESCE(:document_id, document_id),
                        remittance_reference = :remittance_reference,
                        remittance_date = :remittance_date,
                        withdrawal_date = :withdrawal_date,
                        total_amount = :total_amount,
                        raw_json = CAST(:raw_json AS jsonb),
                        updated_at = NOW()
                    WHERE id = :id
                    RETURNING id
                    """
                ),
                {
                    "id": remittance["id"],
                    "document_id": payload.document_id,
                    "remittance_reference": normalize_text(payload.remittance_reference),
                    "remittance_date": payload.remittance_date,
                    "withdrawal_date": payload.withdrawal_date,
                    "total_amount": payload.total_amount,
                    "raw_json": json.dumps(payload.raw_json or {}),
                },
            ).mappings().first()
        else:
            remittance_row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_remittances (
                        entity_id,
                        document_id,
                        remittance_reference,
                        remittance_date,
                        withdrawal_date,
                        total_amount,
                        raw_json
                    ) VALUES (
                        :entity_id,
                        :document_id,
                        :remittance_reference,
                        :remittance_date,
                        :withdrawal_date,
                        :total_amount,
                        CAST(:raw_json AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": payload.document_id,
                    "remittance_reference": normalize_text(payload.remittance_reference),
                    "remittance_date": payload.remittance_date,
                    "withdrawal_date": payload.withdrawal_date,
                    "total_amount": payload.total_amount,
                    "raw_json": json.dumps(payload.raw_json or {}),
                },
            ).mappings().first()

        session.execute(
            text(
                """
                DELETE FROM hh_ap_remittance_lines
                WHERE remittance_id = :remittance_id
                """
            ),
            {"remittance_id": remittance_row["id"]},
        )

        inserted_lines = 0
        for line in payload.lines:
            session.execute(
                text(
                    """
                    INSERT INTO hh_ap_remittance_lines (
                        remittance_id,
                        entity_id,
                        invoice_number,
                        line_description,
                        due_date,
                        line_amount,
                        matched_invoice_id,
                        match_status,
                        raw_json
                    ) VALUES (
                        :remittance_id,
                        :entity_id,
                        :invoice_number,
                        :line_description,
                        :due_date,
                        :line_amount,
                        NULL,
                        'unmatched',
                        CAST(:raw_json AS jsonb)
                    )
                    """
                ),
                {
                    "remittance_id": remittance_row["id"],
                    "entity_id": entity["id"],
                    "invoice_number": normalize_invoice_number(line.invoice_number),
                    "line_description": normalize_text(line.line_description),
                    "due_date": line.due_date,
                    "line_amount": line.line_amount,
                    "raw_json": json.dumps(line.raw_json or {}),
                },
            )
            inserted_lines += 1

        return {
            "entity_code": entity["entity_code"],
            "remittance_id": str(remittance_row["id"]),
            "remittance_line_count": inserted_lines,
        }


@router.post("/statements/upsert")
def hh_ap_statements_upsert(payload: HHAPStatementUpsertRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        statement = None

        if payload.document_id:
            statement = session.execute(
                text(
                    """
                    SELECT id
                    FROM hh_ap_statements
                    WHERE entity_id = :entity_id
                      AND document_id = :document_id
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": payload.document_id,
                },
            ).mappings().first()

        if not statement:
            statement = session.execute(
                text(
                    """
                    SELECT id
                    FROM hh_ap_statements
                    WHERE entity_id = :entity_id
                      AND statement_month_end = :statement_month_end
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "statement_month_end": payload.statement_month_end,
                },
            ).mappings().first()

        if statement:
            statement_row = session.execute(
                text(
                    """
                    UPDATE hh_ap_statements
                    SET document_id = COALESCE(:document_id, document_id),
                        statement_date = :statement_date,
                        statement_month_end = :statement_month_end,
                        total_open_balance = :total_open_balance,
                        raw_json = CAST(:raw_json AS jsonb),
                        updated_at = NOW()
                    WHERE id = :id
                    RETURNING id
                    """
                ),
                {
                    "id": statement["id"],
                    "document_id": payload.document_id,
                    "statement_date": payload.statement_date,
                    "statement_month_end": payload.statement_month_end,
                    "total_open_balance": payload.total_open_balance,
                    "raw_json": json.dumps(payload.raw_json or {}),
                },
            ).mappings().first()
        else:
            statement_row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_statements (
                        entity_id,
                        document_id,
                        statement_date,
                        statement_month_end,
                        total_open_balance,
                        raw_json
                    ) VALUES (
                        :entity_id,
                        :document_id,
                        :statement_date,
                        :statement_month_end,
                        :total_open_balance,
                        CAST(:raw_json AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": payload.document_id,
                    "statement_date": payload.statement_date,
                    "statement_month_end": payload.statement_month_end,
                    "total_open_balance": payload.total_open_balance,
                    "raw_json": json.dumps(payload.raw_json or {}),
                },
            ).mappings().first()

        session.execute(
            text(
                """
                DELETE FROM hh_ap_statement_lines
                WHERE statement_id = :statement_id
                """
            ),
            {"statement_id": statement_row["id"]},
        )

        inserted_lines = 0
        for line in payload.lines:
            session.execute(
                text(
                    """
                    INSERT INTO hh_ap_statement_lines (
                        statement_id,
                        entity_id,
                        invoice_number,
                        invoice_type,
                        invoice_date,
                        due_date,
                        invoice_amount,
                        open_amount,
                        current_amount,
                        past_due_amount,
                        matched_invoice_id,
                        match_status,
                        is_missing_download,
                        raw_json
                    ) VALUES (
                        :statement_id,
                        :entity_id,
                        :invoice_number,
                        :invoice_type,
                        :invoice_date,
                        :due_date,
                        :invoice_amount,
                        :open_amount,
                        :current_amount,
                        :past_due_amount,
                        NULL,
                        'unmatched',
                        FALSE,
                        CAST(:raw_json AS jsonb)
                    )
                    """
                ),
                {
                    "statement_id": statement_row["id"],
                    "entity_id": entity["id"],
                    "invoice_number": normalize_invoice_number(line.invoice_number),
                    "invoice_type": normalize_text(line.invoice_type),
                    "invoice_date": line.invoice_date,
                    "due_date": line.due_date,
                    "invoice_amount": line.invoice_amount,
                    "open_amount": line.open_amount,
                    "current_amount": line.current_amount,
                    "past_due_amount": line.past_due_amount,
                    "raw_json": json.dumps(line.raw_json or {}),
                },
            )
            inserted_lines += 1

        return {
            "entity_code": entity["entity_code"],
            "statement_id": str(statement_row["id"]),
            "statement_line_count": inserted_lines,
            "statement_month_end": str(payload.statement_month_end),
        }


@router.post("/match/run")
def hh_ap_match_run(payload: HHAPMatchRunRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        invoice_rows = session.execute(
            text(
                """
                SELECT id, invoice_number, invoice_type, is_statement_only
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        invoice_map_by_number: dict[str, list[dict]] = {}
        for row in invoice_rows:
            invoice_number = normalize_invoice_number(row["invoice_number"])
            if not invoice_number:
                continue
            invoice_map_by_number.setdefault(invoice_number, []).append(dict(row))

        statement_scope_sql = """
            SELECT id, invoice_number, invoice_type
            FROM hh_ap_statement_lines
            WHERE entity_id = :entity_id
        """
        statement_scope_params: dict[str, Any] = {"entity_id": entity["id"]}

        if payload.statement_month_end:
            statement_scope_sql += """
              AND statement_id IN (
                    SELECT id
                    FROM hh_ap_statements
                    WHERE entity_id = :entity_id
                      AND statement_month_end = :statement_month_end
              )
            """
            statement_scope_params["statement_month_end"] = payload.statement_month_end

        statement_rows = session.execute(
            text(statement_scope_sql),
            statement_scope_params,
        ).mappings().all()

        matched_invoice_ids: set[str] = set()
        matched_statement_count = 0
        missing_download_count = 0

        for row in statement_rows:
            invoice_number = normalize_invoice_number(row["invoice_number"])
            invoice_type = normalize_text(row["invoice_type"])

            candidates = invoice_map_by_number.get(invoice_number or "", [])

            matched_invoice = None
            if invoice_type:
                for candidate in candidates:
                    if normalize_text(candidate["invoice_type"]) == invoice_type:
                        matched_invoice = candidate
                        break

            if not matched_invoice and candidates:
                matched_invoice = candidates[0]

            if matched_invoice:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_statement_lines
                        SET matched_invoice_id = :matched_invoice_id,
                            match_status = 'matched',
                            is_missing_download = FALSE,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "matched_invoice_id": matched_invoice["id"],
                    },
                )
                matched_invoice_ids.add(str(matched_invoice["id"]))
                matched_statement_count += 1
            else:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_statement_lines
                        SET matched_invoice_id = NULL,
                            match_status = 'missing_download',
                            is_missing_download = TRUE,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"]},
                )
                missing_download_count += 1

        remittance_rows = session.execute(
            text(
                """
                SELECT id, invoice_number
                FROM hh_ap_remittance_lines
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        matched_remittance_count = 0
        unmatched_remittance_count = 0

        for row in remittance_rows:
            invoice_number = normalize_invoice_number(row["invoice_number"])
            candidates = invoice_map_by_number.get(invoice_number or "", [])
            matched_invoice = candidates[0] if candidates else None

            if matched_invoice:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_remittance_lines
                        SET matched_invoice_id = :matched_invoice_id,
                            match_status = 'matched',
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "matched_invoice_id": matched_invoice["id"],
                    },
                )
                matched_invoice_ids.add(str(matched_invoice["id"]))
                matched_remittance_count += 1
            else:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_remittance_lines
                        SET matched_invoice_id = NULL,
                            match_status = 'unmatched',
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"]},
                )
                unmatched_remittance_count += 1

        session.execute(
            text(
                """
                UPDATE hh_ap_invoices
                SET match_status = CASE
                    WHEN id::text = ANY(:matched_invoice_ids) THEN 'matched'
                    WHEN is_statement_only = TRUE THEN 'statement_only'
                    ELSE 'unmatched'
                END,
                updated_at = NOW()
                WHERE entity_id = :entity_id
                """
            ),
            {
                "entity_id": entity["id"],
                "matched_invoice_ids": list(matched_invoice_ids),
            },
        )

        return {
            "entity_code": entity["entity_code"],
            "statement_month_end_scope": str(payload.statement_month_end) if payload.statement_month_end else None,
            "matched_statement_line_count": matched_statement_count,
            "missing_download_count": missing_download_count,
            "matched_remittance_line_count": matched_remittance_count,
            "unmatched_remittance_line_count": unmatched_remittance_count,
            "matched_invoice_count": len(matched_invoice_ids),
        }


@router.get("/status")
def hh_ap_status(entity_code: str):
    with db_session() as session:
        entity = get_entity(session, entity_code)

        document_type_counts = session.execute(
            text(
                """
                SELECT document_type, COUNT(*) AS doc_count
                FROM hh_ap_documents
                WHERE entity_id = :entity_id
                GROUP BY document_type
                ORDER BY document_type
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        invoice_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS invoice_count,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_invoice_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_invoice_count,
                    COUNT(*) FILTER (WHERE is_statement_only = TRUE) AS statement_only_invoice_count
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        remittance_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS remittance_count
                FROM hh_ap_remittances
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        remittance_line_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS remittance_line_count,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_remittance_line_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_remittance_line_count
                FROM hh_ap_remittance_lines
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        statement_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS statement_count
                FROM hh_ap_statements
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        statement_line_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS statement_line_count,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_statement_line_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_statement_line_count,
                    COUNT(*) FILTER (WHERE is_missing_download = TRUE) AS missing_download_count
                FROM hh_ap_statement_lines
                WHERE entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        latest_statement = get_statement_by_month_end(
            session=session,
            entity_id=entity["id"],
            statement_month_end=None,
        )

        latest_documents = session.execute(
            text(
                """
                SELECT
                    id,
                    document_type,
                    source_filename,
                    document_date,
                    upload_source,
                    processing_status,
                    created_at
                FROM hh_ap_documents
                WHERE entity_id = :entity_id
                ORDER BY created_at DESC
                LIMIT 10
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        return {
            "entity_code": entity["entity_code"],
            "entity_name": entity["entity_name"],
            "document_counts_by_type": [
                {
                    "document_type": row["document_type"],
                    "count": int(row["doc_count"]),
                }
                for row in document_type_counts
            ],
            "invoice_summary": {
                "invoice_count": int((invoice_summary or {}).get("invoice_count", 0) or 0),
                "matched_invoice_count": int((invoice_summary or {}).get("matched_invoice_count", 0) or 0),
                "unmatched_invoice_count": int((invoice_summary or {}).get("unmatched_invoice_count", 0) or 0),
                "statement_only_invoice_count": int((invoice_summary or {}).get("statement_only_invoice_count", 0) or 0),
            },
            "remittance_summary": {
                "remittance_count": int((remittance_summary or {}).get("remittance_count", 0) or 0),
                "remittance_line_count": int((remittance_line_summary or {}).get("remittance_line_count", 0) or 0),
                "matched_remittance_line_count": int((remittance_line_summary or {}).get("matched_remittance_line_count", 0) or 0),
                "unmatched_remittance_line_count": int((remittance_line_summary or {}).get("unmatched_remittance_line_count", 0) or 0),
            },
            "statement_summary": {
                "statement_count": int((statement_summary or {}).get("statement_count", 0) or 0),
                "statement_line_count": int((statement_line_summary or {}).get("statement_line_count", 0) or 0),
                "matched_statement_line_count": int((statement_line_summary or {}).get("matched_statement_line_count", 0) or 0),
                "unmatched_statement_line_count": int((statement_line_summary or {}).get("unmatched_statement_line_count", 0) or 0),
                "missing_download_count": int((statement_line_summary or {}).get("missing_download_count", 0) or 0),
            },
            "latest_statement": (
                {
                    "id": str(latest_statement["id"]),
                    "statement_date": str(latest_statement["statement_date"]) if latest_statement["statement_date"] else None,
                    "statement_month_end": str(latest_statement["statement_month_end"]) if latest_statement["statement_month_end"] else None,
                    "total_open_balance": float(latest_statement["total_open_balance"] or 0),
                    "created_at": latest_statement["created_at"].isoformat() if latest_statement["created_at"] else None,
                    "updated_at": latest_statement["updated_at"].isoformat() if latest_statement["updated_at"] else None,
                }
                if latest_statement
                else None
            ),
            "latest_documents": [
                {
                    "id": str(row["id"]),
                    "document_type": row["document_type"],
                    "source_filename": row["source_filename"],
                    "document_date": str(row["document_date"]) if row["document_date"] else None,
                    "upload_source": row["upload_source"],
                    "processing_status": row["processing_status"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                }
                for row in latest_documents
            ],
        }


@router.get("/exceptions")
def hh_ap_exceptions(entity_code: str):
    with db_session() as session:
        entity = get_entity(session, entity_code)

        statement_only_invoices = session.execute(
            text(
                """
                SELECT
                    invoice_number,
                    invoice_type,
                    vendor_name,
                    invoice_date,
                    due_date,
                    total_amount,
                    match_status,
                    notes
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id
                  AND is_statement_only = TRUE
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        missing_download_statement_lines = session.execute(
            text(
                """
                SELECT
                    invoice_number,
                    invoice_type,
                    invoice_date,
                    due_date,
                    invoice_amount,
                    open_amount,
                    current_amount,
                    past_due_amount,
                    match_status
                FROM hh_ap_statement_lines
                WHERE entity_id = :entity_id
                  AND is_missing_download = TRUE
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        unmatched_remittance_lines = session.execute(
            text(
                """
                SELECT
                    id,
                    invoice_number,
                    line_description,
                    due_date,
                    line_amount,
                    match_status,
                    remittance_id
                FROM hh_ap_remittance_lines
                WHERE entity_id = :entity_id
                  AND match_status <> 'matched'
                ORDER BY due_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        unmatched_invoices = session.execute(
            text(
                """
                SELECT
                    invoice_number,
                    invoice_type,
                    vendor_name,
                    invoice_date,
                    due_date,
                    total_amount,
                    match_status,
                    is_statement_only
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id
                  AND match_status <> 'matched'
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        return {
            "entity_code": entity["entity_code"],
            "statement_only_invoices": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "vendor_name": row["vendor_name"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "total_amount": float(row["total_amount"] or 0),
                    "match_status": row["match_status"],
                    "notes": row["notes"],
                }
                for row in statement_only_invoices
            ],
            "missing_download_statement_lines": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "invoice_amount": float(row["invoice_amount"] or 0),
                    "open_amount": float(row["open_amount"] or 0),
                    "current_amount": float(row["current_amount"] or 0),
                    "past_due_amount": float(row["past_due_amount"] or 0),
                    "match_status": row["match_status"],
                }
                for row in missing_download_statement_lines
            ],
            "unmatched_remittance_lines": [
                {
                    "id": str(row["id"]),
                    "invoice_number": row["invoice_number"],
                    "line_description": row["line_description"],
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "line_amount": float(row["line_amount"] or 0),
                    "match_status": row["match_status"],
                    "remittance_id": str(row["remittance_id"]) if row["remittance_id"] else None,
                }
                for row in unmatched_remittance_lines
            ],
            "unmatched_invoices": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "vendor_name": row["vendor_name"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "total_amount": float(row["total_amount"] or 0),
                    "match_status": row["match_status"],
                    "is_statement_only": row["is_statement_only"],
                }
                for row in unmatched_invoices
            ],
        }


@router.get("/reconciliation")
def hh_ap_reconciliation(entity_code: str, statement_month_end: str | None = None):
    with db_session() as session:
        entity = get_entity(session, entity_code)
        statement = get_statement_by_month_end(
            session=session,
            entity_id=entity["id"],
            statement_month_end=statement_month_end,
        )

        if not statement:
            raise HTTPException(
                status_code=404,
                detail="No HH AP statement found for this entity and month_end",
            )

        statement_line_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(*) AS statement_line_count,
                    COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_statement_line_count,
                    COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_statement_line_count,
                    COUNT(*) FILTER (WHERE is_missing_download = TRUE) AS missing_download_count,
                    COALESCE(SUM(COALESCE(invoice_amount, 0)), 0) AS invoice_amount_total,
                    COALESCE(SUM(COALESCE(open_amount, 0)), 0) AS open_amount_total,
                    COALESCE(SUM(COALESCE(current_amount, 0)), 0) AS current_amount_total,
                    COALESCE(SUM(COALESCE(past_due_amount, 0)), 0) AS past_due_amount_total
                FROM hh_ap_statement_lines
                WHERE statement_id = :statement_id
                """
            ),
            {"statement_id": statement["id"]},
        ).mappings().first()

        matched_invoice_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(DISTINCT i.id) AS matched_invoice_count,
                    COALESCE(SUM(COALESCE(i.subtotal, 0)), 0) AS subtotal_total,
                    COALESCE(SUM(COALESCE(i.hst_amount, 0)), 0) AS hst_total,
                    COALESCE(SUM(COALESCE(i.surcharge_amount, 0)), 0) AS surcharge_total,
                    COALESCE(SUM(COALESCE(i.advertising_amount, 0)), 0) AS advertising_total,
                    COALESCE(SUM(COALESCE(i.subscribed_shares_amount, 0)), 0) AS subscribed_shares_total,
                    COALESCE(SUM(COALESCE(i.five_year_note_amount, 0)), 0) AS five_year_note_total,
                    COALESCE(SUM(COALESCE(i.total_amount, 0)), 0) AS invoice_total
                FROM hh_ap_statement_lines sl
                JOIN hh_ap_invoices i
                  ON i.id = sl.matched_invoice_id
                WHERE sl.statement_id = :statement_id
                """
            ),
            {"statement_id": statement["id"]},
        ).mappings().first()

        remittance_match_summary = session.execute(
            text(
                """
                SELECT
                    COUNT(DISTINCT rl.id) AS matched_remittance_line_count,
                    COALESCE(SUM(COALESCE(rl.line_amount, 0)), 0) AS matched_remittance_amount_total
                FROM hh_ap_statement_lines sl
                JOIN hh_ap_invoices i
                  ON i.id = sl.matched_invoice_id
                JOIN hh_ap_remittance_lines rl
                  ON rl.matched_invoice_id = i.id
                WHERE sl.statement_id = :statement_id
                """
            ),
            {"statement_id": statement["id"]},
        ).mappings().first()

        sample_statement_lines = session.execute(
            text(
                """
                SELECT
                    invoice_number,
                    invoice_type,
                    invoice_date,
                    due_date,
                    invoice_amount,
                    open_amount,
                    current_amount,
                    past_due_amount,
                    match_status,
                    is_missing_download
                FROM hh_ap_statement_lines
                WHERE statement_id = :statement_id
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"statement_id": statement["id"]},
        ).mappings().all()

        return {
            "entity_code": entity["entity_code"],
            "statement": {
                "id": str(statement["id"]),
                "statement_date": str(statement["statement_date"]) if statement["statement_date"] else None,
                "statement_month_end": str(statement["statement_month_end"]) if statement["statement_month_end"] else None,
                "total_open_balance": float(statement["total_open_balance"] or 0),
                "created_at": statement["created_at"].isoformat() if statement["created_at"] else None,
                "updated_at": statement["updated_at"].isoformat() if statement["updated_at"] else None,
            },
            "statement_line_summary": {
                "statement_line_count": int((statement_line_summary or {}).get("statement_line_count", 0) or 0),
                "matched_statement_line_count": int((statement_line_summary or {}).get("matched_statement_line_count", 0) or 0),
                "unmatched_statement_line_count": int((statement_line_summary or {}).get("unmatched_statement_line_count", 0) or 0),
                "missing_download_count": int((statement_line_summary or {}).get("missing_download_count", 0) or 0),
                "invoice_amount_total": float((statement_line_summary or {}).get("invoice_amount_total", 0) or 0),
                "open_amount_total": float((statement_line_summary or {}).get("open_amount_total", 0) or 0),
                "current_amount_total": float((statement_line_summary or {}).get("current_amount_total", 0) or 0),
                "past_due_amount_total": float((statement_line_summary or {}).get("past_due_amount_total", 0) or 0),
            },
            "matched_invoice_component_totals": {
                "matched_invoice_count": int((matched_invoice_summary or {}).get("matched_invoice_count", 0) or 0),
                "subtotal_total": float((matched_invoice_summary or {}).get("subtotal_total", 0) or 0),
                "hst_total": float((matched_invoice_summary or {}).get("hst_total", 0) or 0),
                "surcharge_total": float((matched_invoice_summary or {}).get("surcharge_total", 0) or 0),
                "advertising_total": float((matched_invoice_summary or {}).get("advertising_total", 0) or 0),
                "subscribed_shares_total": float((matched_invoice_summary or {}).get("subscribed_shares_total", 0) or 0),
                "five_year_note_total": float((matched_invoice_summary or {}).get("five_year_note_total", 0) or 0),
                "invoice_total": float((matched_invoice_summary or {}).get("invoice_total", 0) or 0),
            },
            "remittance_match_summary": {
                "matched_remittance_line_count": int((remittance_match_summary or {}).get("matched_remittance_line_count", 0) or 0),
                "matched_remittance_amount_total": float((remittance_match_summary or {}).get("matched_remittance_amount_total", 0) or 0),
            },
            "sample_statement_lines": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "invoice_amount": float(row["invoice_amount"] or 0),
                    "open_amount": float(row["open_amount"] or 0),
                    "current_amount": float(row["current_amount"] or 0),
                    "past_due_amount": float(row["past_due_amount"] or 0),
                    "match_status": row["match_status"],
                    "is_missing_download": row["is_missing_download"],
                }
                for row in sample_statement_lines
            ],
        }
