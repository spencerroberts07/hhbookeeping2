import json
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/hh-ap", tags=["hh-ap-review"])


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


def money_float(value: Decimal | None) -> float:
    if value is None:
        return 0.0
    return float(Decimal(str(value)).quantize(Decimal("0.01")))


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


class HHAPInvoiceOverrideUpsertRequest(BaseModel):
    entity_code: str
    invoice_number: str
    invoice_type: str

    override_invoice_date: date | None = None
    override_due_date: date | None = None
    override_subtotal: Decimal | None = None
    override_hst_amount: Decimal | None = None
    override_total_amount: Decimal | None = None
    override_special_shares_amount: Decimal | None = None
    override_five_year_note_amount: Decimal | None = None
    override_advertising_amount: Decimal | None = None

    reason: str
    review_status: str = "approved"
    reviewed_by: str | None = None
    is_active: bool = True
    raw_json: dict[str, Any] = Field(default_factory=dict)


@router.post("/invoice-overrides/upsert")
def hh_ap_invoice_override_upsert(payload: HHAPInvoiceOverrideUpsertRequest):
    if payload.review_status not in {"approved", "pending", "rejected"}:
        raise HTTPException(
            status_code=400,
            detail="review_status must be approved, pending, or rejected",
        )

    invoice_number = normalize_invoice_number(payload.invoice_number)
    invoice_type = normalize_text(payload.invoice_type)
    reason = normalize_text(payload.reason)

    if not invoice_number or not invoice_type or not reason:
        raise HTTPException(
            status_code=400,
            detail="entity_code, invoice_number, invoice_type, and reason are required",
        )

    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        existing_invoice = session.execute(
            text(
                """
                SELECT id, invoice_number, invoice_type, invoice_date, due_date
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id
                  AND invoice_number = :invoice_number
                  AND invoice_type = :invoice_type
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "invoice_number": invoice_number,
                "invoice_type": invoice_type,
            },
        ).mappings().first()

        if not existing_invoice:
            raise HTTPException(
                status_code=404,
                detail="Base invoice not found for that entity_code + invoice_number + invoice_type",
            )

        row = session.execute(
            text(
                """
                INSERT INTO hh_ap_invoice_overrides (
                    entity_id,
                    invoice_number,
                    invoice_type,
                    override_invoice_date,
                    override_due_date,
                    override_subtotal,
                    override_hst_amount,
                    override_total_amount,
                    override_special_shares_amount,
                    override_five_year_note_amount,
                    override_advertising_amount,
                    reason,
                    review_status,
                    reviewed_by,
                    is_active,
                    raw_json
                ) VALUES (
                    :entity_id,
                    :invoice_number,
                    :invoice_type,
                    :override_invoice_date,
                    :override_due_date,
                    :override_subtotal,
                    :override_hst_amount,
                    :override_total_amount,
                    :override_special_shares_amount,
                    :override_five_year_note_amount,
                    :override_advertising_amount,
                    :reason,
                    :review_status,
                    :reviewed_by,
                    :is_active,
                    CAST(:raw_json AS jsonb)
                )
                ON CONFLICT (entity_id, invoice_number, invoice_type)
                DO UPDATE SET
                    override_invoice_date = EXCLUDED.override_invoice_date,
                    override_due_date = EXCLUDED.override_due_date,
                    override_subtotal = EXCLUDED.override_subtotal,
                    override_hst_amount = EXCLUDED.override_hst_amount,
                    override_total_amount = EXCLUDED.override_total_amount,
                    override_special_shares_amount = EXCLUDED.override_special_shares_amount,
                    override_five_year_note_amount = EXCLUDED.override_five_year_note_amount,
                    override_advertising_amount = EXCLUDED.override_advertising_amount,
                    reason = EXCLUDED.reason,
                    review_status = EXCLUDED.review_status,
                    reviewed_by = EXCLUDED.reviewed_by,
                    is_active = EXCLUDED.is_active,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                RETURNING id, created_at, updated_at
                """
            ),
            {
                "entity_id": entity["id"],
                "invoice_number": invoice_number,
                "invoice_type": invoice_type,
                "override_invoice_date": payload.override_invoice_date,
                "override_due_date": payload.override_due_date,
                "override_subtotal": payload.override_subtotal,
                "override_hst_amount": payload.override_hst_amount,
                "override_total_amount": payload.override_total_amount,
                "override_special_shares_amount": payload.override_special_shares_amount,
                "override_five_year_note_amount": payload.override_five_year_note_amount,
                "override_advertising_amount": payload.override_advertising_amount,
                "reason": reason,
                "review_status": payload.review_status,
                "reviewed_by": normalize_text(payload.reviewed_by),
                "is_active": payload.is_active,
                "raw_json": json.dumps(payload.raw_json or {}),
            },
        ).mappings().first()

        effective = session.execute(
            text(
                """
                SELECT
                    i.invoice_number,
                    i.invoice_type,
                    i.invoice_date,
                    i.due_date,
                    i.subtotal,
                    i.hst_amount,
                    i.total_amount,
                    i.subscribed_shares_amount,
                    i.five_year_note_amount,
                    i.advertising_amount,
                    i.override_id,
                    i.override_reason,
                    i.override_review_status,
                    i.override_reviewed_by
                FROM hh_ap_invoices_effective i
                WHERE i.entity_id = :entity_id
                  AND i.invoice_number = :invoice_number
                  AND i.invoice_type = :invoice_type
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "invoice_number": invoice_number,
                "invoice_type": invoice_type,
            },
        ).mappings().first()

        return {
            "entity_code": entity["entity_code"],
            "override_id": str(row["id"]),
            "invoice_number": effective["invoice_number"],
            "invoice_type": effective["invoice_type"],
            "effective_values": {
                "invoice_date": str(effective["invoice_date"]) if effective["invoice_date"] else None,
                "due_date": str(effective["due_date"]) if effective["due_date"] else None,
                "subtotal": money_float(effective["subtotal"]),
                "hst_amount": money_float(effective["hst_amount"]),
                "total_amount": money_float(effective["total_amount"]),
                "special_shares_amount": money_float(effective["subscribed_shares_amount"]),
                "five_year_note_amount": money_float(effective["five_year_note_amount"]),
                "advertising_amount": money_float(effective["advertising_amount"]),
            },
            "override_reason": effective["override_reason"],
            "override_review_status": effective["override_review_status"],
            "override_reviewed_by": effective["override_reviewed_by"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }


@router.get("/invoice-overrides")
def hh_ap_invoice_overrides_list(
    entity_code: str,
    invoice_date_from: date | None = Query(default=None),
    invoice_date_to: date | None = Query(default=None),
    invoice_type: str | None = Query(default=None),
):
    with db_session() as session:
        entity = get_entity(session, entity_code)

        sql = """
            SELECT
                i.invoice_number,
                i.invoice_type,
                i.invoice_date,
                i.due_date,
                i.subtotal,
                i.hst_amount,
                i.total_amount,
                i.subscribed_shares_amount,
                i.five_year_note_amount,
                i.advertising_amount,
                i.override_id,
                i.override_reason,
                i.override_review_status,
                i.override_reviewed_by,
                d.source_filename
            FROM hh_ap_invoices_effective i
            LEFT JOIN hh_ap_documents d
              ON d.id = i.document_id
            WHERE i.entity_id = :entity_id
              AND i.override_id IS NOT NULL
        """

        params: dict[str, Any] = {"entity_id": entity["id"]}

        if invoice_date_from:
            sql += " AND i.invoice_date >= :invoice_date_from"
            params["invoice_date_from"] = invoice_date_from

        if invoice_date_to:
            sql += " AND i.invoice_date <= :invoice_date_to"
            params["invoice_date_to"] = invoice_date_to

        if invoice_type:
            sql += " AND i.invoice_type = :invoice_type"
            params["invoice_type"] = invoice_type

        sql += " ORDER BY i.invoice_date, i.invoice_number"

        rows = session.execute(text(sql), params).mappings().all()

        return {
            "entity_code": entity["entity_code"],
            "override_count": len(rows),
            "overrides": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "subtotal": money_float(row["subtotal"]),
                    "hst_amount": money_float(row["hst_amount"]),
                    "total_amount": money_float(row["total_amount"]),
                    "special_shares_amount": money_float(row["subscribed_shares_amount"]),
                    "five_year_note_amount": money_float(row["five_year_note_amount"]),
                    "advertising_amount": money_float(row["advertising_amount"]),
                    "override_id": str(row["override_id"]) if row["override_id"] else None,
                    "override_reason": row["override_reason"],
                    "override_review_status": row["override_review_status"],
                    "override_reviewed_by": row["override_reviewed_by"],
                    "source_filename": row["source_filename"],
                }
                for row in rows
            ],
        }


@router.get("/review-queue")
def hh_ap_review_queue(
    entity_code: str,
    invoice_date_from: date | None = Query(default=None),
    invoice_date_to: date | None = Query(default=None),
    invoice_type: str | None = Query(default=None),
    only_warning_rows: bool = Query(default=True),
    only_without_override: bool = Query(default=False),
):
    with db_session() as session:
        entity = get_entity(session, entity_code)

        sql = """
            SELECT
                i.invoice_number,
                i.invoice_type,
                i.invoice_date,
                i.due_date,
                i.subtotal AS parsed_subtotal,
                i.hst_amount AS parsed_hst_amount,
                i.total_amount AS parsed_total_amount,
                i.subscribed_shares_amount AS parsed_special_shares_amount,
                i.five_year_note_amount AS parsed_five_year_note_amount,
                i.advertising_amount AS parsed_advertising_amount,
                e.subtotal AS effective_subtotal,
                e.hst_amount AS effective_hst_amount,
                e.total_amount AS effective_total_amount,
                e.subscribed_shares_amount AS effective_special_shares_amount,
                e.five_year_note_amount AS effective_five_year_note_amount,
                e.advertising_amount AS effective_advertising_amount,
                e.override_id,
                e.override_reason,
                e.override_review_status,
                e.override_reviewed_by,
                i.raw_json -> 'parser_warnings' AS parser_warnings,
                d.source_filename
            FROM hh_ap_invoices i
            JOIN hh_ap_invoices_effective e
              ON e.id = i.id
            LEFT JOIN hh_ap_documents d
              ON d.id = i.document_id
            WHERE i.entity_id = :entity_id
        """

        params: dict[str, Any] = {"entity_id": entity["id"]}

        if invoice_date_from:
            sql += " AND i.invoice_date >= :invoice_date_from"
            params["invoice_date_from"] = invoice_date_from

        if invoice_date_to:
            sql += " AND i.invoice_date <= :invoice_date_to"
            params["invoice_date_to"] = invoice_date_to

        if invoice_type:
            sql += " AND i.invoice_type = :invoice_type"
            params["invoice_type"] = invoice_type

        if only_warning_rows:
            sql += """
                AND (
                    COALESCE(jsonb_array_length(COALESCE(i.raw_json -> 'parser_warnings', '[]'::jsonb)), 0) > 0
                    OR COALESCE(i.total_amount, 0) = 0
                )
            """

        if only_without_override:
            sql += " AND e.override_id IS NULL"

        sql += " ORDER BY i.invoice_date, i.invoice_number"

        rows = session.execute(text(sql), params).mappings().all()

        return {
            "entity_code": entity["entity_code"],
            "review_row_count": len(rows),
            "rows": [
                {
                    "invoice_number": row["invoice_number"],
                    "invoice_type": row["invoice_type"],
                    "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
                    "due_date": str(row["due_date"]) if row["due_date"] else None,
                    "parsed_values": {
                        "subtotal": money_float(row["parsed_subtotal"]),
                        "hst_amount": money_float(row["parsed_hst_amount"]),
                        "total_amount": money_float(row["parsed_total_amount"]),
                        "special_shares_amount": money_float(row["parsed_special_shares_amount"]),
                        "five_year_note_amount": money_float(row["parsed_five_year_note_amount"]),
                        "advertising_amount": money_float(row["parsed_advertising_amount"]),
                    },
                    "effective_values": {
                        "subtotal": money_float(row["effective_subtotal"]),
                        "hst_amount": money_float(row["effective_hst_amount"]),
                        "total_amount": money_float(row["effective_total_amount"]),
                        "special_shares_amount": money_float(row["effective_special_shares_amount"]),
                        "five_year_note_amount": money_float(row["effective_five_year_note_amount"]),
                        "advertising_amount": money_float(row["effective_advertising_amount"]),
                    },
                    "has_override": row["override_id"] is not None,
                    "override_id": str(row["override_id"]) if row["override_id"] else None,
                    "override_reason": row["override_reason"],
                    "override_review_status": row["override_review_status"],
                    "override_reviewed_by": row["override_reviewed_by"],
                    "parser_warnings": row["parser_warnings"] or [],
                    "source_filename": row["source_filename"],
                }
                for row in rows
            ],
        }
