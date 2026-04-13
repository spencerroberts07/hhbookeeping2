from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/hh-ap", tags=["hh-ap"])


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
