"""
Unified documents library — /api/documents.

Aggregates uploaded-file records from six per-module tables into a
single list the dealer can browse. Each row carries:

  - document_type: which module the file belongs to
  - filename: original filename
  - upload_date: when the row was created
  - parsed_record_count: how many records the parser produced
  - file_url: a 1-hour presigned R2 URL when an R2 file_path was stashed
  - status: 'parsed' | 'pending' | 'error'

We UNION the run tables and order by upload_date desc. Filter by
document_type / month / year via query params.
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import require_role
from ..services_storage import storage_service


router = APIRouter(prefix="/api/documents", tags=["documents"])


# Each tuple: (document_type, source_table, label, query SQL fragment
# that maps the table's columns to the common shape).
_SOURCES = [
    (
        "invoice",
        "invoice_documents",
        """
        SELECT id::text AS doc_id,
               'invoice' AS document_type,
               COALESCE(source_filename, original_filename, 'invoice') AS filename,
               file_path,
               status::text AS status,
               1 AS parsed_record_count,
               created_at AS upload_date
          FROM invoice_documents
         WHERE entity_code = :ec
        """,
    ),
    (
        "bank_pdf",
        "bank_pdf_imports",
        """
        SELECT bpi.id::text,
               'bank_pdf',
               bpi.file_name,
               bpi.file_path,
               bpi.status,
               bpi.transactions_inserted,
               bpi.created_at
          FROM bank_pdf_imports bpi
         WHERE bpi.entity_id = :eid
        """,
    ),
    (
        "bank_csv",
        "bank_csv_import_runs",
        """
        SELECT bcr.id::text,
               'bank_csv',
               bcr.file_name,
               bcr.file_path,
               'parsed',
               bcr.rows_inserted,
               bcr.started_at
          FROM bank_csv_import_runs bcr
         WHERE bcr.entity_id = :eid
        """,
    ),
    (
        "payroll",
        "payroll_runs",
        """
        SELECT pr.id::text,
               'payroll',
               COALESCE(pr.register_file_name, 'payroll-register') AS file_name,
               pr.file_path,
               pr.status,
               COALESCE(pr.employee_count, 0),
               pr.created_at
          FROM payroll_runs pr
         WHERE pr.entity_id = :eid
        """,
    ),
    (
        "pos_import",
        "pos_import_runs",
        """
        SELECT pir.id::text,
               'pos_import',
               COALESCE(pir.file_name, pir.report_type) AS file_name,
               pir.file_path,
               'parsed',
               COALESCE(pir.row_count, 0),
               pir.created_at
          FROM pos_import_runs pir
         WHERE pir.entity_id = :eid
        """,
    ),
    (
        "gl_import",
        "gl_import_runs",
        """
        SELECT gir.id::text,
               'gl_import',
               COALESCE(gir.file_name, 'gl-import') AS file_name,
               gir.file_path,
               'parsed',
               COALESCE(gir.row_count, 0),
               gir.imported_at
          FROM gl_import_runs gir
         WHERE gir.entity_id = :eid
        """,
    ),
    (
        "hh_ap_statement",
        "hh_ap_statements",
        """
        SELECT s.id::text,
               'hh_ap_statement',
               COALESCE(s.statement_date::text, 'hh-ap-statement') AS file_name,
               s.file_path,
               'parsed',
               1,
               s.created_at
          FROM hh_ap_statements s
         WHERE s.entity_id = :eid
        """,
    ),
]


@router.get("")
def list_documents(
    entity_code: str = Query(...),
    type: str | None = Query(default=None),
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Aggregate documents from every upload-tracking table. Failures
    on individual sources are swallowed so a missing column on one
    table doesn't break the whole list."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        all_rows: list[dict[str, Any]] = []
        for doc_type, _table, _label, sql in _SOURCES:
            if type and type != doc_type:
                continue
            try:
                rows = session.execute(
                    text(sql),
                    {"ec": entity_code, "eid": entity["id"]},
                ).all()
            except Exception:
                # One source's schema may be older than this code expects.
                # Skip it rather than 500'ing the whole list.
                continue
            for r in rows:
                doc_id, dtype, filename, file_path, status, count, upload_date = r
                if year and upload_date and upload_date.year != year:
                    continue
                if month and upload_date and upload_date.month != month:
                    continue
                file_url = storage_service.get_presigned_url(file_path)
                all_rows.append({
                    "id": doc_id,
                    "document_type": dtype,
                    "filename": filename,
                    "upload_date": upload_date.isoformat() if upload_date else None,
                    "parsed_record_count": int(count or 0),
                    "file_url": file_url,
                    "status": status,
                })

    all_rows.sort(key=lambda d: d["upload_date"] or "", reverse=True)
    total = len(all_rows)
    paged = all_rows[offset:offset + limit]
    return {
        "entity_code": entity_code,
        "documents": paged,
        "total": total,
        "limit": limit,
        "offset": offset,
    }
