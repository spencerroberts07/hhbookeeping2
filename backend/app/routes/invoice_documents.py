"""
Invoice audit trail — upload, list, match, post, drill-down.

Endpoints (prefix /api/invoice-documents):
    POST   /upload                       multi-file upload + parse + auto-match
    GET    /                              filterable + paginated list
    GET    /unmatched-queue               list-with-suggested-matches
    GET    /{id}                          full detail + linked journals/banks
    POST   /{id}/match                    manual link
    POST   /{id}/post-to-ap               Dr expense / Cr 2020|2030 + link
    POST   /{id}/delete                   soft delete (status -> 'deleted')

Auth model:
    - GET   endpoints require `viewer` (read-only, role hierarchy 10+)
    - POST  endpoints require `bookkeeper` (write, role hierarchy 20+)
    - `enforce_entity_code` is called inside every body/form handler so
      the Clerk org-match check fires under USE_CLERK_AUTH=True.

Parsing strategy (per user clarification):
    - Light regex extraction of total/date/vendor/invoice-number from
      whatever text we can pull out of the PDF.
    - Missing fields stay NULL; the unmatched-queue UI exposes them as
      editable. We never block the upload because a field couldn't be
      extracted.
"""
from __future__ import annotations

import logging
import re
from datetime import date as DateType, date
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_auth_clerk import CurrentUser
from ..services_invoice_matching import (
    auto_match_invoice,
    run_period_match_sweep,
    SweepSummary,
)
from ..services_storage import storage_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/invoice-documents", tags=["invoice-documents"])


# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------


class ManualMatchRequest(BaseModel):
    entity_code: str
    actor_email: str | None = None
    journal_batch_id: str | None = None
    bank_transaction_id: str | None = None
    hh_ap_invoice_id: str | None = None


class PostToApRequest(BaseModel):
    entity_code: str
    actor_email: str
    ap_account: str = Field(pattern="^(2020|2030)$")
    expense_account_code: str = Field(default="6510")
    period_end: str
    memo: str | None = None


class DeleteRequest(BaseModel):
    entity_code: str
    reason: str = Field(min_length=1)


class UpdateInvoiceRequest(BaseModel):
    entity_code: str
    invoice_number: str | None = None
    vendor_name: str | None = None
    invoice_date: str | None = None
    due_date: str | None = None
    amount: Decimal | None = None
    notes: str | None = None


# --------------------------------------------------------------------------
# PDF parsing (light, regex-driven)
# --------------------------------------------------------------------------


_AMOUNT_PATTERNS = [
    re.compile(r"(?:total|balance\s*due|amount\s*due|grand\s*total)[^\d\-]*\$?\s*([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"\$\s*([\d,]+\.\d{2})"),
]

_DATE_PATTERNS = [
    re.compile(r"(?:invoice\s*date|date\s*issued|issued)[^\d]{0,6}([0-9]{4}-[0-9]{2}-[0-9]{2})", re.IGNORECASE),
    re.compile(r"(?:invoice\s*date|date\s*issued|issued)[^\d]{0,6}([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", re.IGNORECASE),
    re.compile(r"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b"),
]

_INVOICE_NUMBER_PATTERN = re.compile(
    r"(?:invoice\s*(?:#|number|no\.?)|inv\s*(?:#|no\.?))\s*[:\-]?\s*([A-Z0-9\-]{3,})",
    re.IGNORECASE,
)


def _extract_pdf_text(file_bytes: bytes) -> str | None:
    """Pull plain text out of a PDF. Returns None if pypdf can't read it."""
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("_extract_pdf_text: pypdf not installed")
        return None
    try:
        reader = PdfReader(BytesIO(file_bytes))
        chunks: list[str] = []
        for page in reader.pages:
            try:
                chunks.append(page.extract_text() or "")
            except Exception:
                continue
        text_blob = "\n".join(chunks)
        return text_blob[:200_000] if text_blob else None
    except Exception:
        logger.exception("_extract_pdf_text: PDF read failed")
        return None


def _parse_amount(blob: str) -> Decimal | None:
    """Pick the biggest amount we can find — the "Total" / "Balance Due"
    line typically wins; the largest $ value across the page is the
    fallback."""
    if not blob:
        return None
    largest: Decimal | None = None
    for pat in _AMOUNT_PATTERNS:
        for m in pat.finditer(blob):
            try:
                value = Decimal(m.group(1).replace(",", ""))
            except (InvalidOperation, IndexError):
                continue
            if largest is None or value > largest:
                largest = value
    return largest


def _parse_date(blob: str) -> DateType | None:
    if not blob:
        return None
    for pat in _DATE_PATTERNS:
        m = pat.search(blob)
        if not m:
            continue
        raw = m.group(1)
        for fmt_attempt in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%m-%d-%Y",
            "%d-%m-%Y",
            "%m/%d/%y",
        ):
            try:
                from datetime import datetime as _dt

                return _dt.strptime(raw, fmt_attempt).date()
            except ValueError:
                continue
    return None


def _parse_invoice_number(blob: str) -> str | None:
    if not blob:
        return None
    m = _INVOICE_NUMBER_PATTERN.search(blob)
    return m.group(1).strip() if m else None


def _parse_vendor_name(blob: str, filename: str | None) -> str | None:
    """Heuristic: the vendor is almost always on the first page, near the
    top, often the first 1-3 non-empty lines. Fall back to the filename
    stem with underscores/dashes replaced if we can't pick anything
    obvious from the text."""
    if blob:
        lines = [ln.strip() for ln in blob.splitlines() if ln.strip()][:8]
        # Pick the first line that's not a generic header like "INVOICE".
        for ln in lines:
            lower = ln.lower()
            if lower in {"invoice", "tax invoice", "purchase order"}:
                continue
            if any(c.isalpha() for c in ln) and len(ln) <= 80:
                return ln
    if filename:
        from pathlib import Path as _P

        stem = _P(filename).stem
        cleaned = re.sub(r"[_\-\s]+", " ", stem).strip()
        return cleaned[:80] if cleaned else None
    return None


# --------------------------------------------------------------------------
# Hashing (matches the convention in routes/hh_ap.py)
# --------------------------------------------------------------------------


def _source_hash(file_bytes: bytes) -> str:
    import hashlib

    return hashlib.sha256(file_bytes).hexdigest()


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------


@router.post("/upload")
async def upload_invoice_documents(
    entity_code: str = Form(...),
    invoice_type: str = Form(...),
    files: list[UploadFile] = File(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Upload one or more invoice PDFs. Parses metadata via regex and
    runs auto_match for each."""
    enforce_entity_code(_user, entity_code)

    if invoice_type not in {"hh_ap", "outside_vendor"}:
        raise HTTPException(
            status_code=400, detail="invoice_type must be 'hh_ap' or 'outside_vendor'",
        )
    ap_account = "2030" if invoice_type == "hh_ap" else "2020"

    clerk_user_id = (
        _user.clerk_user_id if isinstance(_user, CurrentUser) else None
    )

    processed: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    from ..services_entity_validation import (
        raise_or_warn as _raise_or_warn,
        validate_document_entity as _validate_entity,
    )

    for upload in files:
        filename = upload.filename or "invoice.pdf"
        try:
            file_bytes = await upload.read()
            if not file_bytes:
                failed.append({"file_name": filename, "error": "empty file"})
                continue

            # Entity gate. HH AP invoices carry the store number in
            # the filename and PDF; outside-vendor invoices have no
            # entity identifier and skip validation.
            with db_session() as _vsession:
                _raise_or_warn(_validate_entity(
                    _vsession,
                    entity_code=entity_code,
                    file_bytes=file_bytes,
                    filename=filename,
                    document_type="invoice_document",
                    invoice_kind=invoice_type,
                ), None)

            source_hash = _source_hash(file_bytes)
            blob = _extract_pdf_text(file_bytes) or ""

            parsed_amount = _parse_amount(blob) or Decimal("0.00")
            parsed_date = _parse_date(blob)
            parsed_number = _parse_invoice_number(blob)
            parsed_vendor = _parse_vendor_name(blob, filename)

            # Archive to R2 before persisting. None on failure or when R2 is
            # not configured — that's fine; the parsed row is still useful.
            object_key = storage_service.upload_file(
                file_bytes=file_bytes,
                original_filename=filename,
                entity_code=entity_code,
                document_type="invoices",
                content_type=upload.content_type or "application/pdf",
            )

            with db_session() as session:
                existing = session.execute(
                    text(
                        """
                        SELECT id, status FROM invoice_documents
                         WHERE entity_code = :ec AND source_hash = :sh
                         LIMIT 1
                        """
                    ),
                    {"ec": entity_code, "sh": source_hash},
                ).mappings().first()
                if existing:
                    duplicates.append({
                        "file_name": filename,
                        "invoice_document_id": str(existing["id"]),
                        "status": existing["status"],
                    })
                    continue

                row = session.execute(
                    text(
                        """
                        INSERT INTO invoice_documents (
                            entity_code, invoice_type, invoice_number,
                            vendor_name, invoice_date, amount, currency,
                            status, ap_account, file_name, file_size_bytes,
                            file_path, source_hash,
                            uploaded_by_clerk_user_id, uploaded_at
                        ) VALUES (
                            :ec, :itype, :inum, :vname, :idate, :amt, 'CAD',
                            'unmatched', :ap_account, :fname, :fsize,
                            :file_path, :sh, :uid, NOW()
                        )
                        RETURNING id, entity_code, invoice_type, invoice_number,
                                  vendor_name, invoice_date, amount, status,
                                  ap_account, file_name, file_size_bytes,
                                  file_path, uploaded_at
                        """
                    ),
                    {
                        "ec": entity_code,
                        "itype": invoice_type,
                        "inum": parsed_number,
                        "vname": parsed_vendor,
                        "idate": parsed_date,
                        "amt": parsed_amount,
                        "ap_account": ap_account,
                        "fname": filename,
                        "fsize": len(file_bytes),
                        "file_path": object_key,
                        "sh": source_hash,
                        "uid": clerk_user_id,
                    },
                ).mappings().first()
                assert row is not None
                invoice_dict = dict(row)

                # Immediate auto-match attempt — never throws.
                matches = auto_match_invoice(
                    session,
                    invoice=invoice_dict,
                    entity_code=entity_code,
                )

                # Vendor master bridge (outside_vendor only — never HH AP).
                # Best-effort: never blocks upload on failure.
                vendor_master_id: str | None = None
                if invoice_type == "outside_vendor" and parsed_vendor:
                    try:
                        _eid_row = session.execute(
                            text("SELECT id FROM entities WHERE entity_code = :ec"),
                            {"ec": entity_code},
                        ).mappings().first()
                        if _eid_row:
                            from ..services_vendor_master import ensure_vendor as _ensure_vendor
                            from uuid import UUID as _UUID
                            _vendor_row = _ensure_vendor(
                                session,
                                entity_id=_UUID(str(_eid_row["id"])),
                                vendor_name=parsed_vendor,
                                invoice_date=parsed_date,
                                due_date=None,  # due_date not parsed at upload time
                                invoice_id=str(invoice_dict["id"]),
                            )
                            vendor_master_id = str(_vendor_row["id"])

                            # Bridge: create / update the direct_vendor_ap_invoices
                            # spine row so the invoice enters the payment lifecycle.
                            # Uses the upsert (idempotent on vendor_name+invoice_number).
                            _inv_number = parsed_number or f"INV-{str(invoice_dict['id'])[:8]}"
                            _actor = clerk_user_id or f"upload:{entity_code}"
                            if parsed_date:  # invoice_date is required by upsert
                                from ..services import upsert_direct_vendor_ap_invoice as _upsert_dvap
                                _spine = _upsert_dvap(
                                    session,
                                    entity_code=entity_code,
                                    actor_email=_actor,
                                    vendor_name=parsed_vendor,
                                    invoice_number=_inv_number,
                                    invoice_date=parsed_date,
                                    total_amount=parsed_amount,
                                    source_document_name=filename,
                                )
                                # Stamp vendor_id + source_invoice_document_id
                                # (new columns from migration 060, not in the upsert).
                                session.execute(
                                    text("""
                                        UPDATE direct_vendor_ap_invoices
                                           SET vendor_id                   = COALESCE(vendor_id, :vid),
                                               source_invoice_document_id  = COALESCE(source_invoice_document_id, :sid),
                                               updated_at                  = NOW()
                                         WHERE id = :iid
                                    """),
                                    {
                                        "vid": vendor_master_id,
                                        "sid": str(invoice_dict["id"]),
                                        "iid": str(_spine["id"]),
                                    },
                                )
                    except Exception as _ve:
                        logger.warning("vendor master bridge failed: %r", _ve)

            processed.append({
                "invoice_document_id": str(invoice_dict["id"]),
                "file_name": filename,
                "parsed": {
                    "amount": str(parsed_amount) if parsed_amount else None,
                    "invoice_date": parsed_date.isoformat() if parsed_date else None,
                    "invoice_number": parsed_number,
                    "vendor_name": parsed_vendor,
                },
                "needs_review": _needs_review(
                    parsed_amount, parsed_date, parsed_vendor, parsed_number,
                ),
                "match_count": len(matches),
                "auto_matched": any(m.auto_linked for m in matches),
                "top_match_confidence": (
                    matches[0].confidence if matches else None
                ),
            })
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("invoice upload failed: %s", filename)
            failed.append({"file_name": filename, "error": str(exc)})

    return {
        "processed": processed,
        "duplicates": duplicates,
        "failed": failed,
        "record_count": len(processed),
    }


def _needs_review(
    amount: Decimal | None,
    inv_date: DateType | None,
    vendor: str | None,
    inv_number: str | None,
) -> bool:
    """True if the parser couldn't fill at least amount + (date or vendor)."""
    if amount is None or amount <= 0:
        return True
    if inv_date is None and not vendor:
        return True
    return False


# --------------------------------------------------------------------------
# GET / list
# --------------------------------------------------------------------------


@router.get("")
def list_invoice_documents(
    entity_code: str = Query(...),
    status: str | None = Query(default=None),
    invoice_type: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    where = ["entity_code = :ec"]
    params: dict[str, Any] = {"ec": entity_code, "limit": limit, "offset": offset}
    if status:
        where.append("status = :status")
        params["status"] = status
    else:
        # By default exclude soft-deleted rows.
        where.append("status <> 'deleted'")
    if invoice_type:
        where.append("invoice_type = :itype")
        params["itype"] = invoice_type
    if date_from:
        where.append("invoice_date >= :df")
        params["df"] = date_from
    if date_to:
        where.append("invoice_date <= :dt")
        params["dt"] = date_to

    where_clause = " AND ".join(where)

    with db_session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT id, entity_code, invoice_type, invoice_number,
                       vendor_name, invoice_date, due_date, amount,
                       currency, status, ap_account, file_name,
                       file_size_bytes, file_path, uploaded_at, matched_at,
                       match_confidence, notes
                  FROM invoice_documents
                 WHERE {where_clause}
                 ORDER BY uploaded_at DESC
                 LIMIT :limit OFFSET :offset
                """
            ),
            params,
        ).mappings().all()
        total_row = session.execute(
            text(
                f"SELECT COUNT(*) AS c FROM invoice_documents WHERE {where_clause}"
            ),
            {k: v for k, v in params.items() if k not in {"limit", "offset"}},
        ).mappings().first()
        total = int(total_row["c"]) if total_row else 0

    return {
        "invoices": [_invoice_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# --------------------------------------------------------------------------
# GET /unmatched-queue
# --------------------------------------------------------------------------


@router.get("/unmatched-queue")
def get_unmatched_queue(
    entity_code: str = Query(...),
    period_end: str | None = Query(default=None),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """List every unmatched invoice for the entity, each with a
    suggested-matches array (top 5 candidates). Powers /ap/unmatched."""
    with db_session() as session:
        where = ["entity_code = :ec", "status = 'unmatched'"]
        params: dict[str, Any] = {"ec": entity_code}
        if period_end:
            where.append("(invoice_date IS NULL OR invoice_date <= :pe)")
            params["pe"] = period_end
        rows = session.execute(
            text(
                f"""
                SELECT id, entity_code, invoice_type, invoice_number,
                       vendor_name, invoice_date, due_date, amount,
                       currency, status, ap_account, file_name,
                       file_size_bytes, file_path, uploaded_at, notes
                  FROM invoice_documents
                 WHERE {' AND '.join(where)}
                 ORDER BY uploaded_at DESC
                """
            ),
            params,
        ).mappings().all()

        queue: list[dict[str, Any]] = []
        for r in rows:
            inv = dict(r)
            matches = auto_match_invoice(
                session, invoice=inv, entity_code=entity_code,
            )
            queue.append({
                "invoice": _invoice_to_dict(r),
                "suggested_matches": [
                    {
                        "type": m.match_type,
                        "id": m.target_id,
                        "amount": str(m.amount),
                        "date": m.when.isoformat() if m.when else None,
                        "description": m.description,
                        "confidence": m.confidence,
                    }
                    for m in matches[:5]
                ],
            })
    return {"queue": queue, "total": len(queue)}


# --------------------------------------------------------------------------
# GET /{id}
# --------------------------------------------------------------------------


@router.get("/{invoice_id}")
def get_invoice_document(
    invoice_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text(
                """
                SELECT id, entity_code, invoice_type, invoice_number,
                       vendor_name, invoice_date, due_date, amount,
                       currency, status, ap_account, file_name,
                       file_size_bytes, file_path, uploaded_at, matched_at,
                       match_confidence, notes
                  FROM invoice_documents
                 WHERE id = :id AND entity_code = :ec
                """
            ),
            {"id": invoice_id, "ec": entity_code},
        ).mappings().first()
        if not row:
            raise HTTPException(404, "Invoice document not found")

        links = session.execute(
            text(
                """
                SELECT l.id, l.link_type, l.journal_batch_id,
                       l.journal_line_id, l.bank_transaction_id,
                       l.hh_ap_invoice_id, l.linked_at, l.linked_by,
                       l.confidence,
                       jb.source_module      AS journal_source_module,
                       jb.batch_label        AS journal_batch_label,
                       jb.status             AS journal_status,
                       bt.transaction_date   AS bank_date,
                       bt.amount             AS bank_amount,
                       bt.description        AS bank_description,
                       hi.invoice_number     AS hh_invoice_number,
                       hi.vendor_name        AS hh_vendor_name,
                       hi.invoice_date       AS hh_invoice_date,
                       hi.total_amount       AS hh_invoice_amount
                  FROM invoice_journal_links l
             LEFT JOIN journal_batches    jb ON jb.id = l.journal_batch_id
             LEFT JOIN bank_transactions  bt ON bt.id = l.bank_transaction_id
             LEFT JOIN hh_ap_invoices     hi ON hi.id = l.hh_ap_invoice_id
                 WHERE l.invoice_document_id = :id
                 ORDER BY l.linked_at DESC
                """
            ),
            {"id": invoice_id},
        ).mappings().all()

    return {
        "invoice": _invoice_to_dict(row),
        "links": [
            {
                "id": str(l["id"]),
                "link_type": l["link_type"],
                "linked_at": l["linked_at"].isoformat() if l["linked_at"] else None,
                "linked_by": l["linked_by"],
                "confidence": (
                    float(l["confidence"]) if l["confidence"] is not None else None
                ),
                "journal_batch_id": (
                    str(l["journal_batch_id"]) if l["journal_batch_id"] else None
                ),
                "journal_source_module": l["journal_source_module"],
                "journal_batch_label": l["journal_batch_label"],
                "journal_status": l["journal_status"],
                "bank_transaction_id": (
                    str(l["bank_transaction_id"]) if l["bank_transaction_id"] else None
                ),
                "bank_date": (
                    l["bank_date"].isoformat() if l["bank_date"] else None
                ),
                "bank_amount": (
                    str(l["bank_amount"]) if l["bank_amount"] is not None else None
                ),
                "bank_description": l["bank_description"],
                "hh_ap_invoice_id": (
                    str(l["hh_ap_invoice_id"]) if l["hh_ap_invoice_id"] else None
                ),
                "hh_invoice_number": l["hh_invoice_number"],
                "hh_vendor_name": l["hh_vendor_name"],
                "hh_invoice_date": (
                    l["hh_invoice_date"].isoformat() if l["hh_invoice_date"] else None
                ),
                "hh_invoice_amount": (
                    str(l["hh_invoice_amount"])
                    if l["hh_invoice_amount"] is not None
                    else None
                ),
            }
            for l in links
        ],
    }


# --------------------------------------------------------------------------
# POST /{id}/match — manual link
# --------------------------------------------------------------------------


@router.post("/{invoice_id}/match")
def manual_match(
    body: ManualMatchRequest,
    invoice_id: str = Path(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)

    targets_provided = [
        ("journal", body.journal_batch_id),
        ("bank", body.bank_transaction_id),
        ("hh_ap", body.hh_ap_invoice_id),
    ]
    provided = [(t, v) for (t, v) in targets_provided if v]
    if len(provided) != 1:
        raise HTTPException(
            400,
            "Provide exactly one of journal_batch_id, bank_transaction_id, hh_ap_invoice_id",
        )
    match_type, target_id = provided[0]

    clerk_user_id = (
        _user.clerk_user_id if isinstance(_user, CurrentUser) else 'manual'
    )

    with db_session() as session:
        inv = session.execute(
            text(
                "SELECT id, status FROM invoice_documents "
                "WHERE id = :id AND entity_code = :ec"
            ),
            {"id": invoice_id, "ec": body.entity_code},
        ).mappings().first()
        if not inv:
            raise HTTPException(404, "Invoice document not found")
        if inv["status"] == 'deleted':
            raise HTTPException(409, "Invoice document is deleted")

        from .. import services_invoice_matching as svc

        svc._create_link(
            session,
            invoice_id=invoice_id,
            entity_code=body.entity_code,
            match_type=match_type,
            target_id=target_id,
            confidence=100.0,
            linked_by=clerk_user_id or 'manual',
        )
        session.execute(
            text(
                """
                UPDATE invoice_documents
                   SET status = 'matched',
                       matched_at = NOW(),
                       matched_by_clerk_user_id = :uid,
                       match_confidence = 100.0,
                       updated_at = NOW()
                 WHERE id = :id
                """
            ),
            {"uid": clerk_user_id, "id": invoice_id},
        )
    return {"ok": True, "invoice_id": invoice_id, "link_type": match_type}


# --------------------------------------------------------------------------
# POST /{id}/post-to-ap — create the journal entry
# --------------------------------------------------------------------------


@router.post("/{invoice_id}/post-to-ap")
def post_invoice_to_ap(
    body: PostToApRequest,
    invoice_id: str = Path(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)

    from datetime import datetime as _dt

    try:
        period_end = _dt.strptime(body.period_end, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(400, f"period_end must be YYYY-MM-DD: {exc}") from exc

    clerk_user_id = (
        _user.clerk_user_id if isinstance(_user, CurrentUser) else 'manual'
    )

    with db_session() as session:
        inv = session.execute(
            text(
                """
                SELECT id, vendor_name, invoice_number, amount, status,
                       invoice_type
                  FROM invoice_documents
                 WHERE id = :id AND entity_code = :ec
                """
            ),
            {"id": invoice_id, "ec": body.entity_code},
        ).mappings().first()
        if not inv:
            raise HTTPException(404, "Invoice document not found")
        if inv["status"] == 'posted_to_ap':
            raise HTTPException(409, "Already posted to AP")
        if inv["status"] == 'deleted':
            raise HTTPException(409, "Invoice document is deleted")
        amount = Decimal(str(inv["amount"] or 0))
        if amount <= 0:
            raise HTTPException(
                400,
                "Invoice amount is 0 or missing — edit the invoice first",
            )

        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Entity {body.entity_code!r} not found")

        period = session.execute(
            text(
                """
                SELECT id FROM accounting_periods
                 WHERE entity_id = :eid AND period_end = :pe
                 LIMIT 1
                """
            ),
            {"eid": entity["id"], "pe": period_end},
        ).mappings().first()
        if not period:
            raise HTTPException(
                400,
                f"No accounting_periods row for {body.entity_code} period_end {body.period_end}",
            )

        memo = (
            body.memo
            or f"{inv['vendor_name'] or 'Vendor'}"
            + (f" · {inv['invoice_number']}" if inv["invoice_number"] else "")
        )
        # Idempotent batch label keyed by invoice id, so retries don't double-post.
        batch_label = f"invoice-{invoice_id[:8]}"

        # Insert/update the batch.
        batch = session.execute(
            text(
                """
                INSERT INTO journal_batches (
                    entity_id, accounting_period_id, source_module, batch_label,
                    status, workflow_status,
                    total_debits, total_credits, summary_json
                ) VALUES (
                    :eid, :apid, 'invoice_documents', :label,
                    'draft', 'draft_ready',
                    :tot, :tot,
                    CAST(:summary AS jsonb)
                )
                ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label)
                DO UPDATE SET
                    status = 'draft',
                    workflow_status = 'draft_ready',
                    total_debits = EXCLUDED.total_debits,
                    total_credits = EXCLUDED.total_credits,
                    summary_json = EXCLUDED.summary_json,
                    updated_at = NOW()
                RETURNING id
                """
            ),
            {
                "eid": entity["id"],
                "apid": period["id"],
                "label": batch_label,
                "tot": amount,
                "summary": _summary_json(invoice_id, inv, amount, body.ap_account),
            },
        ).mappings().first()
        assert batch is not None
        batch_id = batch["id"]

        # Wipe + re-insert lines for idempotency.
        session.execute(
            text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
            {"id": batch_id},
        )
        dr_line = session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :bid, 1, :acct, :amt, 0, :memo, CAST(:src AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "bid": batch_id,
                "acct": body.expense_account_code,
                "amt": amount,
                "memo": memo,
                "src": '{"source": "invoice_documents", "side": "dr"}',
            },
        ).mappings().first()
        cr_line = session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :bid, 2, :acct, 0, :amt, :memo, CAST(:src AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "bid": batch_id,
                "acct": body.ap_account,
                "amt": amount,
                "memo": memo,
                "src": '{"source": "invoice_documents", "side": "cr"}',
            },
        ).mappings().first()

        # Link the invoice to the newly-created batch.
        from .. import services_invoice_matching as svc

        svc._create_link(
            session,
            invoice_id=invoice_id,
            entity_code=body.entity_code,
            match_type='journal',
            target_id=str(batch_id),
            confidence=100.0,
            linked_by=clerk_user_id or 'manual',
            journal_line_id=cr_line["id"] if cr_line else None,
        )
        session.execute(
            text(
                """
                UPDATE invoice_documents
                   SET status = 'posted_to_ap',
                       ap_account = :ap,
                       matched_at = COALESCE(matched_at, NOW()),
                       matched_by_clerk_user_id = COALESCE(matched_by_clerk_user_id, :uid),
                       match_confidence = COALESCE(match_confidence, 100.0),
                       updated_at = NOW()
                 WHERE id = :id
                """
            ),
            {"ap": body.ap_account, "uid": clerk_user_id, "id": invoice_id},
        )

    return {
        "ok": True,
        "invoice_id": invoice_id,
        "journal_batch_id": str(batch_id),
        "amount": str(amount),
        "ap_account": body.ap_account,
        "expense_account": body.expense_account_code,
    }


def _summary_json(invoice_id: str, inv: Any, amount: Decimal, ap_account: str) -> str:
    import json

    return json.dumps({
        "invoice_document_id": str(invoice_id),
        "vendor_name": inv["vendor_name"],
        "invoice_number": inv["invoice_number"],
        "invoice_type": inv["invoice_type"],
        "amount": str(amount),
        "ap_account": ap_account,
    })


# --------------------------------------------------------------------------
# POST /{id}/delete (soft)
# --------------------------------------------------------------------------


@router.post("/{invoice_id}/delete")
def soft_delete(
    body: DeleteRequest,
    invoice_id: str = Path(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    with db_session() as session:
        row = session.execute(
            text(
                """
                UPDATE invoice_documents
                   SET status = 'deleted',
                       notes = COALESCE(notes || E'\\n', '') || 'DELETED: ' || :reason,
                       updated_at = NOW()
                 WHERE id = :id AND entity_code = :ec
                RETURNING id, status
                """
            ),
            {"id": invoice_id, "ec": body.entity_code, "reason": body.reason},
        ).mappings().first()
    if not row:
        raise HTTPException(404, "Invoice document not found")
    return {"ok": True, "invoice_id": invoice_id, "status": row["status"]}


# --------------------------------------------------------------------------
# PATCH /{id} — edit parsed fields (used by the queue UI to fill gaps)
# --------------------------------------------------------------------------


@router.patch("/{invoice_id}")
def update_invoice(
    body: UpdateInvoiceRequest,
    invoice_id: str = Path(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    updates = body.model_dump(exclude={"entity_code"}, exclude_none=True)
    if not updates:
        raise HTTPException(400, "no fields to update")
    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    params: dict[str, Any] = {**updates, "id": invoice_id, "ec": body.entity_code}
    with db_session() as session:
        row = session.execute(
            text(
                f"""
                UPDATE invoice_documents
                   SET {set_clause}, updated_at = NOW()
                 WHERE id = :id AND entity_code = :ec
                RETURNING id, entity_code, invoice_type, invoice_number,
                          vendor_name, invoice_date, due_date, amount,
                          currency, status, ap_account, file_name,
                          file_size_bytes, file_path, uploaded_at, matched_at,
                          match_confidence, notes
                """
            ),
            params,
        ).mappings().first()
    if not row:
        raise HTTPException(404, "Invoice document not found")
    return _invoice_to_dict(row)


# --------------------------------------------------------------------------
# POST /sweep — admin-triggered period match
# --------------------------------------------------------------------------


@router.post("/sweep")
def run_sweep(
    entity_code: str = Query(...),
    period_end: str | None = Query(default=None),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    from datetime import datetime as _dt

    pe_date = None
    if period_end:
        try:
            pe_date = _dt.strptime(period_end, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    with db_session() as session:
        summary: SweepSummary = run_period_match_sweep(
            session, entity_code=entity_code, period_end=pe_date,
        )
    return {
        "invoices_examined": summary.invoices_examined,
        "auto_matched": summary.auto_matched,
        "suggested": summary.suggested,
        "unmatched": summary.unmatched,
    }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _invoice_to_dict(row: Any) -> dict[str, Any]:
    file_path = row.get("file_path") if hasattr(row, "get") else (
        row["file_path"] if "file_path" in row.keys() else None  # type: ignore[index]
    )
    return {
        "id": str(row["id"]),
        "entity_code": row["entity_code"],
        "invoice_type": row["invoice_type"],
        "invoice_number": row["invoice_number"],
        "vendor_name": row["vendor_name"],
        "invoice_date": (
            row["invoice_date"].isoformat() if row["invoice_date"] else None
        ),
        "due_date": (
            row["due_date"].isoformat() if row.get("due_date") else None
        ),
        "amount": str(row["amount"]) if row["amount"] is not None else None,
        "currency": row["currency"],
        "status": row["status"],
        "ap_account": row["ap_account"],
        "file_name": row["file_name"],
        "file_size_bytes": row["file_size_bytes"],
        "file_path": file_path,
        "file_url": storage_service.get_presigned_url(file_path),
        "uploaded_at": (
            row["uploaded_at"].isoformat() if row["uploaded_at"] else None
        ),
        "matched_at": (
            row["matched_at"].isoformat() if hasattr(row, "get") and row.get("matched_at") else None
        ),
        "match_confidence": (
            float(row["match_confidence"])
            if hasattr(row, "get") and row.get("match_confidence") is not None
            else None
        ),
        "notes": row.get("notes") if hasattr(row, "get") else None,
    }
