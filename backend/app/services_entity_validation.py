"""
Entity-validation guard for every upload endpoint.

Catches the "wrong dealer" upload before it persists. The Lyndhurst-PDFs-
into-Bridlewood incident on 2026-05-24 is the canonical bug this module
prevents: a dealer uploads files belonging to a different store and the
parser happily saves them under the active entity.

The validator works on whatever signal is available — typically the
filename (HH dealer files prefix with the store number, e.g.
'14643_ARSTMT_01312026_D.pdf' = store 1464-3) and the first few KB of
the file's text. For PDFs that requires pdfplumber; for CSV/Excel we
peek at the raw bytes. We never block on signal *absence* — only on
strong evidence of mismatch — so unknown formats degrade to a warning
or no-op rather than a hard 422.

Public surface:
    validate_document_entity(...) -> EntityValidationResult
    raise_or_warn(result, warnings_list) -> bool   # convenience for routes

Response contract (per the spec):
    Block  → HTTPException 422 with detail dict
    Warn   → caller appends result.warning_payload to its response's
             `warnings` list and returns 200
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Result type
# --------------------------------------------------------------------------


@dataclass
class EntityValidationResult:
    matched: bool
    confidence: float  # 0.0 — 1.0
    extracted_identifier: str | None
    expected_identifier: str
    identifier_type: str  # 'hh_store_number' | 'business_number' | 'entity_name' | 'bank_last4' | ...
    warning_only: bool  # True → warn but allow; False → block
    document_type: str
    filename: str
    entity_code: str
    entity_name: str | None = None
    # Free-form notes — populated when no identifier was extractable so
    # the caller can decide whether to log or surface.
    note: str | None = None

    @property
    def has_warning(self) -> bool:
        """Should the route include this in its `warnings` field?"""
        return not self.matched and self.warning_only

    @property
    def should_block(self) -> bool:
        return not self.matched and not self.warning_only

    @property
    def block_payload(self) -> dict[str, Any]:
        """422 body when should_block is True."""
        return {
            "error": "wrong_entity",
            "message": (
                f"This {self.document_type} appears to belong to "
                f"{self.extracted_identifier} but you are uploading to "
                f"{self.entity_code} ({self.entity_name or ''}). "
                f"This file was not saved."
            ),
            "document_entity": self.extracted_identifier,
            "active_entity": self.entity_code,
            "active_entity_name": self.entity_name,
            "filename": self.filename,
            "identifier_type": self.identifier_type,
        }

    @property
    def warning_payload(self) -> dict[str, Any]:
        """Item to append to a route's response `warnings` list."""
        return {
            "kind": "entity_mismatch_warning",
            "message": (
                f"Could not verify this {self.document_type} belongs to "
                f"{self.entity_name or self.entity_code}. Please confirm "
                f"before treating the data as final."
            ),
            "extracted": self.extracted_identifier,
            "expected": self.expected_identifier,
            "identifier_type": self.identifier_type,
            "filename": self.filename,
        }


# --------------------------------------------------------------------------
# Document-type classes
# --------------------------------------------------------------------------


_HH_AP_TYPES = {
    "hh_ap_statement",
    "hh_ap_invoice",
    "hh_ap_remittance",
    "monthly_statement",
    "monthly_invoice",
    "monthly_remittance",
    "remittance",
    "statement",
    "invoice_batch",
}
_PAYROLL_REGISTER_TYPES = {"payroll_register", "payroll-register"}
_PAYROLL_HOURS_TYPES = {"payroll_hours", "payroll-hours"}
_POS_TYPES = {
    "pos_financial",
    "pos_inventory_adj",
    "pos_inventory_value",
    "pos_aged_ar",
    "pos_ar_adjustment",
    "inventory_adjustment",
    "inventory_value",
    "aged_ar",
    "ar_adjustment",
}
_BANK_PDF_TYPES = {"bank_pdf", "bank-pdf"}
_BANK_CSV_TYPES = {"bank_csv", "bank-csv"}
_GL_TYPES = {"gl_export", "gl-export", "gl_import"}
_INVOICE_DOC_TYPES = {"invoice_document", "hh_ap_invoice_pdf", "outside_vendor"}


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


def validate_document_entity(
    db_session,
    *,
    entity_code: str,
    file_bytes: bytes,
    filename: str,
    document_type: str,
    invoice_kind: str | None = None,  # 'hh_ap' or 'outside_vendor' (invoice docs)
) -> EntityValidationResult:
    """Top-level dispatcher. Routes to the right per-type rule based on
    document_type. Caller passes the file_bytes (may be empty) and the
    original filename — both are best-effort signals.
    """
    entity = _load_entity(db_session, entity_code)

    common = {
        "document_type": document_type,
        "filename": filename or "",
        "entity_code": entity_code,
        "entity_name": entity["entity_name"] if entity else None,
        "expected_identifier": entity_code,
    }

    if not entity:
        return EntityValidationResult(
            matched=False,
            confidence=0.0,
            extracted_identifier=None,
            identifier_type="entity_lookup_failed",
            warning_only=False,
            note=f"entity_code {entity_code!r} not found in DB",
            **common,
        )

    dt = (document_type or "").strip().lower()

    if dt in _HH_AP_TYPES:
        return _validate_hh_ap(file_bytes, filename, entity, common)
    if dt in _INVOICE_DOC_TYPES:
        if (invoice_kind or "").lower() == "outside_vendor":
            # No entity identifier on outside-vendor invoices — skip.
            return _ok(0.0, None, "skipped_outside_vendor", True, common)
        return _validate_hh_ap(file_bytes, filename, entity, common)
    if dt in _PAYROLL_REGISTER_TYPES:
        return _validate_payroll_register(file_bytes, filename, entity, common)
    if dt in _PAYROLL_HOURS_TYPES:
        return _validate_fuzzy_name(file_bytes, filename, entity, common)
    if dt in _POS_TYPES:
        return _validate_fuzzy_name(file_bytes, filename, entity, common)
    if dt in _BANK_PDF_TYPES or dt in _BANK_CSV_TYPES:
        return _validate_bank(file_bytes, filename, entity, common)
    if dt in _GL_TYPES:
        return _validate_fuzzy_name(file_bytes, filename, entity, common)

    # Unknown document_type — no-op, warning only.
    return _ok(0.0, None, "unknown_document_type", True, common)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _load_entity(db_session, entity_code: str) -> dict[str, Any] | None:
    row = db_session.execute(
        text(
            """
            SELECT id, entity_code, entity_name
              FROM entities WHERE entity_code = :code
            """
        ),
        {"code": entity_code},
    ).mappings().first()
    return dict(row) if row else None


def _ok(
    confidence: float,
    extracted: str | None,
    identifier_type: str,
    warning_only: bool,
    common: dict[str, Any],
    matched: bool = True,
    note: str | None = None,
) -> EntityValidationResult:
    return EntityValidationResult(
        matched=matched,
        confidence=confidence,
        extracted_identifier=extracted,
        identifier_type=identifier_type,
        warning_only=warning_only,
        note=note,
        **common,
    )


def _peek_text(file_bytes: bytes, max_chars: int = 4000) -> str:
    """Best-effort text peek for fuzzy matching. Tries utf-8 then
    latin-1; for PDFs you should pass already-extracted text instead
    (this fallback only catches readable strings in the PDF stream)."""
    if not file_bytes:
        return ""
    try:
        return file_bytes[:max_chars * 4].decode("utf-8", errors="ignore")[:max_chars]
    except Exception:
        try:
            return file_bytes[:max_chars * 4].decode("latin-1", errors="ignore")[:max_chars]
        except Exception:
            return ""


def _normalize_hh_store_number(raw: str) -> str | None:
    """HH dealer store numbers ship as 5 digits in filenames (e.g.
    '14643') and as 'NNNN-N' in entity_code (e.g. '1464-3'). Normalize
    to 'NNNN-N' for comparison. Returns None when the input doesn't
    look like a store number."""
    s = re.sub(r"\D+", "", raw or "")
    if len(s) != 5:
        return None
    return f"{s[:4]}-{s[4]}"


_HH_STORE_RE_FILENAME = re.compile(r"(?<!\d)(\d{5})(?!\d)")
_HH_STORE_RE_DASHED = re.compile(r"(?<!\d)(\d{4}-\d)(?!\d)")
_BUSINESS_NUMBER_RE = re.compile(r"\b(\d{9}RP\d{4})\b")
_ACCOUNT_LAST4_RE = re.compile(
    r"(?:account|acct|a/c)[^\d]{0,12}(?:\*+|x+|X+)?(\d{4})", re.IGNORECASE
)


# --------------------------------------------------------------------------
# Per-type validators
# --------------------------------------------------------------------------


def _validate_hh_ap(
    file_bytes: bytes, filename: str, entity: dict[str, Any], common: dict[str, Any]
) -> EntityValidationResult:
    """HH AP files (statements, invoices, remittances) reliably encode
    the store number in the filename — e.g. '14643_ARSTMT_*.pdf' or
    '18778_REMIT_*.pdf'. We block on mismatch because the cost of
    cross-contamination here is high (mis-matched AP balances).
    """
    expected = common["entity_code"]

    # Try filename first (most reliable for HH AP).
    candidate: str | None = None
    for m in _HH_STORE_RE_DASHED.finditer(filename or ""):
        candidate = m.group(1)
        break
    if not candidate:
        for m in _HH_STORE_RE_FILENAME.finditer(filename or ""):
            normalized = _normalize_hh_store_number(m.group(1))
            if normalized:
                candidate = normalized
                break

    # Fallback: peek file text.
    if not candidate:
        peek = _peek_text(file_bytes)
        for m in _HH_STORE_RE_DASHED.finditer(peek):
            candidate = m.group(1)
            break
        if not candidate:
            for m in _HH_STORE_RE_FILENAME.finditer(peek):
                normalized = _normalize_hh_store_number(m.group(1))
                if normalized:
                    candidate = normalized
                    break

    if not candidate:
        # No identifier found anywhere — warn so the dealer confirms.
        return _ok(
            0.0, None, "hh_store_number", True, common,
            matched=False,
            note="No store number found in filename or file content",
        )

    if candidate == expected:
        return _ok(1.0, candidate, "hh_store_number", False, common, matched=True)

    return EntityValidationResult(
        matched=False,
        confidence=1.0,
        extracted_identifier=candidate,
        identifier_type="hh_store_number",
        warning_only=False,  # BLOCK
        note=f"Filename/content has store {candidate}, expected {expected}",
        **common,
    )


def _validate_payroll_register(
    file_bytes: bytes, filename: str, entity: dict[str, Any], common: dict[str, Any]
) -> EntityValidationResult:
    """Payroll registers carry the CRA business number (NNNNNNNNNRPNNNN).
    We block when one is present but doesn't match the entity's stored
    number; warn when the entities table doesn't have one yet.
    """
    peek = _peek_text(file_bytes)
    m = _BUSINESS_NUMBER_RE.search(peek) or _BUSINESS_NUMBER_RE.search(filename or "")
    candidate = m.group(1) if m else None

    expected = entity.get("payroll_business_number")
    if not expected:
        # Column not populated for this entity — soft warn.
        return _ok(
            0.0,
            candidate,
            "business_number",
            True,
            common,
            matched=candidate is None,
            note="entities.payroll_business_number not configured",
        )

    if not candidate:
        return _ok(
            0.0, None, "business_number", True, common,
            matched=False,
            note="No business number found in file",
        )
    if candidate.replace(" ", "") == str(expected).replace(" ", ""):
        return _ok(1.0, candidate, "business_number", False, common, matched=True)
    return EntityValidationResult(
        matched=False,
        confidence=1.0,
        extracted_identifier=candidate,
        identifier_type="business_number",
        warning_only=False,
        note=f"File business number {candidate} != expected {expected}",
        **common,
    )


def _validate_fuzzy_name(
    file_bytes: bytes, filename: str, entity: dict[str, Any], common: dict[str, Any]
) -> EntityValidationResult:
    """Fuzzy match the entity_name against the document's text. Used
    for POS reports, GL exports, payroll hours — all of which carry
    the store name but in unpredictable layouts. Warn-only on
    mismatch because names vary too much to block cleanly."""
    expected_name = (entity.get("entity_name") or "").strip()
    if not expected_name:
        return _ok(0.0, None, "entity_name", True, common, matched=True)

    peek = (_peek_text(file_bytes) + " " + (filename or "")).lower()
    expected_lower = expected_name.lower()
    if not peek:
        return _ok(
            0.0, None, "entity_name", True, common,
            matched=False,
            note="No readable text in file for name match",
        )

    # Tier 1: substring of entity name appears anywhere in the peek.
    # Strip "home hardware" / common suffixes to get the distinctive part.
    distinctive = expected_lower.replace("home hardware", "").strip()
    if distinctive and distinctive in peek:
        return _ok(0.95, distinctive, "entity_name", True, common, matched=True)
    if expected_lower in peek:
        return _ok(1.0, expected_name, "entity_name", True, common, matched=True)

    # Tier 2: SequenceMatcher ratio against the first 200 chars.
    score = SequenceMatcher(None, expected_lower, peek[:200]).ratio()
    if score >= 0.45:
        return _ok(score, expected_name, "entity_name", True, common, matched=True)

    # No clear match — warn.
    return _ok(
        score,
        None,
        "entity_name",
        True,
        common,
        matched=False,
        note=f"Fuzzy name match score {score:.2f} below threshold",
    )


def _validate_bank(
    file_bytes: bytes, filename: str, entity: dict[str, Any], common: dict[str, Any]
) -> EntityValidationResult:
    """Bank statements / CSVs carry an account number. We attempt to
    extract the last 4 digits and compare to entities.bank_account_last4
    when present. Warn-only — bank statement formats vary too widely
    to block on a missed extraction.
    """
    peek = _peek_text(file_bytes)
    m = _ACCOUNT_LAST4_RE.search(peek) or _ACCOUNT_LAST4_RE.search(filename or "")
    candidate = m.group(1) if m else None
    expected = entity.get("bank_account_last4")

    if not expected:
        return _ok(
            0.0, candidate, "bank_last4", True, common,
            matched=candidate is None,
            note="entities.bank_account_last4 not configured",
        )
    if not candidate:
        return _ok(
            0.0, None, "bank_last4", True, common,
            matched=False,
            note="No account number found in file",
        )
    if candidate == str(expected):
        return _ok(1.0, candidate, "bank_last4", True, common, matched=True)
    return _ok(
        1.0,
        candidate,
        "bank_last4",
        True,  # Warn-only per spec; bank statements often mask account number
        common,
        matched=False,
        note=f"Account last 4 {candidate} != expected {expected}",
    )


# --------------------------------------------------------------------------
# Convenience for routes
# --------------------------------------------------------------------------


def raise_or_warn(
    result: EntityValidationResult, warnings: list[dict[str, Any]] | None = None
) -> bool:
    """Apply a validation result. Raises HTTPException(422, …) when
    should_block. Otherwise appends to `warnings` if it has a
    warning, and returns True. Returns True on clean match.

    Returns False only when caller passed warnings=None and a warning
    was generated — caller can use the return value to decide whether
    to log instead.
    """
    if result.should_block:
        raise HTTPException(status_code=422, detail=result.block_payload)
    if result.has_warning:
        if warnings is not None:
            warnings.append(result.warning_payload)
            return True
        return False
    return True
