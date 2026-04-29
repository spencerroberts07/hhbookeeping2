"""
Inventory adjustment + month-end POS import — service layer.

Parses the four month-end reports the store POS exports as fixed-width
text and persists them to the tables created in
sql/014_inventory_month_end.sql.

Reports:
    inventory_adjustment   store_use, donation, etc. — line-level
    pos_financial          payment-type / sales / COGS / HST totals
    inventory_value        snapshot of stocked inventory cost & retail
    aged_ar                house-account aging buckets

Public surface (used by routes/pos_import.py):
    Parsers (pure functions, no DB):
        parse_inventory_adjustment_report(text) -> dict
        parse_pos_financial_report(text)        -> dict
        parse_inventory_value_report(text)      -> dict
        parse_aged_ar_report(text)              -> dict

    Importers (parse + persist):
        import_inventory_adjustment(...)
        import_pos_financial(...)
        import_inventory_value(...)
        import_aged_ar(...)

    Journal builders (run after import):
        build_store_use_journal(...)
        build_donation_journal(...)

    Read helpers:
        list_pos_import_runs(...)
        get_pos_import_run_detail(...)
        get_latest_inventory_value_snapshot(...)
        get_latest_aged_ar_snapshot(...)
        get_latest_pos_financial_snapshot(...)

Chart-of-accounts mapping (Bridlewood — see
seeds/Bridlewood_posting_rules_seed.json rule ME-19):
    1120  Inventory
    6510  Store Use Supplies expense
    6695  Charitable Donations expense

The journal builders accept override account codes so other entities
can use the same module with their own COA.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
    get_or_create_accounting_period,
)


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

REPORT_TYPE_INVENTORY_ADJUSTMENT = "inventory_adjustment"
REPORT_TYPE_POS_FINANCIAL = "pos_financial"
REPORT_TYPE_INVENTORY_VALUE = "inventory_value"
REPORT_TYPE_AGED_AR = "aged_ar"

# Adjustment-reason codes the store uses in the inventory adjustment report
# header. Matched case-insensitively, normalized to uppercase tokens.
ADJ_REASON_SUPPLIES = "SUPPLIES"
ADJ_REASON_STORE_USE = "STORE_USE"
ADJ_REASON_DONATION = "DONATION"
ADJ_REASON_OTHER = "OTHER"

# Bridlewood default chart-of-accounts mapping for the journal builders.
DEFAULT_INVENTORY_ACCOUNT_CODE = "1120"
DEFAULT_STORE_USE_EXPENSE_ACCOUNT_CODE = "6510"
DEFAULT_DONATION_EXPENSE_ACCOUNT_CODE = "6695"

# journal_batches.source_module + batch_label conventions
SOURCE_MODULE_POS_IMPORT = "pos_import"
BATCH_LABEL_STORE_USE = "store_use_inventory_reclass"
BATCH_LABEL_DONATION = "donation_inventory_reclass"

# PDF magic-byte prefix — used to detect a binary PDF upload so we can
# extract text via pypdf / OCR before handing it to a parser.
_PDF_MAGIC = b"%PDF"

# Pypdf occasionally returns a "decoded" stream that looks like text but
# actually has bad font glyph mappings (e.g. '1' rendered as 'l',
# '147.50' as '147.5A'). When the count of valid money tokens in the
# text is well below what the report should contain, we fall back to
# OCR. 5 is a conservative floor — even small reports have more than
# that.
_TEXT_QUALITY_MONEY_TOKEN_FLOOR = 5
_RE_MONEY_TOKEN = re.compile(r"\d+\.\d{2}")


def looks_like_pdf(file_bytes: bytes) -> bool:
    return bool(file_bytes) and file_bytes.lstrip().startswith(_PDF_MAGIC)


def _pypdf_extract_text(file_bytes: bytes) -> str:
    """Best-effort plain-text extraction via pypdf. Returns '' on failure."""
    try:
        from pypdf import PdfReader
        from io import BytesIO
    except Exception:
        return ""
    try:
        reader = PdfReader(BytesIO(file_bytes))
    except Exception:
        return ""
    pages: list[str] = []
    for page in reader.pages:
        try:
            pages.append(page.extract_text() or "")
        except Exception:
            pages.append("")
    return "\n".join(pages)


# Common install locations for the Tesseract binary on Windows. We
# probe these so OCR works even when the backend was started before
# Tesseract was added to PATH.
_TESSERACT_BINARY_CANDIDATES = (
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    # User-scope install (Mannheim installer's "for me only" option)
    None,  # placeholder; filled in below
)


def _resolve_tesseract_cmd() -> str | None:
    """
    Return a path to a usable tesseract executable, or None if we can't
    find one. Honors $TESSERACT_CMD, then falls back to the installer's
    standard install paths on Windows, then PATH.
    """
    import os
    import shutil

    explicit = os.environ.get("TESSERACT_CMD")
    if explicit and os.path.isfile(explicit):
        return explicit
    via_path = shutil.which("tesseract")
    if via_path:
        return via_path
    for candidate in _TESSERACT_BINARY_CANDIDATES:
        if candidate and os.path.isfile(candidate):
            return candidate
    user_local = os.path.expandvars(
        r"%LOCALAPPDATA%\Programs\Tesseract-OCR\tesseract.exe"
    )
    if os.path.isfile(user_local):
        return user_local
    return None


def _ocr_pdf_text(file_bytes: bytes) -> str:
    """
    Rasterize the PDF with PyMuPDF and OCR each page with pytesseract.
    Returns '' if any dependency is unavailable, including a missing
    Tesseract binary.
    """
    try:
        import fitz  # type: ignore
    except Exception:
        return ""
    try:
        from PIL import Image
    except Exception:
        return ""
    try:
        import pytesseract  # type: ignore
    except Exception:
        return ""

    tess_cmd = _resolve_tesseract_cmd()
    if not tess_cmd:
        return ""
    # Tell pytesseract exactly where the binary is — works even if the
    # current process started before Tesseract was added to PATH.
    pytesseract.pytesseract.tesseract_cmd = tess_cmd

    pages: list[str] = []
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
    except Exception:
        return ""
    # 3x rasterization keeps small digits sharp; psm 6 ('uniform block
    # of text') is the right segmentation for the Prism/POS reports
    # where rows are columnar and Tesseract's default psm splits
    # columns into separate top-to-bottom blocks.
    ocr_config = "--psm 6 -c preserve_interword_spaces=1"
    try:
        for page in doc:
            try:
                pix = page.get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False)
                img = Image.frombytes(
                    "RGB", [pix.width, pix.height], pix.samples
                )
                pages.append(
                    pytesseract.image_to_string(img, config=ocr_config) or ""
                )
            except Exception:
                pages.append("")
    finally:
        doc.close()
    return "\n".join(pages)


def extract_text_from_upload(file_bytes: bytes) -> tuple[str, str]:
    """
    Read the upload bytes and return (text, source) where `source` is one
    of 'text', 'pdf_text', 'pdf_ocr'. PDFs go through pypdf first; if
    that returns suspiciously few money tokens we fall back to OCR.

    Raises ValueError when nothing produces usable text, with a
    diagnostic that distinguishes "PDF unparseable, no OCR available"
    from "file is empty".
    """
    if not file_bytes:
        raise ValueError("Uploaded file is empty")

    if not looks_like_pdf(file_bytes):
        # Plain text — try utf-8, fall back to latin-1 (POS reports).
        for enc in ("utf-8", "latin-1"):
            try:
                return file_bytes.decode(enc), "text"
            except UnicodeDecodeError:
                continue
        return file_bytes.decode("utf-8", errors="replace"), "text"

    pypdf_text = _pypdf_extract_text(file_bytes)
    money_tokens = len(_RE_MONEY_TOKEN.findall(pypdf_text))
    if pypdf_text and money_tokens >= _TEXT_QUALITY_MONEY_TOKEN_FLOOR:
        # Heuristic check: glyph-substituted PDFs sometimes still emit
        # SOME good money tokens (e.g. 25,551.01) while corrupting
        # others. Accept if we have at least the floor; the parsers
        # tolerate garbage rows.
        return pypdf_text, "pdf_text"

    ocr_text = _ocr_pdf_text(file_bytes)
    if ocr_text and len(_RE_MONEY_TOKEN.findall(ocr_text)) >= _TEXT_QUALITY_MONEY_TOKEN_FLOOR:
        return ocr_text, "pdf_ocr"

    if pypdf_text:
        # Last-resort: hand back the corrupted text so the parser can
        # produce a partial answer / clear warning rather than 500.
        return pypdf_text, "pdf_text"

    raise ValueError(
        "Could not extract usable text from the PDF. The PDF appears to "
        "use a non-standard font that breaks pypdf, and OCR is not "
        "available (Tesseract is not installed or not on PATH). "
        "Install Tesseract from https://github.com/UB-Mannheim/tesseract/wiki "
        "and re-upload, or export the report as plain text from the POS."
    )


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _money(value: Any) -> Decimal:
    """Best-effort -> Decimal('0.00') for None/blank, else 2-dp Decimal."""
    if value is None or value == "":
        return Decimal("0.00")
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def _money4(value: Any) -> Decimal:
    """4-dp Decimal — used for per-line adjustment_cost and quantities."""
    if value is None or value == "":
        return Decimal("0.0000")
    try:
        return Decimal(str(value)).quantize(Decimal("0.0001"))
    except (InvalidOperation, ValueError):
        return Decimal("0.0000")


def _parse_signed_money_token(token: str) -> Decimal:
    """
    Parse a money token that may use trailing-minus convention
    (e.g. '289.19-') as well as leading-minus and parentheses.
    Returns Decimal('0') for unparseable input.
    """
    if token is None:
        return Decimal("0")
    t = str(token).strip().replace(",", "").replace("$", "")
    if not t:
        return Decimal("0")
    negative = False
    if t.endswith("-"):
        negative = True
        t = t[:-1].strip()
    elif t.startswith("(") and t.endswith(")"):
        negative = True
        t = t[1:-1].strip()
    elif t.startswith("-"):
        negative = True
        t = t[1:].strip()
    try:
        value = Decimal(t)
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return -value if negative else value


def _parse_yy_mm_dd(token: str) -> date | None:
    """
    Accept the date formats that show up across the store's reports:
      YY/MM/DD, YYYY/MM/DD, YYYY-MM-DD, MM/DD/YY, MM/DD/YYYY
      Mmm DD/YY (e.g. 'Feb 28/26'), Mmm DD/YYYY,
      Mmm DD, YYYY, Month DD, YYYY, etc.
    """
    if not token:
        return None
    s = str(token).strip()
    if not s:
        return None
    fmts = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%y/%m/%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%b %d/%y",
        "%b %d/%Y",
        "%b %d, %y",
        "%b %d, %Y",
        "%B %d/%y",
        "%B %d/%Y",
        "%B %d, %y",
        "%B %d, %Y",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_reason_token(raw: str) -> str:
    """
    'SUPPLIES' / 'Store Use' / 'Store use - Coffee bar' all collapse to
    SUPPLIES. 'DONATION' / 'Donation - Food bank' collapse to DONATION.
    Everything else is uppercased and underscored verbatim — generic
    headers like 'ALL' or 'MISC' are preserved so the builder can tell
    a specific run from a combined-report run.
    """
    s = (raw or "").strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s).strip("_")
    if not s:
        return ADJ_REASON_OTHER
    if "DONAT" in s:
        return ADJ_REASON_DONATION
    # The store uses several phrasings for store-use of inventory. The
    # one Bridlewood actually emits in their report is "For Store Non
    # Inventory"; older docs / other stores may say "Store Use" or
    # "Supplies". Consolidate them all into SUPPLIES so the builder
    # filter doesn't have to know all spellings.
    if (
        "STORE_NON_INVENTORY" in s
        or "STORE_USE" in s
        or ("STORE" in s and "USE" in s)
        or "SUPPL" in s
    ):
        return ADJ_REASON_SUPPLIES
    return s


# Specific reasons the journal builders care about. If a run's header
# adjustment_reason is one of these, the builder REQUIRES it match. If
# the header is anything else (ALL / OTHER / MISC / blank), the builder
# trusts the per-line adjustment_reason instead.
_SPECIFIC_RUN_REASONS = frozenset(
    {ADJ_REASON_SUPPLIES, ADJ_REASON_DONATION, ADJ_REASON_STORE_USE}
)


def _classify_line_reason(
    line_reason_description: str | None,
    header_reason: str | None,
) -> str:
    """
    Pick the per-line reason: line-level reason_description first, then
    fall back to the header reason. Used so a 'combined' report (header
    Adjustment Reason: ALL) can still produce store_use and donation
    journals from the line-level reason text.
    """
    if line_reason_description:
        token = _normalize_reason_token(line_reason_description)
        if token and token != ADJ_REASON_OTHER:
            return token
    if header_reason:
        return _normalize_reason_token(header_reason)
    return ADJ_REASON_OTHER


def _coalesce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _split_lines(file_text: str) -> list[str]:
    """Normalize line endings and return the raw lines (preserve spacing)."""
    return file_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")


# --------------------------------------------------------------------------
# Parser — Inventory Adjustment Report
# --------------------------------------------------------------------------


# Field column positions (0-based, [start, end)) per the user's spec:
#     SKU(7) Description(36) Date(8) Quantity(10) QtyAfter(10)
#     AdjustmentCost(15) ReasonDesc(30) EmpID(8)
# We allow some slack: each field is whitespace-trimmed after slicing,
# and a fallback whitespace-split parser kicks in if the row is shorter
# than expected.
_INV_ADJ_SLICES = [
    ("sku_number", 0, 7),
    ("description", 7, 43),
    ("date_adjusted", 43, 51),
    ("quantity_adjusted", 51, 61),
    ("quantity_after", 61, 71),
    ("adjustment_cost", 71, 86),
    ("reason_description", 86, 116),
    ("employee_id", 116, 124),
]

_RE_TOTAL_FOR_REPORT = re.compile(
    r"^\s*Total\s+For\s+Report\s*[:.]?\s*([\-\(\)\$0-9,\.]+\-?)",
    re.IGNORECASE,
)
_RE_ADJ_REASON_HEADER = re.compile(
    r"Adjustment\s+Reason\s*[:.]?\s*(.+)$", re.IGNORECASE
)
_RE_FROM_DATE = re.compile(r"From\s+Date\s*[:.]?\s*([0-9/\-]+)", re.IGNORECASE)
_RE_TO_DATE = re.compile(r"To\s+Date\s*[:.]?\s*([0-9/\-]+)", re.IGNORECASE)
_RE_STORE_LABEL = re.compile(r"Store\s*[:.]?\s*([^\s].*?)\s*$", re.IGNORECASE)
# A data line must start with a 5-7 digit SKU (Prism SKUs are commonly
# 6-7 digits). MFG-number continuation lines are indented and contain
# only the MFG token, no date/quantity columns.
_RE_DATA_ROW_START = re.compile(r"^\s*(\d{4,8})\s")
_RE_MFG_LINE = re.compile(
    r"^\s*(?:MFG\s*[#:]?\s*)?([A-Za-z0-9\-./]+)\s*$"
)


def parse_inventory_adjustment_report(file_text: str) -> dict[str, Any]:
    """
    Parse a fixed-width Inventory Adjustment Report.

    Returns:
        {
            "store_label": str | None,
            "adjustment_reason": str        # normalized: SUPPLIES/STORE_USE/DONATION/OTHER
            "adjustment_reason_raw": str,
            "period_start": date | None,
            "period_end": date | None,
            "total_for_report": Decimal,
            "lines": [
                {
                    "sku_number": str,
                    "description": str,
                    "mfg_number": str | None,
                    "date_adjusted": date | None,
                    "quantity_adjusted": Decimal,
                    "quantity_after": Decimal,
                    "adjustment_cost": Decimal,
                    "reason_description": str | None,
                    "employee_id": str | None,
                },
                ...
            ],
            "employee_summary": [{"employee_id": str, "total_cost": Decimal}, ...],
            "warnings": [str, ...],
        }
    """
    raw_lines = _split_lines(file_text or "")

    store_label: str | None = None
    adjustment_reason_raw: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    total_for_report = Decimal("0.00")
    lines: list[dict[str, Any]] = []
    warnings: list[str] = []

    # First pass: header fields anywhere in first ~30 lines.
    for raw in raw_lines[:60]:
        if adjustment_reason_raw is None:
            m = _RE_ADJ_REASON_HEADER.search(raw)
            if m:
                adjustment_reason_raw = m.group(1).strip()
        if period_start is None:
            m = _RE_FROM_DATE.search(raw)
            if m:
                period_start = _parse_yy_mm_dd(m.group(1))
        if period_end is None:
            m = _RE_TO_DATE.search(raw)
            if m:
                period_end = _parse_yy_mm_dd(m.group(1))
        if store_label is None:
            stripped = raw.strip()
            if stripped.lower().startswith("store"):
                m = _RE_STORE_LABEL.search(stripped)
                if m and not _RE_ADJ_REASON_HEADER.search(stripped):
                    store_label = m.group(1).strip() or None

    # Second pass: data rows + total line. We track the "current" line so a
    # following indented MFG line can attach to the most recent data row.
    current: dict[str, Any] | None = None
    in_employee_summary = False
    employee_summary: list[dict[str, Any]] = []

    for raw in raw_lines:
        line = raw.rstrip()
        if not line.strip():
            continue

        # Total for report
        m_tot = _RE_TOTAL_FOR_REPORT.search(line)
        if m_tot:
            total_for_report = _parse_signed_money_token(m_tot.group(1))
            in_employee_summary = True
            current = None
            continue

        # Employee summary table comes after the totals line. We don't need
        # rich parsing — capture (employee_id, total_cost) pairs.
        if in_employee_summary:
            tokens = line.split()
            # Expect "<EMP_ID> .... <amount>"
            if len(tokens) >= 2:
                amount = _parse_signed_money_token(tokens[-1])
                if amount != Decimal("0"):
                    emp_id = tokens[0]
                    if not emp_id.isdigit() and len(emp_id) > 32:
                        # not a sane row
                        continue
                    employee_summary.append(
                        {"employee_id": emp_id, "total_cost": amount}
                    )
            continue

        # Looks like a data row?
        if _RE_DATA_ROW_START.match(line):
            row: dict[str, Any] = {}
            for field, start, end in _INV_ADJ_SLICES:
                row[field] = line[start:end].strip() if len(line) >= start else ""

            sku = row["sku_number"]
            if not sku:
                continue

            row_data: dict[str, Any] = {
                "sku_number": sku,
                "description": row["description"] or None,
                "mfg_number": None,
                "date_adjusted": _parse_yy_mm_dd(row["date_adjusted"]),
                "quantity_adjusted": _money4(
                    row["quantity_adjusted"].replace(",", "")
                ),
                "quantity_after": _money4(row["quantity_after"].replace(",", "")),
                "adjustment_cost": _parse_signed_money_token(row["adjustment_cost"]),
                "reason_description": row["reason_description"] or None,
                "employee_id": row["employee_id"] or None,
            }

            # If the row is short and the slices yielded almost nothing,
            # fall back to whitespace-split parsing.
            if (
                row_data["adjustment_cost"] == Decimal("0")
                and row_data["date_adjusted"] is None
            ):
                tokens = line.split()
                if len(tokens) >= 5:
                    fallback_sku = tokens[0]
                    # heuristic: last token = employee_id, second last = reason desc?
                    # last numeric-ish from end = adjustment_cost
                    cost_token = None
                    cost_idx = None
                    for i in range(len(tokens) - 1, 0, -1):
                        guess = _parse_signed_money_token(tokens[i])
                        if guess != Decimal("0") or re.match(
                            r"^[\-\(\)\$0-9,\.]+$", tokens[i]
                        ):
                            cost_token = tokens[i]
                            cost_idx = i
                            break
                    if cost_token is not None and cost_idx is not None:
                        row_data["sku_number"] = fallback_sku
                        row_data["adjustment_cost"] = _parse_signed_money_token(
                            cost_token
                        )
                        # description = tokens between sku and a date-looking token
                        date_token_idx = None
                        for i in range(1, cost_idx):
                            if _parse_yy_mm_dd(tokens[i]) is not None:
                                date_token_idx = i
                                break
                        if date_token_idx is not None:
                            row_data["date_adjusted"] = _parse_yy_mm_dd(
                                tokens[date_token_idx]
                            )
                            row_data["description"] = " ".join(
                                tokens[1:date_token_idx]
                            ) or None
                        else:
                            row_data["description"] = " ".join(
                                tokens[1:cost_idx]
                            ) or None

            current = row_data
            lines.append(row_data)
            continue

        # MFG continuation row (indented, no leading SKU digits)
        if current is not None and not _RE_DATA_ROW_START.match(line):
            mfg_match = _RE_MFG_LINE.match(line)
            if mfg_match:
                token = mfg_match.group(1)
                if token and not token.lower().startswith(("date", "from", "to", "page")):
                    current["mfg_number"] = token
                    continue

    if adjustment_reason_raw is None:
        warnings.append("Adjustment Reason header not found")
    if not lines:
        warnings.append("No inventory adjustment data rows parsed")

    return {
        "store_label": store_label,
        "adjustment_reason": _normalize_reason_token(adjustment_reason_raw or ""),
        "adjustment_reason_raw": adjustment_reason_raw,
        "period_start": period_start,
        "period_end": period_end,
        "total_for_report": total_for_report,
        "lines": lines,
        "employee_summary": employee_summary,
        "warnings": warnings,
    }


# --------------------------------------------------------------------------
# Parser — POS Financial History Report
# --------------------------------------------------------------------------


# Each tender / sales / COGS / tax line in the POS Financial Report is:
#     <left-justified label>   <debit>   <credit>   <count>
# We normalize the label by lower-casing and stripping non-letters
# to match against canonical keys.
_POS_FINANCIAL_LABEL_MAP: dict[str, str] = {
    # Tender (debit-side)
    "cash": "cash_amount",
    "cheque": "cheque_amount",
    "check": "cheque_amount",
    "visanet": "visa_net",
    "mastercardnet": "mastercard_net",
    "debitcardsnet": "debit_net",
    "debitnet": "debit_net",
    "americanexpressnet": "amex_net",
    "amexnet": "amex_net",
    "ecommercenet": "ecommerce_net",
    "giftcardsnet": "gift_card_net",
    "giftcardnet": "gift_card_net",
    # The store has two distinct gift-card lines in the report
    # ("Home Gift Card (net)" and "e-Gift (net)"). Both roll into the
    # single gift_card_net column on pos_financial_snapshots.
    "homegiftcardnet": "gift_card_net",
    "egiftnet": "gift_card_net",
    # COGS — parent rows on the credit side
    "costofgoodssold": "cogs_merchandise",
    "costofgoodssoldnonmerch": "cogs_non_merchandise",
    "costofgoodssoldnonmerchandise": "cogs_non_merchandise",
    # Tax
    "hst": "hst_collected",
    "hst5": "hst_5pct",
    "hst5pct": "hst_5pct",
}

# Sales rows are bucketed via section-context (the parent label has no
# numbers; the indented child rows carry the credit amounts), so they
# are NOT in _POS_FINANCIAL_LABEL_MAP. The section labels themselves:
_POS_SECTION_LABELS: dict[str, str] = {
    "merchandisesales": "merchandise_sales",
    "nonmerchandisesales": "non_merchandise_sales",
}

# Labels of the indented "Cost of Sales - X" debit rows that mirror the
# COGS credit total. Their debits must contribute to total_debit_side
# (so the report balances) but should NOT be bucketed into
# other_tender — they're internal offsets to COGS, not separate items.
_POS_COST_OF_SALES_PREFIX = "costofsales"

# Period range from the report header. The store actually emits
# "For: <start> - <end>" on its POS Financial History Report. Other
# Prism / POS variants use "From:" or "Date Range:". Match all of them.
_RE_POS_PERIOD = re.compile(
    r"(?:For|From|Period|Date\s*Range)\s*[:.]?\s*"
    r"([0-9/\-]+)\s*(?:to|through|thru|and|-|–|—)\s*([0-9/\-]+)",
    re.IGNORECASE,
)
# Fallback: any line whose label is "For" / "For:" / "Period" /
# "Period:" — even if the separator between the two dates is something
# unusual (multiple spaces, "Through", etc.), we can still pull the
# first two date-shaped tokens off the line.
_RE_DATE_TOKEN = re.compile(r"\d{1,4}[/\-]\d{1,2}[/\-]\d{1,4}")
_PERIOD_LINE_PREFIXES = ("for:", "for ", "period:", "period ", "from:", "from ")


def _extract_pos_period(
    raw_lines: list[str],
) -> tuple[date | None, date | None]:
    for raw in raw_lines[:80]:
        m = _RE_POS_PERIOD.search(raw)
        if m:
            ds = _parse_yy_mm_dd(m.group(1))
            de = _parse_yy_mm_dd(m.group(2))
            if ds and de:
                return ds, de

    for raw in raw_lines[:80]:
        stripped = raw.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if not any(lower.startswith(p) for p in _PERIOD_LINE_PREFIXES):
            continue
        tokens = _RE_DATE_TOKEN.findall(stripped)
        if len(tokens) >= 2:
            ds = _parse_yy_mm_dd(tokens[0])
            de = _parse_yy_mm_dd(tokens[1])
            if ds and de:
                return ds, de
        if len(tokens) == 1:
            d = _parse_yy_mm_dd(tokens[0])
            if d:
                return d, d
    return None, None


def _normalize_pos_label(raw: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (raw or "").lower())


_POS_MONEY_TOKEN = re.compile(r"\(?-?\$?-?\s?[\d,]*\.\d{2}\-?\)?")
_POS_HEADER_RE = re.compile(r"\bDEBIT\b.*\bCREDIT\b")
_POS_STOP_TOKENS = (
    "loyalty:",
    "net h.s.t",
    "paid in/out breakdown",
    "end of report",
)


def _detect_pos_columns(raw_lines: list[str]) -> dict[str, int] | None:
    """
    Find the column-header row ("DEBIT  CREDIT  COUNT") and return the
    end-positions of each header word. Right-aligned amounts in the data
    rows end at (or just past) those positions, so we can place each
    money token into the correct column by its end-offset rather than
    by token order.
    """
    for raw in raw_lines[:80]:
        if not _POS_HEADER_RE.search(raw):
            continue
        debit_m = re.search(r"\bDEBIT\b", raw)
        credit_m = re.search(r"\bCREDIT\b", raw)
        count_m = re.search(r"\bCOUNT\b", raw)
        if not debit_m or not credit_m:
            continue
        return {
            "debit_end": debit_m.end(),
            "credit_end": credit_m.end(),
            "count_end": count_m.end() if count_m else credit_m.end() + 20,
        }
    return None


def _assign_pos_amount(
    line: str,
    cols: dict[str, int] | None,
) -> tuple[Decimal, Decimal, list[str]]:
    """
    Bucket each money token in `line` into debit / credit / (ignored)
    using the column end-offsets from _detect_pos_columns(), with a
    positional first/second fallback if the header wasn't located.
    """
    matches = list(_POS_MONEY_TOKEN.finditer(line))
    if not matches:
        return Decimal("0.00"), Decimal("0.00"), []

    debit_amount = Decimal("0.00")
    credit_amount = Decimal("0.00")
    raw_tokens: list[str] = []

    if cols is not None:
        # Allow tokens to extend a couple of chars past the header end —
        # right-aligned amounts wider than "DEBIT" / "CREDIT" spill left,
        # but the END usually still sits at the header end.
        debit_cutoff = cols["debit_end"] + 2
        credit_cutoff = cols["credit_end"] + 2
        for m in matches:
            tok = m.group()
            value = _parse_signed_money_token(tok)
            end_col = m.end()
            raw_tokens.append(tok)
            if end_col <= debit_cutoff:
                debit_amount += value
            elif end_col <= credit_cutoff:
                credit_amount += value
            # tokens past credit_cutoff are COUNT / decimal noise — ignore
        return debit_amount, credit_amount, raw_tokens

    raw_tokens = [m.group() for m in matches]
    if len(raw_tokens) >= 1:
        debit_amount = _parse_signed_money_token(raw_tokens[0])
    if len(raw_tokens) >= 2:
        credit_amount = _parse_signed_money_token(raw_tokens[1])
    return debit_amount, credit_amount, raw_tokens


def parse_pos_financial_report(file_text: str) -> dict[str, Any]:
    """
    Parse the POS Financial History Report (Prism finhstrp-1 layout).

    Strategy:
        1. Pull period from the "FOR: <start> to <end>" header.
        2. Use the "DEBIT  CREDIT  COUNT" header line to locate the
           end-offsets of the debit and credit columns; bucket each
           money token in subsequent rows by its end offset, so a row
           with just one amount lands in the correct column instead of
           always being treated as the debit.
        3. Track section context: a label-only line whose normalized
           form is "merchandisesales" or "nonmerchandisesales" sets the
           current section. Indented child rows then roll up to that
           parent's canonical field.
        4. Stop at the "$  total  $  total" totals block — page-2
           "Paid In/Out" detail is not double-counted.
    """
    raw_lines = _split_lines(file_text or "")
    period_start, period_end = _extract_pos_period(raw_lines)
    cols = _detect_pos_columns(raw_lines)

    fields: dict[str, Decimal] = {}
    other_tender: dict[str, Decimal] = {}
    house_account_debit = Decimal("0.00")
    house_account_credit = Decimal("0.00")
    total_debit_side = Decimal("0.00")
    total_credit_side = Decimal("0.00")
    raw_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    current_section: str | None = None

    for raw in raw_lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()

        # End-of-data sentinels
        if stripped.startswith("$") and "$" in stripped[1:]:
            break
        if set(stripped) <= set("- ") and stripped.count("-") >= 5:
            break
        if set(stripped) <= set("= "):
            break
        if any(tag in lower for tag in _POS_STOP_TOKENS):
            break

        # Page banners / metadata lines we always skip
        if "finhstrp" in lower or "page:" in lower:
            current_section = None
            continue
        if lower.startswith("store:") or lower.startswith("for:"):
            continue
        if "continued on page" in lower:
            continue
        if _POS_HEADER_RE.search(line):
            # The DEBIT / CREDIT / COUNT header itself
            continue

        debit_amount, credit_amount, tokens = _assign_pos_amount(line, cols)

        if tokens:
            first_match = _POS_MONEY_TOKEN.search(line)
            label_raw = (
                line[: first_match.start()].strip() if first_match else stripped
            )
        else:
            label_raw = stripped

        if not label_raw:
            continue
        norm_label = _normalize_pos_label(label_raw)
        if not norm_label:
            continue

        is_indented = line.startswith(" ") or line.startswith("\t")

        if not tokens:
            # Section header: no numeric tokens
            if norm_label in _POS_SECTION_LABELS:
                current_section = _POS_SECTION_LABELS[norm_label]
            else:
                current_section = None
            continue

        raw_rows.append(
            {
                "label": label_raw,
                "label_normalized": norm_label,
                "section": current_section,
                "indented": is_indented,
                "debit": str(debit_amount),
                "credit": str(credit_amount),
            }
        )

        # Every data row counts toward the column totals, including the
        # COGS-offset Cost-of-Sales children — that's how the report
        # itself balances.
        total_debit_side += debit_amount
        total_credit_side += credit_amount

        if "houseaccount" in norm_label:
            house_account_debit += debit_amount
            house_account_credit += credit_amount
            continue

        # Cost-of-Sales children mirror the COGS credit total on the
        # debit side. Don't bucket them into other_tender — they're
        # internal offsets, not separate tender items.
        if norm_label.startswith(_POS_COST_OF_SALES_PREFIX):
            continue

        # Indented children of the Merchandise / Non-Merchandise Sales
        # section headers carry the credit amounts the parent line
        # itself doesn't.
        if is_indented and current_section in {
            "merchandise_sales",
            "non_merchandise_sales",
        }:
            fields[current_section] = (
                fields.get(current_section, Decimal("0.00")) + credit_amount
            )
            continue

        canonical = _POS_FINANCIAL_LABEL_MAP.get(norm_label)
        if canonical:
            if canonical in {
                "merchandise_sales",
                "non_merchandise_sales",
                "cogs_merchandise",
                "cogs_non_merchandise",
                "hst_collected",
                "hst_5pct",
            }:
                fields[canonical] = (
                    fields.get(canonical, Decimal("0.00")) + credit_amount
                )
            else:
                fields[canonical] = (
                    fields.get(canonical, Decimal("0.00")) + debit_amount
                )
        else:
            net = debit_amount - credit_amount
            other_tender[label_raw] = (
                other_tender.get(label_raw, Decimal("0.00")) + net
            )

    is_balanced = (
        total_debit_side != Decimal("0")
        and abs(total_debit_side - total_credit_side) < Decimal("0.05")
    )
    if not is_balanced:
        warnings.append(
            "POS Financial debit total does not equal credit total: "
            f"debit={total_debit_side} credit={total_credit_side}"
        )

    return {
        "period_start": period_start,
        "period_end": period_end,
        "fields": fields,
        "house_account_debit": house_account_debit,
        "house_account_credit": house_account_credit,
        "other_tender": other_tender,
        "total_debit_side": total_debit_side,
        "total_credit_side": total_credit_side,
        "is_balanced": is_balanced,
        "warnings": warnings,
        "raw_rows": raw_rows,
    }


# --------------------------------------------------------------------------
# Parser — Inventory Value Report
# --------------------------------------------------------------------------


_RE_INV_VALUE_TOTAL = re.compile(
    r"^\s*Total\s+For\s+Report\s*[:.]?\s*(.+)$", re.IGNORECASE
)
_RE_INV_VALUE_AS_OF = re.compile(
    r"(?:As\s+of|Snapshot\s+Date|Report\s+Date)\s*[:.]?\s*([0-9/\-]+)",
    re.IGNORECASE,
)


def parse_inventory_value_report(file_text: str) -> dict[str, Any]:
    """
    Parse the Current Stocked Inventory Value Report.

    The body is a per-department summary with columns:
        Dept Description Qty Cost Retail GM$ GM%

    We split on whitespace from the right (GM%, GM$, Retail, Cost, Qty)
    so a multi-word department description is preserved on the left.
    """
    raw_lines = _split_lines(file_text or "")

    snapshot_date: date | None = None
    departments: list[dict[str, Any]] = []
    total_sku_count: int | None = None
    total_cost_value = Decimal("0.00")
    total_retail_value = Decimal("0.00")
    total_gm_dollars = Decimal("0.00")
    total_gm_pct: Decimal | None = None
    warnings: list[str] = []

    for raw in raw_lines[:60]:
        m = _RE_INV_VALUE_AS_OF.search(raw)
        if m:
            snapshot_date = _parse_yy_mm_dd(m.group(1))
            if snapshot_date:
                break

    money_token_re = re.compile(r"-?\$?-?[\d,]*\.\d{1,4}\-?")
    pct_token_re = re.compile(r"-?\d{1,3}\.\d{1,2}\s*%?")

    for raw in raw_lines:
        line = raw.rstrip()
        if not line.strip():
            continue

        m_total = _RE_INV_VALUE_TOTAL.search(line)
        if m_total:
            tail = m_total.group(1)
            tokens = tail.split()
            # Walk from the right: GM% (often "%" suffix), GM$, Retail, Cost, Qty
            try:
                if tokens:
                    last = tokens[-1].rstrip("%")
                    total_gm_pct = _money4(last) if "." in last else None
                if len(tokens) >= 2:
                    total_gm_dollars = _parse_signed_money_token(tokens[-2])
                if len(tokens) >= 3:
                    total_retail_value = _parse_signed_money_token(tokens[-3])
                if len(tokens) >= 4:
                    total_cost_value = _parse_signed_money_token(tokens[-4])
                if len(tokens) >= 5:
                    total_sku_count = _coalesce_int(tokens[-5])
            except Exception as exc:
                warnings.append(f"Failed to parse Total For Report tail: {exc}")
            continue

        # Department row: starts with a 1-4 digit dept code
        m_dept = re.match(r"^\s*(\d{1,4})\s+(.+)$", line)
        if not m_dept:
            continue
        dept_code = m_dept.group(1)
        rest = m_dept.group(2)

        nums = money_token_re.findall(rest)
        pct_match = pct_token_re.search(rest)
        if len(nums) < 2:
            # Probably a header row, skip
            continue
        gm_pct = None
        if pct_match:
            try:
                gm_pct = Decimal(pct_match.group(0).rstrip("%").strip())
            except (InvalidOperation, ValueError):
                gm_pct = None

        # Right-most numbers are: ... Cost Retail GM$ (and GM% via pct_match)
        right_nums = [_parse_signed_money_token(t) for t in nums[-4:]]
        # Pad to 4 from the left
        while len(right_nums) < 4:
            right_nums.insert(0, Decimal("0"))
        qty, cost, retail, gm_dollars = right_nums

        # Description is whatever's left after stripping the trailing numerics
        # and the (optional) percent token.
        desc = rest
        for t in nums[-4:]:
            desc = desc.rsplit(t, 1)[0]
        if pct_match:
            desc = desc.replace(pct_match.group(0), "")
        desc = desc.strip()

        departments.append(
            {
                "dept_code": dept_code,
                "description": desc or None,
                "qty": qty,
                "cost": cost,
                "retail": retail,
                "gm_dollars": gm_dollars,
                "gm_pct": gm_pct,
            }
        )

    if not departments:
        warnings.append("No department rows parsed in Inventory Value Report")

    return {
        "snapshot_date": snapshot_date,
        "departments": departments,
        "total_sku_count": total_sku_count,
        "total_cost_value": total_cost_value,
        "total_retail_value": total_retail_value,
        "total_gm_dollars": total_gm_dollars,
        "total_gm_pct": total_gm_pct,
        "warnings": warnings,
    }


# --------------------------------------------------------------------------
# Parser — Aged AR Report
# --------------------------------------------------------------------------


_RE_AGED_TOTALS = re.compile(r"^\s*Totals?\s*[:.]?\s*(.*)$", re.IGNORECASE)
# Captures both numeric date forms (YY/MM/DD etc.) AND alpha-month forms
# (e.g. 'Feb 28/26'). The second branch is non-greedy and stops before
# any double-space so trailing "Page: 1" / time stamps don't get pulled
# into the date string.
_RE_AGED_AS_OF = re.compile(
    r"(?:As\s+of|Aging\s+Date|Report\s+Date|Snapshot\s+Date)\s*[:.]?\s*"
    r"([A-Za-z]{3,9}\s+\d{1,2}(?:[/,]\s*)\d{2,4}|[0-9/\-]+)",
    re.IGNORECASE,
)


def parse_aged_ar_report(file_text: str) -> dict[str, Any]:
    """
    Parse the Aged AR Report.

    Customer rows look like:
        <CustNum> <Name>          <Total> <Current> <30+> <60+> <90+> <120+> <CrLim>

    Credit balances may be shown with a trailing minus ("289.19-").
    """
    raw_lines = _split_lines(file_text or "")

    snapshot_date: date | None = None
    customers: list[dict[str, Any]] = []
    total_ar = Decimal("0.00")
    current_amount = Decimal("0.00")
    over_30 = Decimal("0.00")
    over_60 = Decimal("0.00")
    over_90 = Decimal("0.00")
    over_120 = Decimal("0.00")
    warnings: list[str] = []

    for raw in raw_lines[:50]:
        m = _RE_AGED_AS_OF.search(raw)
        if m:
            snapshot_date = _parse_yy_mm_dd(m.group(1))
            if snapshot_date:
                break

    money_token_re = re.compile(r"-?[\d,]*\.\d{2}\-?")

    for raw in raw_lines:
        line = raw.rstrip()
        if not line.strip():
            continue

        # Totals row
        m_tot = _RE_AGED_TOTALS.match(line)
        if m_tot:
            tail = m_tot.group(1)
            tokens = money_token_re.findall(tail)
            # Expect [total, current, 30, 60, 90, 120, (cr_lim?)]
            parsed = [_parse_signed_money_token(t) for t in tokens]
            if len(parsed) >= 6:
                total_ar, current_amount, over_30, over_60, over_90, over_120 = parsed[:6]
            elif len(parsed) >= 1:
                total_ar = parsed[0]
                if len(parsed) >= 2:
                    current_amount = parsed[1]
            continue

        # Customer row: starts with cust num (digits or alphanumeric)
        m_row = re.match(r"^\s*([A-Za-z0-9\-]{1,12})\s+(.+)$", line)
        if not m_row:
            continue
        cust_num = m_row.group(1)
        rest = m_row.group(2)

        # Skip header-ish rows that don't contain money tokens
        nums = money_token_re.findall(rest)
        if len(nums) < 2:
            continue

        parsed_nums = [_parse_signed_money_token(t) for t in nums]
        # Customer rows have 6 aging columns (Total, Current, 30+, 60+,
        # 90+, 120+) optionally followed by a 7th "credit limit" column.
        # Drop the trailing credit-limit if present so we always read the
        # 6 aging columns from the LEFT side of the numeric run.
        if len(parsed_nums) >= 7:
            right = parsed_nums[:6]
        else:
            right = parsed_nums[-6:]
        while len(right) < 6:
            right.insert(0, Decimal("0"))
        c_total, c_current, c_30, c_60, c_90, c_120 = right

        # Customer name = rest with the trailing numeric/cr_lim tokens
        # stripped from the right.
        name = rest
        for t in nums:
            name = name.rsplit(t, 1)[0]
        name = name.strip()

        customers.append(
            {
                "customer_number": cust_num,
                "customer_name": name or None,
                "total": c_total,
                "current": c_current,
                "over_30": c_30,
                "over_60": c_60,
                "over_90": c_90,
                "over_120": c_120,
            }
        )

    if not customers:
        warnings.append("No customer rows parsed in Aged AR Report")

    return {
        "snapshot_date": snapshot_date,
        "customers": customers,
        "total_ar": total_ar,
        "current_amount": current_amount,
        "over_30": over_30,
        "over_60": over_60,
        "over_90": over_90,
        "over_120": over_120,
        "warnings": warnings,
    }


# --------------------------------------------------------------------------
# Persistence helpers
# --------------------------------------------------------------------------


def _resolve_entity_or_raise(session, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    return dict(entity)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return _money(value)


def _json_safe(value: Any) -> Any:
    """Decimals -> str, dates -> ISO. Used before json.dumps()."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _insert_pos_import_run(
    session,
    *,
    entity_id: UUID,
    accounting_period_id: UUID | None,
    report_type: str,
    period_start: date | None,
    period_end: date | None,
    file_name: str,
    adjustment_reason: str | None,
    total_amount: Decimal | None,
    row_count: int,
    parsed_data: dict[str, Any],
    raw_text: str,
    actor_email: str,
) -> UUID:
    row = session.execute(
        text(
            """
            INSERT INTO pos_import_runs (
                entity_id, accounting_period_id, report_type,
                period_start, period_end, file_name,
                adjustment_reason, total_amount, row_count,
                parsed_data_json, raw_text, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id, :report_type,
                :period_start, :period_end, :file_name,
                :adjustment_reason, :total_amount, :row_count,
                CAST(:parsed_data_json AS jsonb), :raw_text, :actor_email
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "report_type": report_type,
            "period_start": period_start,
            "period_end": period_end,
            "file_name": file_name,
            "adjustment_reason": adjustment_reason,
            "total_amount": total_amount,
            "row_count": row_count,
            "parsed_data_json": json.dumps(_json_safe(parsed_data)),
            "raw_text": raw_text,
            "actor_email": actor_email,
        },
    ).mappings().first()
    return row["id"]


# --------------------------------------------------------------------------
# Importer — Inventory Adjustment
# --------------------------------------------------------------------------


def import_inventory_adjustment(
    session,
    *,
    entity_code: str,
    file_text: str,
    file_name: str,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = _resolve_entity_or_raise(session, entity_code)
    parsed = parse_inventory_adjustment_report(file_text)

    period_end = parsed["period_end"] or parsed["period_start"]
    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], period_end
    )

    run_id = _insert_pos_import_run(
        session,
        entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
        report_type=REPORT_TYPE_INVENTORY_ADJUSTMENT,
        period_start=parsed["period_start"],
        period_end=parsed["period_end"],
        file_name=file_name,
        adjustment_reason=parsed["adjustment_reason"],
        total_amount=_money(parsed["total_for_report"]),
        row_count=len(parsed["lines"]),
        parsed_data={
            "store_label": parsed["store_label"],
            "adjustment_reason_raw": parsed["adjustment_reason_raw"],
            "employee_summary": parsed["employee_summary"],
            "warnings": parsed["warnings"],
        },
        raw_text=file_text,
        actor_email=actor_email,
    )

    for ln in parsed["lines"]:
        line_reason = _classify_line_reason(
            ln["reason_description"], parsed["adjustment_reason"]
        )
        session.execute(
            text(
                """
                INSERT INTO inventory_adjustment_lines (
                    entity_id, import_run_id, accounting_period_id,
                    sku_number, description, mfg_number,
                    date_adjusted, quantity_adjusted, quantity_after,
                    adjustment_cost, adjustment_reason,
                    reason_description, employee_id
                ) VALUES (
                    :entity_id, :import_run_id, :accounting_period_id,
                    :sku_number, :description, :mfg_number,
                    :date_adjusted, :quantity_adjusted, :quantity_after,
                    :adjustment_cost, :adjustment_reason,
                    :reason_description, :employee_id
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "import_run_id": run_id,
                "accounting_period_id": accounting_period_id,
                "sku_number": ln["sku_number"],
                "description": ln["description"],
                "mfg_number": ln["mfg_number"],
                "date_adjusted": ln["date_adjusted"],
                "quantity_adjusted": ln["quantity_adjusted"],
                "quantity_after": ln["quantity_after"],
                "adjustment_cost": ln["adjustment_cost"],
                "adjustment_reason": line_reason,
                "reason_description": ln["reason_description"],
                "employee_id": ln["employee_id"],
            },
        )

    return {
        "run_id": str(run_id),
        "entity_code": entity_code,
        "report_type": REPORT_TYPE_INVENTORY_ADJUSTMENT,
        "adjustment_reason": parsed["adjustment_reason"],
        "adjustment_reason_raw": parsed["adjustment_reason_raw"],
        "period_start": parsed["period_start"].isoformat() if parsed["period_start"] else None,
        "period_end": parsed["period_end"].isoformat() if parsed["period_end"] else None,
        "line_count": len(parsed["lines"]),
        "total_for_report": str(parsed["total_for_report"]),
        "warnings": parsed["warnings"],
    }


# --------------------------------------------------------------------------
# Importer — POS Financial
# --------------------------------------------------------------------------


def import_pos_financial(
    session,
    *,
    entity_code: str,
    file_text: str,
    file_name: str,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = _resolve_entity_or_raise(session, entity_code)
    parsed = parse_pos_financial_report(file_text)

    if not parsed["period_start"] or not parsed["period_end"]:
        raise ValueError(
            "POS Financial Report did not contain a parseable period range. "
            "Expected a 'From: <date> to <date>' header."
        )

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], parsed["period_end"]
    )

    fields = parsed["fields"]
    run_id = _insert_pos_import_run(
        session,
        entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
        report_type=REPORT_TYPE_POS_FINANCIAL,
        period_start=parsed["period_start"],
        period_end=parsed["period_end"],
        file_name=file_name,
        adjustment_reason=None,
        total_amount=_money(parsed["total_credit_side"]),
        row_count=len(parsed["raw_rows"]),
        parsed_data={
            "raw_rows": parsed["raw_rows"],
            "warnings": parsed["warnings"],
        },
        raw_text=file_text,
        actor_email=actor_email,
    )

    snap = session.execute(
        text(
            """
            INSERT INTO pos_financial_snapshots (
                entity_id, import_run_id, accounting_period_id,
                period_start, period_end,
                cash_amount, cheque_amount, visa_net, mastercard_net,
                debit_net, amex_net,
                house_account_debit, house_account_credit,
                gift_card_net, ecommerce_net, other_tender_json,
                merchandise_sales, non_merchandise_sales,
                cogs_merchandise, cogs_non_merchandise,
                hst_collected, hst_5pct,
                total_debit_side, total_credit_side, is_balanced,
                raw_parsed_json
            ) VALUES (
                :entity_id, :import_run_id, :accounting_period_id,
                :period_start, :period_end,
                :cash_amount, :cheque_amount, :visa_net, :mastercard_net,
                :debit_net, :amex_net,
                :house_account_debit, :house_account_credit,
                :gift_card_net, :ecommerce_net, CAST(:other_tender_json AS jsonb),
                :merchandise_sales, :non_merchandise_sales,
                :cogs_merchandise, :cogs_non_merchandise,
                :hst_collected, :hst_5pct,
                :total_debit_side, :total_credit_side, :is_balanced,
                CAST(:raw_parsed_json AS jsonb)
            )
            ON CONFLICT (entity_id, period_start, period_end)
            DO UPDATE SET
                import_run_id        = EXCLUDED.import_run_id,
                accounting_period_id = EXCLUDED.accounting_period_id,
                cash_amount          = EXCLUDED.cash_amount,
                cheque_amount        = EXCLUDED.cheque_amount,
                visa_net             = EXCLUDED.visa_net,
                mastercard_net       = EXCLUDED.mastercard_net,
                debit_net            = EXCLUDED.debit_net,
                amex_net             = EXCLUDED.amex_net,
                house_account_debit  = EXCLUDED.house_account_debit,
                house_account_credit = EXCLUDED.house_account_credit,
                gift_card_net        = EXCLUDED.gift_card_net,
                ecommerce_net        = EXCLUDED.ecommerce_net,
                other_tender_json    = EXCLUDED.other_tender_json,
                merchandise_sales    = EXCLUDED.merchandise_sales,
                non_merchandise_sales= EXCLUDED.non_merchandise_sales,
                cogs_merchandise     = EXCLUDED.cogs_merchandise,
                cogs_non_merchandise = EXCLUDED.cogs_non_merchandise,
                hst_collected        = EXCLUDED.hst_collected,
                hst_5pct             = EXCLUDED.hst_5pct,
                total_debit_side     = EXCLUDED.total_debit_side,
                total_credit_side    = EXCLUDED.total_credit_side,
                is_balanced          = EXCLUDED.is_balanced,
                raw_parsed_json      = EXCLUDED.raw_parsed_json
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "import_run_id": run_id,
            "accounting_period_id": accounting_period_id,
            "period_start": parsed["period_start"],
            "period_end": parsed["period_end"],
            "cash_amount": _money(fields.get("cash_amount", 0)),
            "cheque_amount": _money(fields.get("cheque_amount", 0)),
            "visa_net": _money(fields.get("visa_net", 0)),
            "mastercard_net": _money(fields.get("mastercard_net", 0)),
            "debit_net": _money(fields.get("debit_net", 0)),
            "amex_net": _money(fields.get("amex_net", 0)),
            "house_account_debit": _money(parsed["house_account_debit"]),
            "house_account_credit": _money(parsed["house_account_credit"]),
            "gift_card_net": _money(fields.get("gift_card_net", 0)),
            "ecommerce_net": _money(fields.get("ecommerce_net", 0)),
            "other_tender_json": json.dumps(_json_safe(parsed["other_tender"])),
            "merchandise_sales": _money(fields.get("merchandise_sales", 0)),
            "non_merchandise_sales": _money(fields.get("non_merchandise_sales", 0)),
            "cogs_merchandise": _money(fields.get("cogs_merchandise", 0)),
            "cogs_non_merchandise": _money(fields.get("cogs_non_merchandise", 0)),
            "hst_collected": _money(fields.get("hst_collected", 0)),
            "hst_5pct": _money(fields.get("hst_5pct", 0)),
            "total_debit_side": _money(parsed["total_debit_side"]),
            "total_credit_side": _money(parsed["total_credit_side"]),
            "is_balanced": parsed["is_balanced"],
            "raw_parsed_json": json.dumps(_json_safe(parsed["raw_rows"])),
        },
    ).mappings().first()

    return {
        "run_id": str(run_id),
        "snapshot_id": str(snap["id"]) if snap else None,
        "entity_code": entity_code,
        "report_type": REPORT_TYPE_POS_FINANCIAL,
        "period_start": parsed["period_start"].isoformat(),
        "period_end": parsed["period_end"].isoformat(),
        "is_balanced": parsed["is_balanced"],
        "total_debit_side": str(parsed["total_debit_side"]),
        "total_credit_side": str(parsed["total_credit_side"]),
        "warnings": parsed["warnings"],
    }


# --------------------------------------------------------------------------
# Importer — Inventory Value
# --------------------------------------------------------------------------


def import_inventory_value(
    session,
    *,
    entity_code: str,
    file_text: str,
    file_name: str,
    actor_email: str,
    snapshot_date_override: date | None = None,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = _resolve_entity_or_raise(session, entity_code)
    parsed = parse_inventory_value_report(file_text)

    snapshot_date = snapshot_date_override or parsed["snapshot_date"]
    if not snapshot_date:
        raise ValueError(
            "Inventory Value Report did not contain a parseable snapshot "
            "date. Pass snapshot_date_override or fix the report header."
        )

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], snapshot_date
    )

    run_id = _insert_pos_import_run(
        session,
        entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
        report_type=REPORT_TYPE_INVENTORY_VALUE,
        period_start=snapshot_date,
        period_end=snapshot_date,
        file_name=file_name,
        adjustment_reason=None,
        total_amount=_money(parsed["total_cost_value"]),
        row_count=len(parsed["departments"]),
        parsed_data={
            "departments": parsed["departments"],
            "warnings": parsed["warnings"],
        },
        raw_text=file_text,
        actor_email=actor_email,
    )

    snap = session.execute(
        text(
            """
            INSERT INTO inventory_value_snapshots (
                entity_id, import_run_id, accounting_period_id,
                snapshot_date,
                total_sku_count, total_cost_value, total_retail_value,
                total_gm_dollars, total_gm_pct,
                department_breakdown_json
            ) VALUES (
                :entity_id, :import_run_id, :accounting_period_id,
                :snapshot_date,
                :total_sku_count, :total_cost_value, :total_retail_value,
                :total_gm_dollars, :total_gm_pct,
                CAST(:department_breakdown_json AS jsonb)
            )
            ON CONFLICT (entity_id, snapshot_date) DO UPDATE SET
                import_run_id              = EXCLUDED.import_run_id,
                accounting_period_id       = EXCLUDED.accounting_period_id,
                total_sku_count            = EXCLUDED.total_sku_count,
                total_cost_value           = EXCLUDED.total_cost_value,
                total_retail_value         = EXCLUDED.total_retail_value,
                total_gm_dollars           = EXCLUDED.total_gm_dollars,
                total_gm_pct               = EXCLUDED.total_gm_pct,
                department_breakdown_json  = EXCLUDED.department_breakdown_json
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "import_run_id": run_id,
            "accounting_period_id": accounting_period_id,
            "snapshot_date": snapshot_date,
            "total_sku_count": parsed["total_sku_count"],
            "total_cost_value": _money(parsed["total_cost_value"]),
            "total_retail_value": _money(parsed["total_retail_value"]),
            "total_gm_dollars": _money(parsed["total_gm_dollars"]),
            "total_gm_pct": parsed["total_gm_pct"],
            "department_breakdown_json": json.dumps(
                _json_safe(parsed["departments"])
            ),
        },
    ).mappings().first()

    return {
        "run_id": str(run_id),
        "snapshot_id": str(snap["id"]) if snap else None,
        "entity_code": entity_code,
        "report_type": REPORT_TYPE_INVENTORY_VALUE,
        "snapshot_date": snapshot_date.isoformat(),
        "total_sku_count": parsed["total_sku_count"],
        "total_cost_value": str(parsed["total_cost_value"]),
        "total_retail_value": str(parsed["total_retail_value"]),
        "warnings": parsed["warnings"],
    }


# --------------------------------------------------------------------------
# Importer — Aged AR
# --------------------------------------------------------------------------


def import_aged_ar(
    session,
    *,
    entity_code: str,
    file_text: str,
    file_name: str,
    actor_email: str,
    snapshot_date_override: date | None = None,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = _resolve_entity_or_raise(session, entity_code)
    parsed = parse_aged_ar_report(file_text)

    snapshot_date = snapshot_date_override or parsed["snapshot_date"]
    if not snapshot_date:
        raise ValueError(
            "Aged AR Report did not contain a parseable snapshot date. "
            "Pass snapshot_date_override or fix the report header."
        )

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], snapshot_date
    )

    run_id = _insert_pos_import_run(
        session,
        entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
        report_type=REPORT_TYPE_AGED_AR,
        period_start=snapshot_date,
        period_end=snapshot_date,
        file_name=file_name,
        adjustment_reason=None,
        total_amount=_money(parsed["total_ar"]),
        row_count=len(parsed["customers"]),
        parsed_data={
            "warnings": parsed["warnings"],
        },
        raw_text=file_text,
        actor_email=actor_email,
    )

    snap = session.execute(
        text(
            """
            INSERT INTO aged_ar_snapshots (
                entity_id, import_run_id, accounting_period_id,
                snapshot_date,
                total_ar, current_amount,
                over_30, over_60, over_90, over_120,
                customer_detail_json
            ) VALUES (
                :entity_id, :import_run_id, :accounting_period_id,
                :snapshot_date,
                :total_ar, :current_amount,
                :over_30, :over_60, :over_90, :over_120,
                CAST(:customer_detail_json AS jsonb)
            )
            ON CONFLICT (entity_id, snapshot_date) DO UPDATE SET
                import_run_id        = EXCLUDED.import_run_id,
                accounting_period_id = EXCLUDED.accounting_period_id,
                total_ar             = EXCLUDED.total_ar,
                current_amount       = EXCLUDED.current_amount,
                over_30              = EXCLUDED.over_30,
                over_60              = EXCLUDED.over_60,
                over_90              = EXCLUDED.over_90,
                over_120             = EXCLUDED.over_120,
                customer_detail_json = EXCLUDED.customer_detail_json
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "import_run_id": run_id,
            "accounting_period_id": accounting_period_id,
            "snapshot_date": snapshot_date,
            "total_ar": _money(parsed["total_ar"]),
            "current_amount": _money(parsed["current_amount"]),
            "over_30": _money(parsed["over_30"]),
            "over_60": _money(parsed["over_60"]),
            "over_90": _money(parsed["over_90"]),
            "over_120": _money(parsed["over_120"]),
            "customer_detail_json": json.dumps(_json_safe(parsed["customers"])),
        },
    ).mappings().first()

    return {
        "run_id": str(run_id),
        "snapshot_id": str(snap["id"]) if snap else None,
        "entity_code": entity_code,
        "report_type": REPORT_TYPE_AGED_AR,
        "snapshot_date": snapshot_date.isoformat(),
        "total_ar": str(parsed["total_ar"]),
        "buckets": {
            "current": str(parsed["current_amount"]),
            "over_30": str(parsed["over_30"]),
            "over_60": str(parsed["over_60"]),
            "over_90": str(parsed["over_90"]),
            "over_120": str(parsed["over_120"]),
        },
        "warnings": parsed["warnings"],
    }


# --------------------------------------------------------------------------
# Journal builders
# --------------------------------------------------------------------------


def _resolve_import_run_for_journal(
    session, entity_id: UUID, run_id_str: str
) -> dict[str, Any]:
    run_uuid = _parse_uuid(run_id_str, "import_run_id")
    row = session.execute(
        text(
            """
            SELECT id, entity_id, accounting_period_id, report_type,
                   period_start, period_end, adjustment_reason
            FROM pos_import_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity_id},
    ).mappings().first()
    if not row:
        raise ValueError(f"pos_import_run not found: {run_id_str}")
    if row["report_type"] != REPORT_TYPE_INVENTORY_ADJUSTMENT:
        raise ValueError(
            "Journal builders only run on inventory_adjustment imports; "
            f"this run is report_type={row['report_type']}"
        )
    if row["accounting_period_id"] is None:
        raise ValueError(
            "Cannot build journal: pos_import_run has no accounting_period_id "
            "(no accounting_periods row covers the report's period_end)."
        )
    return dict(row)


def _build_inventory_reclass_journal(
    session,
    *,
    entity_code: str,
    import_run_id: str,
    actor_email: str,
    expected_reason: str,
    expense_account_code: str,
    inventory_account_code: str,
    batch_label: str,
    journal_memo: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")

    entity = _resolve_entity_or_raise(session, entity_code)
    run = _resolve_import_run_for_journal(session, entity["id"], import_run_id)

    # Only reject when the run's header pinned a different specific reason.
    # Combined-report headers like 'ALL' pass through; the per-line
    # adjustment_reason filter below picks just the matching lines.
    run_reason = run["adjustment_reason"]
    if run_reason in _SPECIFIC_RUN_REASONS and run_reason != expected_reason:
        raise ValueError(
            f"Run adjustment_reason={run_reason!r} does not match "
            f"builder reason={expected_reason!r}"
        )

    # Sum the lines for this run (filter on reason for safety).
    line_rows = session.execute(
        text(
            """
            SELECT id, sku_number, description, adjustment_cost
            FROM inventory_adjustment_lines
            WHERE entity_id = :entity_id
              AND import_run_id = :import_run_id
              AND adjustment_reason = :adjustment_reason
            ORDER BY date_adjusted, sku_number
            """
        ),
        {
            "entity_id": entity["id"],
            "import_run_id": _parse_uuid(import_run_id, "import_run_id"),
            "adjustment_reason": expected_reason,
        },
    ).mappings().all()

    if not line_rows:
        raise ValueError(
            "No inventory_adjustment_lines for this import run + reason; "
            "nothing to post."
        )

    # The store reports adjustment_cost as a negative number when
    # inventory is leaving (decrease). The journal magnitude is abs().
    raw_total = sum((Decimal(str(r["adjustment_cost"] or 0)) for r in line_rows), Decimal("0"))
    total = abs(_money(raw_total))
    if total == Decimal("0.00"):
        raise ValueError("Total adjustment cost is 0.00; nothing to post.")

    summary = {
        "source_run_id": import_run_id,
        "entity_code": entity_code,
        "adjustment_reason": expected_reason,
        "line_count": len(line_rows),
        "raw_total": str(raw_total),
        "posted_total": str(total),
        "is_balanced": True,
    }

    # Upsert the journal_batches row using the same convention as the
    # other modules (entity_id, accounting_period_id, source_module,
    # batch_label) is unique.
    batch = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status,
                total_debits, total_credits, summary_json
            ) VALUES (
                :entity_id, :accounting_period_id, :source_module, :batch_label,
                'draft', 'draft_ready',
                :total_debits, :total_credits, CAST(:summary_json AS jsonb)
            )
            ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label)
            DO UPDATE SET
                status = 'draft',
                workflow_status = 'draft_ready',
                total_debits = EXCLUDED.total_debits,
                total_credits = EXCLUDED.total_credits,
                summary_json = EXCLUDED.summary_json,
                submitted_by = NULL, submitted_at = NULL,
                reviewed_by = NULL, reviewed_at = NULL,
                approved_by = NULL, approved_at = NULL,
                approval_note = NULL, rejection_note = NULL,
                locked_by = NULL, locked_at = NULL,
                updated_at = NOW()
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": run["accounting_period_id"],
            "source_module": SOURCE_MODULE_POS_IMPORT,
            "batch_label": batch_label,
            "total_debits": total,
            "total_credits": total,
            "summary_json": json.dumps(summary),
        },
    ).mappings().first()
    journal_batch_id = batch["id"]

    # Wipe + rewrite the lines so re-running the builder is idempotent.
    session.execute(
        text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
        {"id": journal_batch_id},
    )
    session.execute(
        text(
            """
            INSERT INTO journal_lines (
                journal_batch_id, line_number, account_code,
                debit_amount, credit_amount, memo, source_json
            ) VALUES
                (:id, 1, :dr_account, :total, 0,        :memo, CAST(:src AS jsonb)),
                (:id, 2, :cr_account, 0,        :total, :memo, CAST(:src AS jsonb))
            """
        ),
        {
            "id": journal_batch_id,
            "dr_account": expense_account_code,
            "cr_account": inventory_account_code,
            "total": total,
            "memo": journal_memo,
            "src": json.dumps(
                {
                    "source_module": SOURCE_MODULE_POS_IMPORT,
                    "import_run_id": import_run_id,
                    "adjustment_reason": expected_reason,
                    "line_count": len(line_rows),
                }
            ),
        },
    )

    # Backfill journal_batch_id on the lines so we know which batch
    # claimed them.
    session.execute(
        text(
            """
            UPDATE inventory_adjustment_lines
               SET journal_batch_id = :journal_batch_id
             WHERE entity_id = :entity_id
               AND import_run_id = :import_run_id
               AND adjustment_reason = :adjustment_reason
            """
        ),
        {
            "journal_batch_id": journal_batch_id,
            "entity_id": entity["id"],
            "import_run_id": _parse_uuid(import_run_id, "import_run_id"),
            "adjustment_reason": expected_reason,
        },
    )

    return {
        "journal_batch_id": str(journal_batch_id),
        "entity_code": entity_code,
        "import_run_id": import_run_id,
        "adjustment_reason": expected_reason,
        "expense_account_code": expense_account_code,
        "inventory_account_code": inventory_account_code,
        "total_debits": str(total),
        "total_credits": str(total),
        "line_count": len(line_rows),
    }


def build_store_use_journal(
    session,
    *,
    entity_code: str,
    import_run_id: str,
    actor_email: str,
    expense_account_code: str = DEFAULT_STORE_USE_EXPENSE_ACCOUNT_CODE,
    inventory_account_code: str = DEFAULT_INVENTORY_ACCOUNT_CODE,
) -> dict[str, Any]:
    """
    Reads inventory_adjustment_lines for `import_run_id` whose
    adjustment_reason is SUPPLIES (store use) and writes a balanced
    journal_batch:

        Dr  Store Supplies Expense  [total]
        Cr  Inventory               [total]
    """
    return _build_inventory_reclass_journal(
        session,
        entity_code=entity_code,
        import_run_id=import_run_id,
        actor_email=actor_email,
        expected_reason=ADJ_REASON_SUPPLIES,
        expense_account_code=expense_account_code,
        inventory_account_code=inventory_account_code,
        batch_label=BATCH_LABEL_STORE_USE,
        journal_memo="Store use / supplies — inventory reclass",
    )


def build_donation_journal(
    session,
    *,
    entity_code: str,
    import_run_id: str,
    actor_email: str,
    expense_account_code: str = DEFAULT_DONATION_EXPENSE_ACCOUNT_CODE,
    inventory_account_code: str = DEFAULT_INVENTORY_ACCOUNT_CODE,
) -> dict[str, Any]:
    """
    Reads inventory_adjustment_lines for `import_run_id` whose
    adjustment_reason is DONATION and writes a balanced journal_batch:

        Dr  Charitable Donations    [total]
        Cr  Inventory               [total]
    """
    return _build_inventory_reclass_journal(
        session,
        entity_code=entity_code,
        import_run_id=import_run_id,
        actor_email=actor_email,
        expected_reason=ADJ_REASON_DONATION,
        expense_account_code=expense_account_code,
        inventory_account_code=inventory_account_code,
        batch_label=BATCH_LABEL_DONATION,
        journal_memo="Charitable donations — inventory reclass",
    )


# --------------------------------------------------------------------------
# Read helpers
# --------------------------------------------------------------------------


def list_pos_import_runs(
    session,
    *,
    entity_code: str,
    period_start: date | None = None,
    period_end: date | None = None,
    report_type: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    entity = _resolve_entity_or_raise(session, entity_code)
    rows = session.execute(
        text(
            """
            SELECT id, report_type, period_start, period_end,
                   file_name, adjustment_reason, total_amount,
                   row_count, status, actor_email, created_at
            FROM pos_import_runs
            WHERE entity_id = :entity_id
              AND (:period_start IS NULL OR period_end >= :period_start)
              AND (:period_end   IS NULL OR period_start <= :period_end)
              AND (:report_type IS NULL OR report_type = :report_type)
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity["id"],
            "period_start": period_start,
            "period_end": period_end,
            "report_type": report_type,
            "limit": int(limit),
        },
    ).mappings().all()

    runs = []
    for r in rows:
        runs.append(
            {
                "id": str(r["id"]),
                "report_type": r["report_type"],
                "period_start": r["period_start"].isoformat() if r["period_start"] else None,
                "period_end": r["period_end"].isoformat() if r["period_end"] else None,
                "file_name": r["file_name"],
                "adjustment_reason": r["adjustment_reason"],
                "total_amount": str(r["total_amount"]) if r["total_amount"] is not None else None,
                "row_count": r["row_count"],
                "status": r["status"],
                "actor_email": r["actor_email"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
        )
    return {"entity_code": entity_code, "count": len(runs), "runs": runs}


def get_pos_import_run_detail(
    session, *, entity_code: str, run_id: str
) -> dict[str, Any]:
    entity = _resolve_entity_or_raise(session, entity_code)
    run_uuid = _parse_uuid(run_id, "run_id")
    row = session.execute(
        text(
            """
            SELECT id, report_type, period_start, period_end,
                   file_name, adjustment_reason, total_amount,
                   row_count, status, actor_email, created_at,
                   parsed_data_json
            FROM pos_import_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not row:
        raise ValueError(f"pos_import_run not found: {run_id}")

    detail: dict[str, Any] = {
        "id": str(row["id"]),
        "entity_code": entity_code,
        "report_type": row["report_type"],
        "period_start": row["period_start"].isoformat() if row["period_start"] else None,
        "period_end": row["period_end"].isoformat() if row["period_end"] else None,
        "file_name": row["file_name"],
        "adjustment_reason": row["adjustment_reason"],
        "total_amount": str(row["total_amount"]) if row["total_amount"] is not None else None,
        "row_count": row["row_count"],
        "status": row["status"],
        "actor_email": row["actor_email"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "parsed_data": row["parsed_data_json"],
    }

    if row["report_type"] == REPORT_TYPE_INVENTORY_ADJUSTMENT:
        line_rows = session.execute(
            text(
                """
                SELECT sku_number, description, mfg_number,
                       date_adjusted, quantity_adjusted, quantity_after,
                       adjustment_cost, reason_description, employee_id,
                       journal_batch_id
                FROM inventory_adjustment_lines
                WHERE import_run_id = :id
                ORDER BY date_adjusted, sku_number
                """
            ),
            {"id": run_uuid},
        ).mappings().all()
        detail["lines"] = [
            {
                **dict(line),
                "date_adjusted": line["date_adjusted"].isoformat() if line["date_adjusted"] else None,
                "quantity_adjusted": str(line["quantity_adjusted"]) if line["quantity_adjusted"] is not None else None,
                "quantity_after": str(line["quantity_after"]) if line["quantity_after"] is not None else None,
                "adjustment_cost": str(line["adjustment_cost"]) if line["adjustment_cost"] is not None else None,
                "journal_batch_id": str(line["journal_batch_id"]) if line["journal_batch_id"] else None,
            }
            for line in line_rows
        ]
    return detail


def _row_to_inventory_value_snapshot(row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "snapshot_date": row["snapshot_date"].isoformat() if row["snapshot_date"] else None,
        "total_sku_count": row["total_sku_count"],
        "total_cost_value": str(row["total_cost_value"]) if row["total_cost_value"] is not None else None,
        "total_retail_value": str(row["total_retail_value"]) if row["total_retail_value"] is not None else None,
        "total_gm_dollars": str(row["total_gm_dollars"]) if row["total_gm_dollars"] is not None else None,
        "total_gm_pct": str(row["total_gm_pct"]) if row["total_gm_pct"] is not None else None,
        "department_breakdown": row["department_breakdown_json"],
        "import_run_id": str(row["import_run_id"]) if row["import_run_id"] else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }


def get_latest_inventory_value_snapshot(
    session, *, entity_code: str
) -> dict[str, Any] | None:
    entity = _resolve_entity_or_raise(session, entity_code)
    row = session.execute(
        text(
            """
            SELECT id, snapshot_date, total_sku_count, total_cost_value,
                   total_retail_value, total_gm_dollars, total_gm_pct,
                   department_breakdown_json, import_run_id, created_at
            FROM inventory_value_snapshots
            WHERE entity_id = :entity_id
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().first()
    return _row_to_inventory_value_snapshot(row) if row else None


def _row_to_aged_ar_snapshot(row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "snapshot_date": row["snapshot_date"].isoformat() if row["snapshot_date"] else None,
        "total_ar": str(row["total_ar"]) if row["total_ar"] is not None else None,
        "current_amount": str(row["current_amount"]) if row["current_amount"] is not None else None,
        "over_30": str(row["over_30"]) if row["over_30"] is not None else None,
        "over_60": str(row["over_60"]) if row["over_60"] is not None else None,
        "over_90": str(row["over_90"]) if row["over_90"] is not None else None,
        "over_120": str(row["over_120"]) if row["over_120"] is not None else None,
        "customer_detail": row["customer_detail_json"],
        "import_run_id": str(row["import_run_id"]) if row["import_run_id"] else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }


def get_latest_aged_ar_snapshot(
    session, *, entity_code: str
) -> dict[str, Any] | None:
    entity = _resolve_entity_or_raise(session, entity_code)
    row = session.execute(
        text(
            """
            SELECT id, snapshot_date, total_ar, current_amount,
                   over_30, over_60, over_90, over_120,
                   customer_detail_json, import_run_id, created_at
            FROM aged_ar_snapshots
            WHERE entity_id = :entity_id
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().first()
    return _row_to_aged_ar_snapshot(row) if row else None


def _row_to_pos_financial_snapshot(row) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(row["id"]),
        "period_start": row["period_start"].isoformat() if row["period_start"] else None,
        "period_end": row["period_end"].isoformat() if row["period_end"] else None,
        "is_balanced": row["is_balanced"],
        "total_debit_side": str(row["total_debit_side"]) if row["total_debit_side"] is not None else None,
        "total_credit_side": str(row["total_credit_side"]) if row["total_credit_side"] is not None else None,
        "import_run_id": str(row["import_run_id"]) if row["import_run_id"] else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "other_tender": row["other_tender_json"],
    }
    for key in (
        "cash_amount", "cheque_amount", "visa_net", "mastercard_net",
        "debit_net", "amex_net",
        "house_account_debit", "house_account_credit",
        "gift_card_net", "ecommerce_net",
        "merchandise_sales", "non_merchandise_sales",
        "cogs_merchandise", "cogs_non_merchandise",
        "hst_collected", "hst_5pct",
    ):
        out[key] = str(row[key]) if row[key] is not None else None
    return out


def get_latest_pos_financial_snapshot(
    session, *, entity_code: str
) -> dict[str, Any] | None:
    entity = _resolve_entity_or_raise(session, entity_code)
    row = session.execute(
        text(
            """
            SELECT id, period_start, period_end,
                   cash_amount, cheque_amount, visa_net, mastercard_net,
                   debit_net, amex_net,
                   house_account_debit, house_account_credit,
                   gift_card_net, ecommerce_net, other_tender_json,
                   merchandise_sales, non_merchandise_sales,
                   cogs_merchandise, cogs_non_merchandise,
                   hst_collected, hst_5pct,
                   total_debit_side, total_credit_side, is_balanced,
                   import_run_id, created_at
            FROM pos_financial_snapshots
            WHERE entity_id = :entity_id
            ORDER BY period_end DESC, created_at DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().first()
    return _row_to_pos_financial_snapshot(row) if row else None


# --------------------------------------------------------------------------
# Close control center: section helper
# --------------------------------------------------------------------------


def section_pos_reports(
    session,
    *,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """
    Reports whether the three month-end POS snapshots have been imported
    for the period. Returns the standard section shape used by
    services_month_end_close.

    Status:
        no_data       — pos_import_runs table missing entirely
        blocked       — at least one of pos_financial / inventory_value /
                        aged_ar is missing for the period
        ready         — all three present
    """
    if not _has_table(session, "pos_import_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "pos_import_runs table not present",
        }

    pos_financial = session.execute(
        text(
            """
            SELECT id, period_start, period_end, is_balanced
            FROM pos_financial_snapshots
            WHERE entity_id = :entity_id
              AND period_end = :period_end
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()

    inventory_value = session.execute(
        text(
            """
            SELECT id, snapshot_date, total_cost_value
            FROM inventory_value_snapshots
            WHERE entity_id = :entity_id
              AND snapshot_date BETWEEN :period_start AND :period_end
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()

    aged_ar = session.execute(
        text(
            """
            SELECT id, snapshot_date, total_ar
            FROM aged_ar_snapshots
            WHERE entity_id = :entity_id
              AND snapshot_date BETWEEN :period_start AND :period_end
            ORDER BY snapshot_date DESC
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()

    missing: list[str] = []
    if not pos_financial:
        missing.append("pos_financial")
    if not inventory_value:
        missing.append("inventory_value")
    if not aged_ar:
        missing.append("aged_ar")

    warnings: list[str] = []
    if pos_financial and not pos_financial["is_balanced"]:
        warnings.append("pos_financial snapshot is not balanced")

    if missing:
        status = "blocked"
        summary = (
            "Missing month-end POS snapshot(s): " + ", ".join(missing)
        )
    elif warnings:
        status = "needs_review"
        summary = "; ".join(warnings)
    else:
        status = "ready"
        summary = (
            "POS financial, inventory value, and aged AR snapshots all "
            "present for the period"
        )

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "pos_financial": {
            "present": pos_financial is not None,
            "is_balanced": bool(pos_financial["is_balanced"]) if pos_financial else None,
            "period_start": pos_financial["period_start"].isoformat() if pos_financial and pos_financial["period_start"] else None,
            "period_end": pos_financial["period_end"].isoformat() if pos_financial and pos_financial["period_end"] else None,
        },
        "inventory_value": {
            "present": inventory_value is not None,
            "snapshot_date": inventory_value["snapshot_date"].isoformat() if inventory_value and inventory_value["snapshot_date"] else None,
            "total_cost_value": str(inventory_value["total_cost_value"]) if inventory_value and inventory_value["total_cost_value"] is not None else None,
        },
        "aged_ar": {
            "present": aged_ar is not None,
            "snapshot_date": aged_ar["snapshot_date"].isoformat() if aged_ar and aged_ar["snapshot_date"] else None,
            "total_ar": str(aged_ar["total_ar"]) if aged_ar and aged_ar["total_ar"] is not None else None,
        },
    }
