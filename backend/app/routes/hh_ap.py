import hashlib
import json
import re
from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/hh-ap", tags=["hh-ap"])

# =========================
# Constants
# =========================

INVOICE_TYPE_VENDOR_DIRECT = "vendor_direct"
INVOICE_TYPE_HH_DIRECT = "hh_direct"
INVOICE_TYPE_WAREHOUSE = "warehouse"

DOCUMENT_TYPE_HH_INVOICE = "hh_invoice"
DOCUMENT_TYPE_HH_INVOICE_DIRECT = "hh_invoice_direct"
DOCUMENT_TYPE_HH_INVOICE_HH_DIRECT = "hh_invoice_hh_direct"
DOCUMENT_TYPE_HH_INVOICE_WAREHOUSE = "hh_invoice_warehouse"
DOCUMENT_TYPE_HH_DOCUMENT = "hh_document"
DOCUMENT_TYPE_HH_STATEMENT = "hh_statement"
DOCUMENT_TYPE_HH_REMITTANCE = "hh_remittance"

PROCESSING_STATUS_UPLOADED_TEXT_READY = "uploaded_text_ready"
PROCESSING_STATUS_UPLOADED_PENDING_PARSE = "uploaded_pending_parse"
PROCESSING_STATUS_PARSED_INVOICE = "parsed_invoice"
PROCESSING_STATUS_PARSED_STATEMENT = "parsed_statement"
PROCESSING_STATUS_PARSED_REMITTANCE = "parsed_remittance"
PROCESSING_STATUS_PARSE_FAILED_INVOICE = "parse_failed_invoice"

MATCH_STATUS_MATCHED = "matched"
MATCH_STATUS_UNMATCHED = "unmatched"
MATCH_STATUS_MISSING_DOWNLOAD = "missing_download"
MATCH_STATUS_STATEMENT_ONLY = "statement_only"

DEFAULT_CURRENCY_CODE = "CAD"

VENDOR_DIRECT_FILENAME_MARKERS = (
    "INV0120E",
    "INV0130E",
    "INV0150E",
    "INV0170E",
    "INV0171E",
)

HH_DIRECT_FILENAME_MARKERS = (
    "INV0140E",
)

WAREHOUSE_FILENAME_MARKERS = (
    "INV0670R",
)

MONEY_TOKEN_PATTERN = r"-?[\d,]*\.?\d+(?:CR|C|-)?"
EIGHT_DIGIT_TOKEN_PATTERN = r"\b\d{8}\b"


def get_allowed_invoice_document_types() -> list[str]:
    return [
        DOCUMENT_TYPE_HH_INVOICE,
        DOCUMENT_TYPE_HH_INVOICE_DIRECT,
        DOCUMENT_TYPE_HH_INVOICE_HH_DIRECT,
        DOCUMENT_TYPE_HH_INVOICE_WAREHOUSE,
        DOCUMENT_TYPE_HH_DOCUMENT,
    ]


def money(value: Any) -> Decimal:
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


def normalize_upper_space_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().upper()


def normalize_invoice_number(value: str | None) -> str | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    return cleaned.upper()


def normalize_optional_date_input(value: str | None) -> date | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    try:
        return date.fromisoformat(cleaned)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="document_date must be blank or in YYYY-MM-DD format",
        ) from exc


def parse_hh_money(value: str) -> Decimal:
    cleaned = normalize_text(value)
    if not cleaned:
        return Decimal("0.00")
    cleaned = cleaned.replace(",", "").replace(" ", "")
    return Decimal(cleaned)


def parse_hh_signed_money(value: str | None) -> Decimal:
    cleaned = normalize_text(value)
    if not cleaned:
        return Decimal("0.00")

    cleaned = cleaned.replace(",", "").replace(" ", "")
    is_negative = False

    if cleaned.startswith("(") and cleaned.endswith(")"):
        is_negative = True
        cleaned = cleaned[1:-1]

    upper_cleaned = cleaned.upper()

    if upper_cleaned.endswith("CR"):
        is_negative = True
        cleaned = cleaned[:-2]
    elif upper_cleaned.endswith("C"):
        is_negative = True
        cleaned = cleaned[:-1]

    if cleaned.endswith("-"):
        is_negative = True
        cleaned = cleaned[:-1]

    if cleaned.startswith("-"):
        is_negative = True
        cleaned = cleaned[1:]

    if cleaned.startswith("+"):
        cleaned = cleaned[1:]

    if cleaned.startswith("."):
        cleaned = f"0{cleaned}"

    if cleaned in {"", ".", "0", "0.0", "0.00"}:
        cleaned = "0.00"

    amount = Decimal(cleaned)
    return -amount if is_negative else amount


def json_dumps(value: Any) -> str:
    return json.dumps(value or {})


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


def build_source_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


def try_extract_text(file_bytes: bytes, filename: str, content_type: str | None) -> str | None:
    suffix = Path(filename).suffix.lower()
    is_text_like = suffix in {".txt", ".csv", ".json", ".xml"} or (content_type or "").startswith("text/")

    if not is_text_like:
        return None

    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            text_value = file_bytes.decode(encoding)
            return text_value[:200000]
        except Exception:
            continue

    return None


def _get_pdf_reader(file_bytes: bytes):
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise HTTPException(
            status_code=500,
            detail="PDF parsing support is not installed. Add pypdf to backend dependencies and redeploy.",
        ) from exc

    return PdfReader(BytesIO(file_bytes))


def extract_pdf_pages_text(file_bytes: bytes) -> list[str]:
    reader = _get_pdf_reader(file_bytes)
    return [(page.extract_text() or "") for page in reader.pages]


def extract_pdf_pages_layout_text(file_bytes: bytes) -> list[str]:
    reader = _get_pdf_reader(file_bytes)
    page_texts: list[str] = []

    for page in reader.pages:
        try:
            text_value = page.extract_text(extraction_mode="layout") or ""
        except TypeError:
            text_value = page.extract_text() or ""
        page_texts.append(text_value)

    return page_texts


def extract_pdf_pages_best_effort_text(file_bytes: bytes) -> list[str]:
    standard_pages = extract_pdf_pages_text(file_bytes)
    layout_pages = extract_pdf_pages_layout_text(file_bytes)

    page_count = max(len(standard_pages), len(layout_pages))
    merged_pages: list[str] = []

    for idx in range(page_count):
        standard_text = standard_pages[idx] if idx < len(standard_pages) else ""
        layout_text = layout_pages[idx] if idx < len(layout_pages) else ""
        chosen = layout_text if len(layout_text.strip()) >= len(standard_text.strip()) else standard_text
        merged_pages.append(chosen or layout_text or standard_text or "")

    return merged_pages


def extract_invoice_parser_context(file_bytes: bytes) -> dict[str, Any]:
    pages = extract_pdf_pages_best_effort_text(file_bytes)

    if not pages or not any(normalize_text(page) for page in pages):
        raise HTTPException(status_code=400, detail="No text could be extracted from PDF")

    all_lines: list[str] = []
    for page in pages:
        all_lines.extend([line.rstrip() for line in page.splitlines()])

    full_text = "\n".join(pages)
    return {
        "pages": pages,
        "all_lines": all_lines,
        "full_text": full_text,
        "full_text_upper": normalize_upper_space_text(full_text),
    }


def parse_hh_short_date(value: str) -> date:
    cleaned = normalize_text(value)
    if not cleaned:
        raise HTTPException(status_code=400, detail="Missing statement date value")

    try:
        yy, mm, dd = cleaned.split("-")
        return date(2000 + int(yy), int(mm), int(dd))
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid HH short date format: {value}",
        ) from exc


def parse_hh_iso_word_date(value: str | None) -> date:
    cleaned = normalize_text(value)
    if not cleaned:
        raise HTTPException(status_code=400, detail="Missing HH invoice date value")

    for fmt in ("%Y-%b-%d", "%Y-%B-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue

    raise HTTPException(
        status_code=400,
        detail=f"Invalid HH invoice date format: {value}",
    )


def parse_hh_mmddyyyy(value: str | None) -> date | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None

    try:
        return datetime.strptime(cleaned, "%m%d%Y").date()
    except ValueError:
        return None


def parse_hh_flexible_date(value: str | None) -> date | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None

    normalized = re.sub(r"\s+", " ", cleaned.replace(" ,", ",")).strip()

    for fmt in (
        "%Y-%b-%d",
        "%Y-%B-%d",
        "%b. %d,%Y",
        "%b. %d, %Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m%d%Y",
    ):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    if re.fullmatch(r"\d{2}-\d{2}-\d{2}", normalized):
        return parse_hh_short_date(normalized)

    return None


def extract_date_tokens_from_line(line: str) -> list[str]:
    patterns = [
        r"\b20\d{2}-[A-Za-z]{3}-\d{2}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s*\d{2},\d{4}\b",
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{2},\s*\d{4}\b",
        r"\b\d{2}-\d{2}-\d{2}\b",
        r"\b\d{8}\b",
        r"\b\d{2}/\d{2}/\d{4}\b",
        r"\b\d{2}/\d{2}/\d{2}\b",
    ]

    tokens: list[str] = []
    for pattern in patterns:
        tokens.extend(re.findall(pattern, line))

    unique_tokens: list[str] = []
    for token in tokens:
        if token not in unique_tokens:
            unique_tokens.append(token)

    return unique_tokens


def find_first_parsed_date_in_line(line: str) -> date | None:
    for token in extract_date_tokens_from_line(line):
        parsed = parse_hh_flexible_date(token)
        if parsed:
            return parsed
    return None


def extract_filename_8digit_tokens(source_filename: str) -> list[str]:
    return re.findall(r"(\d{8})", Path(source_filename).name)


def choose_invoice_filename_fallbacks(source_filename: str) -> dict[str, Any]:
    tokens = extract_filename_8digit_tokens(source_filename)
    invoice_date = parse_hh_mmddyyyy(tokens[0]) if len(tokens) >= 1 else None
    invoice_number = tokens[-2] if len(tokens) >= 2 else None
    remittance_due_date = parse_hh_mmddyyyy(tokens[-1]) if len(tokens) >= 3 else None

    return {
        "invoice_date": invoice_date,
        "invoice_number": invoice_number,
        "remittance_due_date": remittance_due_date,
    }


def choose_remittance_filename_fallbacks(source_filename: str) -> dict[str, Any]:
    tokens = extract_filename_8digit_tokens(source_filename)

    likely_date = None
    for token in reversed(tokens):
        parsed = parse_hh_mmddyyyy(token)
        if parsed:
            likely_date = parsed
            break

    return {
        "remittance_reference": Path(source_filename).stem,
        "remittance_date": likely_date,
        "withdrawal_date": likely_date,
    }


def extract_due_bucket_labels(summary_page_text: str) -> list[str]:
    labels = re.findall(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.\s*\d{2},\d{4}\b",
        summary_page_text,
    )

    cleaned_labels: list[str] = []
    for label in labels:
        cleaned = re.sub(r"\s+", " ", label.strip())
        if cleaned not in cleaned_labels:
            cleaned_labels.append(cleaned)

    return cleaned_labels


def parse_hh_statement_month_end(full_text: str) -> date:
    match = re.search(r"\b(20\d{2})/(\d{2})\b", full_text)
    if not match:
        raise HTTPException(status_code=400, detail="Could not find statement month in HH statement PDF")

    year = int(match.group(1))
    month = int(match.group(2))
    last_day = monthrange(year, month)[1]
    return date(year, month, last_day)


def parse_hh_statement_document(file_bytes: bytes) -> dict[str, Any]:
    pages = extract_pdf_pages_text(file_bytes)
    full_text = "\n".join(pages)
    statement_month_end = parse_hh_statement_month_end(full_text)

    inv_pattern = re.compile(r"^\d{8}$")
    ts_pattern = re.compile(r"^\d+$")
    money_pattern = re.compile(r"^-?\s?[\d,]+\.\d{2}$")
    short_date_pattern = re.compile(r"^\d{2}-\d{2}-\d{2}$")

    detail_lines: list[dict[str, Any]] = []

    for page_text in pages:
        if "Summary Page" in page_text:
            continue

        lines = [line.rstrip() for line in page_text.splitlines()]
        footer_idx = next((i for i, line in enumerate(lines) if "Inv Nbr T/S Invoice Amount" in line), len(lines))
        data_lines = lines[:footer_idx]

        i = 0
        invoice_numbers: list[str] = []
        while i < len(data_lines) and inv_pattern.match(data_lines[i].strip()):
            invoice_numbers.append(data_lines[i].strip())
            i += 1

        ts_codes: list[str] = []
        while i < len(data_lines) and ts_pattern.match(data_lines[i].strip()):
            ts_codes.append(data_lines[i].strip())
            i += 1

        invoice_amounts: list[Decimal] = []
        while i < len(data_lines) and money_pattern.match(data_lines[i].strip()):
            invoice_amounts.append(parse_hh_money(data_lines[i]))
            i += 1

        date_tokens: list[str] = []
        while i < len(data_lines) and short_date_pattern.match(data_lines[i].strip()):
            date_tokens.append(data_lines[i].strip())
            i += 1

        row_count = len(invoice_numbers)
        invoice_dates = date_tokens[:row_count]
        due_dates = date_tokens[row_count : row_count * 2]

        usable_count = min(len(invoice_numbers), len(ts_codes), len(invoice_amounts), len(invoice_dates), len(due_dates))

        for idx in range(usable_count):
            amount = invoice_amounts[idx]
            detail_lines.append(
                {
                    "invoice_number": invoice_numbers[idx],
                    "invoice_type": None,
                    "invoice_date": parse_hh_short_date(invoice_dates[idx]),
                    "due_date": parse_hh_short_date(due_dates[idx]),
                    "invoice_amount": amount,
                    "open_amount": amount,
                    "current_amount": None,
                    "past_due_amount": None,
                    "raw_json": {"statement_ts_code": ts_codes[idx]},
                }
            )

    summary_balances: dict[str, Any] = {}
    due_bucket_totals: dict[str, float] = {}
    summary_components: dict[str, Any] = {}

    summary_page_1 = next((page_text for page_text in pages if "Opening Balance" in page_text and "Balance Owing" in page_text), "")
    if summary_page_1:
        amounts = [parse_hh_money(value) for value in re.findall(r"-?\s?[\d,]+\.\d{2}", summary_page_1)]
        bucket_labels = extract_due_bucket_labels(summary_page_1)

        if len(amounts) >= 5:
            summary_balances = {
                "opening_balance": float(amounts[0]),
                "total_adjustments": float(amounts[1]),
                "total_purchases_this_month": float(amounts[2]),
                "total_payments_this_month": float(amounts[3]),
                "balance_owing": float(amounts[4]),
            }

            bucket_amounts = amounts[5 : 5 + len(bucket_labels)]
            for idx, label in enumerate(bucket_labels):
                if idx < len(bucket_amounts):
                    due_bucket_totals[label] = float(bucket_amounts[idx])

    summary_page_2 = next((page_text for page_text in pages if "This Month" in page_text and "Total Purchases" in page_text), "")
    if summary_page_2:
        amounts = [parse_hh_money(value) for value in re.findall(r"-?\s?[\d,]+\.\d{2}", summary_page_2)]
        metric_labels = [
            "GST/HST",
            "Enviro Amount",
            "Special Shares - Subscribed For",
            "Five Yr Notes - Subscribed For",
            "Service [D. C. Freight]",
            "Total Surcharges",
            "Advertising",
            "Warehouse",
            "Direct",
            "Disc and Promo",
            "Building Supply",
            "Service [T/S 7 Expense]",
            "Red Sur Prom",
            "Total Purchases",
        ]

        if len(amounts) >= len(metric_labels) * 4:
            for idx, label in enumerate(metric_labels):
                summary_components[label] = {
                    "this_month": float(amounts[idx]),
                    "same_month_last_year": float(amounts[idx + 14]),
                    "this_year_to_date": float(amounts[idx + 28]),
                    "last_year_to_date": float(amounts[idx + 42]),
                }

    total_open_balance = summary_balances.get("balance_owing")
    if total_open_balance is None:
        total_open_balance = float(sum(line["open_amount"] for line in detail_lines))

    return {
        "statement_month_end": statement_month_end,
        "total_open_balance": total_open_balance,
        "statement_line_count": len(detail_lines),
        "summary_balances": summary_balances,
        "due_bucket_totals": due_bucket_totals,
        "summary_components": summary_components,
        "lines": detail_lines,
    }


def extract_money_tokens(value: str | None) -> list[str]:
    if not value:
        return []
    return re.findall(MONEY_TOKEN_PATTERN, value, flags=re.IGNORECASE)


def extract_invoice_meta_from_text(full_text: str, source_filename: str) -> tuple[date | None, str | None]:
    fallbacks = choose_invoice_filename_fallbacks(source_filename)
    invoice_meta_match = re.search(r"\b(20\d{2}-[A-Za-z]{3}-\d{2})\s+(\d{8})\b", full_text)

    invoice_date = parse_hh_iso_word_date(invoice_meta_match.group(1)) if invoice_meta_match else fallbacks["invoice_date"]
    invoice_number = invoice_meta_match.group(2) if invoice_meta_match else fallbacks["invoice_number"]
    return invoice_date, invoice_number


def extract_invoice_remittance_due_and_total(lines: list[str]) -> tuple[date | None, Decimal | None]:
    cleaned_lines = [normalize_text(line) for line in lines]
    cleaned_lines = [line for line in cleaned_lines if line]

    for idx in range(len(cleaned_lines)):
        window_lines = cleaned_lines[idx : idx + 6]
        if not window_lines:
            continue

        window_text = " ".join(window_lines)
        upper_window = window_text.upper()

        if "REMITTANCE DUE" not in upper_window and "PLEASE APPLY THIS AMOUNT TO YOUR" not in upper_window:
            continue

        due_date = find_first_parsed_date_in_line(window_text)
        money_tokens = extract_money_tokens(window_text)
        total_amount = parse_hh_signed_money(money_tokens[-1]) if money_tokens else None
        return due_date, total_amount

    return None, None


def extract_terms_summary_amounts(lines: list[str]) -> tuple[Decimal, Decimal, Decimal]:
    for raw_line in reversed(lines):
        cleaned = normalize_text(raw_line)
        if not cleaned:
            continue

        upper_cleaned = cleaned.upper()
        if "SUB TOTAL" in upper_cleaned or "GST/HST" in upper_cleaned:
            continue

        money_tokens = extract_money_tokens(cleaned)
        if len(money_tokens) >= 3 and re.match(r"^\d{2}\b", cleaned):
            subtotal = parse_hh_signed_money(money_tokens[0])
            hst_amount = parse_hh_signed_money(money_tokens[1])
            pst_amount = parse_hh_signed_money(money_tokens[2])
            return subtotal, hst_amount, pst_amount

    return Decimal("0.00"), Decimal("0.00"), Decimal("0.00")


def extract_component_totals_tokens(lines: list[str]) -> list[str] | None:
    candidates: list[list[str]] = []

    for raw_line in lines:
        cleaned = normalize_text(raw_line)
        if not cleaned:
            continue

        upper_cleaned = cleaned.upper()
        if "LESS" in upper_cleaned or "---" in cleaned or "PLEASE APPLY THIS AMOUNT TO YOUR" in upper_cleaned:
            continue

        money_tokens = [token for token in extract_money_tokens(cleaned) if "." in token or token.upper().endswith("CR") or token.upper().endswith("C")]
        if len(money_tokens) >= 8:
            candidates.append(money_tokens[-8:])

    if candidates:
        return candidates[-1]
    return None


def build_component_amounts(*, lines: list[str], subtotal: Decimal) -> tuple[dict[str, Decimal], list[str]]:
    tokens = extract_component_totals_tokens(lines)
    warnings: list[str] = []

    if not tokens:
        warnings.append("component_totals_not_found_used_safe_fallback")
        zero = Decimal("0.00")
        return (
            {
                "c_list_total": subtotal,
                "discount_amount": zero,
                "promo_discount_amount": zero,
                "surcharge_amount": zero,
                "subscribed_shares_amount": zero,
                "five_year_note_amount": zero,
                "advertising_amount": zero,
                "pre_tax_total": subtotal,
            },
            warnings,
        )

    return (
        {
            "c_list_total": parse_hh_signed_money(tokens[0]),
            "discount_amount": parse_hh_signed_money(tokens[1]),
            "promo_discount_amount": parse_hh_signed_money(tokens[2]),
            "surcharge_amount": parse_hh_signed_money(tokens[3]),
            "subscribed_shares_amount": parse_hh_signed_money(tokens[4]),
            "five_year_note_amount": parse_hh_signed_money(tokens[5]),
            "advertising_amount": parse_hh_signed_money(tokens[6]),
            "pre_tax_total": parse_hh_signed_money(tokens[7]),
        },
        warnings,
    )


def extract_labeled_money_from_lines(lines: list[str], label: str) -> Decimal:
    upper_label = label.upper()

    for idx, raw_line in enumerate(lines):
        cleaned = normalize_text(raw_line)
        if not cleaned:
            continue

        if upper_label not in cleaned.upper():
            continue

        same_line_tokens = extract_money_tokens(cleaned)
        if same_line_tokens:
            return parse_hh_signed_money(same_line_tokens[-1])

        for look_ahead in range(idx + 1, min(idx + 3, len(lines))):
            next_line = normalize_text(lines[look_ahead])
            if not next_line:
                continue
            next_tokens = extract_money_tokens(next_line)
            if next_tokens:
                return parse_hh_signed_money(next_tokens[0])

    return Decimal("0.00")


def extract_vendor_direct_metadata(lines: list[str]) -> dict[str, Any]:
    vendor_name = None
    vendor_invoice_number = None
    vendor_invoice_date = None

    for idx, raw_line in enumerate(lines):
        cleaned = normalize_text(raw_line)
        if not cleaned:
            continue

        upper_cleaned = cleaned.upper()
        if "INVOICE DT:" not in upper_cleaned or "INVOICE NBR:" not in upper_cleaned:
            continue

        match = re.search(r"Invoice Dt:\s*(20\d{2}-[A-Za-z]{3}-\d{2})\s+Invoice Nbr:\s*(\S+)", cleaned, flags=re.IGNORECASE)
        if match:
            vendor_invoice_date = parse_hh_iso_word_date(match.group(1))
            vendor_invoice_number = match.group(2)

        for prev_idx in range(idx - 1, -1, -1):
            prev_line = normalize_text(lines[prev_idx])
            if prev_line:
                vendor_name = prev_line
                break

        break

    return {
        "vendor_name": vendor_name,
        "vendor_invoice_number": vendor_invoice_number,
        "vendor_invoice_date": vendor_invoice_date,
    }


def classify_direct_family_invoice(source_filename: str, full_text_upper: str, vendor_invoice_number: str | None, vendor_invoice_date: date | None) -> tuple[str, str]:
    source_name_upper = Path(source_filename).name.upper()

    if any(marker in source_name_upper for marker in VENDOR_DIRECT_FILENAME_MARKERS):
        return INVOICE_TYPE_VENDOR_DIRECT, DOCUMENT_TYPE_HH_INVOICE_DIRECT

    if any(marker in source_name_upper for marker in HH_DIRECT_FILENAME_MARKERS):
        return INVOICE_TYPE_HH_DIRECT, DOCUMENT_TYPE_HH_INVOICE_HH_DIRECT

    if "E DIRECT INVOICE" in full_text_upper:
        return INVOICE_TYPE_VENDOR_DIRECT, DOCUMENT_TYPE_HH_INVOICE_DIRECT

    if vendor_invoice_number or vendor_invoice_date:
        return INVOICE_TYPE_VENDOR_DIRECT, DOCUMENT_TYPE_HH_INVOICE_DIRECT

    return INVOICE_TYPE_HH_DIRECT, DOCUMENT_TYPE_HH_INVOICE_HH_DIRECT


def parse_hh_direct_family_invoice_document(file_bytes: bytes, source_filename: str) -> dict[str, Any]:
    ctx = extract_invoice_parser_context(file_bytes)
    all_lines = ctx["all_lines"]
    full_text = ctx["full_text"]
    full_text_upper = ctx["full_text_upper"]

    invoice_date, invoice_number = extract_invoice_meta_from_text(full_text=full_text, source_filename=source_filename)
    remittance_due_date, total_amount = extract_invoice_remittance_due_and_total(all_lines)
    subtotal, hst_amount, pst_amount = extract_terms_summary_amounts(all_lines)
    component_amounts, parser_warnings = build_component_amounts(lines=all_lines, subtotal=subtotal)

    service_charges = extract_labeled_money_from_lines(all_lines, "Service Charges")
    enviro_fee_amount = extract_labeled_money_from_lines(all_lines, "Enviro Fee Amount")

    vendor_meta = extract_vendor_direct_metadata(all_lines)
    vendor_name = vendor_meta["vendor_name"]
    vendor_invoice_number = vendor_meta["vendor_invoice_number"]
    vendor_invoice_date = vendor_meta["vendor_invoice_date"]

    invoice_type, document_type = classify_direct_family_invoice(
        source_filename=source_filename,
        full_text_upper=full_text_upper,
        vendor_invoice_number=vendor_invoice_number,
        vendor_invoice_date=vendor_invoice_date,
    )

    if invoice_type == INVOICE_TYPE_HH_DIRECT:
        vendor_name = "Home Hardware Stores Limited"
        vendor_invoice_number = None
        vendor_invoice_date = None

    if total_amount is None:
        total_amount = subtotal + hst_amount + pst_amount

    if not invoice_number or not invoice_date:
        raise HTTPException(status_code=400, detail=f"Could not determine invoice number/date for direct-family invoice: {source_filename}")

    return {
        "document_type": document_type,
        "invoice_type": invoice_type,
        "invoice_number": invoice_number,
        "vendor_name": vendor_name,
        "vendor_invoice_number": vendor_invoice_number,
        "po_number": None,
        "invoice_date": invoice_date,
        "due_date": remittance_due_date,
        "remittance_due_date": remittance_due_date,
        "currency_code": DEFAULT_CURRENCY_CODE,
        "subtotal": subtotal,
        "hst_amount": hst_amount,
        "surcharge_amount": component_amounts["surcharge_amount"],
        "advertising_amount": component_amounts["advertising_amount"],
        "subscribed_shares_amount": component_amounts["subscribed_shares_amount"],
        "five_year_note_amount": component_amounts["five_year_note_amount"],
        "total_amount": total_amount,
        "is_statement_only": False,
        "notes": None,
        "raw_json": {
            "parser_version": "invoice_v3",
            "invoice_source_type": invoice_type,
            "vendor_invoice_date": str(vendor_invoice_date) if vendor_invoice_date else None,
            "pst_amount": money_float(pst_amount),
            "c_list_total": money_float(component_amounts["c_list_total"]),
            "discount_amount": money_float(component_amounts["discount_amount"]),
            "promo_discount_amount": money_float(component_amounts["promo_discount_amount"]),
            "pre_tax_total": money_float(component_amounts["pre_tax_total"]),
            "service_charges": money_float(service_charges),
            "enviro_fee_amount": money_float(enviro_fee_amount),
            "source_filename": source_filename,
            "parser_warnings": parser_warnings,
        },
    }


def parse_hh_warehouse_invoice_document(file_bytes: bytes, source_filename: str) -> dict[str, Any]:
    ctx = extract_invoice_parser_context(file_bytes)
    all_lines = ctx["all_lines"]
    full_text = ctx["full_text"]
    fallbacks = choose_invoice_filename_fallbacks(source_filename)

    due_total_line = next(
        (
            normalize_text(line)
            for line in all_lines
            if re.fullmatch(r"20\d{2}-[A-Za-z]{3}-\d{2}\s+[\d,]*\.?\d+(?:CR|C)?", normalize_text(line) or "")
        ),
        None,
    )
    due_total_match = re.match(r"(20\d{2}-[A-Za-z]{3}-\d{2})\s+([\d,]*\.?\d+(?:CR|C)?)", due_total_line or "")

    remittance_due_date = parse_hh_iso_word_date(due_total_match.group(1)) if due_total_match else fallbacks["remittance_due_date"]
    total_amount = parse_hh_signed_money(due_total_match.group(2)) if due_total_match else None

    invoice_date, invoice_number = extract_invoice_meta_from_text(full_text=full_text, source_filename=source_filename)
    subtotal, hst_amount, pst_amount = extract_terms_summary_amounts(all_lines)
    component_amounts, parser_warnings = build_component_amounts(lines=all_lines, subtotal=subtotal)

    service_charges = extract_labeled_money_from_lines(all_lines, "Service Charges")
    enviro_fee_amount = extract_labeled_money_from_lines(all_lines, "Enviro Fee Amount")

    if total_amount is None:
        total_amount = subtotal + hst_amount + pst_amount

    if not invoice_number or not invoice_date:
        raise HTTPException(status_code=400, detail=f"Could not determine invoice number/date for warehouse invoice: {source_filename}")

    return {
        "document_type": DOCUMENT_TYPE_HH_INVOICE_WAREHOUSE,
        "invoice_type": INVOICE_TYPE_WAREHOUSE,
        "invoice_number": invoice_number,
        "vendor_name": "Home Hardware Stores Limited",
        "vendor_invoice_number": None,
        "po_number": None,
        "invoice_date": invoice_date,
        "due_date": remittance_due_date,
        "remittance_due_date": remittance_due_date,
        "currency_code": DEFAULT_CURRENCY_CODE,
        "subtotal": subtotal,
        "hst_amount": hst_amount,
        "surcharge_amount": component_amounts["surcharge_amount"],
        "advertising_amount": component_amounts["advertising_amount"],
        "subscribed_shares_amount": component_amounts["subscribed_shares_amount"],
        "five_year_note_amount": component_amounts["five_year_note_amount"],
        "total_amount": total_amount,
        "is_statement_only": False,
        "notes": None,
        "raw_json": {
            "parser_version": "invoice_v3",
            "invoice_source_type": INVOICE_TYPE_WAREHOUSE,
            "pst_amount": money_float(pst_amount),
            "c_list_total": money_float(component_amounts["c_list_total"]),
            "discount_amount": money_float(component_amounts["discount_amount"]),
            "promo_discount_amount": money_float(component_amounts["promo_discount_amount"]),
            "pre_tax_total": money_float(component_amounts["pre_tax_total"]),
            "service_charges": money_float(service_charges),
            "enviro_fee_amount": money_float(enviro_fee_amount),
            "source_filename": source_filename,
            "parser_warnings": parser_warnings,
        },
    }


def parse_hh_invoice_document(file_bytes: bytes, source_filename: str) -> dict[str, Any]:
    source_name_upper = Path(source_filename).name.upper()

    if any(marker in source_name_upper for marker in WAREHOUSE_FILENAME_MARKERS):
        return parse_hh_warehouse_invoice_document(file_bytes, source_filename)

    direct_error = None
    warehouse_error = None

    try:
        return parse_hh_direct_family_invoice_document(file_bytes, source_filename)
    except HTTPException as exc:
        direct_error = exc.detail

    try:
        return parse_hh_warehouse_invoice_document(file_bytes, source_filename)
    except HTTPException as exc:
        warehouse_error = exc.detail

    raise HTTPException(
        status_code=400,
        detail=(
            f"Could not parse HH invoice document: {source_filename}. "
            f"Direct-family parser error: {direct_error}. "
            f"Warehouse parser error: {warehouse_error}."
        ),
    )


def extract_due_date_from_remittance_text(full_text: str) -> date | None:
    lines = [normalize_text(line) for line in full_text.splitlines()]
    lines = [line for line in lines if line]

    for idx, line in enumerate(lines):
        if "THE FOLLOWING ARE DUE ON" not in line.upper():
            continue

        candidates = [line] + lines[idx + 1 : idx + 3]
        for candidate in candidates:
            parsed = find_first_parsed_date_in_line(candidate)
            if parsed:
                return parsed

        trailing = re.split(r"THE FOLLOWING ARE DUE ON", line, flags=re.IGNORECASE)[-1].strip()
        parsed = parse_hh_flexible_date(trailing)
        if parsed:
            return parsed

    return None


def extract_filename_date_fallback(source_filename: str) -> date | None:
    fallbacks = choose_remittance_filename_fallbacks(source_filename)
    return fallbacks["withdrawal_date"]


def find_money_after_label(full_text: str, label: str) -> Decimal | None:
    money_pattern = r"-?[\d,]+\.\d{2}(?:CR|C|-)?"

    same_line_match = re.search(rf"{re.escape(label)}\s*[:\-]?\s*({money_pattern})", full_text, flags=re.IGNORECASE)
    if same_line_match:
        return parse_hh_signed_money(same_line_match.group(1))

    lines = full_text.splitlines()
    for idx, raw_line in enumerate(lines):
        line = normalize_text(raw_line)
        if not line or label.upper() not in line.upper():
            continue

        for look_ahead in range(idx, min(idx + 3, len(lines))):
            candidate = normalize_text(lines[look_ahead])
            if not candidate:
                continue
            match = re.search(money_pattern, candidate)
            if match:
                return parse_hh_signed_money(match.group(0))

    return None


def parse_remittance_entries_from_layout_line(line: str, common_due_date: date | None, source_filename: str) -> list[dict[str, Any]]:
    cleaned = normalize_text(line)
    if not cleaned:
        return []

    token_pattern = rf"{EIGHT_DIGIT_TOKEN_PATTERN}|{MONEY_TOKEN_PATTERN}"
    tokens = re.findall(token_pattern, cleaned, flags=re.IGNORECASE)

    entries: list[dict[str, Any]] = []
    pending_invoice: str | None = None

    for token in tokens:
        if re.fullmatch(r"\d{8}", token):
            pending_invoice = token
            continue

        if pending_invoice is None:
            continue

        entries.append(
            {
                "invoice_number": pending_invoice,
                "line_description": None,
                "due_date": common_due_date,
                "line_amount": parse_hh_signed_money(token),
                "raw_json": {
                    "parser_version": "remittance_v3",
                    "source_filename": source_filename,
                    "source_line": cleaned,
                },
            }
        )
        pending_invoice = None

    return entries


def parse_hh_remittance_document(file_bytes: bytes, source_filename: str) -> dict[str, Any]:
    pages = extract_pdf_pages_layout_text(file_bytes)
    full_text = "\n".join(pages)

    remittance_reference = Path(source_filename).stem
    due_date = extract_due_date_from_remittance_text(full_text)
    fallback_date = extract_filename_date_fallback(source_filename)

    remittance_date = due_date or fallback_date
    withdrawal_date = due_date or fallback_date

    pay_this_amount = find_money_after_label(full_text, "Pay This Amount")
    total_purchases_due = find_money_after_label(full_text, "Total Purchases Due")
    total_service_expense = find_money_after_label(full_text, "Total Service Expense")

    parsed_lines: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()

    for page_text in pages:
        for raw_line in page_text.splitlines():
            line = normalize_text(raw_line)
            if not line:
                continue

            upper_line = line.upper()
            if (
                "INVOICE NUMBER" in upper_line
                or "THE FOLLOWING ARE DUE ON" in upper_line
                or "TOTAL SERVICE EXPENSE" in upper_line
                or "PAY THIS AMOUNT" in upper_line
                or "TOTAL PURCHASES DUE" in upper_line
            ):
                continue

            if not re.search(EIGHT_DIGIT_TOKEN_PATTERN, line):
                continue

            entries = parse_remittance_entries_from_layout_line(
                line=line,
                common_due_date=due_date or fallback_date,
                source_filename=source_filename,
            )

            for entry in entries:
                key = (entry["invoice_number"], str(entry["line_amount"]))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                parsed_lines.append(entry)

    if not parsed_lines:
        raise HTTPException(status_code=400, detail=f"Could not parse any remittance invoice lines from document: {source_filename}")

    detail_line_total = sum((line["line_amount"] for line in parsed_lines), Decimal("0.00"))
    warnings: list[str] = []

    if pay_this_amount is not None and abs(detail_line_total - pay_this_amount) <= Decimal("0.05"):
        pass
    elif total_purchases_due is not None and abs(detail_line_total - total_purchases_due) <= Decimal("0.05"):
        pass
    elif total_service_expense is not None and abs(detail_line_total - total_service_expense) <= Decimal("0.05"):
        warnings.append("Parsed invoice line total ties to Total Service Expense, not the full remittance amount")
    else:
        warnings.append(
            f"Parsed invoice line total {detail_line_total} does not tie to Pay This Amount "
            f"{pay_this_amount} or Total Purchases Due {total_purchases_due}"
        )

    bank_total_amount = pay_this_amount or total_purchases_due or detail_line_total

    return {
        "document_type": DOCUMENT_TYPE_HH_REMITTANCE,
        "remittance_reference": remittance_reference,
        "remittance_date": remittance_date,
        "withdrawal_date": withdrawal_date,
        "total_amount": bank_total_amount,
        "line_count": len(parsed_lines),
        "lines": parsed_lines,
        "raw_json": {
            "parser_version": "remittance_v3",
            "source_filename": source_filename,
            "page_count": len(pages),
            "pay_this_amount": money_float(pay_this_amount) if pay_this_amount is not None else None,
            "total_purchases_due": money_float(total_purchases_due) if total_purchases_due is not None else None,
            "total_service_expense": money_float(total_service_expense) if total_service_expense is not None else None,
            "detail_line_total": money_float(detail_line_total),
            "warnings": warnings,
        },
    }


def build_invoice_map(invoice_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    invoice_map_by_number: dict[str, list[dict[str, Any]]] = {}
    for row in invoice_rows:
        invoice_number = normalize_invoice_number(row["invoice_number"])
        if not invoice_number:
            continue
        invoice_map_by_number.setdefault(invoice_number, []).append(dict(row))
    return invoice_map_by_number


def choose_invoice_match_candidate(candidates: list[dict[str, Any]], desired_invoice_type: str | None) -> dict[str, Any] | None:
    if not candidates:
        return None

    if desired_invoice_type:
        exact_type_matches = [candidate for candidate in candidates if normalize_text(candidate.get("invoice_type")) == desired_invoice_type]
        if len(exact_type_matches) == 1:
            return exact_type_matches[0]
        if len(exact_type_matches) > 1:
            return None

    if len(candidates) == 1:
        return candidates[0]

    return None


class HHAPInvoiceInput(BaseModel):
    invoice_number: str = Field(...)
    invoice_type: str = Field(...)
    vendor_name: str | None = None
    vendor_invoice_number: str | None = None
    po_number: str | None = None
    invoice_date: date | None = None
    due_date: date | None = None
    remittance_due_date: date | None = None
    currency_code: str = DEFAULT_CURRENCY_CODE
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


class HHAPParseStatementDocumentRequest(BaseModel):
    entity_code: str
    document_id: str | None = None


class HHAPParseRemittanceDocumentRequest(BaseModel):
    entity_code: str
    document_id: str | None = None


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


class HHAPParseInvoiceDocumentRequest(BaseModel):
    entity_code: str
    document_id: str | None = None


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
        normalized_document_date = normalize_optional_date_input(document_date)

        inserted_documents: list[dict[str, Any]] = []
        updated_documents: list[dict[str, Any]] = []
        duplicate_documents: list[dict[str, Any]] = []

        for upload in files:
            file_bytes = await upload.read()
            if not file_bytes:
                continue

            source_hash = build_source_hash(file_bytes)
            extracted_text = try_extract_text(file_bytes=file_bytes, filename=upload.filename or "unknown", content_type=upload.content_type)

            existing = session.execute(
                text(
                    """
                    SELECT
                        id,
                        source_filename,
                        document_type,
                        extracted_text,
                        file_size_bytes
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

            processing_status = PROCESSING_STATUS_UPLOADED_TEXT_READY if extracted_text else PROCESSING_STATUS_UPLOADED_PENDING_PARSE

            if existing:
                needs_upgrade = existing.get("file_size_bytes") in (None, 0) or (extracted_text is not None and not existing.get("extracted_text"))

                if needs_upgrade:
                    updated_row = session.execute(
                        text(
                            """
                            UPDATE hh_ap_documents
                            SET document_date = COALESCE(:document_date, document_date),
                                content_type = :content_type,
                                file_size_bytes = :file_size_bytes,
                                file_bytes = :file_bytes,
                                extracted_text = COALESCE(:extracted_text, extracted_text),
                                processing_status = :processing_status,
                                raw_json = CAST(:raw_json AS jsonb),
                                updated_at = NOW()
                            WHERE id = :id
                            RETURNING id, updated_at
                            """
                        ),
                        {
                            "id": existing["id"],
                            "document_date": normalized_document_date,
                            "content_type": upload.content_type,
                            "file_size_bytes": len(file_bytes),
                            "file_bytes": file_bytes,
                            "extracted_text": extracted_text,
                            "processing_status": processing_status,
                            "raw_json": json_dumps({"content_type": upload.content_type, "file_size_bytes": len(file_bytes)}),
                        },
                    ).mappings().first()

                    updated_documents.append(
                        {
                            "id": str(updated_row["id"]),
                            "source_filename": existing["source_filename"],
                            "document_type": existing["document_type"],
                            "processing_status": processing_status,
                            "updated_at": updated_row["updated_at"].isoformat() if updated_row["updated_at"] else None,
                        }
                    )
                else:
                    duplicate_documents.append(
                        {
                            "id": str(existing["id"]),
                            "source_filename": existing["source_filename"],
                            "document_type": existing["document_type"],
                        }
                    )
                continue

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
                        content_type,
                        file_size_bytes,
                        file_bytes,
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
                        :content_type,
                        :file_size_bytes,
                        :file_bytes,
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
                    "document_date": normalized_document_date,
                    "processing_status": processing_status,
                    "content_type": upload.content_type,
                    "file_size_bytes": len(file_bytes),
                    "file_bytes": file_bytes,
                    "extracted_text": extracted_text,
                    "raw_json": json_dumps({"content_type": upload.content_type, "file_size_bytes": len(file_bytes)}),
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
            "updated_count": len(updated_documents),
            "duplicate_count": len(duplicate_documents),
            "inserted_documents": inserted_documents,
            "updated_documents": updated_documents,
            "duplicate_documents": duplicate_documents,
        }


# Remaining endpoints omitted from here-doc for brevity in generation step.
# The full file continues below in the written artifact.

@router.post("/invoices/upload-and-parse-batch")
async def hh_ap_upload_and_parse_invoices_batch(
    entity_code: str = Form(...),
    document_date: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
):
    normalized_document_date = normalize_optional_date_input(document_date)

    with db_session() as session:
        _ = get_entity(session, entity_code)

    invoice_document_types = get_allowed_invoice_document_types()
    processed_files: list[dict[str, Any]] = []
    failed_files: list[dict[str, Any]] = []

    for upload in files:
        filename = upload.filename or "unknown"

        try:
            file_bytes = await upload.read()
            if not file_bytes:
                failed_files.append({"source_filename": filename, "error": "File was empty"})
                continue

            with db_session() as session:
                entity = get_entity(session, entity_code)

                source_hash = build_source_hash(file_bytes)
                extracted_text = try_extract_text(
                    file_bytes=file_bytes,
                    filename=filename,
                    content_type=upload.content_type,
                )

                existing_document = session.execute(
                    text(
                        """
                        SELECT
                            id,
                            source_filename,
                            document_type,
                            extracted_text,
                            file_size_bytes
                        FROM hh_ap_documents
                        WHERE entity_id = :entity_id
                          AND source_hash = :source_hash
                          AND document_type = ANY(:invoice_document_types)
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "entity_id": entity["id"],
                        "source_hash": source_hash,
                        "invoice_document_types": invoice_document_types,
                    },
                ).mappings().first()

                initial_processing_status = (
                    PROCESSING_STATUS_UPLOADED_TEXT_READY
                    if extracted_text
                    else PROCESSING_STATUS_UPLOADED_PENDING_PARSE
                )

                if existing_document:
                    document_id = existing_document["id"]

                    needs_upgrade = (
                        existing_document.get("file_size_bytes") in (None, 0)
                        or (extracted_text is not None and not existing_document.get("extracted_text"))
                    )

                    if needs_upgrade:
                        session.execute(
                            text(
                                """
                                UPDATE hh_ap_documents
                                SET document_date = COALESCE(:document_date, document_date),
                                    content_type = :content_type,
                                    file_size_bytes = :file_size_bytes,
                                    file_bytes = :file_bytes,
                                    extracted_text = COALESCE(:extracted_text, extracted_text),
                                    processing_status = :processing_status,
                                    raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:raw_json AS jsonb),
                                    updated_at = NOW()
                                WHERE id = :id
                                """
                            ),
                            {
                                "id": document_id,
                                "document_date": normalized_document_date,
                                "content_type": upload.content_type,
                                "file_size_bytes": len(file_bytes),
                                "file_bytes": file_bytes,
                                "extracted_text": extracted_text,
                                "processing_status": initial_processing_status,
                                "raw_json": json_dumps(
                                    {
                                        "content_type": upload.content_type,
                                        "file_size_bytes": len(file_bytes),
                                    }
                                ),
                            },
                        )
                else:
                    inserted_document = session.execute(
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
                                content_type,
                                file_size_bytes,
                                file_bytes,
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
                                :content_type,
                                :file_size_bytes,
                                :file_bytes,
                                :extracted_text,
                                CAST(:raw_json AS jsonb)
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "entity_id": entity["id"],
                            "document_type": DOCUMENT_TYPE_HH_INVOICE,
                            "source_filename": filename,
                            "source_hash": source_hash,
                            "document_date": normalized_document_date,
                            "processing_status": initial_processing_status,
                            "content_type": upload.content_type,
                            "file_size_bytes": len(file_bytes),
                            "file_bytes": file_bytes,
                            "extracted_text": extracted_text,
                            "raw_json": json_dumps(
                                {
                                    "content_type": upload.content_type,
                                    "file_size_bytes": len(file_bytes),
                                }
                            ),
                        },
                    ).mappings().first()

                    document_id = inserted_document["id"]

                try:
                    parsed = parse_hh_invoice_document(
                        file_bytes=file_bytes,
                        source_filename=filename,
                    )

                    invoice_row = session.execute(
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
                                document_id = EXCLUDED.document_id,
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
                            RETURNING id, invoice_number, invoice_type
                            """
                        ),
                        {
                            "entity_id": entity["id"],
                            "document_id": document_id,
                            "invoice_number": parsed["invoice_number"],
                            "invoice_type": parsed["invoice_type"],
                            "vendor_name": parsed["vendor_name"],
                            "vendor_invoice_number": parsed["vendor_invoice_number"],
                            "po_number": parsed["po_number"],
                            "invoice_date": parsed["invoice_date"],
                            "due_date": parsed["due_date"],
                            "remittance_due_date": parsed["remittance_due_date"],
                            "currency_code": parsed["currency_code"],
                            "subtotal": parsed["subtotal"],
                            "hst_amount": parsed["hst_amount"],
                            "surcharge_amount": parsed["surcharge_amount"],
                            "advertising_amount": parsed["advertising_amount"],
                            "subscribed_shares_amount": parsed["subscribed_shares_amount"],
                            "five_year_note_amount": parsed["five_year_note_amount"],
                            "total_amount": parsed["total_amount"],
                            "match_status": MATCH_STATUS_UNMATCHED,
                            "is_statement_only": parsed["is_statement_only"],
                            "notes": parsed["notes"],
                            "raw_json": json_dumps(parsed["raw_json"]),
                        },
                    ).mappings().first()

                    session.execute(
                        text(
                            """
                            UPDATE hh_ap_documents
                            SET processing_status = :processing_status,
                                raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:parser_json AS jsonb),
                                updated_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {
                            "id": document_id,
                            "processing_status": PROCESSING_STATUS_PARSED_INVOICE,
                            "parser_json": json_dumps(
                                {
                                    "parsed_as": parsed["document_type"],
                                    "parsed_invoice_number": parsed["invoice_number"],
                                    "parsed_invoice_type": parsed["invoice_type"],
                                }
                            ),
                        },
                    )

                    processed_files.append(
                        {
                            "source_filename": filename,
                            "document_id": str(document_id),
                            "invoice_id": str(invoice_row["id"]),
                            "invoice_number": invoice_row["invoice_number"],
                            "invoice_type": invoice_row["invoice_type"],
                            "status": "parsed",
                        }
                    )

                except HTTPException as exc:
                    session.execute(
                        text(
                            """
                            UPDATE hh_ap_documents
                            SET processing_status = :processing_status,
                                raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:parser_json AS jsonb),
                                updated_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {
                            "id": document_id,
                            "processing_status": PROCESSING_STATUS_PARSE_FAILED_INVOICE,
                            "parser_json": json_dumps(
                                {
                                    "parse_failed_as": "invoice",
                                    "parse_error": exc.detail,
                                }
                            ),
                        },
                    )

                    failed_files.append(
                        {
                            "source_filename": filename,
                            "document_id": str(document_id),
                            "error": exc.detail,
                        }
                    )

        except Exception as exc:
            failed_files.append({"source_filename": filename, "error": str(exc)})

    return {
        "entity_code": entity_code,
        "file_count": len(files),
        "parsed_count": len(processed_files),
        "failed_count": len(failed_files),
        "parsed_files": processed_files,
        "failed_files": failed_files,
    }


@router.post("/invoices/upsert")
def hh_ap_invoices_upsert(payload: HHAPInvoiceUpsertRequest):
    if not payload.invoices:
        raise HTTPException(status_code=400, detail="At least one invoice is required")

    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        upserted: list[dict[str, Any]] = []

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
                    "currency_code": normalize_text(invoice.currency_code) or DEFAULT_CURRENCY_CODE,
                    "subtotal": invoice.subtotal,
                    "hst_amount": invoice.hst_amount,
                    "surcharge_amount": invoice.surcharge_amount,
                    "advertising_amount": invoice.advertising_amount,
                    "subscribed_shares_amount": invoice.subscribed_shares_amount,
                    "five_year_note_amount": invoice.five_year_note_amount,
                    "total_amount": invoice.total_amount,
                    "match_status": MATCH_STATUS_UNMATCHED,
                    "is_statement_only": invoice.is_statement_only,
                    "notes": normalize_text(invoice.notes),
                    "raw_json": json_dumps(invoice.raw_json),
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
                {"entity_id": entity["id"], "document_id": payload.document_id},
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
                    "raw_json": json_dumps(payload.raw_json),
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
                    "raw_json": json_dumps(payload.raw_json),
                },
            ).mappings().first()

        session.execute(text("DELETE FROM hh_ap_remittance_lines WHERE remittance_id = :remittance_id"), {"remittance_id": remittance_row["id"]})

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
                        :match_status,
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
                    "match_status": MATCH_STATUS_UNMATCHED,
                    "raw_json": json_dumps(line.raw_json),
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
                {"entity_id": entity["id"], "document_id": payload.document_id},
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
                {"entity_id": entity["id"], "statement_month_end": payload.statement_month_end},
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
                    "raw_json": json_dumps(payload.raw_json),
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
                    "raw_json": json_dumps(payload.raw_json),
                },
            ).mappings().first()

        session.execute(text("DELETE FROM hh_ap_statement_lines WHERE statement_id = :statement_id"), {"statement_id": statement_row["id"]})

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
                        :match_status,
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
                    "match_status": MATCH_STATUS_UNMATCHED,
                    "raw_json": json_dumps(line.raw_json),
                },
            )
            inserted_lines += 1

        return {
            "entity_code": entity["entity_code"],
            "statement_id": str(statement_row["id"]),
            "statement_line_count": inserted_lines,
            "statement_month_end": str(payload.statement_month_end),
        }

@router.post("/statements/parse-document")
def hh_ap_parse_statement_document(payload: HHAPParseStatementDocumentRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)

        if payload.document_id:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND id = :document_id
                      AND document_type = :document_type
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "document_id": payload.document_id, "document_type": DOCUMENT_TYPE_HH_STATEMENT},
            ).mappings().first()
        else:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND document_type = :document_type
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "document_type": DOCUMENT_TYPE_HH_STATEMENT},
            ).mappings().first()

        if not document:
            raise HTTPException(status_code=404, detail="No HH statement document found for this entity")
        if not document["file_bytes"]:
            raise HTTPException(status_code=400, detail="This HH statement document has no stored file bytes to parse")

        parsed = parse_hh_statement_document(document["file_bytes"])
        statement_month_end = parsed["statement_month_end"]
        statement_date = document["document_date"] or statement_month_end

        existing_statement = session.execute(
            text(
                """
                SELECT id
                FROM hh_ap_statements
                WHERE entity_id = :entity_id
                  AND (document_id = :document_id OR statement_month_end = :statement_month_end)
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"entity_id": entity["id"], "document_id": document["id"], "statement_month_end": statement_month_end},
        ).mappings().first()

        statement_raw_json = {
            "source_filename": document["source_filename"],
            "summary_balances": parsed["summary_balances"],
            "due_bucket_totals": parsed["due_bucket_totals"],
            "summary_components": parsed["summary_components"],
        }

        if existing_statement:
            statement_row = session.execute(
                text(
                    """
                    UPDATE hh_ap_statements
                    SET document_id = :document_id,
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
                    "id": existing_statement["id"],
                    "document_id": document["id"],
                    "statement_date": statement_date,
                    "statement_month_end": statement_month_end,
                    "total_open_balance": parsed["total_open_balance"],
                    "raw_json": json_dumps(statement_raw_json),
                },
            ).mappings().first()
        else:
            statement_row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_statements (
                        entity_id, document_id, statement_date, statement_month_end, total_open_balance, raw_json
                    ) VALUES (
                        :entity_id, :document_id, :statement_date, :statement_month_end, :total_open_balance, CAST(:raw_json AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": document["id"],
                    "statement_date": statement_date,
                    "statement_month_end": statement_month_end,
                    "total_open_balance": parsed["total_open_balance"],
                    "raw_json": json_dumps(statement_raw_json),
                },
            ).mappings().first()

        session.execute(text("DELETE FROM hh_ap_statement_lines WHERE statement_id = :statement_id"), {"statement_id": statement_row["id"]})

        inserted_lines = 0
        for line in parsed["lines"]:
            session.execute(
                text(
                    """
                    INSERT INTO hh_ap_statement_lines (
                        statement_id, entity_id, invoice_number, invoice_type, invoice_date, due_date,
                        invoice_amount, open_amount, current_amount, past_due_amount,
                        matched_invoice_id, match_status, is_missing_download, raw_json
                    ) VALUES (
                        :statement_id, :entity_id, :invoice_number, :invoice_type, :invoice_date, :due_date,
                        :invoice_amount, :open_amount, :current_amount, :past_due_amount,
                        NULL, :match_status, FALSE, CAST(:raw_json AS jsonb)
                    )
                    """
                ),
                {
                    "statement_id": statement_row["id"],
                    "entity_id": entity["id"],
                    "invoice_number": normalize_invoice_number(line["invoice_number"]),
                    "invoice_type": line["invoice_type"],
                    "invoice_date": line["invoice_date"],
                    "due_date": line["due_date"],
                    "invoice_amount": line["invoice_amount"],
                    "open_amount": line["open_amount"],
                    "current_amount": line["current_amount"],
                    "past_due_amount": line["past_due_amount"],
                    "match_status": MATCH_STATUS_UNMATCHED,
                    "raw_json": json_dumps(line["raw_json"]),
                },
            )
            inserted_lines += 1

        session.execute(
            text(
                """
                UPDATE hh_ap_documents
                SET processing_status = :processing_status,
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:parser_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": document["id"],
                "processing_status": PROCESSING_STATUS_PARSED_STATEMENT,
                "parser_json": json_dumps({
                    "parsed_as": DOCUMENT_TYPE_HH_STATEMENT,
                    "parsed_statement_month_end": str(statement_month_end),
                    "parsed_statement_line_count": inserted_lines,
                }),
            },
        )

        return {
            "entity_code": entity["entity_code"],
            "document_id": str(document["id"]),
            "statement_id": str(statement_row["id"]),
            "statement_month_end": str(statement_month_end),
            "statement_line_count": inserted_lines,
            "total_open_balance": parsed["total_open_balance"],
            "summary_balances": parsed["summary_balances"],
            "summary_components": parsed["summary_components"],
        }


@router.post("/invoices/parse-document")
def hh_ap_parse_invoice_document(payload: HHAPParseInvoiceDocumentRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        allowed_document_types = tuple(get_allowed_invoice_document_types())

        if payload.document_id:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND id = :document_id
                      AND document_type = ANY(:allowed_document_types)
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "document_id": payload.document_id, "allowed_document_types": list(allowed_document_types)},
            ).mappings().first()
        else:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND document_type = ANY(:allowed_document_types)
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "allowed_document_types": list(allowed_document_types)},
            ).mappings().first()

        if not document:
            raise HTTPException(status_code=404, detail="No HH invoice document found for this entity")
        if not document["file_bytes"]:
            raise HTTPException(status_code=400, detail="This HH invoice document has no stored file bytes to parse")

        parsed = parse_hh_invoice_document(file_bytes=document["file_bytes"], source_filename=document["source_filename"])

        invoice_row = session.execute(
            text(
                """
                INSERT INTO hh_ap_invoices (
                    entity_id, document_id, invoice_number, invoice_type, vendor_name, vendor_invoice_number,
                    po_number, invoice_date, due_date, remittance_due_date, currency_code, subtotal, hst_amount,
                    surcharge_amount, advertising_amount, subscribed_shares_amount, five_year_note_amount,
                    total_amount, match_status, is_statement_only, notes, raw_json
                ) VALUES (
                    :entity_id, :document_id, :invoice_number, :invoice_type, :vendor_name, :vendor_invoice_number,
                    :po_number, :invoice_date, :due_date, :remittance_due_date, :currency_code, :subtotal, :hst_amount,
                    :surcharge_amount, :advertising_amount, :subscribed_shares_amount, :five_year_note_amount,
                    :total_amount, :match_status, :is_statement_only, :notes, CAST(:raw_json AS jsonb)
                )
                ON CONFLICT (entity_id, invoice_number, invoice_type)
                DO UPDATE SET
                    document_id = EXCLUDED.document_id,
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
                "document_id": document["id"],
                "invoice_number": parsed["invoice_number"],
                "invoice_type": parsed["invoice_type"],
                "vendor_name": parsed["vendor_name"],
                "vendor_invoice_number": parsed["vendor_invoice_number"],
                "po_number": parsed["po_number"],
                "invoice_date": parsed["invoice_date"],
                "due_date": parsed["due_date"],
                "remittance_due_date": parsed["remittance_due_date"],
                "currency_code": parsed["currency_code"],
                "subtotal": parsed["subtotal"],
                "hst_amount": parsed["hst_amount"],
                "surcharge_amount": parsed["surcharge_amount"],
                "advertising_amount": parsed["advertising_amount"],
                "subscribed_shares_amount": parsed["subscribed_shares_amount"],
                "five_year_note_amount": parsed["five_year_note_amount"],
                "total_amount": parsed["total_amount"],
                "match_status": MATCH_STATUS_UNMATCHED,
                "is_statement_only": parsed["is_statement_only"],
                "notes": parsed["notes"],
                "raw_json": json_dumps(parsed["raw_json"]),
            },
        ).mappings().first()

        session.execute(
            text(
                """
                UPDATE hh_ap_documents
                SET processing_status = :processing_status,
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:parser_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": document["id"],
                "processing_status": PROCESSING_STATUS_PARSED_INVOICE,
                "parser_json": json_dumps({
                    "parsed_as": parsed["document_type"],
                    "parsed_invoice_number": parsed["invoice_number"],
                    "parsed_invoice_type": parsed["invoice_type"],
                }),
            },
        )

        return {
            "entity_code": entity["entity_code"],
            "document_id": str(document["id"]),
            "invoice_id": str(invoice_row["id"]),
            "invoice_number": parsed["invoice_number"],
            "invoice_type": parsed["invoice_type"],
            "invoice_date": str(parsed["invoice_date"]) if parsed["invoice_date"] else None,
            "remittance_due_date": str(parsed["remittance_due_date"]) if parsed["remittance_due_date"] else None,
            "subtotal": money_float(parsed["subtotal"]),
            "hst_amount": money_float(parsed["hst_amount"]),
            "surcharge_amount": money_float(parsed["surcharge_amount"]),
            "advertising_amount": money_float(parsed["advertising_amount"]),
            "subscribed_shares_amount": money_float(parsed["subscribed_shares_amount"]),
            "five_year_note_amount": money_float(parsed["five_year_note_amount"]),
            "total_amount": money_float(parsed["total_amount"]),
            "vendor_name": parsed["vendor_name"],
            "vendor_invoice_number": parsed["vendor_invoice_number"],
            "raw_json": parsed["raw_json"],
        }

@router.post("/remittances/parse-document")
def hh_ap_parse_remittance_document(payload: HHAPParseRemittanceDocumentRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        allowed_document_types = (DOCUMENT_TYPE_HH_REMITTANCE, DOCUMENT_TYPE_HH_DOCUMENT)

        if payload.document_id:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND id = :document_id
                      AND document_type = ANY(:allowed_document_types)
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "document_id": payload.document_id, "allowed_document_types": list(allowed_document_types)},
            ).mappings().first()
        else:
            document = session.execute(
                text(
                    """
                    SELECT id, document_type, source_filename, document_date, processing_status, raw_json, file_bytes
                    FROM hh_ap_documents
                    WHERE entity_id = :entity_id
                      AND document_type = ANY(:allowed_document_types)
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"entity_id": entity["id"], "allowed_document_types": list(allowed_document_types)},
            ).mappings().first()

        if not document:
            raise HTTPException(status_code=404, detail="No HH remittance document found for this entity")
        if not document["file_bytes"]:
            raise HTTPException(status_code=400, detail="This HH remittance document has no stored file bytes to parse")

        parsed = parse_hh_remittance_document(file_bytes=document["file_bytes"], source_filename=document["source_filename"])

        existing_remittance = session.execute(
            text(
                """
                SELECT id
                FROM hh_ap_remittances
                WHERE entity_id = :entity_id
                  AND (
                        document_id = :document_id
                        OR (
                            COALESCE(remittance_reference, '') = COALESCE(:remittance_reference, '')
                            AND withdrawal_date IS NOT DISTINCT FROM :withdrawal_date
                        )
                  )
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "document_id": document["id"],
                "remittance_reference": parsed["remittance_reference"],
                "withdrawal_date": parsed["withdrawal_date"],
            },
        ).mappings().first()

        if existing_remittance:
            remittance_row = session.execute(
                text(
                    """
                    UPDATE hh_ap_remittances
                    SET document_id = :document_id,
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
                    "id": existing_remittance["id"],
                    "document_id": document["id"],
                    "remittance_reference": parsed["remittance_reference"],
                    "remittance_date": parsed["remittance_date"],
                    "withdrawal_date": parsed["withdrawal_date"],
                    "total_amount": parsed["total_amount"],
                    "raw_json": json_dumps(parsed["raw_json"]),
                },
            ).mappings().first()
        else:
            remittance_row = session.execute(
                text(
                    """
                    INSERT INTO hh_ap_remittances (
                        entity_id, document_id, remittance_reference, remittance_date, withdrawal_date, total_amount, raw_json
                    ) VALUES (
                        :entity_id, :document_id, :remittance_reference, :remittance_date, :withdrawal_date, :total_amount, CAST(:raw_json AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "document_id": document["id"],
                    "remittance_reference": parsed["remittance_reference"],
                    "remittance_date": parsed["remittance_date"],
                    "withdrawal_date": parsed["withdrawal_date"],
                    "total_amount": parsed["total_amount"],
                    "raw_json": json_dumps(parsed["raw_json"]),
                },
            ).mappings().first()

        session.execute(text("DELETE FROM hh_ap_remittance_lines WHERE remittance_id = :remittance_id"), {"remittance_id": remittance_row["id"]})

        inserted_lines = 0
        for line in parsed["lines"]:
            session.execute(
                text(
                    """
                    INSERT INTO hh_ap_remittance_lines (
                        remittance_id, entity_id, invoice_number, line_description, due_date, line_amount,
                        matched_invoice_id, match_status, raw_json
                    ) VALUES (
                        :remittance_id, :entity_id, :invoice_number, :line_description, :due_date, :line_amount,
                        NULL, :match_status, CAST(:raw_json AS jsonb)
                    )
                    """
                ),
                {
                    "remittance_id": remittance_row["id"],
                    "entity_id": entity["id"],
                    "invoice_number": normalize_invoice_number(line["invoice_number"]),
                    "line_description": normalize_text(line["line_description"]),
                    "due_date": line["due_date"],
                    "line_amount": line["line_amount"],
                    "match_status": MATCH_STATUS_UNMATCHED,
                    "raw_json": json_dumps(line["raw_json"]),
                },
            )
            inserted_lines += 1

        session.execute(
            text(
                """
                UPDATE hh_ap_documents
                SET processing_status = :processing_status,
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || CAST(:parser_json AS jsonb),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "id": document["id"],
                "processing_status": PROCESSING_STATUS_PARSED_REMITTANCE,
                "parser_json": json_dumps({
                    "parsed_as": parsed["document_type"],
                    "parsed_remittance_reference": parsed["remittance_reference"],
                    "parsed_remittance_line_count": inserted_lines,
                }),
            },
        )

        return {
            "entity_code": entity["entity_code"],
            "document_id": str(document["id"]),
            "remittance_id": str(remittance_row["id"]),
            "remittance_reference": parsed["remittance_reference"],
            "remittance_date": str(parsed["remittance_date"]) if parsed["remittance_date"] else None,
            "withdrawal_date": str(parsed["withdrawal_date"]) if parsed["withdrawal_date"] else None,
            "total_amount": money_float(parsed["total_amount"]),
            "remittance_line_count": inserted_lines,
            "raw_json": parsed["raw_json"],
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

        invoice_map_by_number = build_invoice_map(invoice_rows)

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

        statement_rows = session.execute(text(statement_scope_sql), statement_scope_params).mappings().all()

        matched_invoice_ids: set[str] = set()
        matched_statement_count = 0
        missing_download_count = 0

        for row in statement_rows:
            invoice_number = normalize_invoice_number(row["invoice_number"])
            invoice_type = normalize_text(row["invoice_type"])
            candidates = invoice_map_by_number.get(invoice_number or "", [])
            matched_invoice = choose_invoice_match_candidate(candidates, invoice_type)

            if matched_invoice:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_statement_lines
                        SET matched_invoice_id = :matched_invoice_id,
                            match_status = :match_status,
                            is_missing_download = FALSE,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"], "matched_invoice_id": matched_invoice["id"], "match_status": MATCH_STATUS_MATCHED},
                )
                matched_invoice_ids.add(str(matched_invoice["id"]))
                matched_statement_count += 1
            else:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_statement_lines
                        SET matched_invoice_id = NULL,
                            match_status = :match_status,
                            is_missing_download = TRUE,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"], "match_status": MATCH_STATUS_MISSING_DOWNLOAD},
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
            matched_invoice = choose_invoice_match_candidate(candidates, None)

            if matched_invoice:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_remittance_lines
                        SET matched_invoice_id = :matched_invoice_id,
                            match_status = :match_status,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"], "matched_invoice_id": matched_invoice["id"], "match_status": MATCH_STATUS_MATCHED},
                )
                matched_invoice_ids.add(str(matched_invoice["id"]))
                matched_remittance_count += 1
            else:
                session.execute(
                    text(
                        """
                        UPDATE hh_ap_remittance_lines
                        SET matched_invoice_id = NULL,
                            match_status = :match_status,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"], "match_status": MATCH_STATUS_UNMATCHED},
                )
                unmatched_remittance_count += 1

        if matched_invoice_ids:
            session.execute(
                text(
                    """
                    UPDATE hh_ap_invoices
                    SET match_status = CASE
                        WHEN id::text = ANY(:matched_invoice_ids) THEN :matched_status
                        WHEN is_statement_only = TRUE THEN :statement_only_status
                        ELSE :unmatched_status
                    END,
                    updated_at = NOW()
                    WHERE entity_id = :entity_id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "matched_invoice_ids": list(matched_invoice_ids),
                    "matched_status": MATCH_STATUS_MATCHED,
                    "statement_only_status": MATCH_STATUS_STATEMENT_ONLY,
                    "unmatched_status": MATCH_STATUS_UNMATCHED,
                },
            )
        else:
            session.execute(
                text(
                    """
                    UPDATE hh_ap_invoices
                    SET match_status = CASE
                        WHEN is_statement_only = TRUE THEN :statement_only_status
                        ELSE :unmatched_status
                    END,
                    updated_at = NOW()
                    WHERE entity_id = :entity_id
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "statement_only_status": MATCH_STATUS_STATEMENT_ONLY,
                    "unmatched_status": MATCH_STATUS_UNMATCHED,
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
                SELECT COUNT(*) AS remittance_count
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
                SELECT COUNT(*) AS statement_count
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

        latest_statement = get_statement_by_month_end(session=session, entity_id=entity["id"], statement_month_end=None)

        latest_documents = session.execute(
            text(
                """
                SELECT id, document_type, source_filename, document_date, upload_source, processing_status, created_at
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
            "document_counts_by_type": [{"document_type": row["document_type"], "count": int(row["doc_count"])} for row in document_type_counts],
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
                SELECT invoice_number, invoice_type, vendor_name, invoice_date, due_date, total_amount, match_status, notes
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id AND is_statement_only = TRUE
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        missing_download_statement_lines = session.execute(
            text(
                """
                SELECT invoice_number, invoice_type, invoice_date, due_date, invoice_amount, open_amount, current_amount, past_due_amount, match_status
                FROM hh_ap_statement_lines
                WHERE entity_id = :entity_id AND is_missing_download = TRUE
                ORDER BY invoice_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        unmatched_remittance_lines = session.execute(
            text(
                """
                SELECT id, invoice_number, line_description, due_date, line_amount, match_status, remittance_id
                FROM hh_ap_remittance_lines
                WHERE entity_id = :entity_id AND match_status <> 'matched'
                ORDER BY due_date DESC NULLS LAST, invoice_number
                LIMIT 100
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        unmatched_invoices = session.execute(
            text(
                """
                SELECT invoice_number, invoice_type, vendor_name, invoice_date, due_date, total_amount, match_status, is_statement_only
                FROM hh_ap_invoices
                WHERE entity_id = :entity_id AND match_status <> 'matched'
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
        statement = get_statement_by_month_end(session=session, entity_id=entity["id"], statement_month_end=statement_month_end)

        if not statement:
            raise HTTPException(status_code=404, detail="No HH AP statement found for this entity and month_end")

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
                JOIN hh_ap_invoices i ON i.id = sl.matched_invoice_id
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
                JOIN hh_ap_invoices i ON i.id = sl.matched_invoice_id
                JOIN hh_ap_remittance_lines rl ON rl.matched_invoice_id = i.id
                WHERE sl.statement_id = :statement_id
                """
            ),
            {"statement_id": statement["id"]},
        ).mappings().first()

        sample_statement_lines = session.execute(
            text(
                """
                SELECT invoice_number, invoice_type, invoice_date, due_date, invoice_amount, open_amount, current_amount, past_due_amount, match_status, is_missing_download
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
