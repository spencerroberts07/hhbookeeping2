"""
PDF bank-statement importer (TD Canada Trust format) — service layer.

Why this file exists:
    TD Canada Trust does not provide CSV statements for our business
    chequing/LOC account. The only statement format available is the
    monthly PDF. This module parses that PDF into bank_transactions
    rows using the same idempotent SHA-256 hash pattern as
    services_bank_csv so re-uploading the same statement results in
    zero new rows (only last_seen_at refresh).

source_system value for PDF-imported rows: 'statement_pdf'

Direction classification is rule-based on the description prefix.
Patterns we don't recognise default to 'unknown'; the bank-review UI
lets the bookkeeper correct those manually.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
from collections import defaultdict
from datetime import date
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
from .services_period_close import (
    PeriodLockedError,
    is_date_in_locked_period,
)


SOURCE_SYSTEM = "statement_pdf"
DEFAULT_BANK_ACCOUNT_CODE = "1020"
DEFAULT_BANK_ACCOUNT_NAME = "TD Canada Trust"


# ----------------------------------------------------------------------
# Description pattern → direction & transaction_type classifier
# ----------------------------------------------------------------------
#
# Order matters: more specific patterns must come BEFORE the generic
# ones. (e.g. "VSA FEE" before "VSA DEP".)
# ----------------------------------------------------------------------


_CLASSIFIER_RULES: list[tuple[re.Pattern[str], str, str]] = [
    # (compiled regex on UPPERCASED description, direction, transaction_type)
    #
    # Note: pypdf often concatenates the merchant id directly onto FEE/DEP
    # (e.g. "AMX FEE12593422", "VSA DEP14350"). Patterns must NOT require
    # a trailing word-boundary because between 'E' and '1' there is none.
    (re.compile(r"\bAMX\s*FEE"), "outflow", "amex_fee"),
    (re.compile(r"\bAMEX\s+\d+"), "inflow", "amex_settlement"),
    (re.compile(r"\bCASH\s+DEP\s*FEE"), "outflow", "cash_deposit_fee"),
    (re.compile(r"\bITEMS\s+DEP\s*FEE"), "outflow", "items_deposit_fee"),
    (re.compile(r"\bVSA\s*FEE"), "outflow", "visa_fee"),
    (re.compile(r"\bVSA\s*DEP"), "inflow", "visa_settlement"),
    (re.compile(r"\bMC\s*FEE"), "outflow", "mc_fee"),
    (re.compile(r"\bMC\s*DEP"), "inflow", "mc_settlement"),
    (re.compile(r"\bINT\s*FEE"), "outflow", "interchange_fee"),
    (re.compile(r"\bMON\s*FEE"), "outflow", "monthly_fee"),
    (re.compile(r"\bTAX\s*PYT\s*FEE"), "outflow", "tax_payment_fee"),
    (re.compile(r"\bSEND\s+E-?TFR\s+FEE"), "outflow", "etfr_fee"),
    (re.compile(r"\bSEND\s+E-?TFR"), "outflow", "etfr_outgoing"),
    (re.compile(r"\bE-?TRANSFER\b.*\bFEE"), "outflow", "etfr_fee"),
    (re.compile(r"\bE-?TRANSFER"), "unknown", "etfr"),
    (re.compile(r"\bGC\s*\d+-DEPOSIT", re.IGNORECASE), "inflow", "gift_card_deposit"),
    (re.compile(r"\bGLR\b"), "inflow", "gc_redemption_deposit"),
    (re.compile(r"\bEF\d{4}"), "inflow", "eft_deposit"),
    (re.compile(r"\bTD\s+EXPRESS\s+DEPOSIT"), "inflow", "td_express_deposit"),
    (re.compile(r"\bINTUIT\b"), "outflow", "intuit_fee"),
    (re.compile(r"\bENET\s+EMPLOYER\b", re.IGNORECASE), "outflow", "payroll_withdrawal"),
    (re.compile(r"\bHOME\s+HARDWARE\s+AP\b"), "outflow", "hh_ap_payment"),
    (re.compile(r"\bHOME\s+HARDWARE\s+MSP\b"), "outflow", "hh_msp_remittance"),
    (re.compile(r"\bOVERDRAFT\s+INTEREST\b"), "outflow", "overdraft_interest"),
    (re.compile(r"\bSERVICE\s+CHARGE\b"), "outflow", "service_charge"),
    (re.compile(r"\bGST\b.*\bGST\b|\bGST\d+\b"), "outflow", "gst_remittance"),
    (re.compile(r"\bLN\s*PYMT\b"), "outflow", "loan_principal_payment"),
    (re.compile(r"\bD/?L\s*INT\b"), "outflow", "loan_interest"),
    (re.compile(r"\bCHQ#\d+"), "outflow", "cheque"),
]

_INFLOW_FALLBACK_HINTS = (
    "DEPOSIT",
    "DEP ",
    "DEP-",
    "REFUND",
    "REIMB",
)


def _classify(description: str) -> tuple[str, str]:
    """Returns (direction, transaction_type)."""
    desc_upper = (description or "").upper()
    for pattern, direction, txn_type in _CLASSIFIER_RULES:
        if pattern.search(desc_upper):
            return direction, txn_type
    # Last-ditch: a generic "DEPOSIT" hint
    for hint in _INFLOW_FALLBACK_HINTS:
        if hint in desc_upper:
            return "inflow", "deposit_other"
    return "unknown", "pdf_line"


# ----------------------------------------------------------------------
# Decimal / date helpers
# ----------------------------------------------------------------------


_RE_AMOUNT = re.compile(r"\d{1,3}(?:,\d{3})*\.\d{2}")
_RE_BAL_TOKEN = re.compile(r"^(\d{1,3}(?:,\d{3})*\.\d{2})(OD)?$")
_RE_DATE_TOKEN = re.compile(r"^([A-Z]{3})(\d{1,2})$")
_RE_PERIOD = re.compile(
    r"([A-Z]{3})\s+(\d{1,2})/(\d{2})\s*-\s*([A-Z]{3})\s+(\d{1,2})/(\d{2})",
    re.IGNORECASE,
)
_RE_ACCOUNT = re.compile(r"\b(\d{4})-(\d{7,})\b")
_RE_PAGE_NUMBER = re.compile(r"Page\s+(\d+)\s+of\s+(\d+)", re.IGNORECASE)

_MONTH_TO_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _to_decimal(token: str) -> Decimal:
    try:
        return Decimal(token.replace(",", ""))
    except (InvalidOperation, AttributeError):
        return Decimal("0")


def _normalize_description(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().upper())


# ----------------------------------------------------------------------
# PDF parser
# ----------------------------------------------------------------------


def _extract_pdf_pages(file_bytes: bytes) -> list[str]:
    try:
        from pypdf import PdfReader  # noqa: WPS433
    except ImportError as exc:
        raise ValueError(
            "pypdf is required to parse bank statement PDFs. "
            "Install it: pip install pypdf"
        ) from exc

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f"Could not open PDF: {exc}") from exc

    return [p.extract_text() or "" for p in reader.pages]


def _parse_period_from_text(text_block: str) -> tuple[date | None, date | None]:
    m = _RE_PERIOD.search(text_block)
    if not m:
        return None, None
    s_mon, s_day, s_yy, e_mon, e_day, e_yy = m.groups()
    s_month = _MONTH_TO_NUM.get(s_mon.upper())
    e_month = _MONTH_TO_NUM.get(e_mon.upper())
    if not s_month or not e_month:
        return None, None
    try:
        return (
            date(2000 + int(s_yy), s_month, int(s_day)),
            date(2000 + int(e_yy), e_month, int(e_day)),
        )
    except ValueError:
        return None, None


def _parse_account_from_text(text_block: str) -> tuple[str | None, str | None]:
    m = _RE_ACCOUNT.search(text_block)
    if not m:
        return None, None
    return m.group(1), m.group(0)


def _date_for_token(
    mon_num: int, day: int, period_start: date | None, period_end: date | None
) -> date | None:
    """Pick the right calendar year given a (month, day) token and the
    statement period. Statements that span a year boundary use the
    period_end year for tokens whose month is past period_start's month."""
    if not period_start or not period_end:
        return None
    if period_start.year == period_end.year:
        try:
            return date(period_start.year, mon_num, day)
        except ValueError:
            return None
    # Year wrap (rare for monthly statements, but be safe)
    cand_start = None
    cand_end = None
    try:
        cand_start = date(period_start.year, mon_num, day)
    except ValueError:
        pass
    try:
        cand_end = date(period_end.year, mon_num, day)
    except ValueError:
        pass
    # Prefer whichever falls inside [period_start, period_end]
    for cand in (cand_start, cand_end):
        if cand is not None and period_start <= cand <= period_end:
            return cand
    return cand_end or cand_start


def _parse_page_summary(text_block: str) -> dict[str, Any]:
    """Pulls 'Credits N $X / Debits M $Y' off a page so we can validate
    later, and the Page X of Y indicator."""
    summary: dict[str, Any] = {}
    m_pg = _RE_PAGE_NUMBER.search(text_block)
    if m_pg:
        summary["page_number"] = int(m_pg.group(1))
        summary["page_count"] = int(m_pg.group(2))

    # Look for "Credits<N><AMOUNT>" / "Debits<N><AMOUNT>" in any whitespace form.
    m_cr = re.search(
        r"Credits?\s*(\d+)\s+(\d{1,3}(?:,\d{3})*\.\d{2})", text_block, re.IGNORECASE
    )
    if m_cr:
        summary["credits_count"] = int(m_cr.group(1))
        summary["credits_amount"] = str(_to_decimal(m_cr.group(2)))
    m_db = re.search(
        r"Debits?\s*(\d+)\s+(\d{1,3}(?:,\d{3})*\.\d{2})", text_block, re.IGNORECASE
    )
    if m_db:
        summary["debits_count"] = int(m_db.group(1))
        summary["debits_amount"] = str(_to_decimal(m_db.group(2)))
    return summary


def _is_balance_forward(line: str) -> bool:
    return bool(re.match(r"^\s*BALANCE\s+FORWARD\b", line, re.IGNORECASE))


def parse_td_statement_pdf(file_bytes: bytes) -> dict[str, Any]:
    """
    Parse a TD Canada Trust business statement PDF.

    Returns:
        {
            "account_branch": "1020",
            "account_no": "1020-5660371",
            "period_start": date,
            "period_end": date,
            "page_count": int,
            "warnings": [str, ...],
            "transactions": [
                {
                    "page_number": int,
                    "row_number": int,           # 1-based across whole statement
                    "description": str,
                    "amount": Decimal,           # signed
                    "direction": "inflow" | "outflow" | "unknown",
                    "transaction_type": str,
                    "transaction_date": date | None,
                    "running_balance": Decimal | None,
                    "running_balance_is_overdraft": bool,
                },
                ...
            ],
        }
    """
    pages = _extract_pdf_pages(file_bytes)
    if not pages:
        return {
            "account_branch": None,
            "account_no": None,
            "period_start": None,
            "period_end": None,
            "page_count": 0,
            "warnings": ["PDF has no pages"],
            "transactions": [],
        }

    # Header info from page 1
    first_page = pages[0]
    period_start, period_end = _parse_period_from_text(first_page)
    branch, account_no = _parse_account_from_text(first_page)

    warnings: list[str] = []
    if not period_start or not period_end:
        warnings.append("Could not parse statement period from page 1")
    if not account_no:
        warnings.append("Could not parse account number from page 1")

    transactions: list[dict[str, Any]] = []
    row_counter = 0

    for page_idx, page_text in enumerate(pages):
        page_no = page_idx + 1
        for raw_line in page_text.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                continue
            if _is_balance_forward(line):
                continue

            # Find the date token (pattern like 'FEB02') from the right side.
            tokens = line.split()
            if len(tokens) < 2:
                continue

            date_idx = None
            for i in range(len(tokens) - 1, -1, -1):
                if _RE_DATE_TOKEN.match(tokens[i]):
                    date_idx = i
                    break
            if date_idx is None:
                continue

            mtok = _RE_DATE_TOKEN.match(tokens[date_idx])
            if not mtok:
                continue
            mon_str = mtok.group(1).upper()
            mon_num = _MONTH_TO_NUM.get(mon_str)
            if not mon_num:
                continue
            try:
                day_num = int(mtok.group(2))
            except ValueError:
                continue
            txn_date = _date_for_token(
                mon_num, day_num, period_start, period_end
            )

            # Optional balance after the date
            running_bal: Decimal | None = None
            is_od = False
            if date_idx + 1 < len(tokens):
                m_bal = _RE_BAL_TOKEN.match(tokens[date_idx + 1])
                if m_bal:
                    running_bal = _to_decimal(m_bal.group(1))
                    is_od = bool(m_bal.group(2))

            # Amount = the last money-shaped token BEFORE the date.
            amount: Decimal | None = None
            amount_idx = None
            for i in range(date_idx - 1, -1, -1):
                if _RE_AMOUNT.fullmatch(tokens[i]):
                    amount = _to_decimal(tokens[i])
                    amount_idx = i
                    break
            if amount is None or amount == 0:
                # Could be the page-summary "Credits 22 28,802.27" line or
                # similar header noise — skip.
                continue

            description = " ".join(tokens[:amount_idx]).strip()
            if not description:
                continue
            # Skip rows that look like the column-summary or stat lines.
            if re.search(r"^(CREDITS?|DEBITS?|MONTHLY|DEP CONTENT|TD BUSINESS)\b",
                         description, re.IGNORECASE):
                continue

            direction, txn_type = _classify(description)
            if direction == "inflow":
                signed_amount = amount
            elif direction == "outflow":
                signed_amount = -amount
            else:
                # Unknown direction → store as positive magnitude with
                # direction='unknown'; the review UI lets the user fix it.
                signed_amount = amount

            row_counter += 1
            transactions.append(
                {
                    "page_number": page_no,
                    "row_number": row_counter,
                    "description": description,
                    "amount": signed_amount,
                    "amount_magnitude": amount,
                    "direction": direction,
                    "transaction_type": txn_type,
                    "transaction_date": txn_date,
                    "running_balance": running_bal,
                    "running_balance_is_overdraft": is_od,
                }
            )

    return {
        "account_branch": branch,
        "account_no": account_no,
        "period_start": period_start,
        "period_end": period_end,
        "page_count": len(pages),
        "warnings": warnings,
        "transactions": transactions,
    }


# ----------------------------------------------------------------------
# Deterministic per-row source_transaction_id
# ----------------------------------------------------------------------


def _row_signature(
    source_account_code: str | None,
    txn: dict[str, Any],
    occurrence_index: int,
) -> str:
    txn_date = txn["transaction_date"]
    amount = txn.get("amount") or Decimal("0")
    desc_norm = _normalize_description(txn.get("description"))
    cents = int((amount * 100).quantize(Decimal("1")))
    payload = "|".join(
        [
            (source_account_code or "").strip().upper(),
            txn_date.isoformat() if txn_date else "",
            str(cents),
            desc_norm,
            str(occurrence_index),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _assign_occurrence_indexes(
    transactions: list[dict[str, Any]],
    source_account_code: str | None,
) -> None:
    counter: dict[tuple[str, str, int, str], int] = defaultdict(int)
    for txn in transactions:
        if not txn.get("transaction_date") or not txn.get("amount"):
            txn["occurrence_index"] = None
            txn["source_transaction_id"] = None
            continue
        amount = txn["amount"] or Decimal("0")
        cents = int((amount * 100).quantize(Decimal("1")))
        key = (
            (source_account_code or "").strip().upper(),
            txn["transaction_date"].isoformat(),
            cents,
            _normalize_description(txn.get("description")),
        )
        idx = counter[key]
        counter[key] += 1
        txn["occurrence_index"] = idx
        txn["source_transaction_id"] = _row_signature(
            source_account_code, txn, idx
        )


# ----------------------------------------------------------------------
# Preview + import
# ----------------------------------------------------------------------


def preview_bank_pdf_import(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    source_account_code: str | None,
    source_account_name: str | None,
    sample_limit: int = 25,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    parsed = parse_td_statement_pdf(file_bytes)
    txns = parsed["transactions"]
    _assign_occurrence_indexes(
        txns, source_account_code or DEFAULT_BANK_ACCOUNT_CODE
    )

    valid = [t for t in txns if t.get("source_transaction_id")]
    earliest = min(
        (t["transaction_date"] for t in valid if t["transaction_date"]),
        default=None,
    )
    latest = max(
        (t["transaction_date"] for t in valid if t["transaction_date"]),
        default=None,
    )

    inflow_count = sum(1 for t in valid if t["direction"] == "inflow")
    outflow_count = sum(1 for t in valid if t["direction"] == "outflow")
    unknown_count = sum(1 for t in valid if t["direction"] == "unknown")
    total_inflow = sum(
        (t["amount_magnitude"] for t in valid if t["direction"] == "inflow"),
        Decimal("0"),
    )
    total_outflow = sum(
        (t["amount_magnitude"] for t in valid if t["direction"] == "outflow"),
        Decimal("0"),
    )

    would_insert = 0
    would_dedup = 0
    if valid:
        ids = [t["source_transaction_id"] for t in valid]
        existing = session.execute(
            text(
                """
                SELECT source_transaction_id
                FROM bank_transactions
                WHERE entity_id = :entity_id
                  AND source_system = :source_system
                  AND source_transaction_id = ANY(:ids)
                """
            ),
            {
                "entity_id": entity["id"],
                "source_system": SOURCE_SYSTEM,
                "ids": ids,
            },
        ).mappings().all()
        existing_ids = {r["source_transaction_id"] for r in existing}
        for t in valid:
            if t["source_transaction_id"] in existing_ids:
                would_dedup += 1
            else:
                would_insert += 1

    sample = []
    for t in txns[:sample_limit]:
        sample.append(
            {
                "page_number": t["page_number"],
                "row_number": t["row_number"],
                "description": t["description"],
                "amount": str(t["amount"]) if t.get("amount") is not None else None,
                "direction": t["direction"],
                "transaction_type": t["transaction_type"],
                "transaction_date": (
                    t["transaction_date"].isoformat() if t["transaction_date"] else None
                ),
                "running_balance": (
                    str(t["running_balance"]) if t.get("running_balance") is not None else None
                ),
                "running_balance_is_overdraft": t.get(
                    "running_balance_is_overdraft", False
                ),
            }
        )

    return {
        "entity_code": entity_code,
        "file_name": file_name,
        "source_account_code": source_account_code or DEFAULT_BANK_ACCOUNT_CODE,
        "source_account_name": source_account_name or DEFAULT_BANK_ACCOUNT_NAME,
        "account_branch": parsed["account_branch"],
        "account_no": parsed["account_no"],
        "period_start": parsed["period_start"].isoformat() if parsed["period_start"] else None,
        "period_end": parsed["period_end"].isoformat() if parsed["period_end"] else None,
        "page_count": parsed["page_count"],
        "warnings": parsed["warnings"],
        "total_row_count": len(txns),
        "valid_row_count": len(valid),
        "inflow_count": inflow_count,
        "outflow_count": outflow_count,
        "unknown_count": unknown_count,
        "total_inflow": str(total_inflow),
        "total_outflow": str(total_outflow),
        "earliest_transaction_date": earliest.isoformat() if earliest else None,
        "latest_transaction_date": latest.isoformat() if latest else None,
        "would_insert_count": would_insert,
        "would_dedup_count": would_dedup,
        "sample": sample,
    }


def run_bank_pdf_import(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    source_account_code: str | None,
    source_account_name: str | None,
    actor_email: str,
    note: str | None = None,
    run_auto_match_after: bool = True,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    src_code = source_account_code or DEFAULT_BANK_ACCOUNT_CODE
    src_name = source_account_name or DEFAULT_BANK_ACCOUNT_NAME

    parsed = parse_td_statement_pdf(file_bytes)
    txns = parsed["transactions"]
    _assign_occurrence_indexes(txns, src_code)

    valid = [t for t in txns if t.get("source_transaction_id")]

    earliest = min(
        (t["transaction_date"] for t in valid if t["transaction_date"]),
        default=None,
    )
    latest = max(
        (t["transaction_date"] for t in valid if t["transaction_date"]),
        default=None,
    )

    # Period-lock guard — fetch all closed/locked periods that overlap
    # the import range in ONE query, then check each row in-memory.
    # Doing N round-trips per row would deadline-out on Render Postgres
    # for 100+ rows.
    locked_periods_seen: dict[str, dict[str, Any]] = {}
    if earliest and latest:
        locked_rows = session.execute(
            text(
                """
                SELECT id, period_label, period_start, period_end, status
                FROM accounting_periods
                WHERE entity_id = :entity_id
                  AND status IN ('closed_locked', 'closed', 'locked')
                  AND period_start <= :latest
                  AND period_end >= :earliest
                """
            ),
            {
                "entity_id": entity["id"],
                "earliest": earliest,
                "latest": latest,
            },
        ).mappings().all()
        for period in locked_rows:
            for t in valid:
                d = t["transaction_date"]
                if d and period["period_start"] <= d <= period["period_end"]:
                    locked_periods_seen[str(period["id"])] = {
                        "period_label": period.get("period_label"),
                        "period_end": (
                            period["period_end"].isoformat() if period.get("period_end") else None
                        ),
                        "status": period.get("status"),
                    }
                    break
    if locked_periods_seen:
        raise PeriodLockedError(
            next(iter(locked_periods_seen.values())),
            message=(
                "Bank PDF import refused: one or more rows fall in a "
                "closed_locked accounting period. "
                f"Locked periods affected: {list(locked_periods_seen.values())}. "
                "Reopen the period(s) before importing."
            ),
        )

    # Pre-fetch the accounting_periods that overlap the import range so
    # we can resolve accounting_period_id per row without N round-trips.
    period_lookup_rows = session.execute(
        text(
            """
            SELECT id, period_start, period_end
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND period_start <= :latest
              AND period_end >= :earliest
            """
        ),
        {
            "entity_id": entity["id"],
            "earliest": earliest,
            "latest": latest,
        },
    ).mappings().all() if earliest and latest else []

    def _resolve_period(d: date | None) -> Any:
        if d is None:
            return None
        for p in period_lookup_rows:
            if p["period_start"] <= d <= p["period_end"]:
                return p["id"]
        return None

    # Pre-fetch existing source_transaction_ids in ONE query so we don't
    # round-trip per row.
    existing_ids: dict[str, Any] = {}
    if valid:
        ids = [t["source_transaction_id"] for t in valid]
        rows = session.execute(
            text(
                """
                SELECT id, source_transaction_id
                FROM bank_transactions
                WHERE entity_id = :entity_id
                  AND source_system = :source_system
                  AND source_transaction_id = ANY(:ids)
                """
            ),
            {
                "entity_id": entity["id"],
                "source_system": SOURCE_SYSTEM,
                "ids": ids,
            },
        ).mappings().all()
        existing_ids = {r["source_transaction_id"]: r["id"] for r in rows}

    inserted = 0
    duplicates = 0
    for t in valid:
        accounting_period_id = _resolve_period(t["transaction_date"])

        if t["source_transaction_id"] in existing_ids:
            duplicates += 1
            session.execute(
                text("UPDATE bank_transactions SET last_seen_at = NOW() WHERE id = :id"),
                {"id": existing_ids[t["source_transaction_id"]]},
            )
            continue

        raw_payload = {
            "page_number": t["page_number"],
            "row_number": t["row_number"],
            "running_balance": (
                str(t["running_balance"]) if t.get("running_balance") is not None else None
            ),
            "running_balance_is_overdraft": t.get(
                "running_balance_is_overdraft", False
            ),
            "occurrence_index": t["occurrence_index"],
            "import_file_name": file_name,
            "import_actor_email": actor_email,
            "import_note": note,
            "auto_classified_type": t["transaction_type"],
        }

        session.execute(
            text(
                """
                INSERT INTO bank_transactions (
                    entity_id, accounting_period_id, source_system, source_connection_id,
                    source_account_id, source_account_name, source_account_code,
                    source_transaction_id, source_transaction_type,
                    transaction_date, posted_date,
                    description, normalized_description,
                    counterparty_name, reference_number,
                    amount, currency_code, direction,
                    review_status, raw_json
                )
                VALUES (
                    :entity_id, :accounting_period_id, :source_system, NULL,
                    NULL, :source_account_name, :source_account_code,
                    :source_transaction_id, :source_transaction_type,
                    :transaction_date, :transaction_date,
                    :description, :normalized_description,
                    NULL, NULL,
                    :amount, 'CAD', :direction,
                    'new', CAST(:raw_json AS jsonb)
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "source_system": SOURCE_SYSTEM,
                "source_account_name": src_name,
                "source_account_code": src_code,
                "source_transaction_id": t["source_transaction_id"],
                "source_transaction_type": t["transaction_type"],
                "transaction_date": t["transaction_date"],
                "description": (t["description"] or "")[:500],
                "normalized_description": _normalize_description(t["description"])[:500],
                "amount": t["amount"],
                "direction": t["direction"],
                "raw_json": json.dumps(raw_payload, default=str),
            },
        )
        inserted += 1

    auto_match_summary: dict[str, Any] | None = None
    if run_auto_match_after and inserted > 0 and earliest and latest:
        from .services_auto_match import TRIGGER_CSV_IMPORT, run_auto_match  # noqa: WPS433

        try:
            auto_match_summary = run_auto_match(
                session=session,
                entity_code=entity_code,
                period_start=earliest,
                period_end=latest,
                actor_email=actor_email,
                triggered_by=TRIGGER_CSV_IMPORT,
                trigger_source_id=None,
            )
        except Exception as exc:
            auto_match_summary = {"error": str(exc), "status": "failed"}

    return {
        "entity_code": entity_code,
        "file_name": file_name,
        "source_account_code": src_code,
        "source_account_name": src_name,
        "account_no": parsed["account_no"],
        "period_start": parsed["period_start"].isoformat() if parsed["period_start"] else None,
        "period_end": parsed["period_end"].isoformat() if parsed["period_end"] else None,
        "page_count": parsed["page_count"],
        "total_row_count": len(txns),
        "valid_row_count": len(valid),
        "inserted_count": inserted,
        "duplicate_count": duplicates,
        "earliest_transaction_date": earliest.isoformat() if earliest else None,
        "latest_transaction_date": latest.isoformat() if latest else None,
        "warnings": parsed["warnings"],
        "auto_match": auto_match_summary,
    }
