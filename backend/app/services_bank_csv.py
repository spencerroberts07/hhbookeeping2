"""
Bank CSV upload fallback module — service layer.

Why this file exists separately from services.py:
    services.py is already ~3.8k lines. New modules go in their own
    services_<module>.py file so the existing file stays stable and the
    new module is easy to reason about.

Public functions used by the route layer:
    - list_bank_csv_mapping_profiles()
    - preview_bank_csv_import(...)
    - run_bank_csv_import(...)
    - list_bank_csv_import_runs(...)
    - get_bank_csv_import_run_detail(...)

Conventions kept consistent with the rest of the app:
    - source_system value for CSV-imported rows = 'statement_csv'
    - amount sign:
          outflow / withdrawal -> negative amount, direction='outflow'
          inflow  / deposit    -> positive amount, direction='inflow'
    - source_transaction_id is a deterministic SHA-256 hash so
      re-importing the same file (or an overlapping bank export) is
      idempotent: rows already in bank_transactions are detected and
      counted as duplicates, never inserted twice.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import defaultdict
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
from .services_period_close import (
    PeriodLockedError,
    is_date_in_locked_period,
)


SOURCE_SYSTEM = "statement_csv"


# ----------------------------------------------------------------------
# Mapping profiles
# ----------------------------------------------------------------------
#
# A "profile" is a small dict describing how to read a CSV.
# Keys:
#   label         - human description
#   has_header    - True if the first non-skipped row is a header row
#   delimiter     - CSV delimiter
#   skip_rows     - how many leading rows to discard before reading
#   columns       - dict that says where each logical field comes from.
#                   When has_header=True, values are column header names.
#                   When has_header=False, values are 0-based column indices.
#   date_formats  - list of strptime patterns to try for transaction_date
#
# Logical fields the importer understands:
#   transaction_date   - REQUIRED
#   description        - REQUIRED (can be combined from description + description_2)
#   description_2      - OPTIONAL (appended to description with " | ")
#   amount             - signed amount (positive=inflow, negative=outflow). Use
#                        this OR (withdrawal + deposit), not both.
#   withdrawal         - debit / outflow column (positive number, will be negated)
#   deposit            - credit / inflow column (positive number)
#   reference_number   - OPTIONAL (cheque number, confirmation number)
#   posted_date        - OPTIONAL (defaults to transaction_date)
#
# Profiles can be overridden at upload time by supplying column_map_json.
# ----------------------------------------------------------------------

MAPPING_PROFILES: dict[str, dict[str, Any]] = {
    "generic": {
        "label": "Generic CSV — supply column_map_json to map columns",
        "has_header": True,
        "delimiter": ",",
        "skip_rows": 0,
        "columns": {},
        "date_formats": [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y%m%d",
            "%m-%d-%Y",
            "%d-%m-%Y",
        ],
    },
    "td_personal_chequing": {
        "label": "TD Canada Trust personal chequing CSV (no header row)",
        "has_header": False,
        "delimiter": ",",
        "skip_rows": 0,
        "columns": {
            "transaction_date": 0,
            "description": 1,
            "withdrawal": 2,
            "deposit": 3,
        },
        "date_formats": ["%m/%d/%Y", "%Y-%m-%d"],
    },
    "rbc_personal": {
        "label": "RBC personal banking CSV (with header)",
        "has_header": True,
        "delimiter": ",",
        "skip_rows": 0,
        "columns": {
            "transaction_date": "Transaction Date",
            "description": "Description 1",
            "description_2": "Description 2",
            "amount": "CAD$",
            "reference_number": "Cheque Number",
        },
        "date_formats": ["%m/%d/%Y", "%Y-%m-%d"],
    },
    "scotia_personal": {
        "label": "Scotia personal account CSV (with header)",
        "has_header": True,
        "delimiter": ",",
        "skip_rows": 0,
        "columns": {
            "transaction_date": "Date",
            "description": "Description",
            "amount": "Amount",
        },
        "date_formats": ["%m/%d/%Y", "%Y-%m-%d"],
    },
    "bmo_personal": {
        "label": "BMO personal banking CSV (3 metadata rows, then header)",
        "has_header": True,
        "delimiter": ",",
        "skip_rows": 3,
        "columns": {
            "transaction_date": "Transaction Date",
            "description": "Description",
            "amount": "Transaction Amount",
        },
        "date_formats": ["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"],
    },
}


def list_bank_csv_mapping_profiles() -> dict[str, Any]:
    """Return all built-in mapping profiles (for the GET endpoint)."""
    return {
        "profiles": {
            name: {
                "label": profile["label"],
                "has_header": profile["has_header"],
                "delimiter": profile["delimiter"],
                "skip_rows": profile["skip_rows"],
                "columns": profile["columns"],
                "date_formats": profile["date_formats"],
            }
            for name, profile in MAPPING_PROFILES.items()
        },
        "logical_fields": [
            "transaction_date (required)",
            "description (required)",
            "description_2 (optional, appended)",
            "amount (signed: + = inflow, - = outflow)",
            "withdrawal (use INSTEAD of amount; positive number)",
            "deposit (use INSTEAD of amount; positive number)",
            "reference_number (optional)",
            "posted_date (optional)",
        ],
        "notes": [
            "If your bank gives signed amounts in one column, use 'amount'.",
            "If your bank gives separate debit/credit columns, use 'withdrawal' and 'deposit'.",
            "When mapping_profile='generic', supply column_map_json to map the logical fields.",
        ],
    }


# ----------------------------------------------------------------------
# Number / date / text helpers
# ----------------------------------------------------------------------

_AMOUNT_CLEAN_RE = re.compile(r"[^0-9\.\-\(\)]")


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    is_negative = False
    # Accountancy negatives: (123.45)
    if raw.startswith("(") and raw.endswith(")"):
        is_negative = True
        raw = raw[1:-1]
    raw = _AMOUNT_CLEAN_RE.sub("", raw)
    if raw in ("", "-", "."):
        return None
    try:
        amount = Decimal(raw)
    except InvalidOperation:
        return None
    if is_negative:
        amount = -amount
    return amount


def _to_date_or_none(value: Any, formats: list[str]) -> date | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    # Last-ditch: ISO with extras (e.g. "2026-02-15 00:00:00")
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _normalize_description(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().upper())


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


# ----------------------------------------------------------------------
# Profile resolution + column-map override
# ----------------------------------------------------------------------


def _resolve_profile(
    mapping_profile: str,
    column_map_override: dict[str, Any] | None,
) -> dict[str, Any]:
    if mapping_profile not in MAPPING_PROFILES:
        raise ValueError(
            f"Unknown mapping_profile '{mapping_profile}'. "
            f"Known profiles: {sorted(MAPPING_PROFILES.keys())}"
        )

    profile = {**MAPPING_PROFILES[mapping_profile]}
    profile["columns"] = dict(profile.get("columns") or {})
    profile["date_formats"] = list(profile.get("date_formats") or [])

    if column_map_override:
        # Allow override of any top-level scalar (has_header, delimiter, skip_rows)
        for top_key in ("has_header", "delimiter", "skip_rows"):
            if top_key in column_map_override:
                profile[top_key] = column_map_override[top_key]
        # Allow override of date_formats (full replace)
        if "date_formats" in column_map_override:
            profile["date_formats"] = list(column_map_override["date_formats"] or [])
        # Merge column mappings — the override wins, profile defaults stay for
        # unspecified logical fields.
        override_columns = column_map_override.get("columns")
        if override_columns is None and not any(
            k in column_map_override for k in ("has_header", "delimiter", "skip_rows", "date_formats")
        ):
            # Caller passed a flat dict that IS the column map (common for 'generic')
            override_columns = column_map_override
        if override_columns:
            profile["columns"].update(override_columns)

    if not profile["date_formats"]:
        profile["date_formats"] = MAPPING_PROFILES["generic"]["date_formats"]

    return profile


def _validate_column_map(profile: dict[str, Any]) -> list[str]:
    """Return a list of warnings (empty if profile looks usable)."""
    warnings: list[str] = []
    columns = profile.get("columns") or {}

    if "transaction_date" not in columns:
        warnings.append("column_map missing required field 'transaction_date'")
    if "description" not in columns:
        warnings.append("column_map missing required field 'description'")

    has_amount = "amount" in columns
    has_pair = "withdrawal" in columns or "deposit" in columns
    if not has_amount and not has_pair:
        warnings.append(
            "column_map needs either 'amount' (signed) or "
            "'withdrawal' and/or 'deposit' (positive numbers)"
        )

    return warnings


# ----------------------------------------------------------------------
# CSV reader
# ----------------------------------------------------------------------


def _decode_csv_bytes(file_bytes: bytes) -> str:
    """Try UTF-8, then UTF-8 with BOM, then fall back to latin-1."""
    if not file_bytes:
        return ""
    if file_bytes.startswith(b"\xef\xbb\xbf"):
        return file_bytes.decode("utf-8-sig")
    try:
        return file_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return file_bytes.decode("latin-1", errors="replace")


def _read_rows(
    file_bytes: bytes,
    profile: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Return (parsed_rows, warnings).

    parsed_rows is a list of dicts of the form:
        {
            "transaction_date": date | None,
            "posted_date": date | None,
            "description": str,
            "amount": Decimal | None,    # signed
            "direction": "inflow" | "outflow" | "unknown",
            "reference_number": str | None,
            "raw_row": <original row as dict or list>,
            "row_number": int,           # 1-based position in source file
            "errors": list[str],         # per-row errors (this row will be skipped)
        }
    """
    warnings: list[str] = []
    text_data = _decode_csv_bytes(file_bytes)
    if not text_data.strip():
        warnings.append("File is empty.")
        return [], warnings

    delimiter = profile.get("delimiter") or ","
    has_header = bool(profile.get("has_header"))
    skip_rows = int(profile.get("skip_rows") or 0)
    columns_map: dict[str, Any] = profile.get("columns") or {}
    date_formats: list[str] = profile.get("date_formats") or []

    buf = io.StringIO(text_data)
    raw_lines = buf.readlines()
    if skip_rows:
        raw_lines = raw_lines[skip_rows:]

    if not raw_lines:
        warnings.append("After skip_rows, no data rows remain.")
        return [], warnings

    reader_text = "".join(raw_lines)
    reader = csv.reader(io.StringIO(reader_text), delimiter=delimiter)

    rows = list(reader)
    if not rows:
        return [], warnings

    if has_header:
        header = [str(h).strip() for h in rows[0]]
        body = rows[1:]
    else:
        header = []
        body = rows

    parsed: list[dict[str, Any]] = []

    for index_in_body, raw_row in enumerate(body):
        row_number = skip_rows + (1 if has_header else 0) + index_in_body + 1
        # Skip blank lines
        if not any((c or "").strip() for c in raw_row):
            continue

        per_row_errors: list[str] = []

        def _cell(field_name: str) -> str | None:
            spec = columns_map.get(field_name)
            if spec is None:
                return None
            if has_header:
                if not isinstance(spec, str):
                    per_row_errors.append(
                        f"column_map['{field_name}'] must be a header name when has_header=True"
                    )
                    return None
                try:
                    idx = header.index(spec)
                except ValueError:
                    per_row_errors.append(
                        f"Column '{spec}' (mapped to '{field_name}') not found in header"
                    )
                    return None
            else:
                try:
                    idx = int(spec)
                except (TypeError, ValueError):
                    per_row_errors.append(
                        f"column_map['{field_name}'] must be a 0-based index when has_header=False"
                    )
                    return None
            if idx < 0 or idx >= len(raw_row):
                return None
            return raw_row[idx]

        txn_date = _to_date_or_none(_cell("transaction_date"), date_formats)
        if txn_date is None:
            per_row_errors.append("transaction_date missing or unparseable")

        posted_date = _to_date_or_none(_cell("posted_date"), date_formats) or txn_date

        description_main = _coerce_str(_cell("description"))
        description_extra = _coerce_str(_cell("description_2"))
        if description_extra:
            description = (
                f"{description_main} | {description_extra}".strip(" |")
                if description_main
                else description_extra
            )
        else:
            description = description_main
        if not description:
            per_row_errors.append("description is empty")

        reference_number = _coerce_str(_cell("reference_number")) or None

        amount: Decimal | None = None
        if "amount" in columns_map:
            amount = _to_decimal_or_none(_cell("amount"))
        else:
            withdrawal = _to_decimal_or_none(_cell("withdrawal"))
            deposit = _to_decimal_or_none(_cell("deposit"))
            if withdrawal and withdrawal != 0:
                amount = -abs(withdrawal)
            elif deposit and deposit != 0:
                amount = abs(deposit)

        if amount is None or amount == 0:
            per_row_errors.append("amount missing, zero, or unparseable")

        if amount is not None and amount > 0:
            direction = "inflow"
        elif amount is not None and amount < 0:
            direction = "outflow"
        else:
            direction = "unknown"

        # Build raw_row representation (use header for keys when available)
        if has_header and header:
            raw_repr: Any = {
                header[i] if i < len(header) else f"col_{i}": (raw_row[i] if i < len(raw_row) else None)
                for i in range(max(len(header), len(raw_row)))
            }
        else:
            raw_repr = list(raw_row)

        parsed.append(
            {
                "row_number": row_number,
                "transaction_date": txn_date,
                "posted_date": posted_date,
                "description": description,
                "amount": amount,
                "direction": direction,
                "reference_number": reference_number,
                "raw_row": raw_repr,
                "errors": per_row_errors,
            }
        )

    return parsed, warnings


# ----------------------------------------------------------------------
# Deterministic per-row source_transaction_id
# ----------------------------------------------------------------------


def _row_signature(
    source_account_code: str | None,
    parsed_row: dict[str, Any],
    occurrence_index: int,
) -> str:
    """
    Build a deterministic SHA-256 hex digest that uniquely identifies a CSV row
    so re-importing the same file (or an overlapping export) is idempotent.

    Inputs hashed:
        - account code (or empty string)
        - ISO transaction date
        - signed amount in cents (string)
        - normalized description (whitespace-collapsed, uppercased)
        - reference number (or empty string)
        - occurrence_index_within_day for identical (date, amount, desc, ref)

    Two banks-exported rows with the same date+amount+desc+ref get incrementing
    occurrence_index values — and that index is assigned in the deterministic
    order in which the rows appear in the file. So as long as the bank exports
    repeat rows in the same order across exports of the same period, IDs stay
    stable across re-imports.
    """
    txn_date = parsed_row["transaction_date"]
    amount = parsed_row["amount"] or Decimal("0")
    desc_norm = _normalize_description(parsed_row.get("description"))
    ref = (parsed_row.get("reference_number") or "").strip().upper()

    cents = int((amount * 100).quantize(Decimal("1")))
    payload = "|".join(
        [
            (source_account_code or "").strip().upper(),
            txn_date.isoformat() if txn_date else "",
            str(cents),
            desc_norm,
            ref,
            str(occurrence_index),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _assign_occurrence_indexes(
    rows: list[dict[str, Any]],
    source_account_code: str | None,
) -> None:
    """
    Mutate rows in place: add row['occurrence_index'] and row['source_transaction_id'].
    Rows with errors get None for both.
    """
    counter: dict[tuple[str, str, int, str, str], int] = defaultdict(int)
    for row in rows:
        if row["errors"]:
            row["occurrence_index"] = None
            row["source_transaction_id"] = None
            continue
        amount = row["amount"] or Decimal("0")
        cents = int((amount * 100).quantize(Decimal("1")))
        key = (
            (source_account_code or "").strip().upper(),
            row["transaction_date"].isoformat() if row["transaction_date"] else "",
            cents,
            _normalize_description(row.get("description")),
            (row.get("reference_number") or "").strip().upper(),
        )
        idx = counter[key]
        counter[key] += 1
        row["occurrence_index"] = idx
        row["source_transaction_id"] = _row_signature(source_account_code, row, idx)


# ----------------------------------------------------------------------
# Preview
# ----------------------------------------------------------------------


def preview_bank_csv_import(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    mapping_profile: str,
    source_account_code: str | None,
    source_account_name: str | None,
    column_map_override: dict[str, Any] | None,
    sample_limit: int = 20,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    profile = _resolve_profile(mapping_profile, column_map_override)
    column_map_warnings = _validate_column_map(profile)

    parsed_rows, file_warnings = _read_rows(file_bytes, profile)
    _assign_occurrence_indexes(parsed_rows, source_account_code)

    error_rows = [r for r in parsed_rows if r["errors"]]
    valid_rows = [r for r in parsed_rows if not r["errors"]]

    earliest = min((r["transaction_date"] for r in valid_rows if r["transaction_date"]), default=None)
    latest = max((r["transaction_date"] for r in valid_rows if r["transaction_date"]), default=None)

    # Determine which valid rows would be inserted vs already in DB
    would_insert = 0
    would_dedup = 0
    if valid_rows:
        ids = [r["source_transaction_id"] for r in valid_rows]
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
        existing_ids = {row["source_transaction_id"] for row in existing}
        for r in valid_rows:
            if r["source_transaction_id"] in existing_ids:
                would_dedup += 1
            else:
                would_insert += 1

    sample = []
    for r in parsed_rows[:sample_limit]:
        sample.append(
            {
                "row_number": r["row_number"],
                "transaction_date": r["transaction_date"].isoformat() if r["transaction_date"] else None,
                "posted_date": r["posted_date"].isoformat() if r["posted_date"] else None,
                "description": r["description"],
                "amount": str(r["amount"]) if r["amount"] is not None else None,
                "direction": r["direction"],
                "reference_number": r["reference_number"],
                "errors": r["errors"],
                "source_transaction_id": r.get("source_transaction_id"),
            }
        )

    return {
        "entity_code": entity_code,
        "file_name": file_name,
        "mapping_profile": mapping_profile,
        "source_account_code": source_account_code,
        "source_account_name": source_account_name,
        "column_map_warnings": column_map_warnings,
        "file_warnings": file_warnings,
        "total_row_count": len(parsed_rows),
        "valid_row_count": len(valid_rows),
        "error_row_count": len(error_rows),
        "would_insert_count": would_insert,
        "would_dedup_count": would_dedup,
        "earliest_transaction_date": earliest.isoformat() if earliest else None,
        "latest_transaction_date": latest.isoformat() if latest else None,
        "sample": sample,
    }


# ----------------------------------------------------------------------
# Import (commits rows + writes a run record)
# ----------------------------------------------------------------------


def run_bank_csv_import(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    mapping_profile: str,
    source_account_code: str | None,
    source_account_name: str | None,
    column_map_override: dict[str, Any] | None,
    actor_email: str,
    note: str | None = None,
    run_auto_match_after: bool = True,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if not _has_table(session, "bank_csv_import_runs"):
        raise ValueError(
            "bank_csv_import_runs table does not exist. "
            "Apply migration 009_bank_csv_upload.sql first."
        )

    profile = _resolve_profile(mapping_profile, column_map_override)
    column_map_warnings = _validate_column_map(profile)
    if column_map_warnings:
        # Hard-fail on import (preview is more permissive)
        raise ValueError("column_map issues: " + "; ".join(column_map_warnings))

    file_size_bytes = len(file_bytes or b"")
    file_checksum = (
        hashlib.sha256(file_bytes).hexdigest() if file_bytes else None
    )

    parsed_rows, file_warnings = _read_rows(file_bytes, profile)
    _assign_occurrence_indexes(parsed_rows, source_account_code)

    error_rows = [r for r in parsed_rows if r["errors"]]
    valid_rows = [r for r in parsed_rows if not r["errors"]]

    earliest = min((r["transaction_date"] for r in valid_rows if r["transaction_date"]), default=None)
    latest = max((r["transaction_date"] for r in valid_rows if r["transaction_date"]), default=None)

    # Period lock guard: refuse the import if any valid row's transaction_date
    # falls inside a closed_locked period. Period close is a hard wall — the
    # user must reopen the period before importing into it.
    locked_periods_seen: dict[str, dict[str, Any]] = {}
    for row in valid_rows:
        is_locked, period = is_date_in_locked_period(
            session, entity_id=entity["id"], when=row["transaction_date"]
        )
        if is_locked and period:
            locked_periods_seen[str(period["id"])] = {
                "period_label": period.get("period_label"),
                "period_end": (
                    period["period_end"].isoformat() if period.get("period_end") else None
                ),
                "status": period.get("status"),
            }
    if locked_periods_seen:
        raise PeriodLockedError(
            next(iter(locked_periods_seen.values())),
            message=(
                "Bank CSV import refused: one or more rows fall in a "
                "closed_locked accounting period. "
                f"Locked periods affected: {list(locked_periods_seen.values())}. "
                "Reopen the period(s) before importing."
            ),
        )

    # Insert the run record first so we can stamp source_import_run_id on rows
    run_row = session.execute(
        text(
            """
            INSERT INTO bank_csv_import_runs (
                entity_id, file_name, file_checksum_sha256, file_size_bytes,
                mapping_profile, source_account_code, source_account_name,
                column_map_json, total_row_count, parsed_row_count,
                inserted_count, duplicate_count, skipped_count, error_count,
                earliest_transaction_date, latest_transaction_date,
                status, is_preview, error_text, summary_json, actor_email
            )
            VALUES (
                :entity_id, :file_name, :file_checksum, :file_size_bytes,
                :mapping_profile, :source_account_code, :source_account_name,
                CAST(:column_map_json AS jsonb), :total_row_count, :parsed_row_count,
                0, 0, 0, :error_count,
                :earliest, :latest,
                'running', FALSE, NULL, '{}'::jsonb, :actor_email
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "file_name": file_name,
            "file_checksum": file_checksum,
            "file_size_bytes": file_size_bytes,
            "mapping_profile": mapping_profile,
            "source_account_code": source_account_code,
            "source_account_name": source_account_name,
            "column_map_json": json.dumps(
                {
                    "columns": profile.get("columns") or {},
                    "has_header": profile.get("has_header"),
                    "delimiter": profile.get("delimiter"),
                    "skip_rows": profile.get("skip_rows"),
                    "date_formats": profile.get("date_formats") or [],
                    "override": column_map_override or {},
                },
                default=str,
            ),
            "total_row_count": len(parsed_rows),
            "parsed_row_count": len(valid_rows),
            "error_count": len(error_rows),
            "earliest": earliest,
            "latest": latest,
            "actor_email": actor_email,
        },
    ).mappings().first()
    run_id: UUID = run_row["id"]

    inserted = 0
    duplicates = 0

    for row in valid_rows:
        accounting_period_id = get_or_create_accounting_period(
            session, entity["id"], row["transaction_date"]
        )

        existing = session.execute(
            text(
                """
                SELECT id
                FROM bank_transactions
                WHERE entity_id = :entity_id
                  AND source_system = :source_system
                  AND source_transaction_id = :source_transaction_id
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "source_system": SOURCE_SYSTEM,
                "source_transaction_id": row["source_transaction_id"],
            },
        ).mappings().first()

        if existing:
            duplicates += 1
            session.execute(
                text(
                    "UPDATE bank_transactions SET last_seen_at = NOW() WHERE id = :id"
                ),
                {"id": existing["id"]},
            )
            continue

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
                    review_status, raw_json, source_import_run_id
                )
                VALUES (
                    :entity_id, :accounting_period_id, :source_system, NULL,
                    NULL, :source_account_name, :source_account_code,
                    :source_transaction_id, :source_transaction_type,
                    :transaction_date, :posted_date,
                    :description, :normalized_description,
                    NULL, :reference_number,
                    :amount, 'CAD', :direction,
                    'new', CAST(:raw_json AS jsonb), :source_import_run_id
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "source_system": SOURCE_SYSTEM,
                "source_account_name": source_account_name,
                "source_account_code": source_account_code,
                "source_transaction_id": row["source_transaction_id"],
                "source_transaction_type": (
                    "csv_outflow" if row["direction"] == "outflow"
                    else "csv_inflow" if row["direction"] == "inflow"
                    else "csv_line"
                ),
                "transaction_date": row["transaction_date"],
                "posted_date": row["posted_date"],
                "description": (row["description"] or "")[:500],
                "normalized_description": _normalize_description(row["description"])[:500],
                "reference_number": row["reference_number"],
                "amount": row["amount"],
                "direction": row["direction"],
                "raw_json": json.dumps(
                    {
                        "csv_row": row["raw_row"],
                        "row_number": row["row_number"],
                        "occurrence_index": row["occurrence_index"],
                        "import_run_id": str(run_id),
                        "import_file_name": file_name,
                        "import_mapping_profile": mapping_profile,
                    },
                    default=str,
                ),
                "source_import_run_id": run_id,
            },
        )
        inserted += 1

    final_status = "completed" if not error_rows else "partial"
    summary = {
        "file_warnings": file_warnings,
        "error_rows_first_20": [
            {
                "row_number": r["row_number"],
                "errors": r["errors"],
                "raw_row": r["raw_row"],
            }
            for r in error_rows[:20]
        ],
        "actor_email": actor_email,
        "note": note,
    }

    session.execute(
        text(
            """
            UPDATE bank_csv_import_runs
            SET inserted_count = :inserted_count,
                duplicate_count = :duplicate_count,
                skipped_count = 0,
                status = :status,
                summary_json = CAST(:summary_json AS jsonb)
            WHERE id = :id
            """
        ),
        {
            "id": run_id,
            "inserted_count": inserted,
            "duplicate_count": duplicates,
            "status": final_status,
            "summary_json": json.dumps(summary, default=str),
        },
    )

    # Optional: run the auto-match runner over the date range of the imported
    # rows. Imported into a function here (not at module top) to avoid a
    # circular import: services_auto_match imports things from services.py,
    # which is unrelated to this module but is a heavier import surface.
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
                trigger_source_id=run_id,
            )
        except Exception as exc:
            # Don't fail the CSV import if auto-match has a problem; surface
            # the error in the response so the caller can investigate.
            auto_match_summary = {"error": str(exc), "status": "failed"}

    return {
        "entity_code": entity_code,
        "import_run_id": str(run_id),
        "file_name": file_name,
        "mapping_profile": mapping_profile,
        "source_account_code": source_account_code,
        "source_account_name": source_account_name,
        "total_row_count": len(parsed_rows),
        "valid_row_count": len(valid_rows),
        "inserted_count": inserted,
        "duplicate_count": duplicates,
        "error_count": len(error_rows),
        "earliest_transaction_date": earliest.isoformat() if earliest else None,
        "latest_transaction_date": latest.isoformat() if latest else None,
        "status": final_status,
        "warnings": file_warnings,
        "error_rows_first_20": summary["error_rows_first_20"],
        "auto_match": auto_match_summary,
    }


# ----------------------------------------------------------------------
# List / detail
# ----------------------------------------------------------------------


def list_bank_csv_import_runs(
    session,
    *,
    entity_code: str,
    limit: int = 50,
    source_account_code: str | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if not _has_table(session, "bank_csv_import_runs"):
        return {"entity_code": entity_code, "count": 0, "runs": []}

    rows = session.execute(
        text(
            """
            SELECT id, file_name, file_size_bytes, mapping_profile,
                   source_account_code, source_account_name,
                   total_row_count, parsed_row_count,
                   inserted_count, duplicate_count, skipped_count, error_count,
                   earliest_transaction_date, latest_transaction_date,
                   status, is_preview, actor_email, created_at
            FROM bank_csv_import_runs
            WHERE entity_id = :entity_id
              AND (CAST(:source_account_code AS TEXT) IS NULL OR source_account_code = CAST(:source_account_code AS TEXT))
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity["id"],
            "source_account_code": source_account_code,
            "limit": int(limit),
        },
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "count": len(rows),
        "runs": [
            {
                **{k: v for k, v in dict(row).items() if k != "id"},
                "id": str(row["id"]),
                "earliest_transaction_date": (
                    row["earliest_transaction_date"].isoformat()
                    if row["earliest_transaction_date"]
                    else None
                ),
                "latest_transaction_date": (
                    row["latest_transaction_date"].isoformat()
                    if row["latest_transaction_date"]
                    else None
                ),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
            for row in rows
        ],
    }


def get_bank_csv_import_run_detail(
    session,
    *,
    entity_code: str,
    run_id: str,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    run_uuid = _parse_uuid(run_id, "run_id")

    run = session.execute(
        text(
            """
            SELECT id, entity_id, file_name, file_checksum_sha256, file_size_bytes,
                   mapping_profile, source_account_code, source_account_name,
                   column_map_json, total_row_count, parsed_row_count,
                   inserted_count, duplicate_count, skipped_count, error_count,
                   earliest_transaction_date, latest_transaction_date,
                   status, is_preview, error_text, summary_json,
                   actor_email, created_at
            FROM bank_csv_import_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()

    if not run:
        raise ValueError(f"No bank CSV import run found for id {run_id}")

    transactions = session.execute(
        text(
            """
            SELECT id, transaction_date, posted_date, description,
                   amount, direction, reference_number,
                   review_status, source_transaction_id
            FROM bank_transactions
            WHERE source_import_run_id = :run_id
              AND entity_id = :entity_id
            ORDER BY transaction_date, description
            LIMIT 500
            """
        ),
        {"run_id": run_uuid, "entity_id": entity["id"]},
    ).mappings().all()

    column_map = run["column_map_json"]
    if isinstance(column_map, str):
        try:
            column_map = json.loads(column_map)
        except Exception:
            column_map = {}
    summary = run["summary_json"]
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}

    return {
        "entity_code": entity_code,
        "run": {
            "id": str(run["id"]),
            "file_name": run["file_name"],
            "file_size_bytes": run["file_size_bytes"],
            "file_checksum_sha256": run["file_checksum_sha256"],
            "mapping_profile": run["mapping_profile"],
            "source_account_code": run["source_account_code"],
            "source_account_name": run["source_account_name"],
            "column_map": column_map,
            "total_row_count": run["total_row_count"],
            "parsed_row_count": run["parsed_row_count"],
            "inserted_count": run["inserted_count"],
            "duplicate_count": run["duplicate_count"],
            "skipped_count": run["skipped_count"],
            "error_count": run["error_count"],
            "earliest_transaction_date": (
                run["earliest_transaction_date"].isoformat()
                if run["earliest_transaction_date"]
                else None
            ),
            "latest_transaction_date": (
                run["latest_transaction_date"].isoformat()
                if run["latest_transaction_date"]
                else None
            ),
            "status": run["status"],
            "is_preview": run["is_preview"],
            "error_text": run["error_text"],
            "summary": summary,
            "actor_email": run["actor_email"],
            "created_at": run["created_at"].isoformat() if run["created_at"] else None,
        },
        "transaction_count": len(transactions),
        "transactions": [
            {
                "id": str(txn["id"]),
                "transaction_date": (
                    txn["transaction_date"].isoformat() if txn["transaction_date"] else None
                ),
                "posted_date": (
                    txn["posted_date"].isoformat() if txn["posted_date"] else None
                ),
                "description": txn["description"],
                "amount": str(txn["amount"]) if txn["amount"] is not None else None,
                "direction": txn["direction"],
                "reference_number": txn["reference_number"],
                "review_status": txn["review_status"],
                "source_transaction_id": txn["source_transaction_id"],
            }
            for txn in transactions
        ],
    }
