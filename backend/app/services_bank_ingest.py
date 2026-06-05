"""
Adapter-agnostic bank ingestion (Phase 3A).

`dispatch` picks an adapter (format-memory hint -> deterministic supports() ->
AI fallback). `ingest` writes the normalized rows into bank_transactions with
SHA-256 dedup, runs the statement balance tie-out, and remembers the format so
the next upload from the same dealer parses deterministically.

Read/ingest only — no GL writes.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .bank_adapters.base import AdapterResult, NormalizedBankTxn
from .bank_adapters.pdf_adapter import PdfBankAdapter
from .bank_adapters.csv_adapter import CsvBankAdapter
from .bank_adapters.ofx_adapter import OfxBankAdapter
from .bank_adapters.excel_adapter import ExcelBankAdapter
from .bank_adapters.ai_fallback_adapter import AiFallbackBankAdapter

# Deterministic adapters, tried in order; AI fallback is invoked separately.
_DETERMINISTIC = [PdfBankAdapter(), OfxBankAdapter(), ExcelBankAdapter(), CsvBankAdapter()]
_AI_FALLBACK = AiFallbackBankAdapter()


def dispatch(
    *, file_bytes: bytes, filename: str, content_type: str,
    entity_code: str, hints: dict[str, Any] | None = None,
) -> AdapterResult:
    """Parse a statement with the best available adapter. Deterministic first;
    AI fallback only when nothing matches or the match yields zero rows."""
    sniff = file_bytes[:512]
    for adapter in _DETERMINISTIC:
        try:
            if adapter.supports(filename=filename, content_type=content_type, sniff=sniff):
                result = adapter.parse(file_bytes=file_bytes, filename=filename,
                                       entity_code=entity_code, hints=hints)
                if result.transactions:
                    return result
        except Exception as exc:  # a parser blowing up shouldn't block the fallback
            continue
    # nothing matched, or a deterministic parse came back empty
    return _AI_FALLBACK.parse(file_bytes=file_bytes, filename=filename,
                              entity_code=entity_code, hints=hints)


def _signature(entity_id: str, source_system: str, t: NormalizedBankTxn, occ: int) -> str:
    if t.source_transaction_id:
        return t.source_transaction_id
    payload = "|".join([
        str(entity_id), source_system,
        t.transaction_date.isoformat() if t.transaction_date else "",
        f"{t.amount:.2f}", (t.normalized_description or t.description or "")[:120], str(occ),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ingest(
    session, *, entity_code: str, result: AdapterResult,
    source_account_code: str, source_import_run_id: str | None = None,
    actor_email: str | None = None,
) -> dict[str, Any]:
    """Insert normalized rows into bank_transactions (idempotent). Returns
    counts + the balance tie-out flag."""
    entity_id = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"), {"ec": entity_code}
    ).scalar()
    if not entity_id:
        raise ValueError(f"entity {entity_code} not found")

    source_system = result.meta.source_system
    seen: dict[str, int] = {}
    inserted = duplicate = 0
    for t in result.transactions:
        occ = seen.get(f"{t.transaction_date}|{t.amount}|{t.description}", 0)
        seen[f"{t.transaction_date}|{t.amount}|{t.description}"] = occ + 1
        stid = _signature(entity_id, source_system, t, occ)
        period_id = session.execute(
            text("SELECT id FROM accounting_periods WHERE entity_id=:e AND :d BETWEEN period_start AND period_end LIMIT 1"),
            {"e": entity_id, "d": t.transaction_date},
        ).scalar()
        raw = dict(t.raw or {})
        if t.running_balance is not None:
            raw["running_balance"] = str(t.running_balance)
        res = session.execute(
            text(
                """
                INSERT INTO bank_transactions (
                    entity_id, accounting_period_id, source_system, source_account_code,
                    source_transaction_id, source_transaction_type, transaction_date,
                    description, reference_number, amount, direction, review_status,
                    normalized_description, counterparty_name, raw_json, source_import_run_id
                ) VALUES (
                    :eid, :pid, :ss, :sac, :stid, :stt, :td, :desc, :ref, :amt, :dir, 'new',
                    :ndesc, :cp, CAST(:raw AS jsonb), :run
                )
                ON CONFLICT (entity_id, source_system, source_transaction_id) DO NOTHING
                """
            ),
            {
                "eid": entity_id, "pid": period_id, "ss": source_system, "sac": source_account_code,
                "stid": stid, "stt": t.signed_direction(), "td": t.transaction_date,
                "desc": t.description, "ref": t.reference_number, "amt": t.amount,
                "dir": t.signed_direction(), "ndesc": t.normalized_description,
                "cp": t.counterparty_name, "raw": json.dumps(raw), "run": source_import_run_id,
            },
        )
        if res.rowcount:
            inserted += 1
        else:
            duplicate += 1

    tie_ok, variance = result.tie_out()
    return {
        "inserted": inserted, "duplicate": duplicate,
        "parsed": len(result.transactions),
        "tie_out_ok": tie_ok, "tie_out_variance": float(variance) if variance is not None else None,
        "opening_balance": float(result.meta.opening_balance) if result.meta.opening_balance is not None else None,
        "closing_balance": float(result.meta.closing_balance) if result.meta.closing_balance is not None else None,
        "warnings": result.warnings, "parser_confidence": result.parser_confidence,
        "source_system": source_system,
    }


# -------------------- format memory (assistant_entity_memory) --------------------

def remember_format(session, entity_code: str, source_account_code: str, profile: dict) -> None:
    """Persist a learned statement format so re-uploads parse deterministically."""
    key = f"bank_statement_format:{source_account_code}"
    session.execute(
        text(
            """
            INSERT INTO assistant_entity_memory (entity_code, memory_type, memory_key, memory_value, confidence, times_confirmed)
            VALUES (:ec, 'transaction_pattern', :k, :v, 90, 1)
            ON CONFLICT (entity_code, memory_type, memory_key) DO UPDATE SET
                memory_value = EXCLUDED.memory_value,
                times_confirmed = assistant_entity_memory.times_confirmed + 1,
                confidence = LEAST(assistant_entity_memory.confidence + 5, 100),
                last_seen_at = NOW()
            """
        ),
        {"ec": entity_code, "k": key, "v": json.dumps(profile)},
    )


def recall_format(session, entity_code: str, source_account_code: str) -> dict | None:
    row = session.execute(
        text(
            "SELECT memory_value FROM assistant_entity_memory WHERE entity_code=:ec "
            "AND memory_type='transaction_pattern' AND memory_key=:k"
        ),
        {"ec": entity_code, "k": f"bank_statement_format:{source_account_code}"},
    ).scalar()
    if not row:
        return None
    try:
        return json.loads(row)
    except (json.JSONDecodeError, TypeError):
        return None
