"""
Post-import auto-match runner — service layer.

Why this is its own file:
    services.py is already ~3.8k lines and only HH remittance has an
    existing auto-match function. Card-settlement and direct-vendor-AP
    auto-match logic is implemented here (in this new module), reusing
    the existing _suggest_*, list_*, and create_*_bank_match helpers from
    services.py.

Public surface:
    run_auto_match(session, *, entity_code, period_start, period_end,
                   actor_email, triggered_by, trigger_source_id=None,
                   ...) -> dict[str, Any]
        Runs HH remittance + card settlement + direct vendor AP auto-match
        for the given date range. Records a single auto_match_runs row.
        Idempotent: already-matched records are skipped cleanly.

    list_auto_match_runs(session, *, entity_code, limit=50)
    get_auto_match_run_detail(session, *, entity_code, run_id)

Idempotence rule:
    A target record is considered "already matched" if there is any active
    bank_transaction_matches row pointing to it. The auto-match logic only
    proposes new matches when zero active matches exist, so re-running a
    pass on the same date range produces no duplicate matches.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    CARD_SETTLEMENT_MATCH_TYPE,
    CARD_SETTLEMENT_TARGET_TABLE,
    DIRECT_VENDOR_MATCH_TYPE,
    DIRECT_VENDOR_TARGET_TABLE,
    _has_table,
    _parse_uuid,
    _suggest_bank_transactions_for_card_settlement,
    _suggest_bank_transactions_for_direct_vendor_invoice,
    auto_match_hh_ap_remittances_to_bank,
    create_card_settlement_bank_match,
    create_direct_vendor_ap_invoice_bank_match,
    get_entity_by_code,
    list_card_settlement_batches,
    list_direct_vendor_ap_invoices,
)


TRIGGER_CSV_IMPORT = "csv_import"
TRIGGER_MANUAL = "manual"
TRIGGER_SCHEDULED = "scheduled"

VALID_TRIGGERS = {TRIGGER_CSV_IMPORT, TRIGGER_MANUAL, TRIGGER_SCHEDULED}


# --------------------------------------------------------------------------
# Per-module auto-match (NEW — written here, not added to services.py)
# --------------------------------------------------------------------------


def _auto_match_card_settlement(
    session,
    *,
    entity_code: str,
    entity_id: UUID,
    date_from: date,
    date_to: date,
    actor_email: str,
    date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
    max_to_apply: int = 100,
) -> dict[str, Any]:
    """
    For each unmatched card_settlement_batches row in [date_from, date_to],
    look at the suggested bank transactions. If exactly one suggestion is
    within amount tolerance AND has no active match, apply it.
    """
    review = list_card_settlement_batches(
        session=session,
        entity_code=entity_code,
        date_from=date_from,
        date_to=date_to,
        bank_match_state="unmatched",
    )

    matched: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for batch in review.get("batches", []):
        if len(matched) >= max_to_apply:
            skipped.append(
                {"batch_id": str(batch.get("id")), "reason": "max_to_apply_reached"}
            )
            continue

        suggestions = _suggest_bank_transactions_for_card_settlement(
            session=session,
            entity_id=entity_id,
            batch_row=batch,
            date_window_days=date_window_days,
            amount_tolerance=amount_tolerance,
            limit=10,
        )

        target_amount = abs(Decimal(str(batch.get("net_deposit_amount") or 0)))
        threshold = max(amount_tolerance, target_amount * Decimal("0.05"))

        exact_open = [
            s for s in suggestions
            if abs(Decimal(str(s.get("amount_diff") or 0))) <= threshold
            and int(s.get("active_match_count") or 0) == 0
        ]

        if len(exact_open) != 1:
            skipped.append(
                {
                    "batch_id": str(batch.get("id")),
                    "processor_name": batch.get("processor_name"),
                    "business_date": str(batch.get("business_date")),
                    "reason": "no_unique_exact_candidate",
                    "exact_candidate_count": len(exact_open),
                }
            )
            continue

        selected = exact_open[0]
        try:
            create_card_settlement_bank_match(
                session=session,
                entity_code=entity_code,
                batch_id=str(batch["id"]),
                bank_transaction_id=str(selected["id"]),
                actor_email=actor_email,
                amount_matched=target_amount,
                note="Auto-matched by run_auto_match",
            )
            matched.append(
                {
                    "batch_id": str(batch["id"]),
                    "processor_name": batch.get("processor_name"),
                    "business_date": str(batch.get("business_date")),
                    "bank_transaction_id": str(selected["id"]),
                    "matched_amount": str(target_amount),
                }
            )
        except Exception as exc:
            skipped.append(
                {
                    "batch_id": str(batch["id"]),
                    "reason": "create_match_error",
                    "error": str(exc),
                }
            )

    return {"matched": matched, "skipped": skipped}


def _auto_match_direct_vendor_ap(
    session,
    *,
    entity_code: str,
    entity_id: UUID,
    date_from: date,
    date_to: date,
    actor_email: str,
    date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
    max_to_apply: int = 100,
) -> dict[str, Any]:
    """
    For each unmatched direct_vendor_ap_invoices row dated in
    [date_from, date_to], look at suggested bank transactions. If exactly
    one suggestion within amount tolerance has no active match, apply it.
    """
    review = list_direct_vendor_ap_invoices(
        session=session,
        entity_code=entity_code,
        date_from=date_from,
        date_to=date_to,
        match_state="unmatched",
    )

    matched: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for invoice in review.get("invoices", []):
        if len(matched) >= max_to_apply:
            skipped.append(
                {"invoice_id": str(invoice.get("id")), "reason": "max_to_apply_reached"}
            )
            continue

        suggestions = _suggest_bank_transactions_for_direct_vendor_invoice(
            session=session,
            entity_id=entity_id,
            invoice_row=invoice,
            date_window_days=date_window_days,
            amount_tolerance=amount_tolerance,
            limit=10,
        )

        open_amount = abs(Decimal(str(invoice.get("open_amount") or 0)))
        if open_amount <= Decimal("0.00"):
            skipped.append(
                {
                    "invoice_id": str(invoice.get("id")),
                    "reason": "zero_open_amount",
                }
            )
            continue

        threshold = max(amount_tolerance, open_amount * Decimal("0.05"))
        exact_open = [
            s for s in suggestions
            if abs(Decimal(str(s.get("amount_diff") or 0))) <= threshold
            and int(s.get("active_match_count") or 0) == 0
        ]

        if len(exact_open) != 1:
            skipped.append(
                {
                    "invoice_id": str(invoice.get("id")),
                    "vendor_name": invoice.get("vendor_name"),
                    "invoice_number": invoice.get("invoice_number"),
                    "reason": "no_unique_exact_candidate",
                    "exact_candidate_count": len(exact_open),
                }
            )
            continue

        selected = exact_open[0]
        try:
            create_direct_vendor_ap_invoice_bank_match(
                session=session,
                entity_code=entity_code,
                invoice_id=str(invoice["id"]),
                bank_transaction_id=str(selected["id"]),
                actor_email=actor_email,
                amount_matched=open_amount,
                note="Auto-matched by run_auto_match",
            )
            matched.append(
                {
                    "invoice_id": str(invoice["id"]),
                    "vendor_name": invoice.get("vendor_name"),
                    "invoice_number": invoice.get("invoice_number"),
                    "bank_transaction_id": str(selected["id"]),
                    "matched_amount": str(open_amount),
                }
            )
        except Exception as exc:
            skipped.append(
                {
                    "invoice_id": str(invoice["id"]),
                    "reason": "create_match_error",
                    "error": str(exc),
                }
            )

    return {"matched": matched, "skipped": skipped}


# --------------------------------------------------------------------------
# Run record helpers
# --------------------------------------------------------------------------


def _insert_run(
    session,
    *,
    entity_id: UUID,
    accounting_period_id: UUID | None,
    triggered_by: str,
    trigger_source_id: UUID | None,
    period_start: date,
    period_end: date,
    actor_email: str,
) -> UUID:
    row = session.execute(
        text(
            """
            INSERT INTO auto_match_runs (
                entity_id, accounting_period_id, triggered_by, trigger_source_id,
                period_start, period_end, status, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id, :triggered_by, :trigger_source_id,
                :period_start, :period_end, 'running', :actor_email
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "triggered_by": triggered_by,
            "trigger_source_id": trigger_source_id,
            "period_start": period_start,
            "period_end": period_end,
            "actor_email": actor_email,
        },
    ).mappings().first()
    return row["id"]


def _finalize_run(
    session,
    *,
    run_id: UUID,
    status: str,
    error_text: str | None,
    counts: dict[str, int],
    detail: dict[str, Any],
) -> None:
    session.execute(
        text(
            """
            UPDATE auto_match_runs
               SET status = :status,
                   error_text = :error_text,
                   hh_remittance_matched = :hh_remittance_matched,
                   hh_remittance_skipped = :hh_remittance_skipped,
                   card_settlement_matched = :card_settlement_matched,
                   card_settlement_skipped = :card_settlement_skipped,
                   direct_vendor_matched = :direct_vendor_matched,
                   direct_vendor_skipped = :direct_vendor_skipped,
                   total_matched = :total_matched,
                   total_skipped = :total_skipped,
                   detail_json = CAST(:detail_json AS jsonb),
                   completed_at = NOW()
             WHERE id = :run_id
            """
        ),
        {
            "run_id": run_id,
            "status": status,
            "error_text": error_text,
            "detail_json": json.dumps(detail, default=str),
            **counts,
        },
    )


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


def run_auto_match(
    session,
    *,
    entity_code: str,
    period_start: date,
    period_end: date,
    actor_email: str,
    triggered_by: str = TRIGGER_MANUAL,
    trigger_source_id: str | UUID | None = None,
    date_window_days: int = 7,
    amount_tolerance: Decimal = Decimal("0.05"),
    max_to_apply: int = 100,
) -> dict[str, Any]:
    if triggered_by not in VALID_TRIGGERS:
        raise ValueError(
            f"Invalid triggered_by '{triggered_by}'. Valid: {sorted(VALID_TRIGGERS)}"
        )
    if not actor_email:
        raise ValueError("actor_email is required")

    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    # Resolve which accounting_period contains period_end (if any)
    period_row = session.execute(
        text(
            """
            SELECT id
              FROM accounting_periods
             WHERE entity_id = :entity_id
               AND period_end = :period_end
             LIMIT 1
            """
        ),
        {"entity_id": entity["id"], "period_end": period_end},
    ).mappings().first()
    accounting_period_id: UUID | None = period_row["id"] if period_row else None

    trigger_uuid: UUID | None = None
    if trigger_source_id is not None:
        trigger_uuid = _parse_uuid(str(trigger_source_id), "trigger_source_id")

    run_id = _insert_run(
        session,
        entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
        triggered_by=triggered_by,
        trigger_source_id=trigger_uuid,
        period_start=period_start,
        period_end=period_end,
        actor_email=actor_email,
    )

    error_text: str | None = None
    final_status = "completed"
    detail: dict[str, Any] = {}
    counts = {
        "hh_remittance_matched": 0,
        "hh_remittance_skipped": 0,
        "card_settlement_matched": 0,
        "card_settlement_skipped": 0,
        "direct_vendor_matched": 0,
        "direct_vendor_skipped": 0,
        "total_matched": 0,
        "total_skipped": 0,
    }

    try:
        # 1. HH remittance — uses existing services.py function
        if _has_table(session, "hh_ap_remittances"):
            hh = auto_match_hh_ap_remittances_to_bank(
                session=session,
                entity_code=entity_code,
                date_from=period_start,
                date_to=period_end,
                actor_email=actor_email,
                date_window_days=date_window_days,
                amount_tolerance=amount_tolerance,
                max_to_apply=max_to_apply,
            )
            counts["hh_remittance_matched"] = int(hh.get("matched_count") or 0)
            counts["hh_remittance_skipped"] = int(hh.get("skipped_count") or 0)
            detail["hh_remittance"] = {
                "matched": hh.get("matched") or [],
                "skipped": hh.get("skipped") or [],
            }
        else:
            detail["hh_remittance"] = {"skipped_module": "table_not_present"}

        # 2. Card settlement — implemented in this file
        if _has_table(session, "card_settlement_batches"):
            cs = _auto_match_card_settlement(
                session=session,
                entity_code=entity_code,
                entity_id=entity["id"],
                date_from=period_start,
                date_to=period_end,
                actor_email=actor_email,
                date_window_days=date_window_days,
                amount_tolerance=amount_tolerance,
                max_to_apply=max_to_apply,
            )
            counts["card_settlement_matched"] = len(cs["matched"])
            counts["card_settlement_skipped"] = len(cs["skipped"])
            detail["card_settlement"] = cs
        else:
            detail["card_settlement"] = {"skipped_module": "table_not_present"}

        # 3. Direct vendor AP — implemented in this file
        if _has_table(session, "direct_vendor_ap_invoices"):
            dv = _auto_match_direct_vendor_ap(
                session=session,
                entity_code=entity_code,
                entity_id=entity["id"],
                date_from=period_start,
                date_to=period_end,
                actor_email=actor_email,
                date_window_days=date_window_days,
                amount_tolerance=amount_tolerance,
                max_to_apply=max_to_apply,
            )
            counts["direct_vendor_matched"] = len(dv["matched"])
            counts["direct_vendor_skipped"] = len(dv["skipped"])
            detail["direct_vendor"] = dv
        else:
            detail["direct_vendor"] = {"skipped_module": "table_not_present"}

        counts["total_matched"] = (
            counts["hh_remittance_matched"]
            + counts["card_settlement_matched"]
            + counts["direct_vendor_matched"]
        )
        counts["total_skipped"] = (
            counts["hh_remittance_skipped"]
            + counts["card_settlement_skipped"]
            + counts["direct_vendor_skipped"]
        )

    except Exception as exc:
        error_text = str(exc)
        final_status = "failed"
        detail["fatal_error"] = error_text

    if final_status == "completed" and counts["total_skipped"] > 0:
        final_status = "partial"

    _finalize_run(
        session,
        run_id=run_id,
        status=final_status,
        error_text=error_text,
        counts=counts,
        detail=detail,
    )

    return {
        "id": str(run_id),
        "entity_code": entity_code,
        "triggered_by": triggered_by,
        "trigger_source_id": str(trigger_uuid) if trigger_uuid else None,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "status": final_status,
        "error_text": error_text,
        "actor_email": actor_email,
        **counts,
        "detail": detail,
    }


# --------------------------------------------------------------------------
# Listing
# --------------------------------------------------------------------------


def list_auto_match_runs(
    session, *, entity_code: str, limit: int = 50
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            SELECT id, triggered_by, trigger_source_id, period_start, period_end,
                   hh_remittance_matched, hh_remittance_skipped,
                   card_settlement_matched, card_settlement_skipped,
                   direct_vendor_matched, direct_vendor_skipped,
                   total_matched, total_skipped, status, error_text,
                   actor_email, created_at, completed_at
              FROM auto_match_runs
             WHERE entity_id = :entity_id
             ORDER BY created_at DESC
             LIMIT :limit
            """
        ),
        {"entity_id": entity["id"], "limit": int(limit)},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "count": len(rows),
        "runs": [
            {
                "id": str(r["id"]),
                "triggered_by": r["triggered_by"],
                "trigger_source_id": str(r["trigger_source_id"]) if r["trigger_source_id"] else None,
                "period_start": r["period_start"].isoformat() if r["period_start"] else None,
                "period_end": r["period_end"].isoformat() if r["period_end"] else None,
                "hh_remittance_matched": r["hh_remittance_matched"],
                "hh_remittance_skipped": r["hh_remittance_skipped"],
                "card_settlement_matched": r["card_settlement_matched"],
                "card_settlement_skipped": r["card_settlement_skipped"],
                "direct_vendor_matched": r["direct_vendor_matched"],
                "direct_vendor_skipped": r["direct_vendor_skipped"],
                "total_matched": r["total_matched"],
                "total_skipped": r["total_skipped"],
                "status": r["status"],
                "error_text": r["error_text"],
                "actor_email": r["actor_email"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
            }
            for r in rows
        ],
    }


def get_auto_match_run_detail(
    session, *, entity_code: str, run_id: str
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(run_id, "run_id")
    row = session.execute(
        text(
            """
            SELECT * FROM auto_match_runs
             WHERE id = :id AND entity_id = :entity_id
             LIMIT 1
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not row:
        raise ValueError(f"No auto_match_run found for id {run_id}")

    detail_json = row["detail_json"]
    if isinstance(detail_json, str):
        try:
            detail_json = json.loads(detail_json)
        except Exception:
            detail_json = {}

    return {
        "entity_code": entity_code,
        "run": {
            "id": str(row["id"]),
            "triggered_by": row["triggered_by"],
            "trigger_source_id": str(row["trigger_source_id"]) if row["trigger_source_id"] else None,
            "period_start": row["period_start"].isoformat() if row["period_start"] else None,
            "period_end": row["period_end"].isoformat() if row["period_end"] else None,
            "hh_remittance_matched": row["hh_remittance_matched"],
            "hh_remittance_skipped": row["hh_remittance_skipped"],
            "card_settlement_matched": row["card_settlement_matched"],
            "card_settlement_skipped": row["card_settlement_skipped"],
            "direct_vendor_matched": row["direct_vendor_matched"],
            "direct_vendor_skipped": row["direct_vendor_skipped"],
            "total_matched": row["total_matched"],
            "total_skipped": row["total_skipped"],
            "status": row["status"],
            "error_text": row["error_text"],
            "actor_email": row["actor_email"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            "detail": detail_json or {},
        },
    }
