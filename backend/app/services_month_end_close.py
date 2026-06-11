"""
Month-End Close Control Center — service layer.

A single read-only aggregator that answers:
    "Can this period be closed?"

It pulls a status snapshot from every module that matters for close:
    - cash_balancing
    - hh_ap (statements / invoices / journal batch)
    - hh_ap_remittance_bank (remittances matched to bank withdrawals)
    - direct_vendor_ap
    - card_settlement
    - bank_review (any bank txns still 'new' / 'needs_review')
    - bank_data_sources (recency of QBO sync + CSV imports)
    - journal_batches (cross-module: anything pending approval)

Every section is wrapped in _has_table guards so the endpoint stays
useful even if some optional modules haven't been migrated yet on a
given environment.

The aggregator returns:
    blocking_items    - things that MUST be fixed before close
    warning_items     - things to look at but don't block close
    overall_close_readiness in:
        "closed_locked"  - period.status already says it's closed
        "blocked"        - one or more blockers
        "needs_review"   - no blockers but warnings exist
        "ready"          - no blockers, no warnings
        "not_started"    - no period record, no batches, no data
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import _has_table, get_entity_by_code


READINESS_CLOSED_LOCKED = "closed_locked"
READINESS_BLOCKED = "blocked"
READINESS_NEEDS_REVIEW = "needs_review"
READINESS_READY = "ready"
READINESS_NOT_STARTED = "not_started"

SEVERITY_BLOCKER = "blocker"
SEVERITY_WARNING = "warning"

CLOSED_PERIOD_STATUSES = {"closed_locked", "closed", "locked"}


def _money(value: Any) -> str:
    if value is None:
        return "0.00"
    try:
        return str(Decimal(str(value)).quantize(Decimal("0.01")))
    except Exception:
        return str(value)


def _money_float(value: Any) -> float:
    try:
        return float(Decimal(str(value or 0)).quantize(Decimal("0.01")))
    except Exception:
        return 0.0


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


# ----------------------------------------------------------------------
# Period resolver
# ----------------------------------------------------------------------


def _get_accounting_period(session, entity_id: UUID, period_end: date) -> dict[str, Any] | None:
    return session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, status
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND period_end = :period_end
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()


# ----------------------------------------------------------------------
# Section: cash balancing
# ----------------------------------------------------------------------


def _section_cash_balancing(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "cash_balancing_days"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "cash_balancing_days table not present",
        }

    day_row = session.execute(
        text(
            """
            SELECT COUNT(*) AS day_count,
                   COUNT(*) FILTER (WHERE accounting_period_id IS NOT NULL) AS period_linked_day_count,
                   MIN(business_date) AS earliest_day,
                   MAX(business_date) AS latest_day
            FROM cash_balancing_days
            WHERE entity_id = :entity_id
              AND business_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()

    pending_lines = 0
    mapped_lines = 0
    if _has_table(session, "cash_balancing_lines"):
        line_row = session.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE l.translation_status = 'pending') AS pending_lines,
                    COUNT(*) FILTER (WHERE l.translation_status = 'mapped')  AS mapped_lines
                FROM cash_balancing_lines l
                JOIN cash_balancing_days d ON d.id = l.cash_balancing_day_id
                WHERE d.entity_id = :entity_id
                  AND d.business_date BETWEEN :period_start AND :period_end
                """
            ),
            {
                "entity_id": entity_id,
                "period_start": period_start,
                "period_end": period_end,
            },
        ).mappings().first()
        pending_lines = (line_row or {}).get("pending_lines", 0) or 0
        mapped_lines = (line_row or {}).get("mapped_lines", 0) or 0

    day_count = (day_row or {}).get("day_count", 0) or 0
    if day_count == 0:
        status = "blocked"
        summary = "No cash-balancing days imported for this period"
    elif pending_lines > 0:
        status = "needs_review"
        summary = f"{pending_lines} cash-balancing line(s) still 'pending' translation"
    else:
        status = "ready"
        summary = f"{day_count} cash-balancing day(s) imported"

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "day_count": day_count,
        "period_linked_day_count": (day_row or {}).get("period_linked_day_count", 0) or 0,
        "earliest_day": _iso((day_row or {}).get("earliest_day")),
        "latest_day": _iso((day_row or {}).get("latest_day")),
        "pending_lines": pending_lines,
        "mapped_lines": mapped_lines,
    }


# ----------------------------------------------------------------------
# Section: HH AP (statements / invoices / journal)
# ----------------------------------------------------------------------


def _section_hh_ap(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    has_invoices = _has_table(session, "hh_ap_invoices")
    has_statements = _has_table(session, "hh_ap_statements")
    has_remittances = _has_table(session, "hh_ap_remittances")
    has_journal_batches = _has_table(session, "journal_batches")

    if not (has_invoices or has_statements):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "HH AP tables not present",
        }

    statement_count = 0
    if has_statements:
        statement_count = (
            session.execute(
                text(
                    """
                    SELECT COUNT(*) AS c
                    FROM hh_ap_statements
                    WHERE entity_id = :entity_id
                      AND statement_date BETWEEN :period_start AND :period_end
                    """
                ),
                {
                    "entity_id": entity_id,
                    "period_start": period_start,
                    "period_end": period_end,
                },
            ).mappings().first()
            or {}
        ).get("c", 0) or 0

    invoice_summary: dict[str, Any] = {}
    if has_invoices:
        invoice_summary = (
            session.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS invoice_count,
                        COUNT(*) FILTER (WHERE match_status = 'matched') AS matched_count,
                        COUNT(*) FILTER (WHERE match_status <> 'matched') AS unmatched_count,
                        COUNT(*) FILTER (WHERE is_statement_only = TRUE) AS statement_only_count
                    FROM hh_ap_invoices
                    WHERE entity_id = :entity_id
                      AND invoice_date BETWEEN :period_start AND :period_end
                    """
                ),
                {
                    "entity_id": entity_id,
                    "period_start": period_start,
                    "period_end": period_end,
                },
            ).mappings().first()
            or {}
        )

    remittance_count = 0
    if has_remittances:
        remittance_count = (
            session.execute(
                text(
                    """
                    SELECT COUNT(*) AS c
                    FROM hh_ap_remittances
                    WHERE entity_id = :entity_id
                      AND COALESCE(withdrawal_date, remittance_date)
                          BETWEEN :period_start AND :period_end
                    """
                ),
                {
                    "entity_id": entity_id,
                    "period_start": period_start,
                    "period_end": period_end,
                },
            ).mappings().first()
            or {}
        ).get("c", 0) or 0

    # Journal batch for HH AP for this period
    journal_batch: dict[str, Any] | None = None
    if has_journal_batches:
        journal_batch = (
            session.execute(
                text(
                    """
                    SELECT id, batch_label, status, workflow_status,
                           total_debits, total_credits, summary_json,
                           submitted_at, approved_at, approved_by
                    FROM journal_batches
                    WHERE entity_id = :entity_id
                      AND accounting_period_id = :accounting_period_id
                      AND source_module = 'hh_ap'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "entity_id": entity_id,
                    "accounting_period_id": accounting_period_id,
                },
            ).mappings().first()
        )

    journal_block: dict[str, Any] | None = None
    if journal_batch:
        total_debits = Decimal(str(journal_batch["total_debits"] or 0))
        total_credits = Decimal(str(journal_batch["total_credits"] or 0))
        balance_diff = total_debits - total_credits
        journal_block = {
            "id": str(journal_batch["id"]),
            "batch_label": journal_batch["batch_label"],
            "status": journal_batch["status"],
            "workflow_status": journal_batch["workflow_status"],
            "total_debits": _money_float(total_debits),
            "total_credits": _money_float(total_credits),
            "balance_difference": _money_float(balance_diff),
            "is_balanced": balance_diff == 0,
            "submitted_at": _iso(journal_batch["submitted_at"]),
            "approved_at": _iso(journal_batch["approved_at"]),
            "approved_by": journal_batch["approved_by"],
        }

    # Determine status
    if not journal_batch:
        status = "blocked"
        summary = "HH AP month-end journal batch has not been built yet"
    elif journal_block and not journal_block["is_balanced"]:
        status = "blocked"
        summary = (
            f"HH AP journal batch is unbalanced "
            f"(diff {journal_block['balance_difference']})"
        )
    elif journal_batch["workflow_status"] not in {"approved_to_post", "posted"}:
        status = "blocked"
        summary = (
            f"HH AP journal batch is not yet approved "
            f"(workflow_status={journal_batch['workflow_status']})"
        )
    elif (invoice_summary or {}).get("unmatched_count", 0):
        status = "needs_review"
        summary = (
            f"HH AP journal approved but "
            f"{invoice_summary['unmatched_count']} unmatched HH invoice(s) remain"
        )
    else:
        status = "ready"
        summary = "HH AP journal approved; invoice matching clean"

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "statement_count": statement_count,
        "remittance_count": remittance_count,
        "invoice_summary": dict(invoice_summary) if invoice_summary else {},
        "journal_batch": journal_block,
    }


# ----------------------------------------------------------------------
# Section: HH remittance-to-bank match
# ----------------------------------------------------------------------


def _section_hh_remittance_bank(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "hh_ap_remittances"):
        return {"status": "no_data", "module_present": False, "summary": "no hh_ap_remittances table"}

    has_match_table = _has_table(session, "bank_transaction_matches")

    rows = session.execute(
        text(
            """
            SELECT
                r.id,
                r.remittance_reference,
                COALESCE(r.withdrawal_date, r.remittance_date) AS effective_date,
                r.total_amount
            FROM hh_ap_remittances r
            WHERE r.entity_id = :entity_id
              AND COALESCE(r.withdrawal_date, r.remittance_date)
                  BETWEEN :period_start AND :period_end
            ORDER BY effective_date
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().all()

    total = len(rows)
    if total == 0:
        return {
            "status": "no_data",
            "module_present": True,
            "summary": "No HH remittances dated in this period",
            "remittance_count": 0,
            "matched_count": 0,
            "unmatched_count": 0,
            "unmatched_total_amount": "0.00",
        }

    matched_count = 0
    if has_match_table:
        matched_ids: set[str] = set()
        ids = [str(r["id"]) for r in rows]
        match_rows = session.execute(
            text(
                """
                SELECT target_record_id
                FROM bank_transaction_matches
                WHERE entity_id = :entity_id
                  AND active = TRUE
                  AND target_table_name = 'hh_ap_remittances'
                  AND target_record_id = ANY(:ids)
                """
            ),
            {"entity_id": entity_id, "ids": ids},
        ).mappings().all()
        matched_ids = {str(m["target_record_id"]) for m in match_rows}
        matched_count = len(matched_ids)

    unmatched_count = total - matched_count
    unmatched_total = sum(
        Decimal(str(r["total_amount"] or 0))
        for r in rows
        if str(r["id"]) not in (matched_ids if has_match_table else set())
    )

    if unmatched_count == 0:
        status = "ready"
        summary = f"All {total} remittance(s) matched to bank"
    else:
        status = "needs_review"
        summary = (
            f"{unmatched_count} of {total} remittance(s) not matched to a "
            f"bank withdrawal (total ${_money(unmatched_total)})"
        )

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "remittance_count": total,
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "unmatched_total_amount": _money(unmatched_total),
    }


# ----------------------------------------------------------------------
# Section: direct vendor AP
# ----------------------------------------------------------------------


def _section_direct_vendor_ap(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "direct_vendor_ap_invoices"):
        return {"status": "no_data", "module_present": False, "summary": "module not present"}

    today = date.today()
    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE payment_status = 'unpaid') AS unpaid_count,
                COUNT(*) FILTER (WHERE payment_status = 'partial') AS partial_count,
                COUNT(*) FILTER (WHERE payment_status = 'paid')   AS paid_count,
                COUNT(*) FILTER (WHERE due_date IS NOT NULL AND due_date < :today
                                  AND payment_status <> 'paid')   AS overdue_count,
                COALESCE(SUM(total_amount), 0)                    AS total_amount,
                COALESCE(SUM(open_amount), 0)                     AS open_amount
            FROM direct_vendor_ap_invoices
            WHERE entity_id = :entity_id
              AND active = TRUE
              AND invoice_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
            "today": today,
        },
    ).mappings().first() or {}

    total_count = row.get("total_count", 0) or 0
    unpaid_count = row.get("unpaid_count", 0) or 0
    overdue_count = row.get("overdue_count", 0) or 0
    open_amount = row.get("open_amount", 0) or 0

    if total_count == 0:
        status = "no_data"
        summary = "No direct vendor invoices dated in this period"
    elif overdue_count > 0:
        status = "needs_review"
        summary = (
            f"{overdue_count} overdue direct vendor invoice(s); "
            f"open balance ${_money(open_amount)}"
        )
    elif unpaid_count > 0:
        status = "needs_review"
        summary = f"{unpaid_count} unpaid direct vendor invoice(s)"
    else:
        status = "ready"
        summary = f"All {total_count} direct vendor invoice(s) paid"

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "total_count": total_count,
        "unpaid_count": unpaid_count,
        "partial_count": row.get("partial_count", 0) or 0,
        "paid_count": row.get("paid_count", 0) or 0,
        "overdue_count": overdue_count,
        "total_amount": _money(row.get("total_amount", 0)),
        "open_amount": _money(open_amount),
    }


# ----------------------------------------------------------------------
# Section: card settlement
# ----------------------------------------------------------------------


def _section_card_settlement(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "card_settlement_batches"):
        return {"status": "no_data", "module_present": False, "summary": "module not present"}

    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE matched_bank_amount >= net_deposit_amount) AS matched_count,
                COUNT(*) FILTER (WHERE matched_bank_amount <  net_deposit_amount) AS unmatched_count,
                COALESCE(SUM(net_deposit_amount), 0)        AS total_net_deposit,
                COALESCE(SUM(matched_bank_amount), 0)       AS total_matched,
                COALESCE(SUM(net_deposit_amount), 0)
                  - COALESCE(SUM(matched_bank_amount), 0)   AS unmatched_amount
            FROM card_settlement_batches
            WHERE entity_id = :entity_id
              AND active = TRUE
              AND business_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first() or {}

    total_count = row.get("total_count", 0) or 0
    unmatched_count = row.get("unmatched_count", 0) or 0
    unmatched_amount = row.get("unmatched_amount", 0) or 0

    if total_count == 0:
        status = "no_data"
        summary = "No card settlement batches in this period"
    elif unmatched_count == 0:
        status = "ready"
        summary = f"All {total_count} card settlement batch(es) matched to bank"
    else:
        status = "needs_review"
        summary = (
            f"{unmatched_count} of {total_count} card settlement batch(es) "
            f"not fully matched (${_money(unmatched_amount)} unmatched)"
        )

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "total_count": total_count,
        "matched_count": row.get("matched_count", 0) or 0,
        "unmatched_count": unmatched_count,
        "total_net_deposit": _money(row.get("total_net_deposit", 0)),
        "total_matched": _money(row.get("total_matched", 0)),
        "unmatched_amount": _money(unmatched_amount),
    }


# ----------------------------------------------------------------------
# Section: bank review (txns still 'new' / 'needs_review')
# ----------------------------------------------------------------------


def _section_bank_review(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "bank_transactions"):
        return {"status": "no_data", "module_present": False, "summary": "no bank_transactions table"}

    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE review_status = 'new')          AS new_count,
                COUNT(*) FILTER (WHERE review_status = 'needs_review') AS needs_review_count,
                COUNT(*) FILTER (WHERE review_status = 'matched')      AS matched_count,
                COUNT(*) FILTER (WHERE review_status = 'ignored')      AS ignored_count
            FROM bank_transactions
            WHERE entity_id = :entity_id
              AND transaction_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first() or {}

    total_count = row.get("total_count", 0) or 0
    new_count = row.get("new_count", 0) or 0
    needs_review_count = row.get("needs_review_count", 0) or 0

    if total_count == 0:
        status = "blocked"
        summary = "No bank transactions imported for this period"
    elif new_count + needs_review_count == 0:
        status = "ready"
        summary = f"All {total_count} bank transaction(s) reviewed"
    else:
        status = "needs_review"
        summary = (
            f"{new_count + needs_review_count} bank transaction(s) still need review"
        )

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "total_count": total_count,
        "new_count": new_count,
        "needs_review_count": needs_review_count,
        "matched_count": row.get("matched_count", 0) or 0,
        "ignored_count": row.get("ignored_count", 0) or 0,
    }


# ----------------------------------------------------------------------
# Section: bank reconciliation (Phase 3C) — a locked, tied rec on the
# cash account is required to close. Draft/untied is a warning.
# ----------------------------------------------------------------------


def _section_bank_reconciliation(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
) -> dict[str, Any]:
    if not _has_table(session, "bank_reconciliations"):
        return {"status": "no_data", "module_present": False, "summary": "no bank_reconciliations table"}

    rows = session.execute(
        text(
            """
            SELECT source_account_code, status, ties, variance, statement_date,
                   statement_closing_balance, locked_at
              FROM bank_reconciliations
             WHERE entity_id = :e AND accounting_period_id = :pid
          ORDER BY source_account_code
            """
        ),
        {"e": entity_id, "pid": accounting_period_id},
    ).mappings().all()

    recs = [
        {
            "source_account_code": r["source_account_code"],
            "status": r["status"],
            "ties": r["ties"],
            "variance": _money_float(r["variance"]),
            "statement_date": _iso(r["statement_date"]),
            "statement_closing_balance": _money_float(r["statement_closing_balance"]),
            "locked_at": _iso(r["locked_at"]),
        }
        for r in rows
    ]

    if not recs:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": "No bank reconciliation for this period — reconcile the cash account before close",
            "reconciliations": [],
        }

    locked_tied = [r for r in recs if r["status"] == "locked" and r["ties"]]
    untied = [r for r in recs if not r["ties"]]
    draft = [r for r in recs if r["status"] != "locked"]

    if untied:
        status = "blocked"
        summary = (
            f"{len(untied)} reconciliation(s) do not tie "
            f"(account {', '.join(r['source_account_code'] for r in untied)})"
        )
    elif draft:
        status = "needs_review"
        summary = (
            f"{len(draft)} tied reconciliation(s) still in draft — lock before close "
            f"(account {', '.join(r['source_account_code'] for r in draft)})"
        )
    else:
        status = "ready"
        summary = f"{len(locked_tied)} reconciliation(s) locked and tied"

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "reconciliations": recs,
    }


# ----------------------------------------------------------------------
# Section: bank data sources (QBO sync + CSV imports)
# ----------------------------------------------------------------------


def _section_bank_data_sources(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    qbo_block: dict[str, Any] = {"present": False, "imported_count": 0, "last_imported_at": None}
    csv_block: dict[str, Any] = {"present": False, "run_count": 0, "last_run_at": None, "inserted_count": 0}

    if _has_table(session, "bank_transactions"):
        qbo_row = session.execute(
            text(
                """
                SELECT COUNT(*) AS c, MAX(imported_at) AS last_at
                FROM bank_transactions
                WHERE entity_id = :entity_id
                  AND source_system = 'quickbooks'
                  AND transaction_date BETWEEN :period_start AND :period_end
                """
            ),
            {
                "entity_id": entity_id,
                "period_start": period_start,
                "period_end": period_end,
            },
        ).mappings().first() or {}
        qbo_block = {
            "present": True,
            "imported_count": qbo_row.get("c", 0) or 0,
            "last_imported_at": _iso(qbo_row.get("last_at")),
        }

    if _has_table(session, "bank_csv_import_runs"):
        csv_row = session.execute(
            text(
                """
                SELECT
                    COUNT(*)                              AS run_count,
                    COALESCE(SUM(inserted_count), 0)      AS inserted_count,
                    MAX(created_at)                       AS last_run_at
                FROM bank_csv_import_runs
                WHERE entity_id = :entity_id
                  AND (
                        (earliest_transaction_date IS NULL AND latest_transaction_date IS NULL)
                     OR (earliest_transaction_date <= :period_end
                         AND latest_transaction_date >= :period_start)
                  )
                """
            ),
            {
                "entity_id": entity_id,
                "period_start": period_start,
                "period_end": period_end,
            },
        ).mappings().first() or {}
        csv_block = {
            "present": True,
            "run_count": csv_row.get("run_count", 0) or 0,
            "inserted_count": csv_row.get("inserted_count", 0) or 0,
            "last_run_at": _iso(csv_row.get("last_run_at")),
        }

    has_any_bank_data = (qbo_block["imported_count"] or 0) + (csv_block["inserted_count"] or 0) > 0
    if not has_any_bank_data:
        status = "blocked"
        summary = "No bank data (QBO sync or CSV import) covering this period"
    else:
        status = "ready"
        summary = (
            f"{qbo_block['imported_count']} QBO + {csv_block['inserted_count']} CSV "
            f"bank transaction(s) covering this period"
        )

    return {
        "status": status,
        "summary": summary,
        "qbo_bank_sync": qbo_block,
        "csv_imports": csv_block,
    }


# ----------------------------------------------------------------------
# Section: journal batches across modules
# ----------------------------------------------------------------------


def _section_journal_batches(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
) -> dict[str, Any]:
    if not _has_table(session, "journal_batches"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "journal_batches table not present",
            "batches": [],
        }

    rows = session.execute(
        text(
            """
            SELECT id, source_module, batch_label, status, workflow_status,
                   total_debits, total_credits,
                   submitted_at, approved_at, approved_by, locked_at, locked_by
            FROM journal_batches
            WHERE entity_id = :entity_id
              AND accounting_period_id = :accounting_period_id
            ORDER BY source_module, batch_label, created_at
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().all()

    batches: list[dict[str, Any]] = []
    pending_count = 0
    unbalanced_count = 0
    approved_count = 0
    voided_count = 0

    for row in rows:
        total_debits = Decimal(str(row["total_debits"] or 0))
        total_credits = Decimal(str(row["total_credits"] or 0))
        balance_diff = total_debits - total_credits
        is_balanced = balance_diff == 0
        wf = row["workflow_status"]
        st = row["status"]

        is_voided = (wf == "voided") or (st == "voided")
        if is_voided:
            voided_count += 1
        elif wf in {"approved_to_post", "posted"}:
            approved_count += 1
        else:
            pending_count += 1
        # Voided batches are out-of-band; their balance state doesn't
        # block close (they have no journal_lines anyway).
        if not is_balanced and not is_voided:
            unbalanced_count += 1

        batches.append(
            {
                "id": str(row["id"]),
                "source_module": row["source_module"],
                "batch_label": row["batch_label"],
                "status": row["status"],
                "workflow_status": wf,
                "total_debits": _money_float(total_debits),
                "total_credits": _money_float(total_credits),
                "balance_difference": _money_float(balance_diff),
                "is_balanced": is_balanced,
                "submitted_at": _iso(row["submitted_at"]),
                "approved_at": _iso(row["approved_at"]),
                "approved_by": row["approved_by"],
                "locked_at": _iso(row["locked_at"]),
                "locked_by": row["locked_by"],
            }
        )

    if not batches:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": "No journal batches built for this period",
            "batches": [],
            "pending_count": 0,
            "unbalanced_count": 0,
            "approved_count": 0,
            "voided_count": 0,
        }

    if unbalanced_count > 0:
        status = "blocked"
        summary = f"{unbalanced_count} journal batch(es) are unbalanced"
    elif pending_count > 0:
        status = "blocked"
        summary = f"{pending_count} journal batch(es) not yet approved_to_post"
    else:
        status = "ready"
        summary = f"All {approved_count} journal batch(es) approved"
    if voided_count > 0:
        summary = f"{summary} ({voided_count} voided ignored)"

    return {
        "status": status,
        "module_present": True,
        "summary": summary,
        "batches": batches,
        "pending_count": pending_count,
        "unbalanced_count": unbalanced_count,
        "approved_count": approved_count,
        "voided_count": voided_count,
    }


# ----------------------------------------------------------------------
# Section: payroll control
# ----------------------------------------------------------------------


def _section_payroll(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """
    Wraps services_payroll.section_payroll() — the new (migration 024)
    payroll module's close-center summary. Reports blocked when no
    approved payroll run covers the period.
    """
    if not _has_table(session, "payroll_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "payroll_runs table not present",
        }
    from .services_payroll import section_payroll  # noqa: WPS433

    return section_payroll(
        session,
        entity_id=entity_id,
        period_start=period_start,
        period_end=period_end,
    )


# ----------------------------------------------------------------------
# Section: POS month-end reports (pos_financial / inventory_value / aged_ar)
# ----------------------------------------------------------------------


def _section_pos_reports(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """
    Wraps services_pos_import.section_pos_reports() into the standard
    section shape. Imported lazily so environments without 014 applied
    don't fail at import time.
    """
    if not _has_table(session, "pos_import_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "pos_import_runs table not present",
        }
    from .services_pos_import import section_pos_reports  # noqa: WPS433

    return section_pos_reports(
        session,
        entity_id=entity_id,
        period_start=period_start,
        period_end=period_end,
    )


# ----------------------------------------------------------------------
# Section: AR aging snapshot
# ----------------------------------------------------------------------


def _section_ar_aging(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """
    Checks whether an AR aging snapshot has been uploaded for this period.
    Missing → needs_review (WARNING, not a blocker — AR aging is important
    context but doesn't block the GL from closing).
    Present → ready, with the snapshot date and total AR for the summary.
    """
    if not _has_table(session, "aged_ar_snapshots"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "aged_ar_snapshots table not present",
        }

    row = session.execute(
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

    if not row:
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": "No AR aging snapshot uploaded for this period",
            "snapshot_date": None,
            "total_ar": None,
        }

    return {
        "status": "ready",
        "module_present": True,
        "summary": (
            f"AR aging as of {row['snapshot_date'].isoformat()}, "
            f"total ${_money(row['total_ar'])}"
        ),
        "snapshot_date": row["snapshot_date"].isoformat(),
        "total_ar": _money_float(row["total_ar"]),
    }


def _section_pos_financial_validation(
    session,
    entity_id: UUID,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """
    Wraps services_pos_import.section_pos_financial_validation() — runs
    the monthly-POS vs daily-cash-balancing variance check. Pure read.
    """
    if not _has_table(session, "pos_financial_snapshots"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "pos_financial_snapshots table not present",
        }
    if not _has_table(session, "cash_balancing_lines"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": (
                "cash_balancing_lines table not present; cannot validate "
                "POS monthly against daily cash balancing."
            ),
        }
    from .services_pos_import import section_pos_financial_validation  # noqa: WPS433

    return section_pos_financial_validation(
        session,
        entity_id=entity_id,
        period_start=period_start,
        period_end=period_end,
    )


# ----------------------------------------------------------------------
# Sections: GL import / depreciation / accruals (lazy-imported so close
# control center keeps working when those migrations haven't run yet)
# ----------------------------------------------------------------------


def _section_gl_import_wrapper(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "gl_import_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "gl_import_runs table not present",
        }
    from .services_gl_import import section_gl_import  # noqa: WPS433

    return section_gl_import(
        session,
        entity_id=entity_id,
        accounting_period_id=accounting_period_id,
        period_end=period_end,
    )


def _section_depreciation_wrapper(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "fixed_assets"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "fixed_assets table not present",
        }
    from .services_depreciation import section_depreciation  # noqa: WPS433

    return section_depreciation(
        session,
        entity_id=entity_id,
        accounting_period_id=accounting_period_id,
        period_end=period_end,
    )


def _section_accruals_wrapper(
    session,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "accrual_templates"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "accrual_templates table not present",
        }
    from .services_accruals import section_accruals  # noqa: WPS433

    return section_accruals(
        session,
        entity_id=entity_id,
        accounting_period_id=accounting_period_id,
        period_end=period_end,
    )


def _section_cogs_wrapper(
    session,
    entity_id: UUID,
    accounting_period_id: UUID | None,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "cogs_journal_inputs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "cogs_journal_inputs table not present",
        }
    from .services_cogs import section_cogs  # noqa: WPS433

    return section_cogs(
        session,
        entity_id=entity_id,
        accounting_period_id=accounting_period_id,
        period_start=period_start,
        period_end=period_end,
    )


def _section_recurring_entries_wrapper(
    session,
    entity_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    """Close-readiness section: recurring journal-entry engine (Module A)."""
    if not _has_table(session, "recurring_entry_templates"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "recurring_entry_templates table not present",
        }
    from .services_recurring_entries import get_month_end_status  # noqa: WPS433

    return get_month_end_status(session, entity_id=entity_id, period_end=period_end)


# ----------------------------------------------------------------------
# Roll-up
# ----------------------------------------------------------------------


def _collect_blocking_and_warning_items(sections: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocking: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for category, section in sections.items():
        if not isinstance(section, dict):
            continue
        status = section.get("status")
        summary = section.get("summary") or ""
        if status == "blocked":
            blocking.append(
                {
                    "category": category,
                    "severity": SEVERITY_BLOCKER,
                    "summary": summary,
                }
            )
        elif status == "needs_review":
            warnings.append(
                {
                    "category": category,
                    "severity": SEVERITY_WARNING,
                    "summary": summary,
                }
            )
        # 'ready', 'no_data' contribute nothing to either list

    return blocking, warnings


def _roll_up_readiness(
    period_status: str | None,
    blocking_items: list[dict[str, Any]],
    warning_items: list[dict[str, Any]],
    sections: dict[str, Any],
) -> str:
    if (period_status or "").lower() in CLOSED_PERIOD_STATUSES:
        return READINESS_CLOSED_LOCKED
    if blocking_items:
        return READINESS_BLOCKED

    # If everything is no_data/empty, call it not_started
    nontrivial = False
    for section in sections.values():
        if not isinstance(section, dict):
            continue
        if section.get("status") in {"ready", "needs_review", "blocked"}:
            nontrivial = True
            break
    if not nontrivial:
        return READINESS_NOT_STARTED

    if warning_items:
        return READINESS_NEEDS_REVIEW
    return READINESS_READY


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------


def get_month_end_close_status(
    session,
    *,
    entity_code: str,
    period_end: str,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    try:
        period_end_date = date.fromisoformat(period_end)
    except ValueError as exc:
        raise ValueError(f"period_end must be YYYY-MM-DD, got {period_end}") from exc

    period = _get_accounting_period(session, entity["id"], period_end_date)
    if not period:
        # Period not in accounting_periods at all → degenerate response
        return {
            "entity_code": entity_code,
            "entity_name": entity.get("entity_name"),
            "as_of": datetime.now(timezone.utc).isoformat(),
            "period": None,
            "overall_close_readiness": READINESS_NOT_STARTED,
            "blocking_items": [
                {
                    "category": "accounting_period",
                    "severity": SEVERITY_BLOCKER,
                    "summary": (
                        f"No accounting_periods row for entity={entity_code} "
                        f"period_end={period_end}"
                    ),
                }
            ],
            "warning_items": [],
            "sections": {},
        }

    period_start = period["period_start"]
    accounting_period_id = period["id"]

    sections = {
        "cash_balancing": _section_cash_balancing(
            session, entity["id"], period_start, period_end_date
        ),
        "hh_ap": _section_hh_ap(
            session, entity["id"], accounting_period_id, period_start, period_end_date
        ),
        "hh_ap_remittance_bank": _section_hh_remittance_bank(
            session, entity["id"], period_start, period_end_date
        ),
        "direct_vendor_ap": _section_direct_vendor_ap(
            session, entity["id"], period_start, period_end_date
        ),
        "card_settlement": _section_card_settlement(
            session, entity["id"], period_start, period_end_date
        ),
        "bank_review": _section_bank_review(
            session, entity["id"], period_start, period_end_date
        ),
        "bank_reconciliation": _section_bank_reconciliation(
            session, entity["id"], accounting_period_id
        ),
        "bank_data_sources": _section_bank_data_sources(
            session, entity["id"], period_start, period_end_date
        ),
        "journal_batches": _section_journal_batches(
            session, entity["id"], accounting_period_id
        ),
        "payroll": _section_payroll(
            session, entity["id"], period_start, period_end_date
        ),
        "pos_reports": _section_pos_reports(
            session, entity["id"], period_start, period_end_date
        ),
        "ar_aging": _section_ar_aging(
            session, entity["id"], period_start, period_end_date
        ),
        "pos_financial_validation": _section_pos_financial_validation(
            session, entity["id"], period_start, period_end_date
        ),
        "gl_import": _section_gl_import_wrapper(
            session, entity["id"], accounting_period_id, period_end_date
        ),
        "depreciation": _section_depreciation_wrapper(
            session, entity["id"], accounting_period_id, period_end_date
        ),
        "accruals": _section_accruals_wrapper(
            session, entity["id"], accounting_period_id, period_end_date
        ),
        "cogs": _section_cogs_wrapper(
            session, entity["id"], accounting_period_id,
            period_start, period_end_date,
        ),
        "recurring_entries": _section_recurring_entries_wrapper(
            session, entity["id"], period_end_date
        ),
    }

    blocking_items, warning_items = _collect_blocking_and_warning_items(sections)
    overall = _roll_up_readiness(period.get("status"), blocking_items, warning_items, sections)

    return {
        "entity_code": entity_code,
        "entity_name": entity.get("entity_name"),
        "as_of": datetime.now(timezone.utc).isoformat(),
        "period": {
            "id": str(period["id"]),
            "period_label": period["period_label"],
            "period_start": _iso(period["period_start"]),
            "period_end": _iso(period["period_end"]),
            "status": period.get("status"),
        },
        "overall_close_readiness": overall,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "sections": sections,
    }
