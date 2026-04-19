from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/month-end/hh-ap", tags=["month-end", "hh-ap-month-end"])

HH_AP_SOURCE_MODULE = "hh_ap"
HH_AP_BATCH_LABEL = "hh_ap_month_end"
HH_AP_DEFAULT_MEMO = "HHSL Statement"
CONTROL_TOLERANCE_DEFAULT = Decimal("0.05")

ACCOUNT_1120_INVENTORY = "1120"
ACCOUNT_2300_HST_PAYABLE = "2300"
ACCOUNT_1320_FIVE_YEAR_NOTES = "1320"
ACCOUNT_1330_SPECIAL_SHARES = "1330"
ACCOUNT_6210_ADVERTISING = "6210"
ACCOUNT_2030_AP_HHSL = "2030"

STATEMENT_LABEL_TOTAL_PURCHASES = "Total Purchases"
STATEMENT_LABEL_GST_HST = "GST/HST"
STATEMENT_LABEL_SPECIAL_SHARES = "Special Shares - Subscribed For"
STATEMENT_LABEL_FIVE_YEAR_NOTES = "Five Yr Notes - Subscribed For"
STATEMENT_LABEL_ADVERTISING = "Advertising"

GL_EXPORT_SIGN_BY_ACCOUNT = {
    ACCOUNT_1120_INVENTORY: Decimal("1"),
    ACCOUNT_2300_HST_PAYABLE: Decimal("-1"),
    ACCOUNT_1320_FIVE_YEAR_NOTES: Decimal("1"),
    ACCOUNT_1330_SPECIAL_SHARES: Decimal("1"),
    ACCOUNT_6210_ADVERTISING: Decimal("1"),
    ACCOUNT_2030_AP_HHSL: Decimal("1"),
}


class BuildHHAPMonthEndJournalRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    period_end: str = Field(..., examples=["2026-02-28"])
    statement_month_end: str | None = Field(default=None, examples=["2026-02-28"])
    batch_label: str = Field(default=HH_AP_BATCH_LABEL)
    batch_memo: str | None = Field(default=HH_AP_DEFAULT_MEMO)
    control_tolerance: Decimal = Field(default=CONTROL_TOLERANCE_DEFAULT)


class ReviewHHAPMonthEndQuery(BaseModel):
    entity_code: str
    period_end: str
    batch_label: str = HH_AP_BATCH_LABEL


def money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def money_float(value: Decimal | None) -> float:
    return float(money(value))


def abs_money(value: Decimal) -> Decimal:
    return abs(money(value))


def bool_status(ok: bool) -> str:
    return "ok" if ok else "exception"


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


def get_accounting_period(session, entity_id: str, period_end: str):
    period = session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, fiscal_year, fiscal_period_number, status
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND period_end = :period_end
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()

    if not period:
        raise HTTPException(status_code=404, detail=f"No accounting period found for period_end={period_end}")

    return period


def get_hh_ap_statement(session, entity_id: str, statement_month_end: str):
    row = session.execute(
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
        {"entity_id": entity_id, "statement_month_end": statement_month_end},
    ).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No HH AP statement found for statement_month_end={statement_month_end}",
        )

    return row


def require_statement_dict(raw_json: Any) -> dict[str, Any]:
    if raw_json is None:
        raise HTTPException(status_code=400, detail="HH AP statement raw_json is missing")

    if isinstance(raw_json, dict):
        return raw_json

    if isinstance(raw_json, str):
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="HH AP statement raw_json is not valid JSON") from exc
        if isinstance(parsed, dict):
            return parsed

    raise HTTPException(status_code=400, detail="HH AP statement raw_json is not a JSON object")


def get_statement_component_this_month(statement_json: dict[str, Any], label: str) -> Decimal:
    summary_components = statement_json.get("summary_components") or {}
    component = summary_components.get(label)
    if not isinstance(component, dict):
        raise HTTPException(status_code=400, detail=f"Statement summary component missing: {label}")

    if "this_month" not in component:
        raise HTTPException(status_code=400, detail=f"Statement summary component missing this_month value: {label}")

    return money(component.get("this_month"))


def get_statement_summary_balance(statement_json: dict[str, Any], label: str) -> Decimal:
    summary_balances = statement_json.get("summary_balances") or {}
    if label not in summary_balances:
        raise HTTPException(status_code=400, detail=f"Statement summary balance missing: {label}")
    return money(summary_balances.get(label))


def get_statement_due_bucket_total(statement_json: dict[str, Any]) -> Decimal:
    due_bucket_totals = statement_json.get("due_bucket_totals") or {}
    if not isinstance(due_bucket_totals, dict):
        return Decimal("0.00")
    return money(sum(money(value) for value in due_bucket_totals.values()))


def get_hh_ap_month_invoice_totals(session, entity_id: str, period_start: str, period_end: str) -> dict[str, Decimal | int]:
    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS invoice_count,
                COALESCE(SUM(COALESCE(total_amount, 0)), 0) AS total_amount,
                COALESCE(SUM(COALESCE(hst_amount, 0)), 0) AS hst_amount,
                COALESCE(SUM(COALESCE(subscribed_shares_amount, 0)), 0) AS subscribed_shares_amount,
                COALESCE(SUM(COALESCE(five_year_note_amount, 0)), 0) AS five_year_note_amount,
                COALESCE(SUM(COALESCE(advertising_amount, 0)), 0) AS advertising_amount,
                COALESCE(SUM(COALESCE(subtotal, 0)), 0) AS subtotal_amount,
                COALESCE(SUM(COALESCE(surcharge_amount, 0)), 0) AS surcharge_amount
            FROM hh_ap_invoices
            WHERE entity_id = :entity_id
              AND COALESCE(is_statement_only, FALSE) = FALSE
              AND invoice_date >= :period_start
              AND invoice_date <= :period_end
            """
        ),
        {"entity_id": entity_id, "period_start": period_start, "period_end": period_end},
    ).mappings().first()

    return {
        "invoice_count": int((row or {}).get("invoice_count", 0) or 0),
        "total_amount": money((row or {}).get("total_amount", 0)),
        "hst_amount": money((row or {}).get("hst_amount", 0)),
        "subscribed_shares_amount": money((row or {}).get("subscribed_shares_amount", 0)),
        "five_year_note_amount": money((row or {}).get("five_year_note_amount", 0)),
        "advertising_amount": money((row or {}).get("advertising_amount", 0)),
        "subtotal_amount": money((row or {}).get("subtotal_amount", 0)),
        "surcharge_amount": money((row or {}).get("surcharge_amount", 0)),
    }


def get_hh_ap_invoice_type_totals(session, entity_id: str, period_start: str, period_end: str) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                invoice_type,
                COUNT(*) AS invoice_count,
                COALESCE(SUM(COALESCE(total_amount, 0)), 0) AS total_amount
            FROM hh_ap_invoices
            WHERE entity_id = :entity_id
              AND COALESCE(is_statement_only, FALSE) = FALSE
              AND invoice_date >= :period_start
              AND invoice_date <= :period_end
            GROUP BY invoice_type
            ORDER BY invoice_type
            """
        ),
        {"entity_id": entity_id, "period_start": period_start, "period_end": period_end},
    ).mappings().all()

    return [
        {
            "invoice_type": row["invoice_type"],
            "invoice_count": int(row["invoice_count"] or 0),
            "total_amount": money_float(row["total_amount"]),
        }
        for row in rows
    ]


def get_hh_ap_invoice_type_component_totals(
    session,
    entity_id: str,
    period_start: str,
    period_end: str,
) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                invoice_type,
                COUNT(*) AS invoice_count,
                COALESCE(SUM(COALESCE(total_amount, 0)), 0) AS total_amount,
                COALESCE(SUM(COALESCE(subtotal, 0)), 0) AS subtotal_amount,
                COALESCE(SUM(COALESCE(hst_amount, 0)), 0) AS hst_amount,
                COALESCE(SUM(COALESCE(subscribed_shares_amount, 0)), 0) AS special_shares_amount,
                COALESCE(SUM(COALESCE(five_year_note_amount, 0)), 0) AS five_year_note_amount,
                COALESCE(SUM(COALESCE(advertising_amount, 0)), 0) AS advertising_amount,
                COUNT(*) FILTER (
                    WHERE COALESCE(jsonb_array_length(COALESCE(raw_json -> 'parser_warnings', '[]'::jsonb)), 0) > 0
                ) AS warning_invoice_count,
                COALESCE(SUM(
                    CASE
                        WHEN COALESCE(jsonb_array_length(COALESCE(raw_json -> 'parser_warnings', '[]'::jsonb)), 0) > 0
                        THEN COALESCE(total_amount, 0)
                        ELSE 0
                    END
                ), 0) AS warning_total_amount
            FROM hh_ap_invoices
            WHERE entity_id = :entity_id
              AND COALESCE(is_statement_only, FALSE) = FALSE
              AND invoice_date >= :period_start
              AND invoice_date <= :period_end
            GROUP BY invoice_type
            ORDER BY invoice_type
            """
        ),
        {"entity_id": entity_id, "period_start": period_start, "period_end": period_end},
    ).mappings().all()

    return [
        {
            "invoice_type": row["invoice_type"],
            "invoice_count": int(row["invoice_count"] or 0),
            "total_amount": money_float(row["total_amount"]),
            "subtotal_amount": money_float(row["subtotal_amount"]),
            "hst_amount": money_float(row["hst_amount"]),
            "special_shares_amount": money_float(row["special_shares_amount"]),
            "five_year_note_amount": money_float(row["five_year_note_amount"]),
            "advertising_amount": money_float(row["advertising_amount"]),
            "warning_invoice_count": int(row["warning_invoice_count"] or 0),
            "warning_total_amount": money_float(row["warning_total_amount"]),
        }
        for row in rows
    ]


def get_hh_ap_invoices_with_parser_warnings(
    session,
    entity_id: str,
    period_start: str,
    period_end: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                i.invoice_number,
                i.invoice_type,
                i.invoice_date,
                i.due_date,
                i.total_amount,
                i.subtotal,
                i.hst_amount,
                i.subscribed_shares_amount,
                i.five_year_note_amount,
                i.advertising_amount,
                i.raw_json -> 'parser_warnings' AS parser_warnings,
                d.source_filename
            FROM hh_ap_invoices i
            LEFT JOIN hh_ap_documents d
              ON d.id = i.document_id
            WHERE i.entity_id = :entity_id
              AND COALESCE(i.is_statement_only, FALSE) = FALSE
              AND i.invoice_date >= :period_start
              AND i.invoice_date <= :period_end
              AND COALESCE(jsonb_array_length(COALESCE(i.raw_json -> 'parser_warnings', '[]'::jsonb)), 0) > 0
            ORDER BY ABS(COALESCE(i.total_amount, 0)) DESC, i.invoice_date, i.invoice_number
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
            "limit": limit,
        },
    ).mappings().all()

    return [
        {
            "invoice_number": row["invoice_number"],
            "invoice_type": row["invoice_type"],
            "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
            "due_date": str(row["due_date"]) if row["due_date"] else None,
            "total_amount": money_float(row["total_amount"]),
            "subtotal": money_float(row["subtotal"]),
            "hst_amount": money_float(row["hst_amount"]),
            "special_shares_amount": money_float(row["subscribed_shares_amount"]),
            "five_year_note_amount": money_float(row["five_year_note_amount"]),
            "advertising_amount": money_float(row["advertising_amount"]),
            "parser_warnings": row["parser_warnings"] if isinstance(row["parser_warnings"], list) else (row["parser_warnings"] or []),
            "source_filename": row["source_filename"],
        }
        for row in rows
    ]


def get_hh_ap_top_invoices_by_total_amount(
    session,
    entity_id: str,
    period_start: str,
    period_end: str,
    *,
    direction: str,
    limit: int = 25,
) -> list[dict[str, Any]]:
    if direction not in {"positive", "negative"}:
        raise HTTPException(status_code=400, detail="direction must be positive or negative")

    comparator = ">" if direction == "positive" else "<"
    ordering = "DESC" if direction == "positive" else "ASC"

    sql = f"""
        SELECT
            i.invoice_number,
            i.invoice_type,
            i.invoice_date,
            i.due_date,
            i.total_amount,
            i.subtotal,
            i.hst_amount,
            i.subscribed_shares_amount,
            i.five_year_note_amount,
            i.advertising_amount,
            i.raw_json -> 'parser_warnings' AS parser_warnings,
            d.source_filename
        FROM hh_ap_invoices i
        LEFT JOIN hh_ap_documents d
          ON d.id = i.document_id
        WHERE i.entity_id = :entity_id
          AND COALESCE(i.is_statement_only, FALSE) = FALSE
          AND i.invoice_date >= :period_start
          AND i.invoice_date <= :period_end
          AND COALESCE(i.total_amount, 0) {comparator} 0
        ORDER BY COALESCE(i.total_amount, 0) {ordering}, i.invoice_date, i.invoice_number
        LIMIT :limit
    """

    rows = session.execute(
        text(sql),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
            "limit": limit,
        },
    ).mappings().all()

    return [
        {
            "invoice_number": row["invoice_number"],
            "invoice_type": row["invoice_type"],
            "invoice_date": str(row["invoice_date"]) if row["invoice_date"] else None,
            "due_date": str(row["due_date"]) if row["due_date"] else None,
            "total_amount": money_float(row["total_amount"]),
            "subtotal": money_float(row["subtotal"]),
            "hst_amount": money_float(row["hst_amount"]),
            "special_shares_amount": money_float(row["subscribed_shares_amount"]),
            "five_year_note_amount": money_float(row["five_year_note_amount"]),
            "advertising_amount": money_float(row["advertising_amount"]),
            "parser_warnings": row["parser_warnings"] if isinstance(row["parser_warnings"], list) else (row["parser_warnings"] or []),
            "source_filename": row["source_filename"],
        }
        for row in rows
    ]


def get_hh_ap_current_period_missing_pdf_count(session, statement_id: str, period_start: str, period_end: str) -> int:
    row = session.execute(
        text(
            """
            SELECT COUNT(*) AS row_count
            FROM hh_ap_statement_lines
            WHERE statement_id = :statement_id
              AND is_missing_download = TRUE
              AND invoice_date >= :period_start
              AND invoice_date <= :period_end
            """
        ),
        {"statement_id": statement_id, "period_start": period_start, "period_end": period_end},
    ).mappings().first()
    return int((row or {}).get("row_count", 0) or 0)


def get_hh_ap_prior_period_open_count(session, statement_id: str, period_start: str) -> int:
    row = session.execute(
        text(
            """
            SELECT COUNT(*) AS row_count
            FROM hh_ap_statement_lines
            WHERE statement_id = :statement_id
              AND is_missing_download = TRUE
              AND invoice_date < :period_start
            """
        ),
        {"statement_id": statement_id, "period_start": period_start},
    ).mappings().first()
    return int((row or {}).get("row_count", 0) or 0)


def get_hh_ap_unmatched_remittance_summary(session, entity_id: str, period_start: str, period_end: str) -> dict[str, Decimal | int]:
    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS line_count,
                COALESCE(SUM(COALESCE(rl.line_amount, 0)), 0) AS total_amount
            FROM hh_ap_remittance_lines rl
            JOIN hh_ap_remittances r
              ON r.id = rl.remittance_id
            WHERE rl.entity_id = :entity_id
              AND rl.match_status <> 'matched'
              AND COALESCE(r.withdrawal_date, r.remittance_date) >= :period_start
              AND COALESCE(r.withdrawal_date, r.remittance_date) <= :period_end
            """
        ),
        {"entity_id": entity_id, "period_start": period_start, "period_end": period_end},
    ).mappings().first()

    return {
        "line_count": int((row or {}).get("line_count", 0) or 0),
        "total_amount": money((row or {}).get("total_amount", 0)),
    }


def build_control_result(name: str, status: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "control_name": name,
        "status": status,
        "message": message,
        "details": details or {},
    }


def require_control_ok(control_results: list[dict[str, Any]]):
    failures = [control for control in control_results if control["status"] == "exception"]
    if failures:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "HH AP statement control check failed",
                "failed_controls": failures,
            },
        )


def upsert_journal_batch(
    session,
    entity_id: str,
    accounting_period_id: str,
    source_module: str,
    batch_label: str,
    status: str,
    total_debits: Decimal,
    total_credits: Decimal,
    summary_json: dict[str, Any],
):
    batch = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id,
                accounting_period_id,
                source_module,
                batch_label,
                status,
                total_debits,
                total_credits,
                summary_json
            ) VALUES (
                :entity_id,
                :accounting_period_id,
                :source_module,
                :batch_label,
                :status,
                :total_debits,
                :total_credits,
                CAST(:summary_json AS jsonb)
            )
            ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label)
            DO UPDATE SET
                status = EXCLUDED.status,
                total_debits = EXCLUDED.total_debits,
                total_credits = EXCLUDED.total_credits,
                summary_json = EXCLUDED.summary_json,
                updated_at = NOW()
            RETURNING id, status, total_debits, total_credits, created_at, updated_at
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "source_module": source_module,
            "batch_label": batch_label,
            "status": status,
            "total_debits": total_debits,
            "total_credits": total_credits,
            "summary_json": json.dumps(summary_json),
        },
    ).mappings().first()

    return batch


def rebuild_journal_lines(session, journal_batch_id: str, lines: list[dict[str, Any]]):
    session.execute(
        text(
            """
            DELETE FROM journal_lines
            WHERE journal_batch_id = :journal_batch_id
            """
        ),
        {"journal_batch_id": journal_batch_id},
    )

    for line in lines:
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id,
                    line_number,
                    account_code,
                    debit_amount,
                    credit_amount,
                    memo,
                    source_json
                ) VALUES (
                    :journal_batch_id,
                    :line_number,
                    :account_code,
                    :debit_amount,
                    :credit_amount,
                    :memo,
                    CAST(:source_json AS jsonb)
                )
                """
            ),
            {
                "journal_batch_id": journal_batch_id,
                "line_number": line["line_number"],
                "account_code": line["account_code"],
                "debit_amount": line["debit_amount"],
                "credit_amount": line["credit_amount"],
                "memo": line["memo"],
                "source_json": json.dumps(line["source_json"]),
            },
        )


def signed_gl_export_amount(account_code: str, debit_amount: Decimal, credit_amount: Decimal) -> Decimal:
    sign = GL_EXPORT_SIGN_BY_ACCOUNT.get(account_code)
    if sign is None:
        raise HTTPException(status_code=400, detail=f"Missing GL export sign mapping for account {account_code}")

    if debit_amount > Decimal("0.00") and credit_amount > Decimal("0.00"):
        raise HTTPException(status_code=400, detail=f"Journal line for account {account_code} cannot have both debit and credit")

    base_amount = debit_amount if debit_amount > Decimal("0.00") else credit_amount
    return money(base_amount * sign)


def build_hh_ap_journal_lines(statement_month_end: str, batch_memo: str | None, source_totals: dict[str, Decimal]) -> list[dict[str, Any]]:
    memo = (batch_memo or HH_AP_DEFAULT_MEMO).strip() or HH_AP_DEFAULT_MEMO

    raw_lines = [
        {
            "account_code": ACCOUNT_1120_INVENTORY,
            "debit_amount": source_totals["inventory_amount"],
            "credit_amount": Decimal("0.00"),
            "memo": memo,
            "line_role": "inventory_balancing_debit",
        },
        {
            "account_code": ACCOUNT_2300_HST_PAYABLE,
            "debit_amount": source_totals["hst_amount"],
            "credit_amount": Decimal("0.00"),
            "memo": memo,
            "line_role": "hst_debit",
        },
        {
            "account_code": ACCOUNT_1320_FIVE_YEAR_NOTES,
            "debit_amount": source_totals["five_year_note_amount"],
            "credit_amount": Decimal("0.00"),
            "memo": memo,
            "line_role": "five_year_notes_debit",
        },
        {
            "account_code": ACCOUNT_1330_SPECIAL_SHARES,
            "debit_amount": source_totals["special_shares_amount"],
            "credit_amount": Decimal("0.00"),
            "memo": memo,
            "line_role": "special_shares_debit",
        },
        {
            "account_code": ACCOUNT_6210_ADVERTISING,
            "debit_amount": source_totals["advertising_amount"],
            "credit_amount": Decimal("0.00"),
            "memo": memo,
            "line_role": "advertising_debit",
        },
        {
            "account_code": ACCOUNT_2030_AP_HHSL,
            "debit_amount": Decimal("0.00"),
            "credit_amount": source_totals["total_purchases"],
            "memo": memo,
            "line_role": "accounts_payable_credit",
        },
    ]

    journal_lines: list[dict[str, Any]] = []
    for idx, line in enumerate(raw_lines, start=1):
        gl_amount = signed_gl_export_amount(
            account_code=line["account_code"],
            debit_amount=money(line["debit_amount"]),
            credit_amount=money(line["credit_amount"]),
        )
        journal_lines.append(
            {
                "line_number": idx,
                "account_code": line["account_code"],
                "debit_amount": money(line["debit_amount"]),
                "credit_amount": money(line["credit_amount"]),
                "memo": line["memo"],
                "source_json": {
                    "source_module": HH_AP_SOURCE_MODULE,
                    "statement_month_end": statement_month_end,
                    "line_role": line["line_role"],
                    "gl_export_signed_amount": money_float(gl_amount),
                },
            }
        )

    return journal_lines


def build_hh_ap_build_payload(session, payload: BuildHHAPMonthEndJournalRequest) -> dict[str, Any]:
    entity = get_entity(session, payload.entity_code)
    period = get_accounting_period(session, entity["id"], payload.period_end)

    statement_month_end = payload.statement_month_end or payload.period_end
    tolerance = abs_money(payload.control_tolerance)
    batch_label = (payload.batch_label or HH_AP_BATCH_LABEL).strip() or HH_AP_BATCH_LABEL
    statement = get_hh_ap_statement(session, entity["id"], statement_month_end)
    statement_json = require_statement_dict(statement["raw_json"])

    opening_balance = get_statement_summary_balance(statement_json, "opening_balance")
    total_adjustments = get_statement_summary_balance(statement_json, "total_adjustments")
    total_purchases_summary = get_statement_summary_balance(statement_json, "total_purchases_this_month")
    total_payments_summary = get_statement_summary_balance(statement_json, "total_payments_this_month")
    balance_owing = get_statement_summary_balance(statement_json, "balance_owing")
    due_bucket_total = get_statement_due_bucket_total(statement_json)

    total_purchases_component = get_statement_component_this_month(statement_json, STATEMENT_LABEL_TOTAL_PURCHASES)
    hst_amount = get_statement_component_this_month(statement_json, STATEMENT_LABEL_GST_HST)
    special_shares_amount = get_statement_component_this_month(statement_json, STATEMENT_LABEL_SPECIAL_SHARES)
    five_year_note_amount = get_statement_component_this_month(statement_json, STATEMENT_LABEL_FIVE_YEAR_NOTES)
    advertising_amount = get_statement_component_this_month(statement_json, STATEMENT_LABEL_ADVERTISING)

    inventory_amount = money(
        total_purchases_component
        - hst_amount
        - special_shares_amount
        - five_year_note_amount
        - advertising_amount
    )

    statement_controls = [
        build_control_result(
            name="statement_rollforward_ties",
            status=bool_status(abs_money(opening_balance + total_adjustments + total_purchases_summary - total_payments_summary - balance_owing) <= tolerance),
            message="Opening balance + adjustments + purchases - payments must equal balance owing",
            details={
                "opening_balance": money_float(opening_balance),
                "total_adjustments": money_float(total_adjustments),
                "total_purchases_this_month": money_float(total_purchases_summary),
                "total_payments_this_month": money_float(total_payments_summary),
                "balance_owing": money_float(balance_owing),
                "difference": money_float(opening_balance + total_adjustments + total_purchases_summary - total_payments_summary - balance_owing),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="statement_total_purchases_pages_tie",
            status=bool_status(abs_money(total_purchases_component - total_purchases_summary) <= tolerance),
            message="Statement page 8 total purchases must tie to statement page 9 total purchases",
            details={
                "summary_balances_total_purchases": money_float(total_purchases_summary),
                "summary_components_total_purchases": money_float(total_purchases_component),
                "difference": money_float(total_purchases_component - total_purchases_summary),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="statement_due_buckets_tie_balance_owing",
            status=bool_status(abs_money(due_bucket_total - balance_owing) <= tolerance),
            message="Statement due bucket totals should tie to balance owing",
            details={
                "due_bucket_total": money_float(due_bucket_total),
                "balance_owing": money_float(balance_owing),
                "difference": money_float(due_bucket_total - balance_owing),
                "tolerance": money_float(tolerance),
            },
        ),
    ]
    require_control_ok(statement_controls)

    invoice_month_totals = get_hh_ap_month_invoice_totals(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
    )
    invoice_type_totals = get_hh_ap_invoice_type_totals(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
    )
    invoice_type_component_totals = get_hh_ap_invoice_type_component_totals(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
    )
    invoices_with_parser_warnings = get_hh_ap_invoices_with_parser_warnings(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
        limit=100,
    )
    top_positive_invoices = get_hh_ap_top_invoices_by_total_amount(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
        direction="positive",
        limit=25,
    )
    top_negative_invoices = get_hh_ap_top_invoices_by_total_amount(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
        direction="negative",
        limit=25,
    )

    parsed_inventory_amount = money(
        invoice_month_totals["total_amount"]
        - invoice_month_totals["hst_amount"]
        - invoice_month_totals["subscribed_shares_amount"]
        - invoice_month_totals["five_year_note_amount"]
        - invoice_month_totals["advertising_amount"]
    )

    current_period_missing_pdf_count = get_hh_ap_current_period_missing_pdf_count(
        session=session,
        statement_id=str(statement["id"]),
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
    )
    prior_period_open_invoice_count = get_hh_ap_prior_period_open_count(
        session=session,
        statement_id=str(statement["id"]),
        period_start=str(period["period_start"]),
    )
    unmatched_remittance_summary = get_hh_ap_unmatched_remittance_summary(
        session=session,
        entity_id=entity["id"],
        period_start=str(period["period_start"]),
        period_end=str(period["period_end"]),
    )

    review_controls = [
        build_control_result(
            name="parsed_invoice_total_ties_statement_total_purchases",
            status=bool_status(abs_money(invoice_month_totals["total_amount"] - total_purchases_component) <= tolerance),
            message="Parsed invoice month total should tie statement total purchases",
            details={
                "parsed_invoice_total": money_float(invoice_month_totals["total_amount"]),
                "statement_total_purchases": money_float(total_purchases_component),
                "difference": money_float(invoice_month_totals["total_amount"] - total_purchases_component),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="parsed_invoice_hst_ties_statement_hst",
            status=bool_status(abs_money(invoice_month_totals["hst_amount"] - hst_amount) <= tolerance),
            message="Parsed invoice HST total should tie statement GST/HST",
            details={
                "parsed_hst_amount": money_float(invoice_month_totals["hst_amount"]),
                "statement_hst_amount": money_float(hst_amount),
                "difference": money_float(invoice_month_totals["hst_amount"] - hst_amount),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="parsed_invoice_special_shares_tie_statement",
            status=bool_status(abs_money(invoice_month_totals["subscribed_shares_amount"] - special_shares_amount) <= tolerance),
            message="Parsed invoice special shares total should tie statement special shares",
            details={
                "parsed_special_shares_amount": money_float(invoice_month_totals["subscribed_shares_amount"]),
                "statement_special_shares_amount": money_float(special_shares_amount),
                "difference": money_float(invoice_month_totals["subscribed_shares_amount"] - special_shares_amount),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="parsed_invoice_five_year_notes_tie_statement",
            status=bool_status(abs_money(invoice_month_totals["five_year_note_amount"] - five_year_note_amount) <= tolerance),
            message="Parsed invoice five year notes total should tie statement five year notes",
            details={
                "parsed_five_year_note_amount": money_float(invoice_month_totals["five_year_note_amount"]),
                "statement_five_year_note_amount": money_float(five_year_note_amount),
                "difference": money_float(invoice_month_totals["five_year_note_amount"] - five_year_note_amount),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="parsed_invoice_advertising_ties_statement",
            status=bool_status(abs_money(invoice_month_totals["advertising_amount"] - advertising_amount) <= tolerance),
            message="Parsed invoice advertising total should tie statement advertising",
            details={
                "parsed_advertising_amount": money_float(invoice_month_totals["advertising_amount"]),
                "statement_advertising_amount": money_float(advertising_amount),
                "difference": money_float(invoice_month_totals["advertising_amount"] - advertising_amount),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="parsed_inventory_derived_ties_statement_inventory",
            status=bool_status(abs_money(parsed_inventory_amount - inventory_amount) <= tolerance),
            message="Parsed invoice derived inventory should tie statement-derived inventory",
            details={
                "parsed_inventory_amount": money_float(parsed_inventory_amount),
                "statement_inventory_amount": money_float(inventory_amount),
                "difference": money_float(parsed_inventory_amount - inventory_amount),
                "tolerance": money_float(tolerance),
            },
        ),
        build_control_result(
            name="current_period_missing_pdf_count_zero",
            status=bool_status(current_period_missing_pdf_count == 0),
            message="Current period missing PDF count should be zero",
            details={"current_period_missing_pdf_count": current_period_missing_pdf_count},
        ),
        build_control_result(
            name="unmatched_remittance_lines_zero",
            status=bool_status(unmatched_remittance_summary["line_count"] == 0),
            message="Current period unmatched remittance line count should be zero",
            details={
                "unmatched_remittance_line_count": int(unmatched_remittance_summary["line_count"]),
                "unmatched_remittance_total_amount": money_float(unmatched_remittance_summary["total_amount"]),
            },
        ),
    ]

    source_totals = {
        "total_purchases": total_purchases_component,
        "hst_amount": hst_amount,
        "special_shares_amount": special_shares_amount,
        "five_year_note_amount": five_year_note_amount,
        "advertising_amount": advertising_amount,
        "inventory_amount": inventory_amount,
    }
    journal_lines = build_hh_ap_journal_lines(
        statement_month_end=statement_month_end,
        batch_memo=payload.batch_memo,
        source_totals=source_totals,
    )

    total_debits = money(sum(line["debit_amount"] for line in journal_lines))
    total_credits = money(sum(line["credit_amount"] for line in journal_lines))
    balance_difference = money(total_debits - total_credits)
    journal_balance_control = build_control_result(
        name="journal_balances",
        status=bool_status(abs_money(balance_difference) <= tolerance),
        message="Journal debits and credits must balance",
        details={
            "total_debits": money_float(total_debits),
            "total_credits": money_float(total_credits),
            "difference": money_float(balance_difference),
            "tolerance": money_float(tolerance),
        },
    )
    require_control_ok([journal_balance_control])

    has_review_exception = any(control["status"] == "exception" for control in review_controls)
    batch_status = "draft_exception" if has_review_exception else "draft"

    summary_json = {
        "entity_code": entity["entity_code"],
        "entity_name": entity.get("entity_name"),
        "source_module": HH_AP_SOURCE_MODULE,
        "batch_label": batch_label,
        "period_label": period["period_label"],
        "period_start": str(period["period_start"]),
        "period_end": str(period["period_end"]),
        "statement_month_end": statement_month_end,
        "statement_id": str(statement["id"]),
        "journal_memo": (payload.batch_memo or HH_AP_DEFAULT_MEMO).strip() or HH_AP_DEFAULT_MEMO,
        "statement_totals": {
            "opening_balance": money_float(opening_balance),
            "total_adjustments": money_float(total_adjustments),
            "total_purchases": money_float(total_purchases_component),
            "total_payments": money_float(total_payments_summary),
            "balance_owing": money_float(balance_owing),
            "due_bucket_total": money_float(due_bucket_total),
            "hst_amount": money_float(hst_amount),
            "special_shares_amount": money_float(special_shares_amount),
            "five_year_note_amount": money_float(five_year_note_amount),
            "advertising_amount": money_float(advertising_amount),
            "inventory_amount": money_float(inventory_amount),
        },
        "parsed_invoice_month_totals": {
            "invoice_count": int(invoice_month_totals["invoice_count"]),
            "total_amount": money_float(invoice_month_totals["total_amount"]),
            "hst_amount": money_float(invoice_month_totals["hst_amount"]),
            "special_shares_amount": money_float(invoice_month_totals["subscribed_shares_amount"]),
            "five_year_note_amount": money_float(invoice_month_totals["five_year_note_amount"]),
            "advertising_amount": money_float(invoice_month_totals["advertising_amount"]),
            "subtotal_amount": money_float(invoice_month_totals["subtotal_amount"]),
            "surcharge_amount": money_float(invoice_month_totals["surcharge_amount"]),
            "derived_inventory_amount": money_float(parsed_inventory_amount),
            "invoice_type_totals": invoice_type_totals,
        },
        "variance_support": {
            "component_differences": {
                "total_amount_difference": money_float(invoice_month_totals["total_amount"] - total_purchases_component),
                "hst_amount_difference": money_float(invoice_month_totals["hst_amount"] - hst_amount),
                "special_shares_amount_difference": money_float(invoice_month_totals["subscribed_shares_amount"] - special_shares_amount),
                "five_year_note_amount_difference": money_float(invoice_month_totals["five_year_note_amount"] - five_year_note_amount),
                "advertising_amount_difference": money_float(invoice_month_totals["advertising_amount"] - advertising_amount),
                "inventory_amount_difference": money_float(parsed_inventory_amount - inventory_amount),
            },
            "invoice_type_component_totals": invoice_type_component_totals,
            "parser_warning_invoice_count": len(invoices_with_parser_warnings),
            "invoices_with_parser_warnings": invoices_with_parser_warnings,
            "top_positive_invoices": top_positive_invoices,
            "top_negative_invoices": top_negative_invoices,
        },
        "exception_counts": {
            "current_period_missing_pdf_count": current_period_missing_pdf_count,
            "prior_period_open_invoice_count": prior_period_open_invoice_count,
            "unmatched_remittance_line_count": int(unmatched_remittance_summary["line_count"]),
            "unmatched_remittance_total_amount": money_float(unmatched_remittance_summary["total_amount"]),
        },
        "controls": {
            "hard_stop_controls": statement_controls + [journal_balance_control],
            "review_controls": review_controls,
        },
        "total_debits": money_float(total_debits),
        "total_credits": money_float(total_credits),
        "balance_difference": money_float(balance_difference),
        "is_balanced": balance_difference == Decimal("0.00"),
        "has_review_exception": has_review_exception,
        "built_at": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "entity": entity,
        "period": period,
        "statement": statement,
        "statement_month_end": statement_month_end,
        "batch_label": batch_label,
        "batch_status": batch_status,
        "journal_lines": journal_lines,
        "total_debits": total_debits,
        "total_credits": total_credits,
        "balance_difference": balance_difference,
        "summary_json": summary_json,
    }


@router.post("/build")
def build_hh_ap_month_end_journal(payload: BuildHHAPMonthEndJournalRequest):
    with db_session() as session:
        build_payload = build_hh_ap_build_payload(session, payload)

        batch = upsert_journal_batch(
            session=session,
            entity_id=build_payload["entity"]["id"],
            accounting_period_id=build_payload["period"]["id"],
            source_module=HH_AP_SOURCE_MODULE,
            batch_label=build_payload["batch_label"],
            status=build_payload["batch_status"],
            total_debits=build_payload["total_debits"],
            total_credits=build_payload["total_credits"],
            summary_json=build_payload["summary_json"],
        )

        rebuild_journal_lines(
            session=session,
            journal_batch_id=batch["id"],
            lines=build_payload["journal_lines"],
        )

        response_lines = [
            {
                "line_number": line["line_number"],
                "account_code": line["account_code"],
                "debit_amount": money_float(line["debit_amount"]),
                "credit_amount": money_float(line["credit_amount"]),
                "memo": line["memo"],
                "gl_export_signed_amount": line["source_json"].get("gl_export_signed_amount"),
                "source_json": line["source_json"],
            }
            for line in build_payload["journal_lines"]
        ]

        return {
            "entity_code": build_payload["entity"]["entity_code"],
            "accounting_period": {
                "id": str(build_payload["period"]["id"]),
                "period_label": build_payload["period"]["period_label"],
                "period_start": str(build_payload["period"]["period_start"]),
                "period_end": str(build_payload["period"]["period_end"]),
                "fiscal_year": build_payload["period"]["fiscal_year"],
                "fiscal_period_number": build_payload["period"]["fiscal_period_number"],
            },
            "journal_batch": {
                "id": str(batch["id"]),
                "source_module": HH_AP_SOURCE_MODULE,
                "batch_label": build_payload["batch_label"],
                "status": build_payload["batch_status"],
                "total_debits": money_float(build_payload["total_debits"]),
                "total_credits": money_float(build_payload["total_credits"]),
                "balance_difference": money_float(build_payload["balance_difference"]),
                "is_balanced": build_payload["balance_difference"] == Decimal("0.00"),
            },
            "journal_lines": response_lines,
            "summary_json": build_payload["summary_json"],
        }


@router.get("/review")
def review_hh_ap_month_end_journal(
    entity_code: str,
    period_end: str,
    batch_label: str = HH_AP_BATCH_LABEL,
):
    with db_session() as session:
        entity = get_entity(session, entity_code)
        period = get_accounting_period(session, entity["id"], period_end)

        batch = session.execute(
            text(
                """
                SELECT
                    id,
                    entity_id,
                    accounting_period_id,
                    source_module,
                    batch_label,
                    status,
                    total_debits,
                    total_credits,
                    summary_json,
                    created_at,
                    updated_at
                FROM journal_batches
                WHERE entity_id = :entity_id
                  AND accounting_period_id = :accounting_period_id
                  AND source_module = :source_module
                  AND batch_label = :batch_label
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": period["id"],
                "source_module": HH_AP_SOURCE_MODULE,
                "batch_label": batch_label,
            },
        ).mappings().first()

        if not batch:
            raise HTTPException(status_code=404, detail="No draft HH AP month-end journal batch found for this accounting period")

        lines = session.execute(
            text(
                """
                SELECT
                    line_number,
                    account_code,
                    debit_amount,
                    credit_amount,
                    memo,
                    source_json
                FROM journal_lines
                WHERE journal_batch_id = :journal_batch_id
                ORDER BY line_number
                """
            ),
            {"journal_batch_id": batch["id"]},
        ).mappings().all()

        total_debits = money(batch["total_debits"])
        total_credits = money(batch["total_credits"])
        balance_difference = money(total_debits - total_credits)

        return {
            "entity_code": entity["entity_code"],
            "accounting_period": {
                "id": str(period["id"]),
                "period_label": period["period_label"],
                "period_start": str(period["period_start"]),
                "period_end": str(period["period_end"]),
                "fiscal_year": period["fiscal_year"],
                "fiscal_period_number": period["fiscal_period_number"],
            },
            "journal_batch": {
                "id": str(batch["id"]),
                "source_module": batch["source_module"],
                "batch_label": batch["batch_label"],
                "status": batch["status"],
                "total_debits": money_float(total_debits),
                "total_credits": money_float(total_credits),
                "balance_difference": money_float(balance_difference),
                "is_balanced": balance_difference == Decimal("0.00"),
                "summary_json": batch["summary_json"],
                "created_at": batch["created_at"].isoformat() if batch["created_at"] else None,
                "updated_at": batch["updated_at"].isoformat() if batch["updated_at"] else None,
            },
            "journal_lines": [
                {
                    "line_number": row["line_number"],
                    "account_code": row["account_code"],
                    "debit_amount": money_float(row["debit_amount"]),
                    "credit_amount": money_float(row["credit_amount"]),
                    "memo": row["memo"],
                    "gl_export_signed_amount": (row["source_json"] or {}).get("gl_export_signed_amount") if isinstance(row["source_json"], dict) else None,
                    "source_json": row["source_json"],
                }
                for row in lines
            ],
        }
