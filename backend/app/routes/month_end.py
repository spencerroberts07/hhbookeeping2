from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session

router = APIRouter(prefix="/api/month-end", tags=["month-end"])

CASH_BALANCING_SOURCE_MODULE = "cash_balancing"
CASH_BALANCING_BATCH_LABEL = "cash_balancing_month_end"


class BuildCashBalancingJournalRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    period_end: str = Field(..., examples=["2026-03-31"])


def money(value) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def money_float(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def split_amount_by_direction(amount: Decimal, posting_direction: str) -> tuple[Decimal, Decimal]:
    normalized_direction = str(posting_direction).strip().lower()

    if normalized_direction not in {"debit", "credit"}:
        raise RuntimeError(f"Unsupported posting direction: {posting_direction}")

    if normalized_direction == "debit":
        if amount >= 0:
            return amount, Decimal("0.00")
        return Decimal("0.00"), abs(amount)

    if amount >= 0:
        return Decimal("0.00"), amount
    return abs(amount), Decimal("0.00")


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
        {
            "entity_id": entity_id,
            "period_end": period_end,
        },
    ).mappings().first()

    if not period:
        raise HTTPException(
            status_code=404,
            detail=f"No accounting period found for period_end={period_end}",
        )

    return period


def get_unmapped_period_labels(session, entity_id: str, accounting_period_id: str):
    rows = session.execute(
        text(
            """
            SELECT
                l.line_label,
                COUNT(*) AS line_count
            FROM cash_balancing_lines l
            JOIN cash_balancing_days d
              ON d.id = l.cash_balancing_day_id
            WHERE d.entity_id = :entity_id
              AND d.accounting_period_id = :accounting_period_id
              AND (
                    l.mapped_account_code IS NULL
                 OR l.translation_status <> 'mapped'
              )
            GROUP BY l.line_label
            ORDER BY l.line_label
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().all()

    return [
        {
            "line_label": row["line_label"],
            "line_count": int(row["line_count"]),
        }
        for row in rows
    ]


def get_missing_posting_direction_labels(session, entity_id: str, accounting_period_id: str):
    rows = session.execute(
        text(
            """
            SELECT DISTINCT
                l.line_label,
                l.mapped_account_code
            FROM cash_balancing_lines l
            JOIN cash_balancing_days d
              ON d.id = l.cash_balancing_day_id
            LEFT JOIN account_mapping_rules r
              ON r.entity_id = d.entity_id
             AND r.source_type = 'cash_balancing_line_label'
             AND r.source_key = l.line_label
             AND r.is_active = TRUE
            WHERE d.entity_id = :entity_id
              AND d.accounting_period_id = :accounting_period_id
              AND l.translation_status = 'mapped'
              AND l.mapped_account_code IS NOT NULL
              AND (
                    r.posting_direction IS NULL
                 OR btrim(r.posting_direction) = ''
              )
            ORDER BY l.line_label
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().all()

    return [
        {
            "line_label": row["line_label"],
            "mapped_account_code": row["mapped_account_code"],
        }
        for row in rows
    ]


def get_cash_balancing_day_stats(session, entity_id: str, accounting_period_id: str):
    stats = session.execute(
        text(
            """
            SELECT
                COUNT(*) AS day_count,
                COALESCE(SUM(COALESCE(total_sales, 0)), 0) AS total_sales,
                COALESCE(SUM(COALESCE(total_hst, 0)), 0) AS total_hst
            FROM cash_balancing_days
            WHERE entity_id = :entity_id
              AND accounting_period_id = :accounting_period_id
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().first()

    return {
        "day_count": int((stats or {}).get("day_count", 0) or 0),
        "total_sales": money((stats or {}).get("total_sales", 0)),
        "total_hst": money((stats or {}).get("total_hst", 0)),
    }


def get_aggregated_cash_balancing_lines(session, entity_id: str, accounting_period_id: str):
    rows = session.execute(
        text(
            """
            SELECT
                l.mapped_account_code AS account_code,
                r.posting_direction AS posting_direction,
                COUNT(*) AS source_line_count,
                COALESCE(SUM(l.amount), 0) AS total_amount
            FROM cash_balancing_lines l
            JOIN cash_balancing_days d
              ON d.id = l.cash_balancing_day_id
            LEFT JOIN account_mapping_rules r
              ON r.entity_id = d.entity_id
             AND r.source_type = 'cash_balancing_line_label'
             AND r.source_key = l.line_label
             AND r.is_active = TRUE
            WHERE d.entity_id = :entity_id
              AND d.accounting_period_id = :accounting_period_id
              AND l.translation_status = 'mapped'
              AND l.mapped_account_code IS NOT NULL
            GROUP BY
                l.mapped_account_code,
                r.posting_direction
            ORDER BY
                l.mapped_account_code
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().all()

    return rows


def upsert_journal_batch(
    session,
    entity_id: str,
    accounting_period_id: str,
    status: str,
    total_debits: Decimal,
    total_credits: Decimal,
    summary_json: dict,
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
            RETURNING id, status, total_debits, total_credits
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "source_module": CASH_BALANCING_SOURCE_MODULE,
            "batch_label": CASH_BALANCING_BATCH_LABEL,
            "status": status,
            "total_debits": total_debits,
            "total_credits": total_credits,
            "summary_json": json.dumps(summary_json),
        },
    ).mappings().first()

    return batch


def rebuild_journal_lines(session, journal_batch_id: str, lines: list[dict]):
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


@router.post("/cash-balancing/build")
def build_cash_balancing_month_end_journal(payload: BuildCashBalancingJournalRequest):
    with db_session() as session:
        entity = get_entity(session, payload.entity_code)
        period = get_accounting_period(session, entity["id"], payload.period_end)

        unmapped_lines = get_unmapped_period_labels(session, entity["id"], period["id"])
        if unmapped_lines:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "This accounting period still has unmapped cash balancing lines",
                    "unmapped_lines": unmapped_lines,
                },
            )

        missing_posting_directions = get_missing_posting_direction_labels(
            session,
            entity["id"],
            period["id"],
        )
        if missing_posting_directions:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Some mapped labels are missing posting_direction",
                    "missing_posting_directions": missing_posting_directions,
                },
            )

        day_stats = get_cash_balancing_day_stats(session, entity["id"], period["id"])
        aggregated_rows = get_aggregated_cash_balancing_lines(session, entity["id"], period["id"])

        if not aggregated_rows:
            raise HTTPException(
                status_code=400,
                detail="No mapped cash balancing activity found for this accounting period",
            )

        journal_lines: list[dict] = []
        total_debits = Decimal("0.00")
        total_credits = Decimal("0.00")

        for idx, row in enumerate(aggregated_rows, start=1):
            account_code = str(row["account_code"]).strip()
            posting_direction = str(row["posting_direction"]).strip().lower()
            total_amount = money(row["total_amount"])
            debit_amount, credit_amount = split_amount_by_direction(total_amount, posting_direction)

            total_debits += debit_amount
            total_credits += credit_amount

            journal_lines.append(
                {
                    "line_number": idx,
                    "account_code": account_code,
                    "debit_amount": debit_amount,
                    "credit_amount": credit_amount,
                    "memo": f"Cash balancing close {period['period_label']}",
                    "source_json": {
                        "source_module": CASH_BALANCING_SOURCE_MODULE,
                        "accounting_period_id": str(period["id"]),
                        "period_label": period["period_label"],
                        "posting_direction": posting_direction,
                        "source_line_count": int(row["source_line_count"]),
                        "summed_source_amount": money_float(total_amount),
                    },
                }
            )

        balance_difference = total_debits - total_credits
        is_balanced = balance_difference == Decimal("0.00")
        batch_status = "draft" if is_balanced else "draft_unbalanced"

        summary_json = {
            "entity_code": entity["entity_code"],
            "entity_name": entity.get("entity_name"),
            "source_module": CASH_BALANCING_SOURCE_MODULE,
            "batch_label": CASH_BALANCING_BATCH_LABEL,
            "period_label": period["period_label"],
            "period_start": str(period["period_start"]),
            "period_end": str(period["period_end"]),
            "fiscal_year": period["fiscal_year"],
            "fiscal_period_number": period["fiscal_period_number"],
            "day_count": day_stats["day_count"],
            "source_day_total_sales": money_float(day_stats["total_sales"]),
            "source_day_total_hst": money_float(day_stats["total_hst"]),
            "journal_line_count": len(journal_lines),
            "total_debits": money_float(total_debits),
            "total_credits": money_float(total_credits),
            "balance_difference": money_float(balance_difference),
            "is_balanced": is_balanced,
            "built_at": datetime.now(timezone.utc).isoformat(),
        }

        batch = upsert_journal_batch(
            session=session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            status=batch_status,
            total_debits=total_debits,
            total_credits=total_credits,
            summary_json=summary_json,
        )

        rebuild_journal_lines(
            session=session,
            journal_batch_id=batch["id"],
            lines=journal_lines,
        )

        response_lines = [
            {
                "line_number": line["line_number"],
                "account_code": line["account_code"],
                "debit_amount": money_float(line["debit_amount"]),
                "credit_amount": money_float(line["credit_amount"]),
                "memo": line["memo"],
                "source_json": line["source_json"],
            }
            for line in journal_lines
        ]

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
                "source_module": CASH_BALANCING_SOURCE_MODULE,
                "batch_label": CASH_BALANCING_BATCH_LABEL,
                "status": batch_status,
                "total_debits": money_float(total_debits),
                "total_credits": money_float(total_credits),
                "balance_difference": money_float(balance_difference),
                "is_balanced": is_balanced,
            },
            "source_summary": {
                "day_count": day_stats["day_count"],
                "source_day_total_sales": money_float(day_stats["total_sales"]),
                "source_day_total_hst": money_float(day_stats["total_hst"]),
            },
            "journal_lines": response_lines,
        }


@router.get("/cash-balancing/review")
def review_cash_balancing_month_end_journal(entity_code: str, period_end: str):
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
                "source_module": CASH_BALANCING_SOURCE_MODULE,
                "batch_label": CASH_BALANCING_BATCH_LABEL,
            },
        ).mappings().first()

        if not batch:
            raise HTTPException(
                status_code=404,
                detail="No draft cash balancing journal batch found for this accounting period",
            )

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
        balance_difference = total_debits - total_credits

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
                    "debit_amount": money_float(money(row["debit_amount"])),
                    "credit_amount": money_float(money(row["credit_amount"])),
                    "memo": row["memo"],
                    "source_json": row["source_json"],
                }
                for row in lines
            ],
        }
