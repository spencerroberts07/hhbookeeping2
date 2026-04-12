from datetime import datetime, timezone
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import settings
from ..db import db_session
from ..google_sheets import (
    DailyCashLine,
    GoogleSheetsClient,
    normalize_cash_balancing_rows,
    parse_weekly_cash_sheet,
)

router = APIRouter(prefix="/api/cash-balancing", tags=["cash-balancing"])


EXCLUDED_DAILY_LABELS = {
    "",
    "Opening Cash",
    "Total",
    "Total PAID OUTS",
    "Cash to Account for",
    "Actual Cash count",
    "Closing Cash",
    "Weather",
    "Customer count",
    "PAID OUTS",
    "Other info",
}


class CashBalancingSyncRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    sheet_tabs: list[str] = Field(
        default_factory=list,
        examples=[["Feb1-Feb7", "Feb8-Feb14"]],
    )
    lookback_days: int = Field(default=56, ge=1, le=365)


def build_daily_groups(
    daily_lines: list[DailyCashLine],
) -> dict[str, dict]:
    grouped: dict[str, dict] = {}

    for line in daily_lines:
        if line.line_label in EXCLUDED_DAILY_LABELS:
            continue

        if line.amount is None:
            continue

        if line.business_date not in grouped:
            grouped[line.business_date] = {
                "tab_name": line.source_tab_name,
                "total_sales": None,
                "total_hst": None,
                "lines": [],
            }

        day_bucket = grouped[line.business_date]
        day_bucket["lines"].append(line)

        if line.line_label == "Item Sales":
            day_bucket["total_sales"] = line.amount
        elif line.line_label == "Tax - HST":
            day_bucket["total_hst"] = line.amount

    return grouped


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for value in values:
        cleaned = str(value).strip()
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)

    return result


@router.post("/sync")
async def sync_cash_balancing(payload: CashBalancingSyncRequest):
    selected_tabs: list[str] = dedupe_preserve_order(payload.sheet_tabs)

    with db_session() as session:
        entity = session.execute(
            text("SELECT id, entity_code FROM entities WHERE entity_code = :entity_code"),
            {"entity_code": payload.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        source = session.execute(
            text(
                """
                INSERT INTO cash_balancing_sources (
                    entity_id, source_name, spreadsheet_id, lookback_days
                ) VALUES (
                    :entity_id, 'Bridlewood Cash Balancing', :spreadsheet_id, :lookback_days
                )
                ON CONFLICT (entity_id, source_name)
                DO UPDATE SET
                    spreadsheet_id = EXCLUDED.spreadsheet_id,
                    lookback_days = EXCLUDED.lookback_days,
                    updated_at = NOW()
                RETURNING id, spreadsheet_id, lookback_days
                """
            ),
            {
                "entity_id": entity["id"],
                "spreadsheet_id": settings.google_sheets_spreadsheet_id,
                "lookback_days": payload.lookback_days,
            },
        ).mappings().first()

        import_run = session.execute(
            text(
                """
                INSERT INTO cash_balancing_import_runs (
                    entity_id, source_id, run_type, status, tabs_read, summary_json
                ) VALUES (
                    :entity_id, :source_id, 'manual', 'running', '[]'::jsonb, '{}'::jsonb
                )
                RETURNING id
                """
            ),
            {"entity_id": entity["id"], "source_id": source["id"]},
        ).mappings().first()

        sheet_client = GoogleSheetsClient(
            service_account_email=settings.google_sheets_service_account_email,
            private_key=settings.google_sheets_private_key,
        )

        raw_inserted = 0
        raw_updated = 0
        day_upserted = 0
        line_inserted = 0
        tabs_source = "manual" if selected_tabs else "auto"

        try:
            if not selected_tabs:
                selected_tabs = await sheet_client.select_recent_weekly_tabs(
                    spreadsheet_id=source["spreadsheet_id"],
                    lookback_days=payload.lookback_days,
                )

            selected_tabs = dedupe_preserve_order(selected_tabs)

            if not selected_tabs:
                raise RuntimeError(
                    f"No weekly cash balancing tabs were found overlapping the last "
                    f"{payload.lookback_days} days."
                )

            for tab_name in selected_tabs:
                raw_rows = await sheet_client.get_tab_values(
                    source["spreadsheet_id"],
                    tab_name,
                )
                normalized_rows = normalize_cash_balancing_rows(tab_name, raw_rows)
                parsed_daily_lines = parse_weekly_cash_sheet(tab_name, raw_rows)
                daily_groups = build_daily_groups(parsed_daily_lines)

                # 1) keep raw staging table logic
                for row in normalized_rows:
                    existing = session.execute(
                        text(
                            """
                            SELECT id, row_hash
                            FROM cash_balancing_rows
                            WHERE entity_id = :entity_id
                              AND source_id = :source_id
                              AND source_tab_name = :source_tab_name
                              AND row_key = :row_key
                            """
                        ),
                        {
                            "entity_id": entity["id"],
                            "source_id": source["id"],
                            "source_tab_name": row.source_tab_name,
                            "row_key": row.row_key,
                        },
                    ).mappings().first()

                    values = {
                        "entity_id": entity["id"],
                        "source_id": source["id"],
                        "import_run_id": import_run["id"],
                        "source_tab_name": row.source_tab_name,
                        "business_date": row.business_date,
                        "row_number": row.row_number,
                        "row_key": row.row_key,
                        "row_hash": row.row_hash,
                        "notes": row.notes,
                        "sales_amount": row.sales_amount,
                        "cash_amount": row.cash_amount,
                        "debit_amount": row.debit_amount,
                        "credit_amount": row.credit_amount,
                        "ecommerce_amount": row.ecommerce_amount,
                        "gift_card_amount": row.gift_card_amount,
                        "hst_amount": row.hst_amount,
                        "over_short_amount": row.over_short_amount,
                        "raw_row_json": json.dumps(row.raw_row_json),
                    }

                    if not existing:
                        session.execute(
                            text(
                                """
                                INSERT INTO cash_balancing_rows (
                                    entity_id, source_id, import_run_id, source_tab_name,
                                    business_date, row_number, row_key, row_hash, notes,
                                    sales_amount, cash_amount, debit_amount, credit_amount,
                                    ecommerce_amount, gift_card_amount, hst_amount,
                                    over_short_amount, raw_row_json
                                ) VALUES (
                                    :entity_id, :source_id, :import_run_id, :source_tab_name,
                                    :business_date, :row_number, :row_key, :row_hash, :notes,
                                    :sales_amount, :cash_amount, :debit_amount, :credit_amount,
                                    :ecommerce_amount, :gift_card_amount, :hst_amount,
                                    :over_short_amount, CAST(:raw_row_json AS jsonb)
                                )
                                """
                            ),
                            values,
                        )
                        raw_inserted += 1
                    elif existing["row_hash"] != row.row_hash:
                        session.execute(
                            text(
                                """
                                UPDATE cash_balancing_rows
                                SET import_run_id = :import_run_id,
                                    business_date = :business_date,
                                    row_number = :row_number,
                                    row_hash = :row_hash,
                                    notes = :notes,
                                    sales_amount = :sales_amount,
                                    cash_amount = :cash_amount,
                                    debit_amount = :debit_amount,
                                    credit_amount = :credit_amount,
                                    ecommerce_amount = :ecommerce_amount,
                                    gift_card_amount = :gift_card_amount,
                                    hst_amount = :hst_amount,
                                    over_short_amount = :over_short_amount,
                                    raw_row_json = CAST(:raw_row_json AS jsonb),
                                    imported_at = NOW()
                                WHERE id = :id
                                """
                            ),
                            {**values, "id": existing["id"]},
                        )
                        raw_updated += 1

                # 2) upsert daily headers and rebuild daily lines
                for business_date, day_data in daily_groups.items():
                    existing_day = session.execute(
                        text(
                            """
                            SELECT id
                            FROM cash_balancing_days
                            WHERE entity_id = :entity_id
                              AND business_date = :business_date
                            """
                        ),
                        {
                            "entity_id": entity["id"],
                            "business_date": business_date,
                        },
                    ).mappings().first()

                    raw_json_payload = {
                        "source_tab_name": day_data["tab_name"],
                        "line_count": len(day_data["lines"]),
                        "import_run_id": str(import_run["id"]),
                    }

                    if not existing_day:
                        day_row = session.execute(
                            text(
                                """
                                INSERT INTO cash_balancing_days (
                                    entity_id, business_date, tab_name, total_sales, total_hst, raw_json
                                ) VALUES (
                                    :entity_id, :business_date, :tab_name, :total_sales, :total_hst, CAST(:raw_json AS jsonb)
                                )
                                RETURNING id
                                """
                            ),
                            {
                                "entity_id": entity["id"],
                                "business_date": business_date,
                                "tab_name": day_data["tab_name"],
                                "total_sales": day_data["total_sales"],
                                "total_hst": day_data["total_hst"],
                                "raw_json": json.dumps(raw_json_payload),
                            },
                        ).mappings().first()
                        cash_balancing_day_id = day_row["id"]
                    else:
                        cash_balancing_day_id = existing_day["id"]
                        session.execute(
                            text(
                                """
                                UPDATE cash_balancing_days
                                SET tab_name = :tab_name,
                                    total_sales = :total_sales,
                                    total_hst = :total_hst,
                                    raw_json = CAST(:raw_json AS jsonb)
                                WHERE id = :id
                                """
                            ),
                            {
                                "id": cash_balancing_day_id,
                                "tab_name": day_data["tab_name"],
                                "total_sales": day_data["total_sales"],
                                "total_hst": day_data["total_hst"],
                                "raw_json": json.dumps(raw_json_payload),
                            },
                        )

                    day_upserted += 1

                    # delete and rebuild the lines for this day so reruns stay clean
                    session.execute(
                        text(
                            """
                            DELETE FROM cash_balancing_lines
                            WHERE cash_balancing_day_id = :cash_balancing_day_id
                            """
                        ),
                        {"cash_balancing_day_id": cash_balancing_day_id},
                    )

                    for line in day_data["lines"]:
                        session.execute(
                            text(
                                """
                                INSERT INTO cash_balancing_lines (
                                    cash_balancing_day_id,
                                    line_code,
                                    line_label,
                                    amount,
                                    mapped_account_code,
                                    translation_status
                                ) VALUES (
                                    :cash_balancing_day_id,
                                    :line_code,
                                    :line_label,
                                    :amount,
                                    :mapped_account_code,
                                    :translation_status
                                )
                                """
                            ),
                            {
                                "cash_balancing_day_id": cash_balancing_day_id,
                                "line_code": line.account_code,
                                "line_label": line.line_label,
                                "amount": line.amount,
                                "mapped_account_code": None,
                                "translation_status": "pending",
                            },
                        )
                        line_inserted += 1

            summary = {
                "tabs": selected_tabs,
                "tabs_source": tabs_source,
                "raw_inserted": raw_inserted,
                "raw_updated": raw_updated,
                "day_upserted": day_upserted,
                "line_inserted": line_inserted,
                "lookback_days": payload.lookback_days,
                "excluded_labels": sorted(EXCLUDED_DAILY_LABELS),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }

            session.execute(
                text(
                    """
                    UPDATE cash_balancing_import_runs
                    SET status = 'completed',
                        finished_at = NOW(),
                        tabs_read = CAST(:tabs_read AS jsonb),
                        summary_json = CAST(:summary_json AS jsonb)
                    WHERE id = :id
                    """
                ),
                {
                    "id": import_run["id"],
                    "tabs_read": json.dumps(selected_tabs),
                    "summary_json": json.dumps(summary),
                },
            )

            return {
                "entity_code": payload.entity_code,
                "sync_type": "cash_balancing_google_sheet",
                "raw_inserted_count": raw_inserted,
                "raw_updated_count": raw_updated,
                "day_upserted_count": day_upserted,
                "line_inserted_count": line_inserted,
                "summary": summary,
            }

        except Exception as e:
            session.execute(
                text(
                    """
                    UPDATE cash_balancing_import_runs
                    SET status = 'failed',
                        finished_at = NOW(),
                        tabs_read = CAST(:tabs_read AS jsonb),
                        summary_json = CAST(:summary_json AS jsonb),
                        error_text = :error_text
                    WHERE id = :id
                    """
                ),
                {
                    "id": import_run["id"],
                    "tabs_read": json.dumps(selected_tabs),
                    "summary_json": json.dumps(
                        {
                            "tabs": selected_tabs,
                            "tabs_source": tabs_source,
                            "lookback_days": payload.lookback_days,
                            "failed_at": datetime.now(timezone.utc).isoformat(),
                        }
                    ),
                    "error_text": str(e),
                },
            )
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
def cash_balancing_status(entity_code: str):
    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :entity_code"),
            {"entity_code": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        latest_run = session.execute(
            text(
                """
                SELECT status, started_at, finished_at, summary_json, error_text
                FROM cash_balancing_import_runs
                WHERE entity_id = :entity_id
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        row_count = session.execute(
            text("SELECT COUNT(*) AS row_count FROM cash_balancing_rows WHERE entity_id = :entity_id"),
            {"entity_id": entity["id"]},
        ).mappings().first()

        day_count = session.execute(
            text("SELECT COUNT(*) AS day_count FROM cash_balancing_days WHERE entity_id = :entity_id"),
            {"entity_id": entity["id"]},
        ).mappings().first()

        line_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS line_count
                FROM cash_balancing_lines l
                JOIN cash_balancing_days d ON d.id = l.cash_balancing_day_id
                WHERE d.entity_id = :entity_id
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        return {
            "entity_code": entity_code,
            "has_cash_balancing_rows": (row_count or {}).get("row_count", 0) > 0,
            "row_count": (row_count or {}).get("row_count", 0),
            "day_count": (day_count or {}).get("day_count", 0),
            "line_count": (line_count or {}).get("line_count", 0),
            "latest_run": dict(latest_run) if latest_run else None,
        }
