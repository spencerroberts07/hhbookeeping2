from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import settings
from ..db import db_session
from ..google_sheets import GoogleSheetsClient, normalize_cash_balancing_rows

router = APIRouter(prefix="/api/cash-balancing", tags=["cash-balancing"])


class CashBalancingSyncRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    sheet_tabs: list[str] = Field(default_factory=list, examples=[["February"]])
    lookback_days: int = Field(default=56, ge=1, le=365)


@router.post("/sync")
async def sync_cash_balancing(payload: CashBalancingSyncRequest):
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

        tabs = payload.sheet_tabs or ["February"]
        inserted = 0
        updated = 0

        for tab_name in tabs:
            raw_rows = await sheet_client.get_tab_values(source["spreadsheet_id"], tab_name)
            normalized_rows = normalize_cash_balancing_rows(tab_name, raw_rows)

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
                    "raw_row_json": row.raw_row_json,
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
                        {**values, "raw_row_json": __import__("json").dumps(row.raw_row_json)},
                    )
                    inserted += 1
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
                        {**values, "id": existing["id"], "raw_row_json": __import__("json").dumps(row.raw_row_json)},
                    )
                    updated += 1

        summary = {
            "tabs": tabs,
            "inserted": inserted,
            "updated": updated,
            "lookback_days": payload.lookback_days,
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
                "tabs_read": __import__("json").dumps(tabs),
                "summary_json": __import__("json").dumps(summary),
            },
        )

        return {
            "entity_code": payload.entity_code,
            "sync_type": "cash_balancing_google_sheet",
            "inserted_count": inserted,
            "updated_count": updated,
            "summary": summary,
        }


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
                SELECT status, started_at, finished_at, summary_json
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

        return {
            "entity_code": entity_code,
            "has_cash_balancing_rows": (row_count or {}).get("row_count", 0) > 0,
            "row_count": (row_count or {}).get("row_count", 0),
            "latest_run": dict(latest_run) if latest_run else None,
        }
