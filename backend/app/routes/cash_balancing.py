from datetime import datetime, timezone
import json
import secrets as _secrets_mod
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import settings
from ..db import db_session
from ..services_auth import (
    _bearer_scheme,
    enforce_entity_code,
    require_role,
)
from ..google_sheets import (
    DailyCashLine,
    GoogleSheetsClient,
    guess_date,
    normalize_cash_balancing_rows,
    parse_weekly_cash_sheet,
    safe_decimal,
)

router = APIRouter(prefix="/api/cash-balancing", tags=["cash-balancing"])


def verify_sync_auth(
    request: Request,
    x_cron_secret: str | None = Header(default=None, alias="X-Cron-Secret"),
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """Accept EITHER a valid X-Cron-Secret header OR a Clerk bookkeeper
    session. The cron path is used by the Render scheduled job that
    can't acquire a Clerk JWT; the Clerk path stays for in-app
    triggers.

    Constant-time compare on the secret prevents timing leaks. A
    *wrong* secret returns 403 (not falling through to Clerk) so a
    misconfigured cron job fails loudly instead of silently 401'ing.
    """
    if x_cron_secret is not None:
        if (
            settings.cron_secret
            and _secrets_mod.compare_digest(x_cron_secret, settings.cron_secret)
        ):
            return {
                "id": "cron",
                "clerk_user_id": "cron",
                "email": "cron@bookwize.ca",
                "is_cron": True,
                "is_superadmin": False,
                "role": "bookkeeper",
            }
        raise HTTPException(status_code=403, detail="Invalid cron secret")

    # No cron header → defer to the normal Clerk bookkeeper check.
    return require_role("bookkeeper")(request, creds)


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

SPECIAL_DAY_VALUE_LABELS = {
    "Opening Cash": "opening_cash",
    "Closing Cash": "closing_cash",
}


class CashBalancingSyncRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    sheet_tabs: list[str] = Field(
        default_factory=list,
        examples=[["Feb1-Feb7", "Feb8-Feb14"]],
    )
    lookback_days: int = Field(default=56, ge=1, le=365)


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


def extract_special_day_values(rows: list[list[str]]) -> dict[str, dict[str, float | None]]:
    """
    Pull Opening Cash and Closing Cash by business date from the sideways weekly sheet.
    """
    if not rows or len(rows) < 6:
        return {}

    date_row = rows[3] if len(rows) > 3 else []
    result: dict[str, dict[str, float | None]] = {}

    for row in rows[5:]:
        label = str(row[0]).strip() if len(row) > 0 and row[0] is not None else ""
        target_key = SPECIAL_DAY_VALUE_LABELS.get(label)
        if not target_key:
            continue

        for col_index in range(1, 8):
            raw_date = date_row[col_index] if col_index < len(date_row) else None
            raw_amount = row[col_index] if col_index < len(row) else None

            business_date = guess_date(raw_date)
            amount = safe_decimal(raw_amount)

            if business_date is None:
                continue

            if business_date not in result:
                result[business_date] = {
                    "opening_cash": None,
                    "closing_cash": None,
                }

            result[business_date][target_key] = amount

    return result


def build_daily_groups(
    daily_lines: list[DailyCashLine],
    special_day_values: dict[str, dict[str, float | None]],
) -> dict[str, dict]:
    grouped: dict[str, dict] = {}

    for business_date, values in special_day_values.items():
        grouped[business_date] = {
            "tab_name": None,
            "total_sales": None,
            "total_hst": None,
            "opening_cash": values.get("opening_cash"),
            "closing_cash": values.get("closing_cash"),
            "lines": [],
        }

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
                "opening_cash": None,
                "closing_cash": None,
                "lines": [],
            }

        day_bucket = grouped[line.business_date]
        if not day_bucket["tab_name"]:
            day_bucket["tab_name"] = line.source_tab_name

        day_bucket["lines"].append(line)

        if line.line_label == "Item Sales":
            day_bucket["total_sales"] = line.amount
        elif line.line_label == "Tax - HST":
            day_bucket["total_hst"] = line.amount

    return grouped


def get_accounting_period_for_date(session, entity_id: str, business_date: str):
    period = session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, fiscal_year, fiscal_period_number
            FROM accounting_periods
            WHERE entity_id = :entity_id
              AND :business_date BETWEEN period_start AND period_end
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "business_date": business_date,
        },
    ).mappings().first()

    if not period:
        raise RuntimeError(
            f"No accounting period found for entity_id={entity_id} and business_date={business_date}"
        )

    return period


@router.post("/sync")
async def sync_cash_balancing(
    payload: CashBalancingSyncRequest,
    _user: Any = Depends(verify_sync_auth),
):
    # Cron callers run system-wide; they aren't pinned to a single
    # Clerk org so the per-entity match check doesn't apply.
    if not _user.get("is_cron"):
        enforce_entity_code(_user, payload.entity_code)
    selected_tabs: list[str] = dedupe_preserve_order(payload.sheet_tabs)

    with db_session() as session:
        entity = session.execute(
            text("SELECT id, entity_code FROM entities WHERE entity_code = :entity_code"),
            {"entity_code": payload.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        integration = session.execute(
            text(
                """
                SELECT spreadsheet_id
                FROM entity_integrations
                WHERE entity_id = :entity_id
                  AND integration_type = 'google_sheets'
                  AND integration_name = 'cash_balancing'
                  AND is_active = TRUE
                LIMIT 1
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        if not integration or not integration["spreadsheet_id"]:
            raise HTTPException(
                status_code=400,
                detail="No active Google Sheets cash balancing integration found for this entity",
            )

        mapping_rows = session.execute(
            text(
                """
                SELECT source_key, mapped_account_code
                FROM account_mapping_rules
                WHERE entity_id = :entity_id
                  AND source_type = 'cash_balancing_line_label'
                  AND is_active = TRUE
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()

        mapping_by_label: dict[str, str] = {}
        for row in mapping_rows:
            source_key = str(row["source_key"]).strip()
            mapped_account_code = str(row["mapped_account_code"]).strip()
            if source_key and mapped_account_code:
                mapping_by_label[source_key] = mapped_account_code

        source = session.execute(
            text(
                """
                INSERT INTO cash_balancing_sources (
                    entity_id, source_name, spreadsheet_id, lookback_days
                ) VALUES (
                    :entity_id, 'Cash Balancing', :spreadsheet_id, :lookback_days
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
                "spreadsheet_id": integration["spreadsheet_id"],
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
        mapped_line_count = 0
        unmapped_line_count = 0
        special_value_day_count = 0
        unmapped_labels: set[str] = set()
        period_labels_touched: set[str] = set()
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
                special_day_values = extract_special_day_values(raw_rows)
                daily_groups = build_daily_groups(parsed_daily_lines, special_day_values)

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

                for business_date, day_data in daily_groups.items():
                    accounting_period = get_accounting_period_for_date(
                        session=session,
                        entity_id=entity["id"],
                        business_date=business_date,
                    )
                    accounting_period_id = accounting_period["id"]
                    period_labels_touched.add(str(accounting_period["period_label"]))

                    if day_data.get("opening_cash") is not None or day_data.get("closing_cash") is not None:
                        special_value_day_count += 1

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
                        "opening_cash": day_data["opening_cash"],
                        "closing_cash": day_data["closing_cash"],
                    }

                    if not existing_day:
                        day_row = session.execute(
                            text(
                                """
                                INSERT INTO cash_balancing_days (
                                    entity_id,
                                    accounting_period_id,
                                    business_date,
                                    tab_name,
                                    opening_cash,
                                    closing_cash,
                                    total_sales,
                                    total_hst,
                                    raw_json
                                ) VALUES (
                                    :entity_id,
                                    :accounting_period_id,
                                    :business_date,
                                    :tab_name,
                                    :opening_cash,
                                    :closing_cash,
                                    :total_sales,
                                    :total_hst,
                                    CAST(:raw_json AS jsonb)
                                )
                                RETURNING id
                                """
                            ),
                            {
                                "entity_id": entity["id"],
                                "accounting_period_id": accounting_period_id,
                                "business_date": business_date,
                                "tab_name": day_data["tab_name"],
                                "opening_cash": day_data["opening_cash"],
                                "closing_cash": day_data["closing_cash"],
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
                                SET accounting_period_id = :accounting_period_id,
                                    tab_name = :tab_name,
                                    opening_cash = :opening_cash,
                                    closing_cash = :closing_cash,
                                    total_sales = :total_sales,
                                    total_hst = :total_hst,
                                    raw_json = CAST(:raw_json AS jsonb)
                                WHERE id = :id
                                """
                            ),
                            {
                                "id": cash_balancing_day_id,
                                "accounting_period_id": accounting_period_id,
                                "tab_name": day_data["tab_name"],
                                "opening_cash": day_data["opening_cash"],
                                "closing_cash": day_data["closing_cash"],
                                "total_sales": day_data["total_sales"],
                                "total_hst": day_data["total_hst"],
                                "raw_json": json.dumps(raw_json_payload),
                            },
                        )

                    day_upserted += 1

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
                        normalized_label = str(line.line_label).strip()
                        mapped_account_code = mapping_by_label.get(normalized_label)
                        translation_status = "mapped" if mapped_account_code else "pending"

                        if mapped_account_code:
                            mapped_line_count += 1
                        else:
                            unmapped_line_count += 1
                            unmapped_labels.add(normalized_label)

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
                                "mapped_account_code": mapped_account_code,
                                "translation_status": translation_status,
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
                "mapped_line_count": mapped_line_count,
                "unmapped_line_count": unmapped_line_count,
                "unmapped_labels": sorted(unmapped_labels),
                "period_labels_touched": sorted(period_labels_touched),
                "special_value_day_count": special_value_day_count,
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
                "mapped_line_count": mapped_line_count,
                "unmapped_line_count": unmapped_line_count,
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


@router.get("/latest")
def cash_balancing_latest(entity_code: str = Query(...)) -> dict[str, Any]:
    """
    Return the most recent cash_balancing_days row for the entity. Used by
    the dashboard's Cash Position card. 404 if no rows exist.
    """
    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :entity_code"),
            {"entity_code": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        row = session.execute(
            text(
                """
                SELECT business_date, opening_cash, closing_cash, total_sales,
                       total_hst, tab_name, raw_json
                  FROM cash_balancing_days
                 WHERE entity_id = :entity_id
                 ORDER BY business_date DESC
                 LIMIT 1
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No cash-balancing days for entity {entity_code!r}",
        )

    opening = row["opening_cash"]
    closing = row["closing_cash"]
    sales = row["total_sales"]
    deposits = sales or 0
    # Variance proxy: (closing - opening) vs sales — flags whether the
    # day's till math reconciles. Caller decides what to do with the value.
    variance = (
        (float(closing) - float(opening) - float(sales))
        if (closing is not None and opening is not None and sales is not None)
        else None
    )
    return {
        "business_date": (
            row["business_date"].isoformat()
            if hasattr(row["business_date"], "isoformat")
            else str(row["business_date"])
        ),
        "opening_balance": float(opening) if opening is not None else None,
        "closing_balance": float(closing) if closing is not None else None,
        "total_deposits": float(deposits) if deposits else 0.0,
        "total_withdrawals": None,  # not modelled — Google Sheet doesn't separate
        "variance": variance,
        "status": "balanced" if variance is not None and abs(variance) < 0.05 else "review",
        "tab_name": row["tab_name"],
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

        mapped_line_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS mapped_line_count
                FROM cash_balancing_lines l
                JOIN cash_balancing_days d ON d.id = l.cash_balancing_day_id
                WHERE d.entity_id = :entity_id
                  AND l.translation_status = 'mapped'
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        pending_line_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS pending_line_count
                FROM cash_balancing_lines l
                JOIN cash_balancing_days d ON d.id = l.cash_balancing_day_id
                WHERE d.entity_id = :entity_id
                  AND l.translation_status = 'pending'
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        period_linked_day_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS period_linked_day_count
                FROM cash_balancing_days
                WHERE entity_id = :entity_id
                  AND accounting_period_id IS NOT NULL
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        opening_cash_day_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS opening_cash_day_count
                FROM cash_balancing_days
                WHERE entity_id = :entity_id
                  AND opening_cash IS NOT NULL
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().first()

        closing_cash_day_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS closing_cash_day_count
                FROM cash_balancing_days
                WHERE entity_id = :entity_id
                  AND closing_cash IS NOT NULL
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
            "mapped_line_count": (mapped_line_count or {}).get("mapped_line_count", 0),
            "pending_line_count": (pending_line_count or {}).get("pending_line_count", 0),
            "period_linked_day_count": (period_linked_day_count or {}).get("period_linked_day_count", 0),
            "opening_cash_day_count": (opening_cash_day_count or {}).get("opening_cash_day_count", 0),
            "closing_cash_day_count": (closing_cash_day_count or {}).get("closing_cash_day_count", 0),
            "latest_run": dict(latest_run) if latest_run else None,
        }


# --------------------------------------------------------------------------
# Tab UI endpoints (added with the /bank Cash Balancing tab)
#
# These power the three tabs on the /bank page. Heavy work happens in
# SQL; minimal Python-side grouping.
# --------------------------------------------------------------------------


def _classify_status(over_short: float) -> str:
    """Same $10 threshold as the spec for status colour-coding."""
    if over_short > 10:
        return "over"
    if over_short < -10:
        return "short"
    return "balanced"


@router.get("/days")
def cash_balancing_days(
    entity_code: str,
    date_from: str | None = None,
    date_to: str | None = None,
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Daily summary + tender breakdown for the date range. Default
    range: current calendar month. Returns days[] + summary{} where
    each day carries computed over_short, paid_outs, and lines[]."""
    from datetime import date as _Date, datetime as _DateTime
    import calendar as _cal

    today = _DateTime.utcnow().date()
    df = (
        _Date.fromisoformat(date_from)
        if date_from
        else _Date(today.year, today.month, 1)
    )
    if date_to:
        dt = _Date.fromisoformat(date_to)
    else:
        dt = _Date(df.year, df.month, _cal.monthrange(df.year, df.month)[1])

    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        day_rows = session.execute(
            text(
                """
                SELECT id, business_date, opening_cash, closing_cash,
                       total_sales, total_hst
                  FROM cash_balancing_days
                 WHERE entity_id = :eid
                   AND business_date BETWEEN :df AND :dt
              ORDER BY business_date ASC
                """
            ),
            {"eid": entity["id"], "df": df, "dt": dt},
        ).mappings().all()
        day_ids = [r["id"] for r in day_rows]
        line_rows = (
            session.execute(
                text(
                    """
                    SELECT cash_balancing_day_id, line_label, line_code,
                           amount, mapped_account_code
                      FROM cash_balancing_lines
                     WHERE cash_balancing_day_id = ANY(:ids)
                  ORDER BY cash_balancing_day_id, line_label
                    """
                ),
                {"ids": day_ids},
            ).mappings().all()
            if day_ids
            else []
        )

    lines_by_day: dict[Any, list[dict[str, Any]]] = {}
    for r in line_rows:
        lines_by_day.setdefault(r["cash_balancing_day_id"], []).append({
            "line_label": r["line_label"],
            "line_code": r["line_code"],
            "amount": float(r["amount"] or 0),
            "mapped_account_code": r["mapped_account_code"],
        })

    days: list[dict[str, Any]] = []
    total_sales = total_hst = total_over = total_short = 0.0
    balanced_days = over_days = short_days = 0
    for d in day_rows:
        lines = lines_by_day.get(d["id"], [])
        # Per the design call:
        #   over_short = amount on the 'Cash over (short)' line.
        #   paid_outs  = sum of lines whose mapped GL is 6xxx.
        over_short = next(
            (l["amount"] for l in lines if l["line_label"] == "Cash over (short)"),
            0.0,
        )
        paid_outs = sum(
            l["amount"]
            for l in lines
            if (l["mapped_account_code"] or "").startswith("6")
        )
        status = _classify_status(over_short)
        sales = float(d["total_sales"] or 0)
        hst = float(d["total_hst"] or 0)
        total_sales += sales
        total_hst += hst
        if status == "over":
            total_over += over_short
            over_days += 1
        elif status == "short":
            total_short += over_short
            short_days += 1
        else:
            balanced_days += 1

        days.append({
            "id": str(d["id"]),
            "business_date": d["business_date"].isoformat(),
            "day_of_week": d["business_date"].strftime("%a"),
            "opening_cash": float(d["opening_cash"] or 0),
            "closing_cash": float(d["closing_cash"] or 0),
            "total_sales": sales,
            "total_hst": hst,
            "paid_outs": paid_outs,
            "over_short": over_short,
            "status": status,
            "lines": lines,
        })

    return {
        "entity_code": entity_code,
        "date_from": df.isoformat(),
        "date_to": dt.isoformat(),
        "days": days,
        "summary": {
            "total_sales": round(total_sales, 2),
            "total_hst": round(total_hst, 2),
            "total_over": round(total_over, 2),
            "total_short": round(total_short, 2),
            "net_variance": round(total_over + total_short, 2),
            "day_count": len(days),
            "balanced_days": balanced_days,
            "over_days": over_days,
            "short_days": short_days,
        },
    }


@router.get("/month-end-batch")
def cash_balancing_month_end_batch(
    entity_code: str,
    period_id: str | None = None,
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """The cash_balancing journal batch for a period. period_id omitted
    returns the most-recent batch. Carries an explicit imbalance number
    so the UI can show the UNBALANCED banner without recomputing."""
    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        params: dict[str, Any] = {"eid": entity["id"]}
        where_period = ""
        if period_id:
            where_period = "AND accounting_period_id = :pid"
            params["pid"] = period_id

        batch = session.execute(
            text(
                f"""
                SELECT id, status, total_debits, total_credits,
                       batch_label, summary_json, accounting_period_id
                  FROM journal_batches
                 WHERE entity_id = :eid
                   AND source_module = 'cash_balancing'
                   {where_period}
              ORDER BY created_at DESC LIMIT 1
                """
            ),
            params,
        ).mappings().first()

        if not batch:
            return {
                "batch_id": None,
                "status": None,
                "period_id": period_id,
                "total_debits": 0.0,
                "total_credits": 0.0,
                "imbalance": 0.0,
                "is_balanced": True,
                "lines": [],
            }

        lines = session.execute(
            text(
                """
                SELECT line_number, account_code,
                       debit_amount, credit_amount, memo
                  FROM journal_lines
                 WHERE journal_batch_id = :bid
              ORDER BY line_number
                """
            ),
            {"bid": batch["id"]},
        ).mappings().all()

        line_dr = sum((float(l["debit_amount"] or 0) for l in lines), 0.0)
        line_cr = sum((float(l["credit_amount"] or 0) for l in lines), 0.0)
        imbalance = round(line_dr - line_cr, 2)

        return {
            "batch_id": str(batch["id"]),
            "status": batch["status"],
            "period_id": str(batch["accounting_period_id"]),
            "batch_label": batch["batch_label"],
            "total_debits": float(batch["total_debits"] or 0),
            "total_credits": float(batch["total_credits"] or 0),
            "imbalance": imbalance,
            "is_balanced": abs(imbalance) < 0.01,
            "summary_json": batch.get("summary_json") or {},
            "lines": [
                {
                    "line_number": l["line_number"],
                    "account_code": l["account_code"],
                    "memo": l["memo"],
                    "debit_amount": float(l["debit_amount"] or 0),
                    "credit_amount": float(l["credit_amount"] or 0),
                }
                for l in lines
            ],
        }


class FixImbalanceRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    period_id: str = Field(...)
    offset_account_code: str = Field(default="1020")
    offset_description: str = Field(
        default="Cash balancing close — balancing offset (float movement + misc reconciliation)"
    )
    actor_email: str | None = None


@router.post("/fix-imbalance")
def cash_balancing_fix_imbalance(
    body: FixImbalanceRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Post a one-line balancing entry to the cash_balancing batch
    for the period. Only runs on draft_unbalanced. After posting, the
    batch is recomputed and flipped to status='draft'."""
    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {body.entity_code}")

        batch = session.execute(
            text(
                """
                SELECT id, status, total_debits, total_credits
                  FROM journal_batches
                 WHERE entity_id = :eid
                   AND accounting_period_id = :pid
                   AND source_module = 'cash_balancing'
              ORDER BY created_at DESC LIMIT 1
                """
            ),
            {"eid": entity["id"], "pid": body.period_id},
        ).mappings().first()
        if not batch:
            raise HTTPException(404, "No cash_balancing batch for this period.")
        if batch["status"] != "draft_unbalanced":
            raise HTTPException(
                409,
                f"Batch is in '{batch['status']}' — fix-imbalance only runs on draft_unbalanced.",
            )

        dr = float(batch["total_debits"] or 0)
        cr = float(batch["total_credits"] or 0)
        imbalance = round(dr - cr, 2)
        if abs(imbalance) < 0.01:
            raise HTTPException(409, "Batch totals already balance.")

        next_line_no = (
            session.execute(
                text(
                    """
                    SELECT COALESCE(MAX(line_number), 0) + 1 AS n
                      FROM journal_lines
                     WHERE journal_batch_id = :bid
                    """
                ),
                {"bid": batch["id"]},
            ).scalar()
        ) or 1

        # imbalance > 0 (Dr > Cr) → post a Cr to balance.
        # imbalance < 0 (Cr > Dr) → post a Dr to balance.
        if imbalance > 0:
            line_dr, line_cr = 0.0, abs(imbalance)
        else:
            line_dr, line_cr = abs(imbalance), 0.0

        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :bid, :ln, :acct, :dr, :cr, :memo, :sj
                )
                """
            ),
            {
                "bid": batch["id"],
                "ln": next_line_no,
                "acct": body.offset_account_code,
                "dr": line_dr,
                "cr": line_cr,
                "memo": body.offset_description,
                "sj": json.dumps(
                    {
                        "reason": "fix_imbalance",
                        "actor": body.actor_email,
                        "imbalance_resolved": imbalance,
                    }
                ),
            },
        )

        new_dr = round(dr + line_dr, 2)
        new_cr = round(cr + line_cr, 2)
        session.execute(
            text(
                """
                UPDATE journal_batches
                   SET total_debits = :dr,
                       total_credits = :cr,
                       status = 'draft',
                       updated_at = NOW()
                 WHERE id = :id
                """
            ),
            {"dr": new_dr, "cr": new_cr, "id": batch["id"]},
        )

        return {
            "batch_id": str(batch["id"]),
            "status": "draft",
            "total_debits": new_dr,
            "total_credits": new_cr,
            "imbalance_resolved": imbalance,
            "balancing_line": {
                "line_number": next_line_no,
                "account_code": body.offset_account_code,
                "debit_amount": line_dr,
                "credit_amount": line_cr,
                "memo": body.offset_description,
            },
        }


@router.get("/sync-history")
def cash_balancing_sync_history(
    entity_code: str,
    limit: int = 20,
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Recent cash_balancing_import_runs for the entity."""
    with db_session() as session:
        entity = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Unknown entity: {entity_code}")

        rows = session.execute(
            text(
                """
                SELECT id, run_type, status, started_at, finished_at,
                       tabs_read, summary_json, error_text
                  FROM cash_balancing_import_runs
                 WHERE entity_id = :eid
              ORDER BY started_at DESC
                 LIMIT :limit
                """
            ),
            {"eid": entity["id"], "limit": int(limit)},
        ).mappings().all()

    out: list[dict[str, Any]] = []
    for r in rows:
        started = r["started_at"]
        finished = r["finished_at"]
        duration = (
            (finished - started).total_seconds()
            if started and finished
            else None
        )
        tabs_read = r.get("tabs_read") or []
        sj = r.get("summary_json") or {}
        out.append({
            "id": str(r["id"]),
            "run_type": r["run_type"],
            "status": r["status"],
            "started_at": started.isoformat() if started else None,
            "finished_at": finished.isoformat() if finished else None,
            "duration_seconds": round(duration, 1) if duration else None,
            "tabs_read": (
                len(tabs_read) if isinstance(tabs_read, list) else None
            ),
            "days_upserted": sj.get("days_updated") or sj.get("days_upserted"),
            "lines_inserted": sj.get("lines_inserted"),
            "error_text": r.get("error_text"),
        })
    return {"runs": out, "count": len(out)}
