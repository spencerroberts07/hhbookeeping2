"""
Fixed asset / depreciation — service layer.

Canadian CCA declining-balance method:
    annual_dep = opening_NBV * cca_rate
    half-year rule (year of acquisition only): annual_dep *= 0.5

Bridlewood asset classes (seeded by seed_fixed_assets):
    Equipment / Furniture & Fixtures  Class 8   15%   GL 1510 / 1610 / 6900
    Computer Equipment                Class 8   15%   GL 1540 / 1640 / 6900
    Vehicles                          Class 10  30%   GL 1520 / 1620 / 6900

Depreciation expense rolls up to a single P&L account (6900); only
accumulated-depn (1610/1620/1640) splits per class on the balance sheet.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
    get_or_create_accounting_period,
)


SOURCE_MODULE_DEPRECIATION = "depreciation"
BATCH_LABEL_DEPRECIATION = "monthly_depreciation"

CCA_CLASS_8_EQUIPMENT = "class_8_equipment"
CCA_CLASS_8_COMPUTER = "class_8_computer"
CCA_CLASS_10_VEHICLE = "class_10_vehicle"

# Bridlewood collapses depreciation expense into a single P&L account.
# Accumulated depreciation stays split per class on the balance sheet.
DEPN_EXPENSE_GL_ACCOUNT = "6900"

# Canonical Bridlewood seed list (driven from the actual fixed-asset
# schedule the user supplied). opening_nbv_date = fiscal year start.
_BRIDLEWOOD_SEED = [
    {
        "asset_code": "EQUIP-001",
        "description": "Equipment / Furniture & Fixtures",
        "cca_class": CCA_CLASS_8_EQUIPMENT,
        "cca_rate": Decimal("0.15"),
        "asset_gl_account": "1510",
        "accum_depn_gl_account": "1610",
        "depn_expense_gl_account": DEPN_EXPENSE_GL_ACCOUNT,
        "cost": Decimal("386378.00"),
        "opening_nbv": Decimal("357400.00"),
        "opening_nbv_date": date(2024, 9, 30),
    },
    {
        "asset_code": "COMP-001",
        "description": "Computer Equipment",
        "cca_class": CCA_CLASS_8_COMPUTER,
        "cca_rate": Decimal("0.15"),
        "asset_gl_account": "1540",
        "accum_depn_gl_account": "1640",
        "depn_expense_gl_account": DEPN_EXPENSE_GL_ACCOUNT,
        "cost": Decimal("13098.00"),
        "opening_nbv": Decimal("12116.00"),
        "opening_nbv_date": date(2024, 9, 30),
    },
    {
        "asset_code": "VEH-001",
        "description": "Vehicles",
        "cca_class": CCA_CLASS_10_VEHICLE,
        "cca_rate": Decimal("0.30"),
        "asset_gl_account": "1520",
        "accum_depn_gl_account": "1620",
        "depn_expense_gl_account": DEPN_EXPENSE_GL_ACCOUNT,
        "cost": Decimal("30000.00"),
        "opening_nbv": Decimal("21000.00"),
        "opening_nbv_date": date(2024, 9, 30),
    },
]


def _money(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------------
# Seed
# ----------------------------------------------------------------------


def seed_fixed_assets(
    session, *, entity_code: str, actor_email: str
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    inserted = 0
    skipped = 0
    rows = []
    for cfg in _BRIDLEWOOD_SEED:
        existing = session.execute(
            text(
                "SELECT id FROM fixed_assets "
                "WHERE entity_id = :entity_id AND asset_code = :asset_code"
            ),
            {"entity_id": entity["id"], "asset_code": cfg["asset_code"]},
        ).mappings().first()
        if existing:
            skipped += 1
            rows.append({"asset_code": cfg["asset_code"], "status": "exists"})
            continue
        ins = session.execute(
            text(
                """
                INSERT INTO fixed_assets (
                    entity_id, asset_code, description, cca_class, cca_rate,
                    asset_gl_account, accum_depn_gl_account,
                    depn_expense_gl_account,
                    acquisition_date, cost, opening_nbv, opening_nbv_date,
                    is_active, notes
                ) VALUES (
                    :entity_id, :asset_code, :description, :cca_class, :cca_rate,
                    :asset_gl, :accum_gl, :exp_gl,
                    NULL, :cost, :opening_nbv, :opening_nbv_date,
                    TRUE, :notes
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "asset_code": cfg["asset_code"],
                "description": cfg["description"],
                "cca_class": cfg["cca_class"],
                "cca_rate": cfg["cca_rate"],
                "asset_gl": cfg["asset_gl_account"],
                "accum_gl": cfg["accum_depn_gl_account"],
                "exp_gl": cfg["depn_expense_gl_account"],
                "cost": cfg["cost"],
                "opening_nbv": cfg["opening_nbv"],
                "opening_nbv_date": cfg["opening_nbv_date"],
                "notes": f"Seeded for {entity_code} by {actor_email}",
            },
        ).mappings().first()
        inserted += 1
        rows.append(
            {
                "asset_code": cfg["asset_code"],
                "id": str(ins["id"]),
                "status": "inserted",
            }
        )

    return {
        "entity_code": entity_code,
        "inserted": inserted,
        "skipped": skipped,
        "assets": rows,
    }


# ----------------------------------------------------------------------
# Depreciation math
# ----------------------------------------------------------------------


def calculate_annual_depreciation(
    opening_nbv: Decimal, cca_rate: Decimal, half_year_rule: bool
) -> Decimal:
    base = Decimal(str(opening_nbv)) * Decimal(str(cca_rate))
    if half_year_rule:
        base = base * Decimal("0.5")
    return _money(base)


def generate_depreciation_schedule(
    session,
    *,
    entity_code: str,
    fiscal_year: int,
    actor_email: str,
    half_year_asset_codes: list[str] | None = None,
) -> dict[str, Any]:
    """
    Compute and store a per-asset annual schedule. half_year_asset_codes
    is the list of asset codes acquired in this fiscal year (the
    half-year rule applies to those only).
    """
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    half_year_codes = set(half_year_asset_codes or [])

    assets = session.execute(
        text(
            """
            SELECT id, asset_code, description, cca_class, cca_rate,
                   opening_nbv, opening_nbv_date,
                   asset_gl_account, accum_depn_gl_account,
                   depn_expense_gl_account
            FROM fixed_assets
            WHERE entity_id = :entity_id AND is_active = TRUE
            ORDER BY asset_code
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()

    if not assets:
        raise ValueError(
            "No active fixed_assets found. Call /api/depreciation/seed-assets first."
        )

    rows_out: list[dict[str, Any]] = []
    total_annual = Decimal("0.00")

    for asset in assets:
        # If a prior fiscal year's schedule exists, the current opening_nbv
        # is the prior year's closing_nbv; otherwise use the asset's
        # opening_nbv from the asset record.
        prior = session.execute(
            text(
                """
                SELECT closing_nbv
                FROM depreciation_schedules
                WHERE entity_id = :entity_id
                  AND fixed_asset_id = :asset_id
                  AND fiscal_year < :fiscal_year
                ORDER BY fiscal_year DESC
                LIMIT 1
                """
            ),
            {
                "entity_id": entity["id"],
                "asset_id": asset["id"],
                "fiscal_year": int(fiscal_year),
            },
        ).mappings().first()
        opening_nbv = _money(
            prior["closing_nbv"] if prior else asset["opening_nbv"]
        )
        rate = Decimal(str(asset["cca_rate"]))
        half_year = asset["asset_code"] in half_year_codes
        annual = calculate_annual_depreciation(opening_nbv, rate, half_year)
        monthly = (annual / Decimal("12")).quantize(Decimal("0.01"))
        closing = _money(opening_nbv - annual)

        session.execute(
            text(
                """
                INSERT INTO depreciation_schedules (
                    entity_id, fixed_asset_id, fiscal_year,
                    opening_nbv, annual_cca_rate, half_year_rule_applies,
                    annual_depreciation, monthly_depreciation, closing_nbv
                ) VALUES (
                    :entity_id, :asset_id, :fiscal_year,
                    :opening_nbv, :rate, :half_year,
                    :annual, :monthly, :closing
                )
                ON CONFLICT (entity_id, fixed_asset_id, fiscal_year)
                DO UPDATE SET
                    opening_nbv = EXCLUDED.opening_nbv,
                    annual_cca_rate = EXCLUDED.annual_cca_rate,
                    half_year_rule_applies = EXCLUDED.half_year_rule_applies,
                    annual_depreciation = EXCLUDED.annual_depreciation,
                    monthly_depreciation = EXCLUDED.monthly_depreciation,
                    closing_nbv = EXCLUDED.closing_nbv
                """
            ),
            {
                "entity_id": entity["id"],
                "asset_id": asset["id"],
                "fiscal_year": int(fiscal_year),
                "opening_nbv": opening_nbv,
                "rate": rate,
                "half_year": half_year,
                "annual": annual,
                "monthly": monthly,
                "closing": closing,
            },
        )

        total_annual += annual
        rows_out.append(
            {
                "asset_code": asset["asset_code"],
                "description": asset["description"],
                "cca_class": asset["cca_class"],
                "cca_rate": str(rate),
                "half_year_rule_applies": half_year,
                "opening_nbv": str(opening_nbv),
                "annual_depreciation": str(annual),
                "monthly_depreciation": str(monthly),
                "closing_nbv": str(closing),
            }
        )

    return {
        "entity_code": entity_code,
        "fiscal_year": int(fiscal_year),
        "asset_count": len(rows_out),
        "total_annual_depreciation": str(total_annual.quantize(Decimal("0.01"))),
        "rows": rows_out,
    }


# ----------------------------------------------------------------------
# Journal builder
# ----------------------------------------------------------------------


def _fiscal_year_for_period(entity: dict[str, Any], period_end: date) -> int:
    """
    Bridlewood's fiscal year ends 9/30 (per entities.fiscal_year_end_*).
    Fiscal year is labelled by the calendar year in which it ends.
    """
    fy_month = entity.get("fiscal_year_end_month") or 9
    fy_day = entity.get("fiscal_year_end_day") or 30
    if (period_end.month, period_end.day) <= (fy_month, fy_day):
        return period_end.year
    return period_end.year + 1


def build_depreciation_journal(
    session,
    *,
    entity_code: str,
    period_end: date,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    period_start = period_end.replace(day=1)
    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], period_end
    )
    fiscal_year = _fiscal_year_for_period(entity, period_end)

    # Pull active assets and their schedule rows for this fiscal year.
    rows = session.execute(
        text(
            """
            SELECT fa.id AS asset_id, fa.asset_code, fa.description,
                   fa.depn_expense_gl_account, fa.accum_depn_gl_account,
                   ds.id AS schedule_id, ds.monthly_depreciation
            FROM fixed_assets fa
            LEFT JOIN depreciation_schedules ds
                ON ds.fixed_asset_id = fa.id
                   AND ds.entity_id = fa.entity_id
                   AND ds.fiscal_year = :fiscal_year
            WHERE fa.entity_id = :entity_id AND fa.is_active = TRUE
            ORDER BY fa.asset_code
            """
        ),
        {"entity_id": entity["id"], "fiscal_year": fiscal_year},
    ).mappings().all()

    if not rows:
        raise ValueError(
            "No active fixed_assets found. Seed assets and generate "
            "the schedule before posting depreciation."
        )
    missing = [r["asset_code"] for r in rows if r["schedule_id"] is None]
    if missing:
        raise ValueError(
            f"No depreciation_schedules row for fiscal_year={fiscal_year} "
            f"on assets: {missing}. Generate the schedule first."
        )

    # Build the journal: one Dr+Cr pair per asset.
    journal_lines: list[dict[str, Any]] = []
    total = Decimal("0.00")
    line_number = 0
    for r in rows:
        monthly = _money(r["monthly_depreciation"])
        if monthly == Decimal("0.00"):
            continue
        total += monthly
        line_number += 1
        journal_lines.append(
            {
                "line_number": line_number,
                "account_code": r["depn_expense_gl_account"],
                "debit_amount": monthly,
                "credit_amount": Decimal("0.00"),
                "memo": f"Monthly depreciation — {r['description']}",
                "asset_id": r["asset_id"],
            }
        )
        line_number += 1
        journal_lines.append(
            {
                "line_number": line_number,
                "account_code": r["accum_depn_gl_account"],
                "debit_amount": Decimal("0.00"),
                "credit_amount": monthly,
                "memo": f"Monthly depreciation — {r['description']}",
                "asset_id": r["asset_id"],
            }
        )

    if total == Decimal("0.00"):
        raise ValueError("All assets have zero monthly depreciation; nothing to post.")

    # Idempotency: refuse if any per-asset journal line already exists for
    # this period. (Combined check; the unique constraint also enforces.)
    already_posted = session.execute(
        text(
            """
            SELECT fixed_asset_id
            FROM depreciation_journal_lines
            WHERE entity_id = :entity_id
              AND period_start = :period_start
            """
        ),
        {"entity_id": entity["id"], "period_start": period_start},
    ).mappings().all()
    if already_posted:
        # Still allow re-posting by overwriting the journal_batches row,
        # but warn the caller via the response.
        existing_count = len(already_posted)
    else:
        existing_count = 0

    summary = {
        "fiscal_year": fiscal_year,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "asset_count": len(rows),
        "total_monthly_depreciation": str(total.quantize(Decimal("0.01"))),
        "previously_posted_asset_count": existing_count,
    }

    batch = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status,
                total_debits, total_credits, summary_json
            ) VALUES (
                :entity_id, :accounting_period_id, :source_module, :batch_label,
                'draft', 'draft_ready',
                :total_debits, :total_credits, CAST(:summary_json AS jsonb)
            )
            ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label)
            DO UPDATE SET
                status = 'draft',
                workflow_status = 'draft_ready',
                total_debits = EXCLUDED.total_debits,
                total_credits = EXCLUDED.total_credits,
                summary_json = EXCLUDED.summary_json,
                submitted_by = NULL, submitted_at = NULL,
                reviewed_by = NULL, reviewed_at = NULL,
                approved_by = NULL, approved_at = NULL,
                approval_note = NULL, rejection_note = NULL,
                locked_by = NULL, locked_at = NULL,
                updated_at = NOW()
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
            "source_module": SOURCE_MODULE_DEPRECIATION,
            "batch_label": BATCH_LABEL_DEPRECIATION,
            "total_debits": total,
            "total_credits": total,
            "summary_json": json.dumps(summary),
        },
    ).mappings().first()
    journal_batch_id = batch["id"]

    # Wipe + rewrite journal_lines and depreciation_journal_lines for this batch.
    session.execute(
        text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
        {"id": journal_batch_id},
    )
    session.execute(
        text(
            """
            DELETE FROM depreciation_journal_lines
            WHERE entity_id = :entity_id AND period_start = :period_start
            """
        ),
        {"entity_id": entity["id"], "period_start": period_start},
    )

    for jl in journal_lines:
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :id, :line_number, :account_code,
                    :debit_amount, :credit_amount, :memo, CAST(:src AS jsonb)
                )
                """
            ),
            {
                "id": journal_batch_id,
                "line_number": jl["line_number"],
                "account_code": jl["account_code"],
                "debit_amount": jl["debit_amount"],
                "credit_amount": jl["credit_amount"],
                "memo": jl["memo"],
                "src": json.dumps(
                    {
                        "source_module": SOURCE_MODULE_DEPRECIATION,
                        "fixed_asset_id": str(jl["asset_id"]),
                        "fiscal_year": fiscal_year,
                    }
                ),
            },
        )

    for r in rows:
        monthly = _money(r["monthly_depreciation"])
        if monthly == Decimal("0.00"):
            continue
        session.execute(
            text(
                """
                INSERT INTO depreciation_journal_lines (
                    entity_id, accounting_period_id, fixed_asset_id,
                    journal_batch_id,
                    period_start, period_end, monthly_depreciation,
                    debit_account, credit_account, posted_at
                ) VALUES (
                    :entity_id, :accounting_period_id, :asset_id,
                    :batch_id,
                    :period_start, :period_end, :monthly,
                    :dr, :cr, NOW()
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "asset_id": r["asset_id"],
                "batch_id": journal_batch_id,
                "period_start": period_start,
                "period_end": period_end,
                "monthly": monthly,
                "dr": r["depn_expense_gl_account"],
                "cr": r["accum_depn_gl_account"],
            },
        )

    return {
        "journal_batch_id": str(journal_batch_id),
        "entity_code": entity_code,
        "fiscal_year": fiscal_year,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "asset_count": len(rows),
        "total_debits": str(total.quantize(Decimal("0.01"))),
        "total_credits": str(total.quantize(Decimal("0.01"))),
        "previously_posted_asset_count": existing_count,
    }


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------


def list_fixed_assets(session, *, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "fixed_assets"):
        return {"entity_code": entity_code, "count": 0, "assets": []}

    rows = session.execute(
        text(
            """
            SELECT id, asset_code, description, cca_class, cca_rate,
                   asset_gl_account, accum_depn_gl_account,
                   depn_expense_gl_account,
                   acquisition_date, cost, opening_nbv, opening_nbv_date,
                   is_active, disposal_date, disposal_proceeds, notes,
                   created_at
            FROM fixed_assets
            WHERE entity_id = :entity_id
            ORDER BY asset_code
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "count": len(rows),
        "assets": [
            {
                "id": str(r["id"]),
                "asset_code": r["asset_code"],
                "description": r["description"],
                "cca_class": r["cca_class"],
                "cca_rate": str(r["cca_rate"]),
                "asset_gl_account": r["asset_gl_account"],
                "accum_depn_gl_account": r["accum_depn_gl_account"],
                "depn_expense_gl_account": r["depn_expense_gl_account"],
                "acquisition_date": (
                    r["acquisition_date"].isoformat() if r["acquisition_date"] else None
                ),
                "cost": str(r["cost"]),
                "opening_nbv": str(r["opening_nbv"]),
                "opening_nbv_date": r["opening_nbv_date"].isoformat() if r["opening_nbv_date"] else None,
                "is_active": r["is_active"],
                "disposal_date": (
                    r["disposal_date"].isoformat() if r["disposal_date"] else None
                ),
                "disposal_proceeds": (
                    str(r["disposal_proceeds"]) if r["disposal_proceeds"] is not None else None
                ),
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


def get_depreciation_schedule(
    session, *, entity_code: str, fiscal_year: int
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            SELECT fa.asset_code, fa.description, fa.cca_class,
                   ds.opening_nbv, ds.annual_cca_rate, ds.half_year_rule_applies,
                   ds.annual_depreciation, ds.monthly_depreciation, ds.closing_nbv
            FROM depreciation_schedules ds
            JOIN fixed_assets fa ON fa.id = ds.fixed_asset_id
            WHERE ds.entity_id = :entity_id AND ds.fiscal_year = :fiscal_year
            ORDER BY fa.asset_code
            """
        ),
        {"entity_id": entity["id"], "fiscal_year": int(fiscal_year)},
    ).mappings().all()

    total_annual = sum(
        (Decimal(str(r["annual_depreciation"])) for r in rows), Decimal("0")
    )
    total_monthly = sum(
        (Decimal(str(r["monthly_depreciation"])) for r in rows), Decimal("0")
    )

    return {
        "entity_code": entity_code,
        "fiscal_year": int(fiscal_year),
        "row_count": len(rows),
        "total_annual_depreciation": str(total_annual.quantize(Decimal("0.01"))),
        "total_monthly_depreciation": str(total_monthly.quantize(Decimal("0.01"))),
        "rows": [
            {
                "asset_code": r["asset_code"],
                "description": r["description"],
                "cca_class": r["cca_class"],
                "opening_nbv": str(r["opening_nbv"]),
                "annual_cca_rate": str(r["annual_cca_rate"]),
                "half_year_rule_applies": r["half_year_rule_applies"],
                "annual_depreciation": str(r["annual_depreciation"]),
                "monthly_depreciation": str(r["monthly_depreciation"]),
                "closing_nbv": str(r["closing_nbv"]),
            }
            for r in rows
        ],
    }


def get_depreciation_summary(
    session, *, entity_code: str, period_end: date
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    fiscal_year = _fiscal_year_for_period(entity, period_end)

    monthly_rows = session.execute(
        text(
            """
            SELECT fa.cca_class, fa.description,
                   ds.monthly_depreciation, ds.opening_nbv, ds.closing_nbv
            FROM depreciation_schedules ds
            JOIN fixed_assets fa ON fa.id = ds.fixed_asset_id
            WHERE ds.entity_id = :entity_id AND ds.fiscal_year = :fiscal_year
            ORDER BY fa.cca_class
            """
        ),
        {"entity_id": entity["id"], "fiscal_year": fiscal_year},
    ).mappings().all()

    posted_rows = session.execute(
        text(
            """
            SELECT fa.cca_class,
                   COALESCE(SUM(djl.monthly_depreciation), 0) AS posted_total,
                   COUNT(*) AS posted_periods
            FROM depreciation_journal_lines djl
            JOIN fixed_assets fa ON fa.id = djl.fixed_asset_id
            WHERE djl.entity_id = :entity_id
              AND djl.period_end <= :period_end
              AND djl.period_start >= :ytd_start
            GROUP BY fa.cca_class
            """
        ),
        {
            "entity_id": entity["id"],
            "period_end": period_end,
            "ytd_start": date(
                fiscal_year - 1,
                int(entity.get("fiscal_year_end_month") or 9),
                int(entity.get("fiscal_year_end_day") or 30),
            ),
        },
    ).mappings().all()
    posted_by_class = {r["cca_class"]: r for r in posted_rows}

    by_class: list[dict[str, Any]] = []
    grand_monthly = Decimal("0")
    grand_ytd = Decimal("0")
    for r in monthly_rows:
        cls = r["cca_class"]
        monthly = Decimal(str(r["monthly_depreciation"]))
        ytd = Decimal(str(posted_by_class.get(cls, {}).get("posted_total") or 0))
        grand_monthly += monthly
        grand_ytd += ytd
        by_class.append(
            {
                "cca_class": cls,
                "description": r["description"],
                "monthly_depreciation": str(monthly),
                "ytd_posted": str(ytd),
                "current_nbv_estimate": str(
                    (Decimal(str(r["opening_nbv"])) - ytd).quantize(Decimal("0.01"))
                ),
            }
        )

    return {
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "fiscal_year": fiscal_year,
        "grand_total_monthly": str(grand_monthly.quantize(Decimal("0.01"))),
        "grand_total_ytd_posted": str(grand_ytd.quantize(Decimal("0.01"))),
        "by_class": by_class,
    }


# ----------------------------------------------------------------------
# Close-control-center section
# ----------------------------------------------------------------------


def section_depreciation(
    session,
    *,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    if not _has_table(session, "depreciation_journal_lines"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "depreciation_journal_lines table not present",
        }

    period_start = period_end.replace(day=1)
    posted = session.execute(
        text(
            """
            SELECT fixed_asset_id, monthly_depreciation, journal_batch_id
            FROM depreciation_journal_lines
            WHERE entity_id = :entity_id
              AND period_start = :period_start
            """
        ),
        {"entity_id": entity_id, "period_start": period_start},
    ).mappings().all()

    active_count = session.execute(
        text(
            """
            SELECT COUNT(*) AS n FROM fixed_assets
            WHERE entity_id = :entity_id AND is_active = TRUE
            """
        ),
        {"entity_id": entity_id},
    ).mappings().first()["n"] or 0

    if not posted:
        if active_count == 0:
            return {
                "status": "needs_review",
                "module_present": True,
                "summary": (
                    "No fixed assets seeded. POST /api/depreciation/seed-assets."
                ),
            }
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"No depreciation journal posted for "
                f"{period_end.isoformat()} (active assets: {active_count}). "
                "POST /api/depreciation/build-journal."
            ),
        }

    total = sum(
        (Decimal(str(r["monthly_depreciation"])) for r in posted), Decimal("0")
    )
    return {
        "status": "ready",
        "module_present": True,
        "summary": (
            f"Depreciation posted for {len(posted)} of {active_count} assets, "
            f"total ${total.quantize(Decimal('0.01'))}."
        ),
        "asset_count_posted": len(posted),
        "active_asset_count": active_count,
        "total_posted": str(total.quantize(Decimal("0.01"))),
    }
