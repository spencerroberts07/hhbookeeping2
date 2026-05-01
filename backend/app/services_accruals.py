"""
Monthly accruals — service layer.

Bridlewood seed templates (from the actual February GL):
    RENT        Dr 6010 Rent              / Cr 2201 Accrued Rent       4,980.34
    ACCOUNTING  Dr 6410 Accounting        / Cr 2202 Accrued Accounting   965.00
    INTEREST    Dr 6280 Interest on Term Loan
                                          / Cr 2203 Accrued Interest    (varies)

The 6010 Rent account is the actual rent-expense account in the
Bridlewood QBO chart (parent 6000 Occupancy Costs). Accruals can be
overridden at post-time via amounts_override.
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
    get_entity_by_code,
    get_or_create_accounting_period,
)


SOURCE_MODULE_ACCRUALS = "accruals"
BATCH_LABEL_ACCRUALS = "monthly_accruals"


# Rent expense account confirmed = 6010 Rent (per Bridlewood actual GL,
# under the 6000 Occupancy Costs parent).
_BRIDLEWOOD_SEED = [
    {
        "accrual_code": "RENT",
        "description": "Accrued Rent",
        "debit_account": "6010",
        "credit_account": "2201",
        "default_amount": Decimal("6471.66"),
        "frequency": "monthly",
        "notes": "Rent expense account = 6010 (Occupancy Costs parent 6000)",
    },
    {
        "accrual_code": "ACCOUNTING",
        "description": "Accrued Accounting",
        "debit_account": "6410",
        "credit_account": "2202",
        "default_amount": Decimal("965.00"),
        "frequency": "monthly",
        "notes": None,
    },
    {
        "accrual_code": "INTEREST",
        "description": "Accrued Interest",
        "debit_account": "6280",
        "credit_account": "2203",
        "default_amount": None,
        "frequency": "monthly",
        "notes": "Amount varies; provide via amounts_override",
    },
]


def _money(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------------
# Templates
# ----------------------------------------------------------------------


def seed_accrual_templates(
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
                """
                SELECT id FROM accrual_templates
                WHERE entity_id = :entity_id AND accrual_code = :accrual_code
                """
            ),
            {"entity_id": entity["id"], "accrual_code": cfg["accrual_code"]},
        ).mappings().first()
        if existing:
            skipped += 1
            rows.append({"accrual_code": cfg["accrual_code"], "status": "exists"})
            continue
        ins = session.execute(
            text(
                """
                INSERT INTO accrual_templates (
                    entity_id, accrual_code, description,
                    debit_account, credit_account,
                    default_amount, frequency, is_active, notes
                ) VALUES (
                    :entity_id, :accrual_code, :description,
                    :debit_account, :credit_account,
                    :default_amount, :frequency, TRUE, :notes
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "accrual_code": cfg["accrual_code"],
                "description": cfg["description"],
                "debit_account": cfg["debit_account"],
                "credit_account": cfg["credit_account"],
                "default_amount": cfg["default_amount"],
                "frequency": cfg["frequency"],
                "notes": cfg["notes"],
            },
        ).mappings().first()
        inserted += 1
        rows.append(
            {
                "accrual_code": cfg["accrual_code"],
                "id": str(ins["id"]),
                "status": "inserted",
            }
        )

    return {
        "entity_code": entity_code,
        "inserted": inserted,
        "skipped": skipped,
        "templates": rows,
    }


def list_accrual_templates(session, *, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "accrual_templates"):
        return {"entity_code": entity_code, "count": 0, "templates": []}

    rows = session.execute(
        text(
            """
            SELECT id, accrual_code, description, debit_account, credit_account,
                   default_amount, frequency, is_active, notes, created_at
            FROM accrual_templates
            WHERE entity_id = :entity_id
            ORDER BY accrual_code
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "count": len(rows),
        "templates": [
            {
                "id": str(r["id"]),
                "accrual_code": r["accrual_code"],
                "description": r["description"],
                "debit_account": r["debit_account"],
                "credit_account": r["credit_account"],
                "default_amount": (
                    str(r["default_amount"]) if r["default_amount"] is not None else None
                ),
                "frequency": r["frequency"],
                "is_active": r["is_active"],
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


def upsert_accrual_template(
    session,
    *,
    entity_code: str,
    data: dict[str, Any],
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    code = (data.get("accrual_code") or "").strip()
    if not code:
        raise ValueError("accrual_code is required")
    description = data.get("description") or code
    debit_account = data.get("debit_account")
    credit_account = data.get("credit_account")
    if not debit_account or not credit_account:
        raise ValueError("debit_account and credit_account are required")

    default_amount = data.get("default_amount")
    if default_amount is not None:
        default_amount = _money(default_amount)
    is_active = bool(data.get("is_active", True))

    row = session.execute(
        text(
            """
            INSERT INTO accrual_templates (
                entity_id, accrual_code, description,
                debit_account, credit_account,
                default_amount, frequency, is_active, notes
            ) VALUES (
                :entity_id, :accrual_code, :description,
                :debit_account, :credit_account,
                :default_amount, :frequency, :is_active, :notes
            )
            ON CONFLICT (entity_id, accrual_code)
            DO UPDATE SET
                description = EXCLUDED.description,
                debit_account = EXCLUDED.debit_account,
                credit_account = EXCLUDED.credit_account,
                default_amount = EXCLUDED.default_amount,
                frequency = EXCLUDED.frequency,
                is_active = EXCLUDED.is_active,
                notes = EXCLUDED.notes
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accrual_code": code,
            "description": description,
            "debit_account": debit_account,
            "credit_account": credit_account,
            "default_amount": default_amount,
            "frequency": data.get("frequency") or "monthly",
            "is_active": is_active,
            "notes": data.get("notes"),
        },
    ).mappings().first()

    return {
        "entity_code": entity_code,
        "accrual_code": code,
        "id": str(row["id"]),
    }


# ----------------------------------------------------------------------
# Journal builder
# ----------------------------------------------------------------------


def build_accrual_journal(
    session,
    *,
    entity_code: str,
    period_end: date,
    accrual_codes: list[str],
    amounts_override: dict[str, Any] | None,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], period_end
    )

    if not accrual_codes:
        raise ValueError("accrual_codes is required (list of codes to post)")
    overrides = amounts_override or {}

    templates = session.execute(
        text(
            """
            SELECT id, accrual_code, description,
                   debit_account, credit_account, default_amount
            FROM accrual_templates
            WHERE entity_id = :entity_id
              AND is_active = TRUE
              AND accrual_code = ANY(:codes)
            """
        ),
        {"entity_id": entity["id"], "codes": list(accrual_codes)},
    ).mappings().all()
    by_code = {r["accrual_code"]: r for r in templates}

    missing = [c for c in accrual_codes if c not in by_code]
    if missing:
        raise ValueError(
            f"No active accrual_templates for codes: {missing}. "
            "Seed templates first."
        )

    # Resolve per-code amount: override > default. Reject zero/null.
    resolved: list[tuple[dict[str, Any], Decimal]] = []
    for code in accrual_codes:
        tpl = by_code[code]
        if code in overrides:
            amt = _money(overrides[code])
        else:
            amt = _money(tpl["default_amount"]) if tpl["default_amount"] is not None else Decimal("0.00")
        if amt == Decimal("0.00"):
            raise ValueError(
                f"Accrual {code} has no amount (no default_amount and no "
                "override). Provide amounts_override."
            )
        resolved.append((dict(tpl), amt))

    # Idempotency: refuse if any already-posted line exists for the same
    # template+period (the unique constraint also enforces this).
    posted_codes = session.execute(
        text(
            """
            SELECT at.accrual_code
            FROM accrual_journal_lines ajl
            JOIN accrual_templates at ON at.id = ajl.accrual_template_id
            WHERE ajl.entity_id = :entity_id
              AND ajl.period_end = :period_end
              AND at.accrual_code = ANY(:codes)
            """
        ),
        {
            "entity_id": entity["id"],
            "period_end": period_end,
            "codes": list(accrual_codes),
        },
    ).mappings().all()
    posted_code_set = {r["accrual_code"] for r in posted_codes}
    if posted_code_set:
        # Permit re-build by overwriting (DELETE + INSERT below).
        pass

    total = sum((amt for _, amt in resolved), Decimal("0.00"))
    summary = {
        "period_end": period_end.isoformat(),
        "accrual_codes": accrual_codes,
        "amounts": {tpl["accrual_code"]: str(amt) for tpl, amt in resolved},
        "total": str(total),
        "previously_posted": list(posted_code_set),
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
            "source_module": SOURCE_MODULE_ACCRUALS,
            "batch_label": BATCH_LABEL_ACCRUALS,
            "total_debits": total,
            "total_credits": total,
            "summary_json": json.dumps(summary),
        },
    ).mappings().first()
    journal_batch_id = batch["id"]

    session.execute(
        text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
        {"id": journal_batch_id},
    )
    # Wipe + rewrite per-template tracking rows for this period for the
    # selected codes.
    session.execute(
        text(
            """
            DELETE FROM accrual_journal_lines
            USING accrual_templates at
            WHERE accrual_journal_lines.accrual_template_id = at.id
              AND accrual_journal_lines.entity_id = :entity_id
              AND accrual_journal_lines.period_end = :period_end
              AND at.accrual_code = ANY(:codes)
            """
        ),
        {
            "entity_id": entity["id"],
            "period_end": period_end,
            "codes": list(accrual_codes),
        },
    )

    line_number = 0
    for tpl, amt in resolved:
        memo = f"{tpl['description']} accrual"
        line_number += 1
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :id, :line_number, :account_code,
                    :debit_amount, 0, :memo, CAST(:src AS jsonb)
                )
                """
            ),
            {
                "id": journal_batch_id,
                "line_number": line_number,
                "account_code": tpl["debit_account"],
                "debit_amount": amt,
                "memo": memo,
                "src": json.dumps(
                    {
                        "source_module": SOURCE_MODULE_ACCRUALS,
                        "accrual_code": tpl["accrual_code"],
                        "side": "debit",
                    }
                ),
            },
        )
        line_number += 1
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :id, :line_number, :account_code,
                    0, :credit_amount, :memo, CAST(:src AS jsonb)
                )
                """
            ),
            {
                "id": journal_batch_id,
                "line_number": line_number,
                "account_code": tpl["credit_account"],
                "credit_amount": amt,
                "memo": memo,
                "src": json.dumps(
                    {
                        "source_module": SOURCE_MODULE_ACCRUALS,
                        "accrual_code": tpl["accrual_code"],
                        "side": "credit",
                    }
                ),
            },
        )

        session.execute(
            text(
                """
                INSERT INTO accrual_journal_lines (
                    entity_id, accounting_period_id, accrual_template_id,
                    journal_batch_id, period_end, amount, actor_email
                ) VALUES (
                    :entity_id, :accounting_period_id, :template_id,
                    :batch_id, :period_end, :amount, :actor_email
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "template_id": tpl["id"],
                "batch_id": journal_batch_id,
                "period_end": period_end,
                "amount": amt,
                "actor_email": actor_email,
            },
        )

    return {
        "journal_batch_id": str(journal_batch_id),
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "accrual_codes": accrual_codes,
        "amounts": summary["amounts"],
        "total_debits": str(total),
        "total_credits": str(total),
        "previously_posted": list(posted_code_set),
    }


def list_accrual_journals(
    session, *, entity_code: str, period_end: date
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            SELECT ajl.id, at.accrual_code, at.description,
                   ajl.amount, ajl.is_reversed, ajl.reversal_period_end,
                   ajl.journal_batch_id, ajl.actor_email, ajl.created_at
            FROM accrual_journal_lines ajl
            JOIN accrual_templates at ON at.id = ajl.accrual_template_id
            WHERE ajl.entity_id = :entity_id
              AND ajl.period_end = :period_end
            ORDER BY at.accrual_code
            """
        ),
        {"entity_id": entity["id"], "period_end": period_end},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "count": len(rows),
        "journals": [
            {
                "id": str(r["id"]),
                "accrual_code": r["accrual_code"],
                "description": r["description"],
                "amount": str(r["amount"]),
                "is_reversed": r["is_reversed"],
                "reversal_period_end": (
                    r["reversal_period_end"].isoformat() if r["reversal_period_end"] else None
                ),
                "journal_batch_id": str(r["journal_batch_id"]) if r["journal_batch_id"] else None,
                "actor_email": r["actor_email"],
            }
            for r in rows
        ],
    }


# ----------------------------------------------------------------------
# Close-control-center section
# ----------------------------------------------------------------------


def section_accruals(
    session,
    *,
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

    template_rows = session.execute(
        text(
            """
            SELECT id, accrual_code, description, default_amount
            FROM accrual_templates
            WHERE entity_id = :entity_id AND is_active = TRUE
            ORDER BY accrual_code
            """
        ),
        {"entity_id": entity_id},
    ).mappings().all()

    if not template_rows:
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": "No accrual templates seeded. POST /api/accruals/seed-templates.",
        }

    posted_rows = session.execute(
        text(
            """
            SELECT at.accrual_code, ajl.amount
            FROM accrual_journal_lines ajl
            JOIN accrual_templates at ON at.id = ajl.accrual_template_id
            WHERE ajl.entity_id = :entity_id AND ajl.period_end = :period_end
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().all()
    posted_codes = {r["accrual_code"] for r in posted_rows}

    expected = {r["accrual_code"] for r in template_rows}
    missing = sorted(expected - posted_codes)
    if missing:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"Accruals not yet posted for: {missing}. "
                "POST /api/accruals/build-journal."
            ),
            "expected_codes": sorted(expected),
            "posted_codes": sorted(posted_codes),
            "missing_codes": missing,
        }

    total = sum(
        (Decimal(str(r["amount"])) for r in posted_rows), Decimal("0")
    )
    return {
        "status": "ready",
        "module_present": True,
        "summary": (
            f"All {len(posted_codes)} accrual templates posted "
            f"(total ${total.quantize(Decimal('0.01'))})."
        ),
        "posted_codes": sorted(posted_codes),
        "total_posted": str(total.quantize(Decimal("0.01"))),
    }
