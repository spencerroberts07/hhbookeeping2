"""
Monthly COGS journal builder — service layer.

Three components, posted as a single journal_batch:
    1. POS-based COGS:        Dr 5010 / Cr 1120  (base_cogs)
    2. Reverse prior dating:  Dr 5010 / Cr 2030  (dating_reversal_amount)
    3. New month-end dating:  Dr 2030 / Cr 5010  (dating_new_amount)

Sign convention (positive amounts only in the standard case):
  - dating_reversal increases COGS (Dr 5010)
  - dating_new       decreases COGS (Cr 5010)
  - if either is entered as negative, the dr/cr flip

dating_new_amount auto-carries forward as next period's
suggested dating_reversal_amount.

Sanity checks (warnings only — never block):
  - net 5010 activity vs. GL's 5010 period_activity (if a GL import
    run covers this period)
  - implied COGS from inventory movement (1120 purchases minus net
    1120 change) vs. base_cogs

Workflow: status='draft', workflow_status='draft_ready' (a normal
draft per the journal_batch_workflow contract). The bookkeeper
submits and approves through the standard endpoints.
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


SOURCE_MODULE_COGS = "cogs"
BATCH_LABEL_COGS = "monthly_cogs"

ACCOUNT_COGS = "5010"
ACCOUNT_INVENTORY = "1120"
ACCOUNT_AP_HHSL = "2030"

SANITY_TOLERANCE = Decimal("1000.00")

# Inventory-adjustment reasons that the spec treats as shrinkage. Match
# against the cleaned reason (leading digits + underscore stripped, then
# uppercased). The POS parser used to concatenate the cost's fractional
# digits onto the reason — the v0.7 fix strips that — but historical
# rows in the DB may still carry truncated tails like "…ON_HA" / "…ON_H"
# from the era when the digit-bleed shifted the slice. Both forms are
# accepted here.
SHRINKAGE_REASONS = frozenset({
    "CYCLE_COUNT",
    "CYCLE_COUNT_ADJ_QTY_ON_HAND",
    "CYCLE_COUNT_ADJ_QTY_ON_HAN",  # legacy truncation (-1 char)
    "CYCLE_COUNT_ADJ_QTY_ON_HA",   # legacy truncation (-2 chars)
    "BROKEN_IN_STORE",
    "EXPIRED_GOODS",
    "STOLEN_ITEMS",
    "ITEM_RECOUNT_ADJ_QTY_ON_HAND",
    "ITEM_RECOUNT_ADJ_QTY_ON_HAN",  # legacy truncation
    "ITEM_RECOUNT_ADJ_QTY_ON_HA",   # legacy truncation
    "ITEM_RECOUNT_ADJ_QTY_ON_H",    # legacy truncation
    "LOSS_OTHER_REASONS",
})

GREETING_CARD_REASONS = frozenset({
    "GREETING_CARD_RETURNS",
})


def _clean_reason(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).upper().strip()
    # Strip leading "NNNN_" prefix added by the POS parser
    i = 0
    while i < len(raw) and raw[i].isdigit():
        i += 1
    if i > 0 and i < len(raw) and raw[i] == "_":
        return raw[i + 1 :]
    return raw


def _money(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------------
# Inputs
# ----------------------------------------------------------------------


def _resolve_pos_snapshot(
    session, *, entity_id: UUID, period_end: date
) -> dict[str, Any] | None:
    period_start = period_end.replace(day=1)
    row = session.execute(
        text(
            """
            SELECT s.id, s.import_run_id, s.period_start, s.period_end,
                   s.cogs_merchandise, s.cogs_non_merchandise,
                   s.merchandise_sales, s.non_merchandise_sales
            FROM pos_financial_snapshots s
            WHERE s.entity_id = :entity_id
              AND s.period_start = :period_start
              AND s.period_end = :period_end
            ORDER BY s.created_at DESC
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()
    return dict(row) if row else None


def _last_period_dating_new(
    session, *, entity_id: UUID, period_end: date
) -> Decimal:
    row = session.execute(
        text(
            """
            SELECT dating_new_amount
            FROM cogs_journal_inputs
            WHERE entity_id = :entity_id AND period_end < :period_end
            ORDER BY period_end DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()
    if not row:
        return Decimal("0.00")
    return _money(row["dating_new_amount"])


def _resolve_shrinkage(
    session, *, entity_id: UUID, period_start: date, period_end: date
) -> dict[str, Any]:
    """
    Walk inventory_adjustment_lines for the period and bucket them into
    shrinkage (losses on cycle counts, broken, expired, stolen, recount,
    loss-other), greeting card returns, and other (informational).

    Returns:
        shrinkage_cogs: ABS(sum of negative shrinkage costs) — positive
        greeting_card_adj: signed sum of greeting-card-returns lines
        shrinkage_breakdown: list of (reason, n, total, losses_only)
    """
    if not _has_table(session, "inventory_adjustment_lines"):
        return {
            "shrinkage_cogs": Decimal("0.00"),
            "greeting_card_adj": Decimal("0.00"),
            "shrinkage_breakdown": [],
            "greeting_card_breakdown": [],
            "module_present": False,
        }
    rows = session.execute(
        text(
            """
            SELECT adjustment_reason, adjustment_cost
            FROM inventory_adjustment_lines
            WHERE entity_id = :entity_id
              AND date_adjusted BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().all()

    shrinkage_total = Decimal("0.00")
    greeting_total = Decimal("0.00")
    shrinkage_by_reason: dict[str, dict[str, Any]] = {}
    greeting_by_reason: dict[str, dict[str, Any]] = {}
    for r in rows:
        reason_clean = _clean_reason(r["adjustment_reason"])
        cost = _money(r["adjustment_cost"])
        if reason_clean in SHRINKAGE_REASONS:
            entry = shrinkage_by_reason.setdefault(
                reason_clean, {"n": 0, "total": Decimal("0.00"), "losses": Decimal("0.00")}
            )
            entry["n"] += 1
            entry["total"] += cost
            if cost < 0:
                entry["losses"] += cost
                shrinkage_total += cost  # negative
        elif reason_clean in GREETING_CARD_REASONS:
            entry = greeting_by_reason.setdefault(
                reason_clean, {"n": 0, "total": Decimal("0.00")}
            )
            entry["n"] += 1
            entry["total"] += cost
            greeting_total += cost  # signed

    return {
        "shrinkage_cogs": abs(shrinkage_total),
        "greeting_card_adj": greeting_total,
        "shrinkage_breakdown": [
            {
                "reason": reason,
                "n": entry["n"],
                "total": str(entry["total"]),
                "losses_only": str(entry["losses"]),
            }
            for reason, entry in sorted(shrinkage_by_reason.items())
        ],
        "greeting_card_breakdown": [
            {
                "reason": reason,
                "n": entry["n"],
                "total": str(entry["total"]),
            }
            for reason, entry in sorted(greeting_by_reason.items())
        ],
        "module_present": True,
    }


def get_suggested_dating_reversal(
    session, *, entity_code: str, period_end: date
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    suggested = _last_period_dating_new(
        session, entity_id=entity["id"], period_end=period_end
    )
    return {
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "suggested_dating_reversal_amount": str(suggested),
        "source": "prior_period_dating_new",
    }


# ----------------------------------------------------------------------
# Sanity checks
# ----------------------------------------------------------------------


def _gl_period_activity(
    session, *, entity_id: UUID, account_code: str, period_end: date
) -> Decimal | None:
    """Latest gl_account_balances row for the period_end on this account."""
    period_start = period_end.replace(day=1)
    row = session.execute(
        text(
            """
            SELECT b.period_activity
            FROM gl_account_balances b
            JOIN gl_import_runs r ON r.id = b.import_run_id
            WHERE b.entity_id = :entity_id
              AND b.account_code = :account_code
              AND r.period_start = :period_start
              AND r.period_end = :period_end
            ORDER BY r.created_at DESC
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "account_code": account_code,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()
    if not row or row["period_activity"] is None:
        return None
    return _money(row["period_activity"])


def _app_inventory_purchases(
    session, *, entity_id: UUID, accounting_period_id: UUID,
    exclude_batch_id: UUID | None = None,
) -> Decimal:
    """
    Sum of debits to 1120 from app journal_lines for the period — i.e.
    inventory IN. Excludes the COGS journal batch itself (a credit, but
    we exclude defensively).
    """
    params: dict[str, Any] = {
        "entity_id": entity_id,
        "accounting_period_id": accounting_period_id,
        "account_code": ACCOUNT_INVENTORY,
    }
    exclude_clause = ""
    if exclude_batch_id is not None:
        exclude_clause = "AND jb.id <> :exclude_batch_id"
        params["exclude_batch_id"] = exclude_batch_id
    row = session.execute(
        text(
            f"""
            SELECT COALESCE(SUM(jl.debit_amount), 0) AS dr
            FROM journal_lines jl
            JOIN journal_batches jb ON jb.id = jl.journal_batch_id
            WHERE jb.entity_id = :entity_id
              AND jb.accounting_period_id = :accounting_period_id
              AND jl.account_code = :account_code
              {exclude_clause}
            """
        ),
        params,
    ).mappings().first()
    return _money(row["dr"]) if row else Decimal("0.00")


# ----------------------------------------------------------------------
# Builder
# ----------------------------------------------------------------------


def build_cogs_journal(
    session,
    *,
    entity_code: str,
    period_end: date,
    dating_new_amount: Decimal | None,
    dating_reversal_amount: Decimal | None,
    other_adjustment_amount: Decimal | None,
    other_adjustment_memo: str | None,
    actor_email: str,
    shrinkage_included: bool = True,
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
    if accounting_period_id is None:
        raise ValueError(
            f"No accounting_period covers {period_end.isoformat()} for {entity_code}"
        )

    snapshot = _resolve_pos_snapshot(
        session, entity_id=entity["id"], period_end=period_end
    )
    if not snapshot:
        raise ValueError(
            f"No pos_financial_snapshots row for {entity_code} "
            f"period {period_start.isoformat()}..{period_end.isoformat()}. "
            "Import the POS Financial report first."
        )

    base_cogs = _money(snapshot["cogs_merchandise"]) + _money(
        snapshot["cogs_non_merchandise"]
    )
    if base_cogs == Decimal("0.00"):
        raise ValueError(
            "POS snapshot reports zero COGS (cogs_merchandise + "
            "cogs_non_merchandise). Refusing to build a $0 journal."
        )

    shrinkage_data = _resolve_shrinkage(
        session, entity_id=entity["id"],
        period_start=period_start, period_end=period_end,
    )
    shrinkage_cogs = (
        shrinkage_data["shrinkage_cogs"] if shrinkage_included else Decimal("0.00")
    )
    greeting_card_adj = shrinkage_data["greeting_card_adj"]

    new_amt = _money(dating_new_amount) if dating_new_amount is not None else Decimal("0.00")
    rev_amt = (
        _money(dating_reversal_amount)
        if dating_reversal_amount is not None
        else _last_period_dating_new(
            session, entity_id=entity["id"], period_end=period_end
        )
    )
    other_amt = _money(other_adjustment_amount) if other_adjustment_amount is not None else Decimal("0.00")

    # Net effects (in QBO debit-positive convention):
    #   net_5010  = base_cogs + shrinkage + dating_reversal - dating_new + other
    #   net_1120  = -base_cogs - shrinkage  (credits — inventory went down)
    #   net_2030  = -dating_reversal + dating_new   (credit on reversal,
    #                                                debit on new dating)
    cogs_dr_5010 = base_cogs + shrinkage_cogs
    net_5010 = cogs_dr_5010 + rev_amt - new_amt + other_amt
    net_1120 = -cogs_dr_5010
    net_2030_dating = -rev_amt + new_amt

    # ------------------------------------------------------------------
    # Build journal lines (one component at a time so the batch is
    # readable to a bookkeeper opening it in the UI).
    # ------------------------------------------------------------------
    journal_lines: list[dict[str, Any]] = []

    def _add_pair(
        dr_account: str,
        cr_account: str,
        amount: Decimal,
        memo: str,
        component: str,
    ) -> None:
        if amount <= Decimal("0.00"):
            return
        journal_lines.append(
            {
                "account_code": dr_account,
                "debit_amount": amount,
                "credit_amount": Decimal("0.00"),
                "memo": memo,
                "component": component,
            }
        )
        journal_lines.append(
            {
                "account_code": cr_account,
                "debit_amount": Decimal("0.00"),
                "credit_amount": amount,
                "memo": memo,
                "component": component,
            }
        )

    # COMPONENT 1: POS-based COGS + shrinkage  Dr 5010 / Cr 1120
    if cogs_dr_5010 > 0:
        if shrinkage_cogs > 0:
            line1_memo = (
                f"Inventory adjustment - POS COGS ${base_cogs} + "
                f"shrinkage ${shrinkage_cogs}"
            )
        else:
            line1_memo = "Inventory adjustment"
        _add_pair(ACCOUNT_COGS, ACCOUNT_INVENTORY, cogs_dr_5010,
                  line1_memo, "pos_cogs")

    # COMPONENT 2: Reverse prior month dating
    if rev_amt > 0:
        _add_pair(ACCOUNT_COGS, ACCOUNT_AP_HHSL, rev_amt,
                  "Reverse Home dating", "dating_reversal")
    elif rev_amt < 0:
        _add_pair(ACCOUNT_AP_HHSL, ACCOUNT_COGS, abs(rev_amt),
                  "Reverse Home dating", "dating_reversal")

    # COMPONENT 3: New month-end dating
    if new_amt > 0:
        _add_pair(ACCOUNT_AP_HHSL, ACCOUNT_COGS, new_amt,
                  "Home dating", "dating_new")
    elif new_amt < 0:
        _add_pair(ACCOUNT_COGS, ACCOUNT_AP_HHSL, abs(new_amt),
                  "Home dating", "dating_new")

    # OPTIONAL: Other COGS adjustment Dr 5010 / Cr 1120
    if other_amt > 0:
        memo = (
            f"Other COGS adjustment — {other_adjustment_memo}"
            if other_adjustment_memo
            else "Other COGS adjustment"
        )
        _add_pair(ACCOUNT_COGS, ACCOUNT_INVENTORY, other_amt, memo, "other")
    elif other_amt < 0:
        memo = (
            f"Other COGS adjustment — {other_adjustment_memo}"
            if other_adjustment_memo
            else "Other COGS adjustment"
        )
        _add_pair(ACCOUNT_INVENTORY, ACCOUNT_COGS, abs(other_amt), memo, "other")

    total_debits = sum(l["debit_amount"] for l in journal_lines)
    total_credits = sum(l["credit_amount"] for l in journal_lines)

    # ------------------------------------------------------------------
    # Sanity checks (warnings only)
    # ------------------------------------------------------------------
    sanity_notes: list[str] = []
    sanity_warning = False

    gl_5010 = _gl_period_activity(
        session, entity_id=entity["id"], account_code=ACCOUNT_COGS,
        period_end=period_end,
    )
    if gl_5010 is not None:
        var_vs_gl = (net_5010 - gl_5010).quantize(Decimal("0.01"))
        if abs(var_vs_gl) > SANITY_TOLERANCE:
            sanity_warning = True
            sanity_notes.append(
                f"Net 5010 ${net_5010} vs GL period activity ${gl_5010} — "
                f"variance ${var_vs_gl} exceeds ${SANITY_TOLERANCE} tolerance."
            )
        else:
            sanity_notes.append(
                f"Net 5010 ${net_5010} matches GL period activity ${gl_5010} "
                f"within ${SANITY_TOLERANCE} (variance ${var_vs_gl})."
            )
    else:
        var_vs_gl = None
        sanity_notes.append("No GL import for this period — skipped GL 5010 check.")

    gl_1120 = _gl_period_activity(
        session, entity_id=entity["id"], account_code=ACCOUNT_INVENTORY,
        period_end=period_end,
    )
    app_purchases_to_1120 = _app_inventory_purchases(
        session, entity_id=entity["id"],
        accounting_period_id=accounting_period_id,
    )
    if gl_1120 is not None and app_purchases_to_1120 > 0:
        # Inventory movement: implied_cogs ≈ purchases - net_1120_change
        implied_cogs = (app_purchases_to_1120 - gl_1120).quantize(Decimal("0.01"))
        var_vs_inv = (base_cogs - implied_cogs).quantize(Decimal("0.01"))
        if abs(var_vs_inv) > SANITY_TOLERANCE:
            sanity_warning = True
            sanity_notes.append(
                f"Base COGS ${base_cogs} vs implied COGS ${implied_cogs} from "
                f"app purchases ${app_purchases_to_1120} - GL 1120 movement "
                f"${gl_1120} — variance ${var_vs_inv} exceeds "
                f"${SANITY_TOLERANCE} tolerance."
            )
        else:
            sanity_notes.append(
                f"Base COGS ${base_cogs} matches inventory-movement implied "
                f"${implied_cogs} within ${SANITY_TOLERANCE} (variance ${var_vs_inv})."
            )
    else:
        var_vs_inv = None
        sanity_notes.append(
            "Skipped inventory-movement check (need GL import + app inventory purchases)."
        )

    # ------------------------------------------------------------------
    # Persist journal_batch + journal_lines
    # ------------------------------------------------------------------
    summary = {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "base_cogs": str(base_cogs),
        "shrinkage_cogs": str(shrinkage_cogs),
        "shrinkage_included": shrinkage_included,
        "shrinkage_breakdown": shrinkage_data["shrinkage_breakdown"],
        "greeting_card_adj": str(greeting_card_adj),
        "greeting_card_breakdown": shrinkage_data["greeting_card_breakdown"],
        "dating_new_amount": str(new_amt),
        "dating_reversal_amount": str(rev_amt),
        "other_adjustment_amount": str(other_amt),
        "other_adjustment_memo": other_adjustment_memo,
        "net_5010_amount": str(net_5010),
        "net_1120_amount": str(net_1120),
        "net_2030_dating_amount": str(net_2030_dating),
        "sanity_check": {
            "warning": sanity_warning,
            "vs_gl_variance": str(var_vs_gl) if var_vs_gl is not None else None,
            "vs_inventory_movement_variance": str(var_vs_inv) if var_vs_inv is not None else None,
            "notes": sanity_notes,
        },
        "components": [l["component"] for l in journal_lines[::2]],
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
            "source_module": SOURCE_MODULE_COGS,
            "batch_label": BATCH_LABEL_COGS,
            "total_debits": total_debits,
            "total_credits": total_credits,
            "summary_json": json.dumps(summary),
        },
    ).mappings().first()
    journal_batch_id = batch["id"]

    session.execute(
        text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
        {"id": journal_batch_id},
    )
    line_number = 0
    for l in journal_lines:
        line_number += 1
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
                "line_number": line_number,
                "account_code": l["account_code"],
                "debit_amount": l["debit_amount"],
                "credit_amount": l["credit_amount"],
                "memo": l["memo"],
                "src": json.dumps(
                    {
                        "source_module": SOURCE_MODULE_COGS,
                        "component": l["component"],
                    }
                ),
            },
        )

    # ------------------------------------------------------------------
    # Persist cogs_journal_inputs row (for carry-forward + audit)
    # ------------------------------------------------------------------
    session.execute(
        text(
            """
            INSERT INTO cogs_journal_inputs (
                entity_id, accounting_period_id, pos_import_run_id,
                period_start, period_end,
                base_cogs, shrinkage_cogs, shrinkage_included, greeting_card_adj,
                dating_new_amount, dating_reversal_amount,
                other_adjustment_amount, other_adjustment_memo,
                net_5010_amount, net_1120_amount, net_2030_dating_amount,
                sanity_check_vs_gl_variance, sanity_check_vs_inventory_movement,
                sanity_check_warning, sanity_check_notes,
                journal_batch_id, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id, :pos_import_run_id,
                :period_start, :period_end,
                :base_cogs, :shrinkage_cogs, :shrinkage_included, :greeting_card_adj,
                :dating_new_amount, :dating_reversal_amount,
                :other_adjustment_amount, :other_adjustment_memo,
                :net_5010_amount, :net_1120_amount, :net_2030_dating_amount,
                :sanity_check_vs_gl_variance, :sanity_check_vs_inventory_movement,
                :sanity_check_warning, :sanity_check_notes,
                :journal_batch_id, :actor_email
            )
            ON CONFLICT (entity_id, period_start, period_end)
            DO UPDATE SET
                accounting_period_id = EXCLUDED.accounting_period_id,
                pos_import_run_id = EXCLUDED.pos_import_run_id,
                base_cogs = EXCLUDED.base_cogs,
                shrinkage_cogs = EXCLUDED.shrinkage_cogs,
                shrinkage_included = EXCLUDED.shrinkage_included,
                greeting_card_adj = EXCLUDED.greeting_card_adj,
                dating_new_amount = EXCLUDED.dating_new_amount,
                dating_reversal_amount = EXCLUDED.dating_reversal_amount,
                other_adjustment_amount = EXCLUDED.other_adjustment_amount,
                other_adjustment_memo = EXCLUDED.other_adjustment_memo,
                net_5010_amount = EXCLUDED.net_5010_amount,
                net_1120_amount = EXCLUDED.net_1120_amount,
                net_2030_dating_amount = EXCLUDED.net_2030_dating_amount,
                sanity_check_vs_gl_variance = EXCLUDED.sanity_check_vs_gl_variance,
                sanity_check_vs_inventory_movement = EXCLUDED.sanity_check_vs_inventory_movement,
                sanity_check_warning = EXCLUDED.sanity_check_warning,
                sanity_check_notes = EXCLUDED.sanity_check_notes,
                journal_batch_id = EXCLUDED.journal_batch_id,
                actor_email = EXCLUDED.actor_email,
                updated_at = NOW()
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
            "pos_import_run_id": snapshot["import_run_id"],
            "period_start": period_start,
            "period_end": period_end,
            "base_cogs": base_cogs,
            "shrinkage_cogs": shrinkage_cogs,
            "shrinkage_included": shrinkage_included,
            "greeting_card_adj": greeting_card_adj,
            "dating_new_amount": new_amt,
            "dating_reversal_amount": rev_amt,
            "other_adjustment_amount": other_amt,
            "other_adjustment_memo": other_adjustment_memo,
            "net_5010_amount": net_5010,
            "net_1120_amount": net_1120,
            "net_2030_dating_amount": net_2030_dating,
            "sanity_check_vs_gl_variance": var_vs_gl,
            "sanity_check_vs_inventory_movement": var_vs_inv,
            "sanity_check_warning": sanity_warning,
            "sanity_check_notes": "\n".join(sanity_notes),
            "journal_batch_id": journal_batch_id,
            "actor_email": actor_email,
        },
    )

    return {
        "entity_code": entity_code,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "journal_batch_id": str(journal_batch_id),
        "base_cogs": str(base_cogs),
        "shrinkage_cogs": str(shrinkage_cogs),
        "shrinkage_included": shrinkage_included,
        "shrinkage_breakdown": shrinkage_data["shrinkage_breakdown"],
        "greeting_card_adj": str(greeting_card_adj),
        "greeting_card_breakdown": shrinkage_data["greeting_card_breakdown"],
        "greeting_card_note": (
            "Greeting card returns are surfaced separately. Bookkeeper "
            "decides whether they belong in COGS (Dr 5010 / Cr 1120) or "
            "as a vendor return AP adjustment (Dr 2020 / Cr 1120)."
        ) if abs(greeting_card_adj) > 0 else None,
        "dating_new_amount": str(new_amt),
        "dating_reversal_amount": str(rev_amt),
        "other_adjustment_amount": str(other_amt),
        "net_5010_amount": str(net_5010),
        "net_1120_amount": str(net_1120),
        "net_2030_dating_amount": str(net_2030_dating),
        "total_debits": str(total_debits),
        "total_credits": str(total_credits),
        "lines": [
            {
                "account_code": l["account_code"],
                "debit_amount": str(l["debit_amount"]),
                "credit_amount": str(l["credit_amount"]),
                "memo": l["memo"],
                "component": l["component"],
            }
            for l in journal_lines
        ],
        "sanity_check": {
            "warning": sanity_warning,
            "vs_gl_variance": str(var_vs_gl) if var_vs_gl is not None else None,
            "vs_inventory_movement_variance": str(var_vs_inv) if var_vs_inv is not None else None,
            "notes": sanity_notes,
        },
    }


# ----------------------------------------------------------------------
# Status (close-control-center + status endpoint)
# ----------------------------------------------------------------------


def get_cogs_status(
    session, *, entity_code: str, period_end: date
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    period_start = period_end.replace(day=1)
    return _cogs_status_for_entity(
        session, entity_id=entity["id"],
        period_start=period_start, period_end=period_end,
        include_entity_code=entity_code,
    )


def _cogs_status_for_entity(
    session,
    *,
    entity_id: UUID,
    period_start: date,
    period_end: date,
    include_entity_code: str | None = None,
) -> dict[str, Any]:
    snapshot = _resolve_pos_snapshot(
        session, entity_id=entity_id, period_end=period_end
    )
    base_cogs_pos = (
        _money(snapshot["cogs_merchandise"]) + _money(snapshot["cogs_non_merchandise"])
        if snapshot
        else None
    )

    inputs_row = session.execute(
        text(
            """
            SELECT base_cogs, shrinkage_cogs, shrinkage_included, greeting_card_adj,
                   dating_new_amount, dating_reversal_amount,
                   other_adjustment_amount, net_5010_amount,
                   sanity_check_warning, sanity_check_notes,
                   journal_batch_id, updated_at
            FROM cogs_journal_inputs
            WHERE entity_id = :entity_id
              AND period_start = :period_start AND period_end = :period_end
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()

    batch_row = None
    if inputs_row and inputs_row["journal_batch_id"]:
        batch_row = session.execute(
            text(
                """
                SELECT id, status, workflow_status,
                       total_debits, total_credits,
                       submitted_by, submitted_at,
                       approved_by, approved_at
                FROM journal_batches
                WHERE id = :id
                """
            ),
            {"id": inputs_row["journal_batch_id"]},
        ).mappings().first()

    suggested_reversal = _last_period_dating_new(
        session, entity_id=entity_id, period_end=period_end
    )

    return {
        "entity_code": include_entity_code,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "pos_snapshot_present": snapshot is not None,
        "base_cogs_from_pos": str(base_cogs_pos) if base_cogs_pos is not None else None,
        "suggested_dating_reversal_amount": str(suggested_reversal),
        "journal_built": inputs_row is not None,
        "inputs": (
            {
                "base_cogs": str(inputs_row["base_cogs"]),
                "shrinkage_cogs": str(inputs_row["shrinkage_cogs"]),
                "shrinkage_included": inputs_row["shrinkage_included"],
                "greeting_card_adj": str(inputs_row["greeting_card_adj"]),
                "dating_new_amount": str(inputs_row["dating_new_amount"]),
                "dating_reversal_amount": str(inputs_row["dating_reversal_amount"]),
                "other_adjustment_amount": str(inputs_row["other_adjustment_amount"]),
                "net_5010_amount": str(inputs_row["net_5010_amount"]),
                "sanity_check_warning": inputs_row["sanity_check_warning"],
                "sanity_check_notes": (
                    (inputs_row["sanity_check_notes"] or "").splitlines()
                    if inputs_row["sanity_check_notes"]
                    else []
                ),
                "updated_at": inputs_row["updated_at"].isoformat()
                if inputs_row["updated_at"]
                else None,
            }
            if inputs_row
            else None
        ),
        "batch": (
            {
                "id": str(batch_row["id"]),
                "status": batch_row["status"],
                "workflow_status": batch_row["workflow_status"],
                "total_debits": str(batch_row["total_debits"]),
                "total_credits": str(batch_row["total_credits"]),
                "submitted_by": batch_row["submitted_by"],
                "submitted_at": batch_row["submitted_at"].isoformat()
                if batch_row["submitted_at"]
                else None,
                "approved_by": batch_row["approved_by"],
                "approved_at": batch_row["approved_at"].isoformat()
                if batch_row["approved_at"]
                else None,
            }
            if batch_row
            else None
        ),
    }


# ----------------------------------------------------------------------
# Close-control-center section
# ----------------------------------------------------------------------


def section_cogs(
    session,
    *,
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

    state = _cogs_status_for_entity(
        session, entity_id=entity_id,
        period_start=period_start, period_end=period_end,
    )

    if not state["pos_snapshot_present"]:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"No POS Financial snapshot for {period_end.isoformat()}; "
                "import POS.txt before building the COGS journal."
            ),
            "state": state,
        }

    if not state["journal_built"]:
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"COGS journal not built. POST /api/cogs/build-journal "
                f"with dating_new_amount (suggested reversal: "
                f"{state['suggested_dating_reversal_amount']})."
            ),
            "state": state,
        }

    batch = state.get("batch") or {}
    workflow_status = batch.get("workflow_status")
    if workflow_status != "approved_to_post":
        return {
            "status": "blocked",
            "module_present": True,
            "summary": (
                f"COGS journal built (batch {batch.get('id')}) but workflow_status="
                f"{workflow_status}. Submit and approve before closing."
            ),
            "state": state,
        }

    # Approved — surface sanity warning as 'needs_review' rather than 'ready'
    inputs = state.get("inputs") or {}
    if inputs.get("sanity_check_warning"):
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": (
                f"COGS journal approved (batch {batch.get('id')}) — "
                "sanity check raised a warning; review before closing."
            ),
            "state": state,
        }

    return {
        "status": "ready",
        "module_present": True,
        "summary": (
            f"COGS journal approved. Net 5010 ${inputs.get('net_5010_amount')}."
        ),
        "state": state,
    }
