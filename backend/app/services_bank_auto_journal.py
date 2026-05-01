"""
Bank transaction auto-journal builder — service layer.

After the bank PDF/CSV is imported into bank_transactions, this module
walks each row, matches it to a GL account using the
bank_transaction_rules table, and writes a single journal_batch
containing all matched entries (Dr expense / Cr 1020 for outflows,
Dr 1020 / Cr revenue for inflows).

Skipped categories:
    - HOME HARDWARE MSP / AP                  → handled by HH AP module
    - VSA DEP / MC DEP / AMEX settlements     → handled by card_settlement
    - BALANCE FORWARD or zero-amount rows     → noise

Idempotency: bank_auto_journal_lines has UNIQUE(entity_id,
bank_transaction_id), so re-running a period is safe — only
unprocessed transactions are added.

Loan principal-vs-interest splits are flagged with
matched_status='split_required' and left for manual entry by the
bookkeeper because we don't have the amortization schedule yet.
"""
from __future__ import annotations

import json
import re
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
from .services_vendor_classification import (
    LAYER_CLAUDE,
    LAYER_RULES,
    LAYER_VENDOR_MEMORY,
    VENDOR_MEMORY_AUTO_DRAFT_THRESHOLD,
    CLAUDE_MIN_CONFIDENCE,
    record_suggestion,
    vendor_memory_lookup,
    vendor_memory_similar,
)
from .services_claude_classifier import (
    classify_with_claude,
    is_claude_available,
)


SOURCE_MODULE_AUTO_JOURNAL = "bank_auto_journal"
BATCH_LABEL_AUTO_JOURNAL = "auto_bank_journal"
DEFAULT_BANK_ACCOUNT_CODE = "1020"

MATCH_STATUS_MATCHED = "matched"
MATCH_STATUS_UNMATCHED = "unmatched"
MATCH_STATUS_SKIPPED = "skipped"
MATCH_STATUS_SPLIT_REQUIRED = "split_required"
MATCH_STATUS_AUTO_DRAFT_PAYROLL = "auto_draft_payroll"

TRANSACTION_TYPE_AUTO_DRAFT_PAYROLL = "auto_draft_payroll"

# Description prefixes / fragments that should always be skipped
# because another module handles them.
_HARD_SKIP_RULES = [
    ("HOME HARDWARE", "handled by hh_ap module (HH AP / MSP remittance matching)"),
    ("VSA DEP", "handled by card_settlement module"),
    ("MC DEP", "handled by card_settlement module"),
    # AMEX settlements come in as "AMEX 1995722444 MSP" — the digits
    # distinguish a settlement from a fee. The classifier in
    # services_bank_pdf already tags settlements as 'amex_settlement'.
    ("AMEX 1", "handled by card_settlement module"),
    ("AMEX 2", "handled by card_settlement module"),
    ("AMEX 3", "handled by card_settlement module"),
    ("AMEX 4", "handled by card_settlement module"),
    ("AMEX 5", "handled by card_settlement module"),
    ("AMEX 6", "handled by card_settlement module"),
    ("AMEX 7", "handled by card_settlement module"),
    ("AMEX 8", "handled by card_settlement module"),
    ("AMEX 9", "handled by card_settlement module"),
    # AMEX fees are "AMX FEE..." — those go through the rule table.
]


# Bridlewood seed list. Priority sorts ascending (1 = check first).
# Patterns must NOT use trailing \b — pypdf often concatenates the
# merchant id directly onto FEE/DEP (e.g. "VSA DEP14350").
_BRIDLEWOOD_SEED: list[dict[str, Any]] = [
    # ---------- Card processor fees ----------
    {
        "rule_code": "AMX_FEE",
        "description_pattern": "AMX FEE",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "card_fee",
        "priority": 10,
    },
    {
        "rule_code": "VSA_FEE",
        "description_pattern": "VSA FEE",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "card_fee",
        "priority": 10,
    },
    {
        "rule_code": "MC_FEE",
        "description_pattern": "MC FEE",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "card_fee",
        "priority": 10,
    },
    {
        "rule_code": "INT_FEE",
        "description_pattern": "INT FEE",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "card_fee",
        "priority": 10,
    },
    {
        "rule_code": "MON_FEE",
        "description_pattern": "MON FEE",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "card_fee",
        "priority": 10,
    },
    # ---------- Bank charges ----------
    {
        "rule_code": "TAX_PYT_FEE",
        "description_pattern": "TAX PYT FEE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "SEND_ETFR_FEE",
        "description_pattern": "SEND E-TFR FEE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "CHQ_IMAGE_FEE",
        "description_pattern": "CHQ-IMAGE FEE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "BRW_BILLING",
        "description_pattern": "BRW BILLING",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "EFT_BILLING",
        "description_pattern": "EFT BILLING",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "CASH_DEP_FEE",
        "description_pattern": "CASH DEP FEE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "ITEMS_DEP_FEE",
        "description_pattern": "ITEMS DEP FEE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "SERVICE_CHARGE",
        "description_pattern": "SERVICE CHARGE",
        "match_type": "contains",
        "debit_account": "6260",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    {
        "rule_code": "OVERDRAFT_INTEREST",
        "description_pattern": "OVERDRAFT INTEREST",
        "match_type": "contains",
        "debit_account": "6270",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "bank_charge",
        "priority": 5,
    },
    # ---------- Loan ----------
    {
        "rule_code": "LOAN_PYMT",
        "description_pattern": "LN PYMT",
        "match_type": "contains",
        "debit_account": "2500",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "loan_payment",
        "priority": 20,
        "requires_split": True,
        "split_config_json": {
            "components": [
                {"label": "principal", "debit_account": "2500"},
                {"label": "interest", "debit_account": "6280"},
            ],
            "note": "Provide principal/interest split from amortization schedule",
        },
    },
    {
        "rule_code": "LOAN_INTEREST",
        "description_pattern": "D/L INT",
        "match_type": "contains",
        "debit_account": "6280",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 5,
    },
    # ---------- Government ----------
    {
        "rule_code": "GST_REMITTANCE",
        "description_pattern": "GST",
        "match_type": "regex",
        "debit_account": "2300",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "liability_payment",
        "priority": 5,
        "split_config_json": {
            "regex": r"GST(?:34|\s+GST)",
            "note": "Matches GST34 ... and GST GST ... patterns",
        },
    },
    # ---------- Payroll ----------
    {
        "rule_code": "ENET_EMPLOYER",
        "description_pattern": "ENET EMPLOYER",
        "match_type": "contains",
        "debit_account": "6120",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": TRANSACTION_TYPE_AUTO_DRAFT_PAYROLL,
        "priority": 15,
        "notes": "Auto-drafts a payroll_run from the bank net-pay debit. The bookkeeper splits gross/CPP/EI/tax/fees in the payroll module before approving.",
    },
    # ---------- Utilities ----------
    {
        "rule_code": "HYDRO_OTTAWA",
        "description_pattern": "HYDRO OTTAWA",
        "match_type": "contains",
        "debit_account": "6030",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "HYDRO_ONE",
        "description_pattern": "HYDRO ONE",
        "match_type": "contains",
        "debit_account": "6030",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "ENBRIDGE",
        "description_pattern": "ENBRIDGE",
        "match_type": "contains",
        "debit_account": "6040",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "INTRSTE_BILL",
        "description_pattern": "INTRSTE BILL",
        "match_type": "contains",
        "debit_account": "6050",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "OTTAWA_WATER",
        "description_pattern": "OTTAWA WATER",
        "match_type": "contains",
        "debit_account": "6620",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    # ---------- Insurance ----------
    {
        "rule_code": "CANADALIFE_INS",
        "description_pattern": "CANADALIFE",
        "match_type": "contains",
        "debit_account": "6360",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "GM_FINANCIAL",
        "description_pattern": "GM FINANCIAL",
        "match_type": "contains",
        "debit_account": "6370",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    # ---------- Vendors ----------
    {
        "rule_code": "EPICOR_SOFTWARE",
        "description_pattern": "EPICOR",
        "match_type": "contains",
        "debit_account": "6670",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "PENINSULA_EMPLO",
        "description_pattern": "PENINSULA EMPLO",
        "match_type": "contains",
        "debit_account": "6665",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "ALLSTREAM",
        "description_pattern": "ALLSTREAM",
        "match_type": "contains",
        "debit_account": "6610",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 10,
    },
    {
        "rule_code": "INTUIT",
        "description_pattern": "INTUIT",
        "match_type": "contains",
        "debit_account": "6310",
        "credit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "transaction_type": "expense",
        "priority": 15,
        "notes": "Intuit billing routed through merchant fee bucket; user can recategorize",
    },
    # ---------- Inflow / deposit rules (debit = bank, credit = clearing) ----------
    {
        "rule_code": "EFT_BATCH_DEPOSIT",
        "description_pattern": "EF0",
        "match_type": "starts_with",
        # Inflow-style: bank receives the money (Dr 1020), Ecommerce
        # clearing settles down (Cr 1095). Pattern matches Moneris EFT
        # batches like "EF0130 14350 MSP", "EF0202 14350 MSP", etc.
        "debit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "credit_account": "1095",
        "transaction_type": "card_settlement_deposit",
        "priority": 8,
        "notes": "Moneris EFT batch settlements (debit-card / e-commerce). Clears 1095.",
    },
    {
        "rule_code": "TD_EXPRESS_DEPOSIT",
        "description_pattern": "TD EXPRESS DEPOSIT",
        "match_type": "starts_with",
        # Inflow-style: physical cash + cheques deposited via TD's
        # express deposit slot at the branch. Dr 1020 / Cr 1010 moves
        # till cash (already booked to 1010 via the daily POS journal)
        # into the chequing account.
        "debit_account": DEFAULT_BANK_ACCOUNT_CODE,
        "credit_account": "1010",
        "transaction_type": "cash_deposit",
        "priority": 8,
        "notes": "Daily till deposit. Clears 1010 Cash Float into 1020.",
    },
]


def _money(value: Any) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------------
# Seed
# ----------------------------------------------------------------------


def seed_rules(session, *, entity_code: str, actor_email: str) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    inserted = 0
    skipped = 0
    rows: list[dict[str, Any]] = []
    for cfg in _BRIDLEWOOD_SEED:
        existing = session.execute(
            text(
                """
                SELECT id FROM bank_transaction_rules
                WHERE entity_id = :entity_id AND rule_code = :rule_code
                """
            ),
            {"entity_id": entity["id"], "rule_code": cfg["rule_code"]},
        ).mappings().first()
        if existing:
            skipped += 1
            rows.append({"rule_code": cfg["rule_code"], "status": "exists"})
            continue
        ins = session.execute(
            text(
                """
                INSERT INTO bank_transaction_rules (
                    entity_id, rule_code, description_pattern, match_type,
                    debit_account, credit_account, transaction_type,
                    requires_split, split_config_json,
                    is_active, priority, notes
                ) VALUES (
                    :entity_id, :rule_code, :pattern, :match_type,
                    :debit, :credit, :txn_type,
                    :requires_split, CAST(:split_config AS jsonb),
                    TRUE, :priority, :notes
                )
                RETURNING id
                """
            ),
            {
                "entity_id": entity["id"],
                "rule_code": cfg["rule_code"],
                "pattern": cfg["description_pattern"],
                "match_type": cfg.get("match_type", "contains"),
                "debit": cfg.get("debit_account"),
                "credit": cfg.get("credit_account"),
                "txn_type": cfg.get("transaction_type"),
                "requires_split": bool(cfg.get("requires_split", False)),
                "split_config": json.dumps(cfg.get("split_config_json") or {}),
                "priority": int(cfg.get("priority", 100)),
                "notes": cfg.get("notes"),
            },
        ).mappings().first()
        inserted += 1
        rows.append(
            {
                "rule_code": cfg["rule_code"],
                "id": str(ins["id"]),
                "status": "inserted",
            }
        )

    return {
        "entity_code": entity_code,
        "inserted": inserted,
        "skipped": skipped,
        "rules": rows,
    }


def list_rules(session, *, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "bank_transaction_rules"):
        return {"entity_code": entity_code, "count": 0, "rules": []}
    rows = session.execute(
        text(
            """
            SELECT id, rule_code, description_pattern, match_type,
                   debit_account, credit_account, transaction_type,
                   requires_split, is_active, priority, notes
            FROM bank_transaction_rules
            WHERE entity_id = :entity_id
            ORDER BY priority, rule_code
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()
    return {
        "entity_code": entity_code,
        "count": len(rows),
        "rules": [
            {
                "id": str(r["id"]),
                "rule_code": r["rule_code"],
                "description_pattern": r["description_pattern"],
                "match_type": r["match_type"],
                "debit_account": r["debit_account"],
                "credit_account": r["credit_account"],
                "transaction_type": r["transaction_type"],
                "requires_split": r["requires_split"],
                "is_active": r["is_active"],
                "priority": r["priority"],
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


# ----------------------------------------------------------------------
# Matching
# ----------------------------------------------------------------------


def _hard_skip_reason(description_upper: str) -> str | None:
    for prefix, reason in _HARD_SKIP_RULES:
        if prefix in description_upper:
            return reason
    return None


def _matches_rule(rule: dict[str, Any], description_upper: str) -> bool:
    pattern = (rule["description_pattern"] or "").upper()
    if not pattern:
        return False
    match_type = rule["match_type"] or "contains"
    if match_type == "contains":
        return pattern in description_upper
    if match_type == "starts_with":
        return description_upper.startswith(pattern)
    if match_type == "exact":
        return description_upper == pattern
    if match_type == "regex":
        try:
            return bool(re.search(pattern, description_upper))
        except re.error:
            return False
    return False


def _find_matching_rule(
    rules: list[dict[str, Any]], description: str
) -> dict[str, Any] | None:
    desc_upper = (description or "").upper()
    for rule in rules:  # already ordered by priority
        if not rule["is_active"]:
            continue
        if _matches_rule(rule, desc_upper):
            return rule
    return None


# ----------------------------------------------------------------------
# Run
# ----------------------------------------------------------------------


def run_auto_journal(
    session,
    *,
    entity_code: str,
    period_start: date,
    period_end: date,
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

    rules = session.execute(
        text(
            """
            SELECT id, rule_code, description_pattern, match_type,
                   debit_account, credit_account, transaction_type,
                   requires_split, is_active, priority
            FROM bank_transaction_rules
            WHERE entity_id = :entity_id AND is_active = TRUE
            ORDER BY priority, rule_code
            """
        ),
        {"entity_id": entity["id"]},
    ).mappings().all()
    rules = [dict(r) for r in rules]

    # Pull bank_transactions for the period that haven't already been
    # processed by this module.
    txns = session.execute(
        text(
            """
            SELECT bt.id, bt.transaction_date, bt.description,
                   bt.amount, bt.direction, bt.source_system
            FROM bank_transactions bt
            WHERE bt.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :period_start AND :period_end
              AND NOT EXISTS (
                  SELECT 1 FROM bank_auto_journal_lines ajl
                  WHERE ajl.bank_transaction_id = bt.id
                    AND ajl.matched_status = :matched_status
              )
            ORDER BY bt.transaction_date, bt.id
            """
        ),
        {
            "entity_id": entity["id"],
            "period_start": period_start,
            "period_end": period_end,
            "matched_status": MATCH_STATUS_MATCHED,
        },
    ).mappings().all()

    # Pre-create the run row so we have a UUID for the lines.
    run_row = session.execute(
        text(
            """
            INSERT INTO bank_auto_journal_runs (
                entity_id, accounting_period_id,
                period_start, period_end,
                transactions_reviewed,
                status, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id,
                :period_start, :period_end,
                :reviewed,
                'running', :actor_email
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
            "period_start": period_start,
            "period_end": period_end,
            "reviewed": len(txns),
            "actor_email": actor_email,
        },
    ).mappings().first()
    run_id: UUID = run_row["id"]

    matched_rows: list[dict[str, Any]] = []
    unmatched_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    split_required_rows: list[dict[str, Any]] = []
    auto_draft_payroll_rows: list[dict[str, Any]] = []

    # Per-layer counters so the run summary can show the human where
    # each match came from (rule vs memory vs LLM).
    layer_counts = {
        LAYER_RULES: 0,
        LAYER_VENDOR_MEMORY: 0,
        LAYER_CLAUDE: 0,
    }

    # Pull the chart of accounts once for Layer 3 (Claude). Done outside
    # the loop so the prompt-cache hit-rate is high for a run.
    chart_of_accounts: list[dict[str, Any]] = []
    if is_claude_available() and _has_table(session, "accounts"):
        chart_rows = session.execute(
            text(
                """
                SELECT account_code, account_name, account_class
                FROM accounts
                WHERE entity_id = :entity_id AND is_active = TRUE
                ORDER BY account_code
                """
            ),
            {"entity_id": entity["id"]},
        ).mappings().all()
        chart_of_accounts = [dict(r) for r in chart_rows]

    for txn in txns:
        desc = txn["description"] or ""
        desc_upper = desc.upper()

        # Hard-skip rules (other modules handle these)
        skip_reason = _hard_skip_reason(desc_upper)
        if skip_reason:
            session.execute(
                text(
                    """
                    INSERT INTO bank_auto_journal_lines (
                        entity_id, auto_journal_run_id, bank_transaction_id,
                        matched_status, skip_reason, amount, notes
                    ) VALUES (
                        :entity_id, :run_id, :bt_id,
                        :status, :reason, :amount, NULL
                    )
                    ON CONFLICT (entity_id, bank_transaction_id) DO NOTHING
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "run_id": run_id,
                    "bt_id": txn["id"],
                    "status": MATCH_STATUS_SKIPPED,
                    "reason": skip_reason,
                    "amount": txn["amount"],
                },
            )
            skipped_rows.append(
                {
                    "bank_transaction_id": str(txn["id"]),
                    "description": desc,
                    "amount": str(txn["amount"]),
                    "reason": skip_reason,
                }
            )
            continue

        rule = _find_matching_rule(rules, desc)
        if rule is None:
            # Layer 2: vendor memory.
            mem = vendor_memory_lookup(
                session,
                entity_id=entity["id"],
                description=desc,
            )
            if (
                mem is not None
                and mem["confidence_score"] >= VENDOR_MEMORY_AUTO_DRAFT_THRESHOLD
            ):
                # Synthesize a rule-like dict for the matched_rows path.
                # For outflows: Dr expense / Cr 1020. For inflows we
                # flip in the prepared_lines block below using
                # rule['debit_account'] vs rule['credit_account'].
                rule = {
                    "id": None,
                    "rule_code": f"VENDOR_MEMORY:{mem['normalized_vendor_key']}",
                    "debit_account": (
                        mem["account_code"]
                        if mem["debit_or_credit"] == "debit"
                        else DEFAULT_BANK_ACCOUNT_CODE
                    ),
                    "credit_account": (
                        DEFAULT_BANK_ACCOUNT_CODE
                        if mem["debit_or_credit"] == "debit"
                        else mem["account_code"]
                    ),
                    "transaction_type": "vendor_memory",
                    "requires_split": False,
                }
                record_suggestion(
                    session,
                    entity_id=entity["id"],
                    bank_transaction_id=txn["id"],
                    auto_journal_run_id=run_id,
                    layer=LAYER_VENDOR_MEMORY,
                    suggested_account_code=mem["account_code"],
                    suggested_debit_or_credit=mem["debit_or_credit"],
                    confidence_score=mem["confidence_score"],
                    reasoning=(
                        f"Vendor memory: matched key "
                        f"{mem['normalized_vendor_key']} "
                        f"(seen {mem['occurrences_count']}x, "
                        f"source={mem['source']})"
                    ),
                )
                layer_counts[LAYER_VENDOR_MEMORY] += 1
            # Layer 3: Claude API.
            elif is_claude_available() and chart_of_accounts:
                similar = vendor_memory_similar(
                    session, entity_id=entity["id"], description=desc, limit=5
                )
                claude_result = classify_with_claude(
                    description=desc,
                    amount=txn["amount"] or Decimal("0"),
                    direction=txn["direction"] or "outflow",
                    chart_of_accounts=chart_of_accounts,
                    similar_past=similar,
                )
                if (
                    claude_result is not None
                    and claude_result["account_code"] not in (None, "", "UNCLASSIFIED")
                    and claude_result["confidence"] >= CLAUDE_MIN_CONFIDENCE
                ):
                    dr_or_cr = claude_result["debit_or_credit"]
                    rule = {
                        "id": None,
                        "rule_code": f"CLAUDE:{claude_result['account_code']}",
                        "debit_account": (
                            claude_result["account_code"]
                            if dr_or_cr == "debit"
                            else DEFAULT_BANK_ACCOUNT_CODE
                        ),
                        "credit_account": (
                            DEFAULT_BANK_ACCOUNT_CODE
                            if dr_or_cr == "debit"
                            else claude_result["account_code"]
                        ),
                        "transaction_type": "claude",
                        "requires_split": False,
                    }
                    record_suggestion(
                        session,
                        entity_id=entity["id"],
                        bank_transaction_id=txn["id"],
                        auto_journal_run_id=run_id,
                        layer=LAYER_CLAUDE,
                        suggested_account_code=claude_result["account_code"],
                        suggested_debit_or_credit=dr_or_cr,
                        confidence_score=claude_result["confidence"],
                        reasoning=claude_result["reasoning"],
                        raw_response=claude_result.get("raw_response"),
                    )
                    layer_counts[LAYER_CLAUDE] += 1
                else:
                    # Claude was indecisive — record the failed attempt
                    # so the review queue surfaces it.
                    record_suggestion(
                        session,
                        entity_id=entity["id"],
                        bank_transaction_id=txn["id"],
                        auto_journal_run_id=run_id,
                        layer=LAYER_CLAUDE,
                        suggested_account_code=None,
                        suggested_debit_or_credit=None,
                        confidence_score=(
                            claude_result["confidence"] if claude_result else None
                        ),
                        reasoning=(
                            claude_result["reasoning"]
                            if claude_result
                            else "Claude API unavailable"
                        ),
                        raw_response=(
                            claude_result.get("raw_response")
                            if claude_result
                            else None
                        ),
                    )
                    rule = None  # stay unmatched

        if rule is None:
            session.execute(
                text(
                    """
                    INSERT INTO bank_auto_journal_lines (
                        entity_id, auto_journal_run_id, bank_transaction_id,
                        matched_status, amount
                    ) VALUES (
                        :entity_id, :run_id, :bt_id, :status, :amount
                    )
                    ON CONFLICT (entity_id, bank_transaction_id) DO NOTHING
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "run_id": run_id,
                    "bt_id": txn["id"],
                    "status": MATCH_STATUS_UNMATCHED,
                    "amount": txn["amount"],
                },
            )
            unmatched_rows.append(
                {
                    "bank_transaction_id": str(txn["id"]),
                    "transaction_date": txn["transaction_date"].isoformat(),
                    "description": desc,
                    "amount": str(txn["amount"]),
                    "direction": txn["direction"],
                }
            )
            continue

        if rule["requires_split"]:
            session.execute(
                text(
                    """
                    INSERT INTO bank_auto_journal_lines (
                        entity_id, auto_journal_run_id, bank_transaction_id,
                        rule_id, matched_status, amount, notes
                    ) VALUES (
                        :entity_id, :run_id, :bt_id,
                        :rule_id, :status, :amount,
                        'Requires manual principal/interest split from amortization schedule'
                    )
                    ON CONFLICT (entity_id, bank_transaction_id) DO NOTHING
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "run_id": run_id,
                    "bt_id": txn["id"],
                    "rule_id": rule["id"],
                    "status": MATCH_STATUS_SPLIT_REQUIRED,
                    "amount": txn["amount"],
                },
            )
            split_required_rows.append(
                {
                    "bank_transaction_id": str(txn["id"]),
                    "transaction_date": txn["transaction_date"].isoformat(),
                    "description": desc,
                    "amount": str(txn["amount"]),
                    "rule_code": rule["rule_code"],
                }
            )
            continue

        if rule.get("transaction_type") == TRANSACTION_TYPE_AUTO_DRAFT_PAYROLL:
            # ENetEmployer (or any payroll-processor) bank withdrawal:
            # create a payroll_run draft seeded with net_pay = abs(amount)
            # and let the bookkeeper fill in gross / CPP / EI / fees
            # before approving. We do NOT post a journal entry here —
            # the eventual journal comes out of the payroll module.
            net_pay = abs(_money(txn["amount"]))
            txn_date = txn["transaction_date"]
            payroll_reference = f"ENET-{txn_date.isoformat()}"
            session.execute(
                text(
                    """
                    INSERT INTO payroll_runs (
                        entity_id, accounting_period_id, payroll_reference,
                        pay_period_start, pay_period_end, pay_date,
                        processor, net_pay, status, workflow_status,
                        bank_transaction_id, notes, actor_email,
                        raw_import_json
                    ) VALUES (
                        :entity_id, :accounting_period_id, :payroll_reference,
                        :pay_period_start, :pay_period_end, :pay_date,
                        'ENetEmployer', :net_pay, 'draft', 'draft',
                        :bank_transaction_id, :notes, :actor_email,
                        CAST(:raw AS jsonb)
                    )
                    ON CONFLICT (entity_id, payroll_reference)
                    DO UPDATE SET
                        net_pay = EXCLUDED.net_pay,
                        bank_transaction_id = EXCLUDED.bank_transaction_id,
                        notes = COALESCE(payroll_runs.notes, EXCLUDED.notes),
                        raw_import_json = EXCLUDED.raw_import_json,
                        updated_at = NOW()
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "accounting_period_id": accounting_period_id,
                    "payroll_reference": payroll_reference,
                    "pay_period_start": txn_date,
                    "pay_period_end": txn_date,
                    "pay_date": txn_date,
                    "net_pay": net_pay,
                    "bank_transaction_id": txn["id"],
                    "notes": (
                        "Auto-drafted from bank transaction - "
                        "complete gross/deductions before approving"
                    ),
                    "actor_email": actor_email,
                    "raw": json.dumps(
                        {
                            "auto_drafted_from_bank": True,
                            "bank_description": desc,
                            "bank_amount": str(txn["amount"]),
                            "bank_transaction_id": str(txn["id"]),
                            "rule_code": rule["rule_code"],
                        }
                    ),
                },
            )
            session.execute(
                text(
                    """
                    INSERT INTO bank_auto_journal_lines (
                        entity_id, auto_journal_run_id, bank_transaction_id,
                        rule_id, matched_status, amount, notes
                    ) VALUES (
                        :entity_id, :run_id, :bt_id,
                        :rule_id, :status, :amount,
                        'Auto-drafted payroll_run; awaiting gross/deductions split'
                    )
                    ON CONFLICT (entity_id, bank_transaction_id)
                    DO UPDATE SET
                        auto_journal_run_id = EXCLUDED.auto_journal_run_id,
                        rule_id = EXCLUDED.rule_id,
                        matched_status = EXCLUDED.matched_status,
                        amount = EXCLUDED.amount,
                        notes = EXCLUDED.notes,
                        debit_account = NULL,
                        credit_account = NULL,
                        journal_batch_id = NULL
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "run_id": run_id,
                    "bt_id": txn["id"],
                    "rule_id": rule["id"],
                    "status": MATCH_STATUS_AUTO_DRAFT_PAYROLL,
                    "amount": txn["amount"],
                },
            )
            # Keep the bank transaction in needs_review so the bookkeeper
            # sees it until they approve the payroll run.
            session.execute(
                text(
                    """
                    UPDATE bank_transactions
                       SET review_status = 'needs_review'
                     WHERE id = :id
                       AND review_status NOT IN ('matched','ignored')
                    """
                ),
                {"id": txn["id"]},
            )
            auto_draft_payroll_rows.append(
                {
                    "bank_transaction_id": str(txn["id"]),
                    "transaction_date": txn_date.isoformat(),
                    "description": desc,
                    "amount": str(txn["amount"]),
                    "rule_code": rule["rule_code"],
                    "payroll_reference": payroll_reference,
                }
            )
            continue

        # If rule['id'] is a real UUID, this came from Layer 1 (the
        # bank_transaction_rules table). We don't record a
        # bank_classification_suggestions row for Layer 1 — that table
        # is for Layers 2/3 where the human reviews the AI's answer.
        # Layer 1 is deterministic, no review needed, and skipping the
        # write keeps the per-transaction round-trip count low enough
        # for Render's free-tier Postgres.
        if rule.get("id") is not None:
            layer_counts[LAYER_RULES] += 1

        matched_rows.append(
            {
                "txn": dict(txn),
                "rule": rule,
            }
        )

    # Build the journal_batch (one batch per run).
    journal_batch_id: UUID | None = None
    total_debits = Decimal("0.00")
    total_credits = Decimal("0.00")

    if matched_rows:
        # Compute per-line magnitude. For outflows, amount is negative;
        # we post Dr expense / Cr 1020 with abs(amount). For inflows
        # (rare in the rule list — INTUIT etc), we post Dr 1020 / Cr
        # rule.debit_account using abs(amount).
        prepared_lines: list[dict[str, Any]] = []
        for m in matched_rows:
            txn = m["txn"]
            rule = m["rule"]
            magnitude = abs(_money(txn["amount"]))
            if magnitude == Decimal("0.00"):
                continue
            direction = txn["direction"]
            # Direction-aware account resolution. Rules are stored with
            # the bank account (1020) on either side:
            #   outflow-style: debit_account=expense, credit_account=1020
            #   inflow-style:  debit_account=1020, credit_account=other
            # If the txn direction matches the rule's natural direction,
            # use the accounts as-stored. If it doesn't, flip (e.g. a
            # refund on a normally-outflow vendor → inflow txn → flip).
            rule_is_outflow_style = (
                rule["credit_account"] == DEFAULT_BANK_ACCOUNT_CODE
            )
            rule_is_inflow_style = (
                rule["debit_account"] == DEFAULT_BANK_ACCOUNT_CODE
            )
            if direction == "outflow" and rule_is_outflow_style:
                dr, cr = rule["debit_account"], rule["credit_account"]
            elif direction == "inflow" and rule_is_inflow_style:
                dr, cr = rule["debit_account"], rule["credit_account"]
            else:
                # Direction mismatches the rule's natural side — flip.
                dr, cr = rule["credit_account"], rule["debit_account"]
            prepared_lines.append(
                {
                    "txn_id": txn["id"],
                    "rule_id": rule["id"],
                    "amount": magnitude,
                    "debit_account": dr,
                    "credit_account": cr,
                    "transaction_date": txn["transaction_date"],
                    "description": txn["description"],
                    "rule_code": rule["rule_code"],
                }
            )
            total_debits += magnitude
            total_credits += magnitude

        if prepared_lines:
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
                    "source_module": SOURCE_MODULE_AUTO_JOURNAL,
                    "batch_label": BATCH_LABEL_AUTO_JOURNAL,
                    "total_debits": total_debits,
                    "total_credits": total_credits,
                    "summary_json": json.dumps(
                        {
                            "auto_journal_run_id": str(run_id),
                            "matched_count": len(prepared_lines),
                            "period_start": period_start.isoformat(),
                            "period_end": period_end.isoformat(),
                        }
                    ),
                },
            ).mappings().first()
            journal_batch_id = batch["id"]

            session.execute(
                text("DELETE FROM journal_lines WHERE journal_batch_id = :id"),
                {"id": journal_batch_id},
            )
            line_number = 0
            for ln in prepared_lines:
                line_number += 1
                memo = (
                    f"{ln['transaction_date'].isoformat()} {ln['rule_code']} "
                    f"— {ln['description'][:100]}"
                )
                src = json.dumps(
                    {
                        "source_module": SOURCE_MODULE_AUTO_JOURNAL,
                        "auto_journal_run_id": str(run_id),
                        "bank_transaction_id": str(ln["txn_id"]),
                        "rule_id": (
                            str(ln["rule_id"]) if ln.get("rule_id") else None
                        ),
                        "rule_code": ln["rule_code"],
                    }
                )
                session.execute(
                    text(
                        """
                        INSERT INTO journal_lines (
                            journal_batch_id, line_number, account_code,
                            debit_amount, credit_amount, memo, source_json
                        ) VALUES (
                            :id, :ln, :acct, :amt, 0, :memo, CAST(:src AS jsonb)
                        )
                        """
                    ),
                    {
                        "id": journal_batch_id,
                        "ln": line_number,
                        "acct": ln["debit_account"],
                        "amt": ln["amount"],
                        "memo": memo,
                        "src": src,
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
                            :id, :ln, :acct, 0, :amt, :memo, CAST(:src AS jsonb)
                        )
                        """
                    ),
                    {
                        "id": journal_batch_id,
                        "ln": line_number,
                        "acct": ln["credit_account"],
                        "amt": ln["amount"],
                        "memo": memo,
                        "src": src,
                    },
                )

                session.execute(
                    text(
                        """
                        INSERT INTO bank_auto_journal_lines (
                            entity_id, auto_journal_run_id, bank_transaction_id,
                            rule_id, journal_batch_id,
                            matched_status, debit_account, credit_account,
                            amount
                        ) VALUES (
                            :entity_id, :run_id, :bt_id,
                            :rule_id, :batch_id,
                            :status, :debit_account, :credit_account,
                            :amount
                        )
                        ON CONFLICT (entity_id, bank_transaction_id)
                        DO UPDATE SET
                            auto_journal_run_id = EXCLUDED.auto_journal_run_id,
                            rule_id = EXCLUDED.rule_id,
                            journal_batch_id = EXCLUDED.journal_batch_id,
                            matched_status = EXCLUDED.matched_status,
                            debit_account = EXCLUDED.debit_account,
                            credit_account = EXCLUDED.credit_account,
                            amount = EXCLUDED.amount
                        """
                    ),
                    {
                        "entity_id": entity["id"],
                        "run_id": run_id,
                        "bt_id": ln["txn_id"],
                        "rule_id": ln["rule_id"],
                        "batch_id": journal_batch_id,
                        "status": MATCH_STATUS_MATCHED,
                        "debit_account": ln["debit_account"],
                        "credit_account": ln["credit_account"],
                        "amount": ln["amount"],
                    },
                )

    # Compute the CUMULATIVE period totals — what the bank_auto_journal
    # state looks like across all runs in this period combined. This
    # answers "where does the period stand?" rather than "what did this
    # one run change?". Necessary because the run loop only processes
    # transactions not already matched (the WHERE NOT EXISTS filter at
    # the top), so per-run counts go to 0 once everything is matched
    # but the journal_batch still holds the matched lines.
    period_totals = session.execute(
        text(
            """
            SELECT
                COUNT(*) FILTER (WHERE ajl.matched_status = 'matched')
                    AS total_matched,
                COUNT(*) FILTER (
                    WHERE ajl.matched_status = 'matched'
                      AND ajl.rule_id IS NOT NULL
                ) AS by_rules,
                COUNT(*) FILTER (
                    WHERE ajl.matched_status = 'matched'
                      AND bcs.layer = 'vendor_memory'
                ) AS by_vendor_memory,
                COUNT(*) FILTER (
                    WHERE ajl.matched_status = 'matched'
                      AND bcs.layer = 'claude'
                ) AS by_claude,
                COUNT(*) FILTER (WHERE ajl.matched_status = 'unmatched')
                    AS total_unmatched,
                COUNT(*) FILTER (WHERE ajl.matched_status = 'skipped')
                    AS total_skipped,
                COUNT(*) FILTER (
                    WHERE ajl.matched_status = 'split_required'
                ) AS total_split_required,
                COUNT(*) FILTER (
                    WHERE ajl.matched_status = 'auto_draft_payroll'
                ) AS total_auto_draft_payroll
            FROM bank_auto_journal_lines ajl
            JOIN bank_transactions bt ON bt.id = ajl.bank_transaction_id
            LEFT JOIN bank_classification_suggestions bcs
                ON bcs.bank_transaction_id = ajl.bank_transaction_id
            WHERE ajl.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity["id"],
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first()
    period_summary = {
        "matched": int(period_totals["total_matched"] or 0),
        "by_layer": {
            LAYER_RULES: int(period_totals["by_rules"] or 0),
            LAYER_VENDOR_MEMORY: int(period_totals["by_vendor_memory"] or 0),
            LAYER_CLAUDE: int(period_totals["by_claude"] or 0),
        },
        "unmatched": int(period_totals["total_unmatched"] or 0),
        "skipped": int(period_totals["total_skipped"] or 0),
        "split_required": int(period_totals["total_split_required"] or 0),
        "auto_draft_payroll": int(period_totals["total_auto_draft_payroll"] or 0),
    }

    # Finalize the run row.
    summary_json = {
        "this_run": {
            "matched_count": len(matched_rows),
            "unmatched_count": len(unmatched_rows),
            "skipped_count": len(skipped_rows),
            "split_required_count": len(split_required_rows),
            "auto_draft_payroll_count": len(auto_draft_payroll_rows),
            "matched_by_layer": layer_counts,
        },
        "period_totals": period_summary,
        "total_debits": str(total_debits),
        "total_credits": str(total_credits),
        "unmatched_sample": unmatched_rows[:50],
        "split_required_sample": split_required_rows[:50],
        "auto_draft_payroll_sample": auto_draft_payroll_rows[:50],
    }
    session.execute(
        text(
            """
            UPDATE bank_auto_journal_runs
               SET transactions_matched = :matched,
                   transactions_unmatched = :unmatched,
                   transactions_skipped = :skipped,
                   transactions_split_required = :split_req,
                   journal_batch_id = :batch_id,
                   status = 'completed',
                   summary_json = CAST(:summary_json AS jsonb)
             WHERE id = :id
            """
        ),
        {
            "matched": len(matched_rows),
            "unmatched": len(unmatched_rows),
            "skipped": len(skipped_rows),
            "split_req": len(split_required_rows),
            "batch_id": journal_batch_id,
            "summary_json": json.dumps(summary_json),
            "id": run_id,
        },
    )

    return {
        "auto_journal_run_id": str(run_id),
        "entity_code": entity_code,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        # ---- this_run: what THIS execution processed ----
        "transactions_reviewed": len(txns),
        "transactions_matched": len(matched_rows),
        "transactions_unmatched": len(unmatched_rows),
        "transactions_skipped": len(skipped_rows),
        "transactions_split_required": len(split_required_rows),
        "transactions_auto_draft_payroll": len(auto_draft_payroll_rows),
        "matched_by_layer": layer_counts,
        # ---- period_totals: cumulative state for the period ----
        # These reflect the bank_auto_journal_lines table after this
        # run completes, including transactions matched in earlier
        # runs. Use these to answer "where does the period stand?".
        "period_totals": period_summary,
        "claude_available": is_claude_available(),
        "journal_batch_id": str(journal_batch_id) if journal_batch_id else None,
        "total_debits": str(total_debits),
        "total_credits": str(total_credits),
        "unmatched_sample": unmatched_rows[:25],
        "split_required_sample": split_required_rows[:25],
        "auto_draft_payroll_sample": auto_draft_payroll_rows[:25],
    }


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------


def list_auto_journal_runs(
    session, *, entity_code: str, limit: int = 50
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    if not _has_table(session, "bank_auto_journal_runs"):
        return {"entity_code": entity_code, "count": 0, "runs": []}
    rows = session.execute(
        text(
            """
            SELECT id, period_start, period_end,
                   transactions_reviewed, transactions_matched,
                   transactions_unmatched, transactions_skipped,
                   transactions_split_required,
                   journal_batch_id, status, actor_email, created_at
            FROM bank_auto_journal_runs
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
                "period_start": r["period_start"].isoformat() if r["period_start"] else None,
                "period_end": r["period_end"].isoformat() if r["period_end"] else None,
                "transactions_reviewed": r["transactions_reviewed"],
                "transactions_matched": r["transactions_matched"],
                "transactions_unmatched": r["transactions_unmatched"],
                "transactions_skipped": r["transactions_skipped"],
                "transactions_split_required": r["transactions_split_required"],
                "journal_batch_id": (
                    str(r["journal_batch_id"]) if r["journal_batch_id"] else None
                ),
                "status": r["status"],
                "actor_email": r["actor_email"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


def get_auto_journal_run_detail(
    session, *, entity_code: str, run_id: str
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(run_id, "run_id")

    run = session.execute(
        text(
            """
            SELECT id, period_start, period_end,
                   transactions_reviewed, transactions_matched,
                   transactions_unmatched, transactions_skipped,
                   transactions_split_required,
                   journal_batch_id, status, actor_email,
                   summary_json, created_at
            FROM bank_auto_journal_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"Auto-journal run not found: {run_id}")

    lines = session.execute(
        text(
            """
            SELECT ajl.id, ajl.matched_status, ajl.skip_reason,
                   ajl.debit_account, ajl.credit_account, ajl.amount,
                   ajl.bank_transaction_id, ajl.rule_id,
                   bt.transaction_date, bt.description, bt.direction,
                   btr.rule_code
            FROM bank_auto_journal_lines ajl
            JOIN bank_transactions bt ON bt.id = ajl.bank_transaction_id
            LEFT JOIN bank_transaction_rules btr ON btr.id = ajl.rule_id
            WHERE ajl.auto_journal_run_id = :run_id
              AND ajl.entity_id = :entity_id
            ORDER BY bt.transaction_date, ajl.matched_status
            """
        ),
        {"run_id": run_uuid, "entity_id": entity["id"]},
    ).mappings().all()

    summary = run["summary_json"]
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}

    return {
        "entity_code": entity_code,
        "run": {
            "id": str(run["id"]),
            "period_start": run["period_start"].isoformat() if run["period_start"] else None,
            "period_end": run["period_end"].isoformat() if run["period_end"] else None,
            "transactions_reviewed": run["transactions_reviewed"],
            "transactions_matched": run["transactions_matched"],
            "transactions_unmatched": run["transactions_unmatched"],
            "transactions_skipped": run["transactions_skipped"],
            "transactions_split_required": run["transactions_split_required"],
            "journal_batch_id": (
                str(run["journal_batch_id"]) if run["journal_batch_id"] else None
            ),
            "status": run["status"],
            "actor_email": run["actor_email"],
            "created_at": run["created_at"].isoformat() if run["created_at"] else None,
            "summary": summary,
        },
        "line_count": len(lines),
        "lines": [
            {
                "id": str(r["id"]),
                "bank_transaction_id": str(r["bank_transaction_id"]),
                "transaction_date": (
                    r["transaction_date"].isoformat() if r["transaction_date"] else None
                ),
                "description": r["description"],
                "direction": r["direction"],
                "matched_status": r["matched_status"],
                "skip_reason": r["skip_reason"],
                "rule_code": r["rule_code"],
                "debit_account": r["debit_account"],
                "credit_account": r["credit_account"],
                "amount": str(r["amount"]) if r["amount"] is not None else None,
            }
            for r in lines
        ],
    }


def list_unmatched_transactions(
    session,
    *,
    entity_code: str,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    rows = session.execute(
        text(
            """
            SELECT bt.id, bt.transaction_date, bt.description,
                   bt.amount, bt.direction, bt.source_system
            FROM bank_transactions bt
            WHERE bt.entity_id = :entity_id
              AND bt.transaction_date BETWEEN :period_start AND :period_end
              AND NOT EXISTS (
                  SELECT 1 FROM bank_auto_journal_lines ajl
                  WHERE ajl.bank_transaction_id = bt.id
                    AND ajl.matched_status = 'matched'
              )
            ORDER BY bt.transaction_date, bt.amount DESC
            """
        ),
        {
            "entity_id": entity["id"],
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "count": len(rows),
        "transactions": [
            {
                "id": str(r["id"]),
                "transaction_date": (
                    r["transaction_date"].isoformat() if r["transaction_date"] else None
                ),
                "description": r["description"],
                "amount": str(r["amount"]) if r["amount"] is not None else None,
                "direction": r["direction"],
                "source_system": r["source_system"],
            }
            for r in rows
        ],
    }
