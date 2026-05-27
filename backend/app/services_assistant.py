"""
BookWize AI assistant — service layer.

The brain. The chat-widget routes (routes/assistant.py) are thin wrappers
around the functions below.

Public surface (used by routes/assistant.py):

    build_entity_context(session, entity_code) -> dict
        One rich JSON-safe context dict — entity profile, current
        period state, chart of accounts, learned memory, recent
        observations, vendor-classification hints, recent
        unclassified bank txns.

    parse_intent(message, context) -> IntentResult
        Calls Claude to return a structured intent JSON. Falls back to
        a heuristic when the model is unavailable or returns garbage.

    find_matching_transaction(session, entity_code, amount, date,
                              description) -> list[TransactionMatch]
        Scored matches against bank_transactions. Empty list means we
        should drop the request into assistant_pending_intents.

    generate_response(session, entity_code, message, intent,
                      matches, history) -> AssistantReply
        Calls Claude to produce the human-facing reply text and a
        machine-readable proposed_action.

    execute_action(session, entity_code, action_type, ..., clerk_user_id)
        Applies a confirmed action — classifies a bank txn, adds a
        note, drops a pending intent — and writes the audit row.

    learn_from_interaction(session, entity_code, *, message, intent,
                            action_taken, was_corrected, correction_details)
        Upserts entity memory after every confirm / correct round.

Cost / reliability notes:
    - Both Claude calls use the same model the classifier already uses
      (claude-haiku-4-5-20251001) — cheap + fast.
    - JSON responses come back through json.loads with a fallback so a
      bad model output never 500s the route.
    - When ANTHROPIC_API_KEY is unset, both call sites degrade to a
      rules-only heuristic — the assistant still works for trivial
      transactions and pure queries; it just can't reason freely.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date as DateType, datetime as DateTimeType, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .config import settings

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------


CLAUDE_MODEL_ID = "claude-sonnet-4-6"
CLAUDE_INTENT_MAX_TOKENS = 800
CLAUDE_REPLY_MAX_TOKENS = 600


INTENT_TYPES = {
    "classify_transaction",
    "query_balance",
    "add_note",
    "query_period",
    "correction",
    "general_question",
    # Payroll intents (Part 2 — payroll action capability)
    "update_employee_rate",
    "update_employee_salary",
    "one_time_pay_override",
    "add_bonus_line",
    "other",
}


ACTION_TYPES = {
    "classify_transaction",
    "add_note",
    "post_to_pending",
    # Payroll actions
    "update_employee_rate",
    "update_employee_salary",
    "one_time_pay_override",
    "add_bonus_line",
    "none",
}


# 2026 CRA rates used for back-of-envelope tax delta in payroll
# previews. Real numbers come from services_payroll_calc.py on
# rebuild; these are only for the BEFORE/AFTER card.
_CPP_RATE_2026 = Decimal("0.0595")
_EI_RATE_2026 = Decimal("0.0166")
_FED_MARGINAL_TAX_ESTIMATE = Decimal("0.205")    # 20.5% — middle bracket
_PROV_ON_MARGINAL_TAX_ESTIMATE = Decimal("0.0915")  # 9.15% — ON middle
_DEFAULT_BIWEEKLY_HOURS = Decimal("73")


# --------------------------------------------------------------------------
# Result dataclasses
# --------------------------------------------------------------------------


@dataclass
class IntentResult:
    intent: str
    amount: Decimal | None = None
    date: DateType | None = None
    description: str | None = None
    suggested_debit_account: str | None = None
    suggested_credit_account: str | None = None
    confidence: float = 0.0
    reasoning: str = ""
    needs_clarification: bool = False
    clarification_question: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    # Payroll-intent extras. employee_name is a free-form string
    # (fuzzy-matched downstream); new_value is the target rate /
    # bi-weekly salary / one-off gross / bonus amount.
    employee_name: str | None = None
    new_value: Decimal | None = None
    period_id: str | None = None


@dataclass
class TransactionMatch:
    transaction_id: str
    transaction_date: str
    amount: Decimal
    description: str
    direction: str
    score: float


@dataclass
class ProposedAction:
    action_type: str  # see ACTION_TYPES
    transaction_id: str | None = None
    transaction_preview: dict[str, Any] | None = None
    journal_preview: dict[str, Any] | None = None
    # Payroll-action preview (before/after snapshot). Populated only
    # for action_type in {'update_employee_rate',
    # 'update_employee_salary', 'one_time_pay_override',
    # 'add_bonus_line'}.
    payroll_preview: dict[str, Any] | None = None
    # Cached references for the executor — kept here so the route
    # can persist them in proposal_json without re-running the
    # fuzzy match on confirm.
    employee_id: str | None = None
    payroll_run_id: str | None = None
    payroll_run_line_id: str | None = None
    new_value: float | None = None
    pending_intent_id: str | None = None


@dataclass
class AssistantReply:
    reply: str
    proposed_action: ProposedAction
    needs_confirmation: bool
    intent: str
    matched_transactions: list[TransactionMatch] = field(default_factory=list)


# --------------------------------------------------------------------------
# 2A. Context builder
# --------------------------------------------------------------------------


def build_entity_context(session, entity_code: str) -> dict[str, Any]:
    """Pull everything the assistant should know about this entity into
    one dict. Used by both parse_intent() and generate_response().
    Result is JSON-serializable so it can also be cached / logged."""
    entity = session.execute(
        text(
            """
            SELECT id, entity_code, entity_name, province,
                   fiscal_year_end_month, fiscal_year_end_day
              FROM entities
             WHERE entity_code = :ec
            """
        ),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        # Caller decides what to do; we return an empty-ish shell so the
        # generator can still build a polite refusal.
        return {
            "entity": {"entity_code": entity_code, "entity_name": None},
            "current_period": None,
            "chart_of_accounts": [],
            "recent_unclassified": [],
            "memory": [],
            "recent_observations": [],
            "vendor_classifications": [],
        }

    # Tiered current-period resolution — mirrors routes/period_close.py
    # ::get_current_period. Oldest-first so the assistant surfaces the
    # next period that needs to be closed rather than the most recent.
    period = session.execute(
        text(
            """
            SELECT ap.period_end, ap.period_label, ap.status
              FROM accounting_periods ap
             WHERE ap.entity_id = :eid
               AND ap.period_end <= CURRENT_DATE
               AND ap.status NOT IN ('closed_locked', 'approved_to_close')
               AND EXISTS (
                   SELECT 1 FROM journal_batches jb
                    WHERE jb.accounting_period_id = ap.id
                      AND jb.status = 'approved_to_post'
               )
             ORDER BY ap.period_end ASC
             LIMIT 1
            """
        ),
        {"eid": entity["id"]},
    ).mappings().first()
    if not period:
        period = session.execute(
            text(
                """
                SELECT period_end, period_label, status
                  FROM accounting_periods
                 WHERE entity_id = :eid
                   AND period_end <= CURRENT_DATE
                   AND status NOT IN ('closed_locked', 'approved_to_close')
                 ORDER BY period_end ASC
                 LIMIT 1
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()
    if not period:
        period = session.execute(
            text(
                """
                SELECT period_end, period_label, status
                  FROM accounting_periods
                 WHERE entity_id = :eid
                   AND period_end <= CURRENT_DATE
                 ORDER BY period_end DESC
                 LIMIT 1
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()

    current_period: dict[str, Any] | None = None
    if period:
        period_end_iso = (
            period["period_end"].isoformat()
            if hasattr(period["period_end"], "isoformat")
            else str(period["period_end"])
        )
        today = DateTimeType.utcnow().date()
        try:
            days_remaining = (period["period_end"] - today).days
        except TypeError:
            days_remaining = None

        # How many journal_batches are in flight for this period?
        open_batches = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM journal_batches
                 WHERE entity_id = :eid
                   AND status NOT IN ('voided', 'rejected')
                   AND accounting_period_id = (
                       SELECT id FROM accounting_periods
                        WHERE entity_id = :eid AND period_end = :pe
                        LIMIT 1
                   )
                """
            ),
            {"eid": entity["id"], "pe": period["period_end"]},
        ).mappings().first()

        unclassified = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM bank_transactions
                 WHERE entity_id = :eid
                   AND review_status IN ('new', 'needs_review')
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()

        unmatched_invoices = session.execute(
            text(
                """
                SELECT COUNT(*) AS c FROM invoice_documents
                 WHERE entity_code = :ec AND status = 'unmatched'
                """
            ),
            {"ec": entity_code},
        ).mappings().first()

        current_period = {
            "period_end": period_end_iso,
            "period_label": period["period_label"],
            "status": period["status"],
            "days_remaining": days_remaining,
            "open_journals_count": int((open_batches or {}).get("c") or 0),
            "unclassified_transactions_count": int(
                (unclassified or {}).get("c") or 0
            ),
            "unmatched_invoices_count": int(
                (unmatched_invoices or {}).get("c") or 0
            ),
        }

    # Chart of accounts — pull the latest known names from gl_account_balances
    # (most-recent import per code). When the entity has no GL imports yet,
    # we derive a thin chart from the journal_lines that actually exist.
    chart_rows = session.execute(
        text(
            """
            WITH coa AS (
                SELECT DISTINCT ON (account_code)
                       account_code, account_name
                  FROM gl_account_balances
                 WHERE entity_id = :eid
              ORDER BY account_code, created_at DESC
            ),
            seen AS (
                SELECT DISTINCT jl.account_code
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                 WHERE jb.entity_id = :eid
            )
            SELECT s.account_code,
                   COALESCE(c.account_name, s.account_code) AS account_name
              FROM seen s
         LEFT JOIN coa c ON c.account_code = s.account_code
          ORDER BY s.account_code
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()
    chart_of_accounts = [
        {
            "code": r["account_code"],
            "name": r["account_name"],
            "type": _account_type(r["account_code"]),
            "normal_balance": _normal_balance(r["account_code"]),
        }
        for r in chart_rows
    ]

    # Recent unclassified bank txns (last 30 days).
    unclassified_rows = session.execute(
        text(
            """
            SELECT id, transaction_date, amount, description, direction,
                   source_account_code
              FROM bank_transactions
             WHERE entity_id = :eid
               AND review_status IN ('new', 'needs_review')
               AND transaction_date >= NOW() - INTERVAL '30 days'
             ORDER BY transaction_date DESC
             LIMIT 25
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()
    recent_unclassified = [
        {
            "id": str(r["id"]),
            "date": r["transaction_date"].isoformat(),
            "amount": float(r["amount"]),
            "description": r["description"],
            "direction": r["direction"],
            "bank_account": r["source_account_code"],
        }
        for r in unclassified_rows
    ]

    # Entity memory — sorted highest confidence first, capped.
    memory_rows = session.execute(
        text(
            """
            SELECT memory_type, memory_key, memory_value, confidence,
                   times_confirmed, times_corrected
              FROM assistant_entity_memory
             WHERE entity_code = :ec
             ORDER BY confidence DESC, times_confirmed DESC
             LIMIT 100
            """
        ),
        {"ec": entity_code},
    ).mappings().all()
    memory = [dict(r) | {"confidence": float(r["confidence"])} for r in memory_rows]

    # Recent period observations.
    obs_rows = session.execute(
        text(
            """
            SELECT observation_type, observation, severity, period_end
              FROM assistant_period_observations
             WHERE entity_code = :ec
             ORDER BY period_end DESC, created_at DESC
             LIMIT 15
            """
        ),
        {"ec": entity_code},
    ).mappings().all()
    recent_observations = [
        {
            "observation_type": r["observation_type"],
            "observation": r["observation"],
            "severity": r["severity"],
            "period_end": r["period_end"].isoformat(),
        }
        for r in obs_rows
    ]

    # Top vendor-classification memory rows.
    vendor_rows = session.execute(
        text(
            """
            SELECT normalized_vendor_key, account_code, debit_or_credit,
                   occurrences_count, confidence_score
              FROM vendor_classification_memory
             WHERE entity_id = :eid
             ORDER BY occurrences_count DESC
             LIMIT 50
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()
    vendor_classifications = [
        {
            "vendor_pattern": r["normalized_vendor_key"],
            "account_code": r["account_code"],
            "debit_or_credit": r["debit_or_credit"],
            "times_used": int(r["occurrences_count"]),
            "confidence": float(r["confidence_score"]),
        }
        for r in vendor_rows
    ]

    return {
        "entity": {
            "entity_code": entity["entity_code"],
            "entity_name": entity["entity_name"],
            "province": entity["province"],
            "fiscal_year_end": f"{entity['fiscal_year_end_month']:02d}-{entity['fiscal_year_end_day']:02d}",
        },
        "current_period": current_period,
        "chart_of_accounts": chart_of_accounts,
        "recent_unclassified": recent_unclassified,
        "memory": memory,
        "recent_observations": recent_observations,
        "vendor_classifications": vendor_classifications,
    }


def _account_type(code: str) -> str:
    p = (code or "").strip()[:1]
    return {
        "1": "asset", "2": "liability", "3": "equity", "4": "revenue",
        "5": "cogs", "6": "operating_expense", "7": "other_income_expense",
        "8": "other_income_expense", "9": "other_income_expense",
    }.get(p, "other")


def _normal_balance(code: str) -> str:
    p = (code or "").strip()[:1]
    if p in {"1", "5", "6"}:
        return "debit"
    return "credit"


# --------------------------------------------------------------------------
# Claude client (lazy + degradable)
# --------------------------------------------------------------------------


def _claude_available() -> bool:
    return bool(getattr(settings, "anthropic_api_key", None))


def _claude_client():
    api_key = getattr(settings, "anthropic_api_key", None)
    if not api_key:
        return None
    try:
        from anthropic import Anthropic
    except ImportError:
        logger.warning("anthropic SDK not installed — assistant degraded to heuristic")
        return None
    return Anthropic(api_key=api_key)


# --------------------------------------------------------------------------
# 2B. Intent parser
# --------------------------------------------------------------------------


_INTENT_SYSTEM = """You are the intent parser for the BookWize accounting assistant.

You receive a free-form message from a Home Hardware dealer and must
extract a structured intent. You ALSO have access to the dealer's
chart of accounts, what BookWize has learned about this dealer, and
their recent unclassified bank transactions.

When the message describes a transaction (e.g. "I paid $4,250 for
rent on May 12"), propose the appropriate journal — pick accounts
ONLY from the chart provided. Use the dealer's learned terminology
(e.g. if memory says "garbage tags" = 2150, respect that).

When the message is about PAYROLL — changing an employee's rate,
salary, giving a one-time pay bump, or adding a bonus — extract the
employee name and the new value as numbers. Examples:
  "Change Noah's hourly rate to $20"             → update_employee_rate, employee_name='Noah', new_value=20
  "Bump Cynthia to $52000 annual"                → update_employee_salary, employee_name='Cynthia', new_value=52000 (annual; backend converts to bi-weekly)
  "Give Alan a one-time $500 bonus on this run"  → add_bonus_line, employee_name='Alan', new_value=500
  "One-time override Cynthia's pay to $2400"     → one_time_pay_override, employee_name='Cynthia', new_value=2400

Respond with valid JSON ONLY. No code fences. No prose.

Required shape:
{
  "intent": one of [
    "classify_transaction", "query_balance", "add_note",
    "query_period", "correction", "general_question",
    "update_employee_rate", "update_employee_salary",
    "one_time_pay_override", "add_bonus_line",
    "other"
  ],
  "amount": number | null,
  "date": "YYYY-MM-DD" | null,
  "description": string | null,
  "suggested_debit_account": "<account_code>" | null,
  "suggested_credit_account": "<account_code>" | null,
  "employee_name": string | null,
  "new_value": number | null,
  "confidence": number 0..100,
  "reasoning": "one short sentence",
  "needs_clarification": boolean,
  "clarification_question": string | null
}
"""


def parse_intent(message: str, context: dict[str, Any]) -> IntentResult:
    """Parse the user message into a structured intent via Claude. Falls
    back to a regex heuristic when Claude is unavailable."""
    client = _claude_client()
    if not client:
        return _heuristic_intent(message)

    chart = context.get("chart_of_accounts") or []
    memory = context.get("memory") or []
    recent = context.get("recent_unclassified") or []

    system_blocks = [
        {"type": "text", "text": _INTENT_SYSTEM},
        {
            "type": "text",
            "text": (
                "CHART OF ACCOUNTS (code  name  type):\n"
                + "\n".join(
                    f"{a['code']}  {a['name']}  ({a['type']})" for a in chart
                )
                if chart
                else "CHART OF ACCOUNTS: (none — entity has no journal history yet)"
            ),
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": "LEARNED MEMORY:\n" + _memory_summary(memory),
            "cache_control": {"type": "ephemeral"},
        },
        {
            "type": "text",
            "text": "RECENT UNCLASSIFIED TRANSACTIONS:\n" + _unclassified_summary(recent),
        },
    ]

    try:
        msg = client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=CLAUDE_INTENT_MAX_TOKENS,
            system=system_blocks,
            messages=[{"role": "user", "content": message}],
        )
    except Exception as exc:
        logger.warning("Claude intent call failed: %r", exc)
        return _heuristic_intent(message)

    text_out = _collect_text(msg)
    parsed = _parse_json_robust(text_out)
    if not parsed:
        return _heuristic_intent(message)

    return _intent_result_from_json(parsed)


def _memory_summary(memory: list[dict[str, Any]]) -> str:
    if not memory:
        return "(nothing learned yet — fresh entity)"
    lines: list[str] = []
    for m in memory[:30]:
        lines.append(
            f"  - {m['memory_type']}: {m['memory_key']} -> {m['memory_value']} "
            f"(confidence {m['confidence']:.0f}, confirmed {m['times_confirmed']}x)"
        )
    return "\n".join(lines)


def _unclassified_summary(recent: list[dict[str, Any]]) -> str:
    if not recent:
        return "(none in the last 30 days)"
    lines: list[str] = []
    for r in recent[:15]:
        lines.append(
            f"  {r['date']}  ${abs(r['amount']):,.2f}  {r['direction']:8s}  {r['description'][:60]}"
        )
    return "\n".join(lines)


def _collect_text(msg: Any) -> str:
    """Anthropic returns a list of content blocks — concatenate text blocks."""
    out = ""
    for block in getattr(msg, "content", None) or []:
        if getattr(block, "type", None) == "text":
            out += block.text or ""
    return out.strip()


def _parse_json_robust(text_out: str) -> dict[str, Any] | None:
    if not text_out:
        return None
    # Strip code fences the model may have added.
    s = text_out.strip()
    if s.startswith("```"):
        s = s.strip("`").lstrip()
        if s.lower().startswith("json"):
            s = s[4:].lstrip("\n")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # Last-ditch: try to find the first { ... } block.
        m = re.search(r"\{.*\}", s, flags=re.DOTALL)
        if not m:
            logger.warning("Claude returned non-JSON intent: %r", text_out[:200])
            return None
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            logger.warning("Claude returned non-JSON intent: %r", text_out[:200])
            return None


def _intent_result_from_json(parsed: dict[str, Any]) -> IntentResult:
    intent = (parsed.get("intent") or "").strip()
    if intent not in INTENT_TYPES:
        intent = "other"
    amount = _coerce_decimal(parsed.get("amount"))
    date_val = _coerce_date(parsed.get("date"))
    description = (parsed.get("description") or None) or None
    confidence = float(parsed.get("confidence") or 0)
    return IntentResult(
        intent=intent,
        amount=amount,
        date=date_val,
        description=description,
        suggested_debit_account=(parsed.get("suggested_debit_account") or None),
        suggested_credit_account=(parsed.get("suggested_credit_account") or None),
        confidence=max(0.0, min(100.0, confidence)),
        reasoning=str(parsed.get("reasoning") or "")[:500],
        needs_clarification=bool(parsed.get("needs_clarification")),
        clarification_question=parsed.get("clarification_question") or None,
        raw=parsed,
        employee_name=(parsed.get("employee_name") or None),
        new_value=_coerce_decimal(parsed.get("new_value")),
    )


def _coerce_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _coerce_date(value: Any) -> DateType | None:
    if not value:
        return None
    try:
        return DateTimeType.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _heuristic_intent(message: str) -> IntentResult:
    """Tiny rules-only fallback when Claude isn't available. Identifies
    a few common query patterns so the assistant isn't completely
    helpless without an API key."""
    lower = message.lower()
    if any(kw in lower for kw in ("cash balance", "cash position", "bank balance", "how much cash")):
        return IntentResult(
            intent="query_balance",
            confidence=60.0,
            reasoning="heuristic: 'cash balance' query",
        )
    if any(kw in lower for kw in ("what's left", "what is left", "close status", "month end status", "month-end")):
        return IntentResult(
            intent="query_period",
            confidence=60.0,
            reasoning="heuristic: month-end query",
        )
    # Try to pull an amount out of the message.
    amt_match = re.search(r"\$?\s*([\d,]+\.\d{2}|[\d,]+)", message)
    amount: Decimal | None = None
    if amt_match:
        try:
            amount = Decimal(amt_match.group(1).replace(",", ""))
        except InvalidOperation:
            amount = None
    if amount and amount > 0:
        return IntentResult(
            intent="classify_transaction",
            amount=amount,
            description=message[:120],
            confidence=40.0,
            reasoning="heuristic: amount detected — classify_transaction (low-confidence)",
        )
    return IntentResult(
        intent="other",
        confidence=20.0,
        reasoning="heuristic: no pattern matched",
    )


# --------------------------------------------------------------------------
# 2C. Transaction finder
# --------------------------------------------------------------------------


def find_matching_transaction(
    session,
    *,
    entity_code: str,
    amount: Decimal | None,
    target_date: DateType | None,
    description: str | None = None,
    top_n: int = 3,
) -> list[TransactionMatch]:
    """Look in bank_transactions for unmatched / needs-review rows whose
    amount matches ±$0.01 within a small date window. Score by date
    proximity. Returns at most top_n matches; empty list when nothing
    plausible is in the DB."""
    if amount is None or amount <= 0:
        return []

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return []

    # Pull a wide net: ±3 days, amount within $0.50, unmatched/needs_review.
    params: dict[str, Any] = {
        "eid": entity["id"],
        "amt_lo": amount - Decimal("0.50"),
        "amt_hi": amount + Decimal("0.50"),
    }
    where_date = ""
    if target_date is not None:
        params["d_lo"] = target_date - timedelta(days=3)
        params["d_hi"] = target_date + timedelta(days=3)
        where_date = "AND transaction_date BETWEEN :d_lo AND :d_hi"

    rows = session.execute(
        text(
            f"""
            SELECT id, transaction_date, amount, description, direction
              FROM bank_transactions
             WHERE entity_id = :eid
               AND ABS(amount) BETWEEN :amt_lo AND :amt_hi
               AND review_status IN ('new', 'needs_review')
               {where_date}
             ORDER BY transaction_date DESC
             LIMIT 25
            """
        ),
        params,
    ).mappings().all()

    matches: list[TransactionMatch] = []
    for r in rows:
        bank_amt = abs(Decimal(str(r["amount"])))
        amt_exact = bank_amt == amount
        amt_close = abs(bank_amt - amount) <= Decimal("0.50")
        if target_date is not None and r["transaction_date"] is not None:
            days = abs((r["transaction_date"] - target_date).days)
        else:
            days = 999

        if amt_exact and days == 0:
            score = 100.0
        elif amt_exact and days <= 1:
            score = 90.0
        elif amt_exact and days <= 3:
            score = 75.0
        elif amt_close and days == 0:
            score = 60.0
        elif amt_close and days <= 3:
            score = 45.0
        else:
            score = 0.0
        if score <= 0:
            continue
        matches.append(
            TransactionMatch(
                transaction_id=str(r["id"]),
                transaction_date=r["transaction_date"].isoformat(),
                amount=bank_amt,
                description=r["description"] or "",
                direction=r["direction"] or "unknown",
                score=score,
            )
        )
    matches.sort(key=lambda m: m.score, reverse=True)
    return matches[:top_n]


# --------------------------------------------------------------------------
# 2D. Response generator
# --------------------------------------------------------------------------


ASSISTANT_SYSTEM_PROMPT = """You are the BookWize accounting assistant for {entity_name} ({entity_code}), a Home Hardware dealer in {province}, Canada.

Your job is to help the dealer manage their books through natural conversation. You are their virtual bookkeeper — knowledgeable, practical, and concise.

WHAT YOU KNOW:
- Full chart of accounts for this entity
- All recent unclassified bank transactions
- Current month-end status and blockers
- What you have learned about this entity over time (terminology, vendors, patterns)
- Recent period observations
- Which page of the app the user is currently viewing

RULES:
1. Never invent account codes. Only use accounts from the chart provided.
2. Always show journal entries in Dr/Cr format before asking for confirmation.
3. Keep responses under 80 words unless asked for detail.
4. Speak like a bookkeeper, not a chatbot. Skip "Great question!" / "Certainly!".
5. If you're not sure, say so and ask.
6. Amounts are always in CAD.
7. Always ask for confirmation before classifying anything or changing payroll.
8. When you learn something new about this business, acknowledge it briefly.
9. For payroll changes (rate, salary, one-time override, bonus), produce a
   before/after preview. Never apply a payroll change without the user
   confirming the preview.
10. Use the PAGE CONTEXT hint below to tailor the reply (e.g. on
    /payroll/runs/<id> the user is looking at a specific pay run, so
    "this run" refers to that one).

PAGE CONTEXT:
{page_context}

WHAT YOU HAVE LEARNED ABOUT THIS ENTITY:
{memory_summary}

CURRENT PERIOD STATUS:
{period_status}

CHART OF ACCOUNTS:
{chart_of_accounts}

RECENT UNCLASSIFIED TRANSACTIONS:
{recent_unclassified}
"""


def generate_response(
    session,
    *,
    entity_code: str,
    user_message: str,
    intent: IntentResult,
    matches: list[TransactionMatch],
    history: list[dict[str, Any]],
    context: dict[str, Any],
    page_context: str | None = None,
) -> AssistantReply:
    """Build the human-facing reply text + the machine-readable proposed
    action. Uses Claude when available; falls back to a deterministic
    response otherwise.

    page_context is the route the user is currently viewing (e.g.
    '/payroll/runs/abc...'). It's injected into the system prompt so
    Claude can interpret deictic references like "this run" or "this
    employee" correctly.
    """
    proposed_action = _propose_action(intent, matches, context, session=session)

    client = _claude_client()
    if not client:
        return AssistantReply(
            reply=_fallback_reply(intent, matches, proposed_action, context),
            proposed_action=proposed_action,
            needs_confirmation=proposed_action.action_type != "none",
            intent=intent.intent,
            matched_transactions=matches,
        )

    entity = context.get("entity") or {}
    current_period = context.get("current_period") or {}
    system_text = ASSISTANT_SYSTEM_PROMPT.format(
        entity_name=entity.get("entity_name") or entity_code,
        entity_code=entity_code,
        province=entity.get("province") or "Canada",
        page_context=(page_context or "(unknown page)"),
        memory_summary=_memory_summary(context.get("memory") or []),
        period_status=(
            f"{current_period.get('period_label')} ({current_period.get('status')}) — "
            f"{current_period.get('open_journals_count', 0)} open journals, "
            f"{current_period.get('unclassified_transactions_count', 0)} unclassified txns, "
            f"{current_period.get('unmatched_invoices_count', 0)} unmatched invoices"
            if current_period
            else "No accounting period exists yet."
        ),
        chart_of_accounts="\n".join(
            f"{a['code']}  {a['name']}  ({a['type']})"
            for a in (context.get("chart_of_accounts") or [])
        ) or "(no journal history yet)",
        recent_unclassified=_unclassified_summary(context.get("recent_unclassified") or []),
    )

    # Build a tight messages array: last 5 turns then the new user
    # message + a structured note about what we already extracted.
    msg_list: list[dict[str, Any]] = []
    for h in history[-5:]:
        msg_list.append({"role": h["role"], "content": h["content"]})
    intent_summary = json.dumps(
        {
            "intent": intent.intent,
            "amount": (str(intent.amount) if intent.amount is not None else None),
            "date": (intent.date.isoformat() if intent.date else None),
            "description": intent.description,
            "suggested_debit_account": intent.suggested_debit_account,
            "suggested_credit_account": intent.suggested_credit_account,
            "confidence": intent.confidence,
        }
    )
    match_summary = json.dumps([
        {
            "transaction_id": m.transaction_id,
            "date": m.transaction_date,
            "amount": str(m.amount),
            "description": m.description,
            "score": m.score,
        }
        for m in matches[:3]
    ])
    msg_list.append({
        "role": "user",
        "content": (
            f"{user_message}\n\n"
            f"[parsed intent: {intent_summary}]\n"
            f"[candidate transactions: {match_summary}]"
        ),
    })

    try:
        msg = client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=CLAUDE_REPLY_MAX_TOKENS,
            system=[{"type": "text", "text": system_text}],
            messages=msg_list,
        )
        reply_text = _collect_text(msg) or _fallback_reply(intent, matches, proposed_action, context)
    except Exception as exc:
        logger.warning("Claude response call failed: %r", exc)
        reply_text = _fallback_reply(intent, matches, proposed_action, context)

    return AssistantReply(
        reply=reply_text,
        proposed_action=proposed_action,
        needs_confirmation=proposed_action.action_type != "none",
        intent=intent.intent,
        matched_transactions=matches,
    )


_PAYROLL_INTENTS = frozenset({
    "update_employee_rate",
    "update_employee_salary",
    "one_time_pay_override",
    "add_bonus_line",
})


def _propose_action(
    intent: IntentResult,
    matches: list[TransactionMatch],
    context: dict[str, Any],
    *,
    session: Any = None,
) -> ProposedAction:
    """Map (intent, matches) -> a structured action the route layer can
    persist and the frontend can render. Payroll intents take session
    so we can do the fuzzy employee match + before/after calc inline."""
    if intent.intent in _PAYROLL_INTENTS and session is not None:
        entity_code = (context.get("entity") or {}).get("entity_code")
        if entity_code:
            try:
                return _propose_payroll_action(intent, session, entity_code)
            except Exception:
                logger.exception("payroll proposal failed for intent=%s", intent.intent)
                return ProposedAction(action_type="none")
    if intent.intent == "classify_transaction" and matches:
        top = matches[0]
        debit = intent.suggested_debit_account
        credit = intent.suggested_credit_account
        # Match accounts to chart for human-readable names.
        chart_by_code = {
            a["code"]: a["name"] for a in (context.get("chart_of_accounts") or [])
        }
        return ProposedAction(
            action_type="classify_transaction",
            transaction_id=top.transaction_id,
            transaction_preview={
                "date": top.transaction_date,
                "amount": float(top.amount),
                "description": top.description,
                "direction": top.direction,
            },
            journal_preview={
                "debit_account_code": debit,
                "debit_account_name": chart_by_code.get(debit) if debit else None,
                "credit_account_code": credit,
                "credit_account_name": chart_by_code.get(credit) if credit else None,
                "amount": float(top.amount),
                "note": intent.description or "",
            },
        )
    if intent.intent == "classify_transaction" and not matches:
        return ProposedAction(action_type="post_to_pending")
    if intent.intent == "add_note":
        return ProposedAction(action_type="add_note")
    return ProposedAction(action_type="none")


# --------------------------------------------------------------------------
# Payroll-action proposer
# --------------------------------------------------------------------------


def _fuzzy_match_employee(
    session, *, entity_id: Any, name_query: str | None
) -> list[dict[str, Any]]:
    """Return active employees whose first/last/full name matches
    name_query (ILIKE substring). Multiple results = caller must ask
    for clarification."""
    if not name_query:
        return []
    q = name_query.strip().lower()
    if len(q) < 2:
        return []
    rows = session.execute(
        text(
            """
            SELECT id, full_name, first_name, last_name, employment_type,
                   hourly_rate, biweekly_salary, vacation_rate, is_active
              FROM payroll_employees
             WHERE entity_id = :eid
               AND is_active = TRUE
               AND (
                 lower(full_name)  LIKE :pat OR
                 lower(first_name) LIKE :pat OR
                 lower(last_name)  LIKE :pat
               )
             ORDER BY full_name
            """
        ),
        {"eid": entity_id, "pat": f"%{q}%"},
    ).mappings().all()
    return [dict(r) for r in rows]


def _find_active_draft_run(
    session, *, entity_id: Any
) -> dict[str, Any] | None:
    """The most recent payroll_runs row that's still editable (not yet
    approved/posted/voided). Used by one_time_pay_override and
    add_bonus_line to identify which run to touch."""
    row = session.execute(
        text(
            """
            SELECT id, pay_run_number, period_start, period_end, pay_date,
                   status, workflow_status, accounting_period_id
              FROM payroll_runs
             WHERE entity_id = :eid
               AND COALESCE(workflow_status, status) NOT IN
                   ('approved_to_post', 'posted', 'voided')
             ORDER BY period_end DESC
             LIMIT 1
            """
        ),
        {"eid": entity_id},
    ).mappings().first()
    return dict(row) if row else None


def _avg_recent_biweekly_hours(
    session, *, entity_id: Any, employee_id: Any
) -> Decimal:
    """Average total_hours across the last 3 payroll_run_lines for this
    employee. Falls back to _DEFAULT_BIWEEKLY_HOURS if no history."""
    rows = session.execute(
        text(
            """
            SELECT prl.total_hours
              FROM payroll_run_lines prl
              JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
             WHERE pr.entity_id = :eid AND prl.employee_id = :emp
               AND prl.total_hours > 0
             ORDER BY pr.period_end DESC
             LIMIT 3
            """
        ),
        {"eid": entity_id, "emp": employee_id},
    ).mappings().all()
    if not rows:
        return _DEFAULT_BIWEEKLY_HOURS
    total = sum((Decimal(str(r["total_hours"])) for r in rows), Decimal("0"))
    return (total / Decimal(len(rows))).quantize(Decimal("0.01"))


def _estimate_net_pay(gross: Decimal) -> Decimal:
    """Quick deterministic net-pay estimate for the BEFORE/AFTER card.
    Uses 2026 CRA rates × marginal-tax approximation. NOT a substitute
    for services_payroll_calc on the actual run — purely for the
    preview card the user sees BEFORE confirming."""
    if gross <= 0:
        return Decimal("0.00")
    cpp = (gross * _CPP_RATE_2026).quantize(Decimal("0.01"))
    ei = (gross * _EI_RATE_2026).quantize(Decimal("0.01"))
    fed = (gross * _FED_MARGINAL_TAX_ESTIMATE).quantize(Decimal("0.01"))
    prov = (gross * _PROV_ON_MARGINAL_TAX_ESTIMATE).quantize(Decimal("0.01"))
    return (gross - cpp - ei - fed - prov).quantize(Decimal("0.01"))


def _propose_payroll_action(
    intent: IntentResult, session: Any, entity_code: str
) -> ProposedAction:
    """Construct a BEFORE/AFTER preview for a payroll change request.

    Hard requirements:
      - Entity match: every query is scoped by entity_id.
      - Single-employee resolution: if name matches 2+, return a
        clarification-style action (action_type='none' with a
        clarification note in the payroll_preview).
      - No DB mutations here — that happens only on confirm in
        _execute_payroll_action.
    """
    entity_row = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity_row:
        return ProposedAction(action_type="none")
    entity_id = entity_row["id"]

    if not intent.employee_name or intent.new_value is None:
        return ProposedAction(
            action_type="none",
            payroll_preview={
                "missing": True,
                "note": (
                    "I need both the employee name and the new amount. "
                    "Try: 'Change Noah's rate to $20' or 'Give Cynthia a $500 bonus'."
                ),
            },
        )

    candidates = _fuzzy_match_employee(
        session, entity_id=entity_id, name_query=intent.employee_name
    )
    if not candidates:
        return ProposedAction(
            action_type="none",
            payroll_preview={
                "missing": True,
                "note": f"No active employee matches '{intent.employee_name}'.",
            },
        )
    if len(candidates) > 1:
        names = ", ".join(c["full_name"] for c in candidates[:4])
        return ProposedAction(
            action_type="none",
            payroll_preview={
                "ambiguous": True,
                "note": (
                    f"'{intent.employee_name}' matches multiple: {names}. "
                    "Which one — first + last name?"
                ),
            },
        )

    emp = candidates[0]
    new_val = Decimal(str(intent.new_value))

    if intent.intent == "update_employee_rate":
        return _propose_rate_change(emp, new_val, session, entity_id)
    if intent.intent == "update_employee_salary":
        return _propose_salary_change(emp, new_val, session, entity_id)
    if intent.intent in ("one_time_pay_override", "add_bonus_line"):
        run = _find_active_draft_run(session, entity_id=entity_id)
        if not run:
            return ProposedAction(
                action_type="none",
                payroll_preview={
                    "missing": True,
                    "note": (
                        "No draft pay run is open. Create one from "
                        "Payroll → New pay run before applying this change."
                    ),
                },
            )
        # Locked-period guard.
        if run.get("accounting_period_id"):
            period = session.execute(
                text(
                    "SELECT status FROM accounting_periods WHERE id = :id"
                ),
                {"id": run["accounting_period_id"]},
            ).mappings().first()
            if period and period["status"] in (
                "closed_locked", "approved_to_close"
            ):
                return ProposedAction(
                    action_type="none",
                    payroll_preview={
                        "blocked": True,
                        "note": (
                            "That pay run sits in a locked accounting period "
                            "and can't be edited."
                        ),
                    },
                )
        if intent.intent == "one_time_pay_override":
            return _propose_one_time_override(emp, new_val, run, session)
        return _propose_bonus_line(emp, new_val, run, session)
    return ProposedAction(action_type="none")


def _propose_rate_change(
    emp: dict[str, Any], new_rate: Decimal, session: Any, entity_id: Any,
) -> ProposedAction:
    old_rate = Decimal(str(emp["hourly_rate"] or "0"))
    avg_hours = _avg_recent_biweekly_hours(
        session, entity_id=entity_id, employee_id=emp["id"]
    )
    sample_before = (old_rate * avg_hours).quantize(Decimal("0.01"))
    sample_after = (new_rate * avg_hours).quantize(Decimal("0.01"))
    net_before = _estimate_net_pay(sample_before)
    net_after = _estimate_net_pay(sample_after)
    delta = sample_after - sample_before
    annual_impact = (delta * Decimal("26")).quantize(Decimal("0.01"))
    cpp_delta = (delta * _CPP_RATE_2026).quantize(Decimal("0.01"))
    ei_delta = (delta * _EI_RATE_2026).quantize(Decimal("0.01"))
    fed_delta = (delta * (_FED_MARGINAL_TAX_ESTIMATE + _PROV_ON_MARGINAL_TAX_ESTIMATE)).quantize(Decimal("0.01"))
    return ProposedAction(
        action_type="update_employee_rate",
        employee_id=str(emp["id"]),
        new_value=float(new_rate),
        payroll_preview={
            "employee_id": str(emp["id"]),
            "employee_name": emp["full_name"],
            "change_type": "hourly_rate",
            "before": {
                "hourly_rate": float(old_rate),
                "sample_biweekly_gross": float(sample_before),
                "net_est": float(net_before),
            },
            "after": {
                "hourly_rate": float(new_rate),
                "sample_biweekly_gross": float(sample_after),
                "net_est": float(net_after),
            },
            "cpp_delta_per_period": float(cpp_delta),
            "ei_delta_per_period": float(ei_delta),
            "fed_tax_delta_per_period": float(fed_delta),
            "annual_impact": float(annual_impact),
            "note": (
                f"Based on avg {avg_hours} hrs/period over the last 3 runs. "
                "Net pay is an estimate using 2026 CRA rates."
            ),
        },
    )


def _propose_salary_change(
    emp: dict[str, Any], new_annual: Decimal, session: Any, entity_id: Any,
) -> ProposedAction:
    # Caller speaks in annual dollars; storage is bi-weekly.
    new_biweekly = (new_annual / Decimal("26")).quantize(Decimal("0.01"))
    old_biweekly = Decimal(str(emp["biweekly_salary"] or "0"))
    old_annual = (old_biweekly * Decimal("26")).quantize(Decimal("0.01"))
    net_before = _estimate_net_pay(old_biweekly)
    net_after = _estimate_net_pay(new_biweekly)
    delta = new_biweekly - old_biweekly
    annual_impact = (new_annual - old_annual).quantize(Decimal("0.01"))
    return ProposedAction(
        action_type="update_employee_salary",
        employee_id=str(emp["id"]),
        new_value=float(new_biweekly),  # stored as biweekly
        payroll_preview={
            "employee_id": str(emp["id"]),
            "employee_name": emp["full_name"],
            "change_type": "biweekly_salary",
            "before": {
                "biweekly_salary": float(old_biweekly),
                "sample_biweekly_gross": float(old_biweekly),
                "net_est": float(net_before),
            },
            "after": {
                "biweekly_salary": float(new_biweekly),
                "sample_biweekly_gross": float(new_biweekly),
                "net_est": float(net_after),
            },
            "cpp_delta_per_period": float(
                (delta * _CPP_RATE_2026).quantize(Decimal("0.01"))
            ),
            "ei_delta_per_period": float(
                (delta * _EI_RATE_2026).quantize(Decimal("0.01"))
            ),
            "fed_tax_delta_per_period": float(
                (delta * (_FED_MARGINAL_TAX_ESTIMATE + _PROV_ON_MARGINAL_TAX_ESTIMATE)).quantize(Decimal("0.01"))
            ),
            "annual_impact": float(annual_impact),
            "note": (
                f"Annual goes ${old_annual:,.2f} → ${new_annual:,.2f}. "
                "Stored as bi-weekly = annual / 26."
            ),
        },
    )


def _propose_one_time_override(
    emp: dict[str, Any], new_gross: Decimal, run: dict[str, Any], session: Any,
) -> ProposedAction:
    line = session.execute(
        text(
            """
            SELECT id, gross_pay, net_pay FROM payroll_run_lines
             WHERE payroll_run_id = :rid AND employee_id = :emp
             LIMIT 1
            """
        ),
        {"rid": run["id"], "emp": emp["id"]},
    ).mappings().first()
    old_gross = Decimal(str(line["gross_pay"] or "0")) if line else Decimal("0")
    old_net = Decimal(str(line["net_pay"] or "0")) if line else Decimal("0")
    new_net = _estimate_net_pay(new_gross)
    period_label = (
        f"{run['period_start']} – {run['period_end']}"
        if run.get("period_start") and run.get("period_end")
        else run.get("pay_run_number") or "(current run)"
    )
    return ProposedAction(
        action_type="one_time_pay_override",
        employee_id=str(emp["id"]),
        payroll_run_id=str(run["id"]),
        payroll_run_line_id=str(line["id"]) if line else None,
        new_value=float(new_gross),
        payroll_preview={
            "employee_id": str(emp["id"]),
            "employee_name": emp["full_name"],
            "payroll_run_id": str(run["id"]),
            "run_period_label": period_label,
            "change_type": "one_time_override",
            "before": {
                "sample_biweekly_gross": float(old_gross),
                "net_est": float(old_net) if old_net > 0 else float(_estimate_net_pay(old_gross)),
            },
            "after": {
                "sample_biweekly_gross": float(new_gross),
                "net_est": float(new_net),
            },
            "note": (
                "One-time only — permanent rate/salary unchanged. "
                "Tax recalc happens on the next /build-journal."
            ),
        },
    )


def _propose_bonus_line(
    emp: dict[str, Any], bonus: Decimal, run: dict[str, Any], session: Any,
) -> ProposedAction:
    line = session.execute(
        text(
            """
            SELECT id, gross_pay, net_pay FROM payroll_run_lines
             WHERE payroll_run_id = :rid AND employee_id = :emp
             LIMIT 1
            """
        ),
        {"rid": run["id"], "emp": emp["id"]},
    ).mappings().first()
    old_gross = Decimal(str(line["gross_pay"] or "0")) if line else Decimal("0")
    new_gross = (old_gross + bonus).quantize(Decimal("0.01"))
    # Bonus tax: federal-source-deduction bonus method approximates
    # marginal rate; we use the same combined ON+Fed for simplicity.
    bonus_tax = (
        bonus
        * (_CPP_RATE_2026 + _EI_RATE_2026 + _FED_MARGINAL_TAX_ESTIMATE + _PROV_ON_MARGINAL_TAX_ESTIMATE)
    ).quantize(Decimal("0.01"))
    bonus_net = (bonus - bonus_tax).quantize(Decimal("0.01"))
    period_label = (
        f"{run['period_start']} – {run['period_end']}"
        if run.get("period_start") and run.get("period_end")
        else run.get("pay_run_number") or "(current run)"
    )
    return ProposedAction(
        action_type="add_bonus_line",
        employee_id=str(emp["id"]),
        payroll_run_id=str(run["id"]),
        payroll_run_line_id=str(line["id"]) if line else None,
        new_value=float(bonus),
        payroll_preview={
            "employee_id": str(emp["id"]),
            "employee_name": emp["full_name"],
            "payroll_run_id": str(run["id"]),
            "run_period_label": period_label,
            "change_type": "bonus_line",
            "before": {"sample_biweekly_gross": float(old_gross)},
            "after": {
                "sample_biweekly_gross": float(new_gross),
                "net_est": float(_estimate_net_pay(new_gross)),
            },
            "annual_impact": float(bonus_net),
            "note": (
                f"Bonus ${bonus} → est. take-home ${bonus_net} after CPP/EI/tax. "
                "Tax recalc happens on the next /build-journal."
            ),
        },
    )


def _fallback_reply(
    intent: IntentResult,
    matches: list[TransactionMatch],
    action: ProposedAction,
    context: dict[str, Any],
) -> str:
    """Deterministic reply used when Claude is unavailable or errored."""
    if intent.intent == "query_balance":
        # We can't compute a real balance without an additional query;
        # the route layer will catch query_balance and inject the
        # cash-balancing latest value.
        return "Pulling the latest cash position now…"
    if intent.intent == "query_period":
        cp = context.get("current_period")
        if not cp:
            return "There's no accounting period started yet. Create one from Month-end → Start period."
        return (
            f"{cp.get('period_label')} is {cp.get('status')}. "
            f"{cp.get('unclassified_transactions_count', 0)} transactions still need classifying, "
            f"{cp.get('unmatched_invoices_count', 0)} invoices are unmatched."
        )
    if intent.intent == "classify_transaction":
        if action.action_type == "post_to_pending":
            return (
                "I couldn't find a matching bank transaction. I'll hold this note "
                "for 7 days and apply it when the bank entry arrives."
            )
        if action.journal_preview:
            jp = action.journal_preview
            return (
                f"Proposed:\n"
                f"  Dr {jp['debit_account_name'] or jp['debit_account_code']}    "
                f"${jp['amount']:,.2f}\n"
                f"  Cr {jp['credit_account_name'] or jp['credit_account_code']}    "
                f"${jp['amount']:,.2f}\n\n"
                f"Confirm?"
            )
    return "I'm here. Tell me about a transaction or ask me anything about the books."


# --------------------------------------------------------------------------
# 2E. Action executor
# --------------------------------------------------------------------------


def execute_action(
    session,
    *,
    entity_code: str,
    action_type: str,
    transaction_id: str | None = None,
    debit_account: str | None = None,
    credit_account: str | None = None,
    note: str | None = None,
    amount: Decimal | None = None,
    target_date: DateType | None = None,
    clerk_user_id: str | None = None,
    original_message: str | None = None,
    conversation_id: str | None = None,
    # Payroll-action params (used only when action_type ∈ _PAYROLL_INTENTS)
    employee_id: str | None = None,
    payroll_run_id: str | None = None,
    payroll_run_line_id: str | None = None,
    new_value: Any = None,
) -> dict[str, Any]:
    """Apply a confirmed proposed_action. Returns a summary dict the
    route layer can echo back to the frontend. Never raises through;
    on failure returns {"ok": false, "error": ...}."""
    try:
        if action_type == "classify_transaction":
            return _execute_classify(
                session,
                entity_code=entity_code,
                transaction_id=transaction_id,
                debit_account=debit_account,
                credit_account=credit_account,
                note=note,
                clerk_user_id=clerk_user_id,
            )
        if action_type == "add_note":
            return _execute_add_note(
                session,
                entity_code=entity_code,
                transaction_id=transaction_id,
                note=note,
                clerk_user_id=clerk_user_id,
            )
        if action_type == "post_to_pending":
            return _execute_post_pending(
                session,
                entity_code=entity_code,
                conversation_id=conversation_id,
                original_message=original_message or "",
                amount=amount,
                target_date=target_date,
                description=note,
                suggested_account=debit_account or credit_account,
            )
        if action_type in _PAYROLL_INTENTS:
            return _execute_payroll_action(
                session,
                entity_code=entity_code,
                action_type=action_type,
                employee_id=employee_id,
                payroll_run_id=payroll_run_id,
                payroll_run_line_id=payroll_run_line_id,
                new_value=new_value,
                clerk_user_id=clerk_user_id,
            )
        return {"ok": False, "error": f"unknown action_type {action_type!r}"}
    except Exception:
        logger.exception("execute_action failed for action_type=%s", action_type)
        return {"ok": False, "error": "execute_action raised — see server logs"}


def _execute_classify(
    session, *, entity_code, transaction_id, debit_account, credit_account,
    note, clerk_user_id,
) -> dict[str, Any]:
    if not transaction_id:
        return {"ok": False, "error": "transaction_id required for classify"}

    # Persist review-status flip on the bank transaction. We DON'T create
    # a full journal_batch here — the existing bank_auto_journal flow
    # owns that. Recording the user's chosen account_code on the txn
    # plus a vendor_classification_memory hint is enough for the next
    # auto-journal run to pick it up.
    target_account = debit_account or credit_account
    if not target_account:
        return {"ok": False, "error": "no account code chosen"}

    session.execute(
        text(
            """
            UPDATE bank_transactions
               SET review_status = 'reviewed',
                   review_note = COALESCE(:note, review_note),
                   reviewed_by = :who,
                   reviewed_at = NOW(),
                   last_reviewed_at = NOW()
             WHERE id = :tid
               AND entity_id = (SELECT id FROM entities WHERE entity_code = :ec)
            """
        ),
        {
            "tid": transaction_id,
            "ec": entity_code,
            "note": note,
            "who": clerk_user_id or "assistant",
        },
    )

    # Add to vendor_classification_memory so the classifier learns.
    txn = session.execute(
        text(
            """
            SELECT description, direction, normalized_description, counterparty_name
              FROM bank_transactions
             WHERE id = :tid
             LIMIT 1
            """
        ),
        {"tid": transaction_id},
    ).mappings().first()
    if txn:
        vendor_key = (txn.get("counterparty_name") or txn.get("normalized_description")
                      or (txn["description"] or "")[:80]).strip().upper()
        if vendor_key:
            dr_cr = "debit" if (txn["direction"] or "") == "outflow" else "credit"
            entity = session.execute(
                text("SELECT id FROM entities WHERE entity_code = :ec"),
                {"ec": entity_code},
            ).mappings().first()
            if entity:
                # vendor_classification_memory schema notes:
                #   * confidence_score is numeric(4,3) — 0.000 to ~9.999.
                #     Production data uses a 0-1 scale; user-confirmed
                #     classifications go in at the max.
                #   * source CHECK allows
                #     ('gl_history','user_confirmed','ai_seeded').
                #   * Unique constraint is the three-column
                #     (entity_id, normalized_vendor_key, account_code).
                #   * No `created_at` column — first_seen_at is
                #     auto-defaulted by the column.
                session.execute(
                    text(
                        """
                        INSERT INTO vendor_classification_memory (
                            entity_id, normalized_vendor_key, account_code,
                            debit_or_credit, occurrences_count, confidence_score,
                            source, last_seen_at
                        ) VALUES (
                            :eid, :key, :acct, :dr_cr, 1, 1.0,
                            'user_confirmed', NOW()
                        )
                        ON CONFLICT (entity_id, normalized_vendor_key, account_code) DO UPDATE
                           SET debit_or_credit = EXCLUDED.debit_or_credit,
                               occurrences_count = vendor_classification_memory.occurrences_count + 1,
                               confidence_score = LEAST(
                                   1,
                                   vendor_classification_memory.confidence_score + 0.05
                               ),
                               source = 'user_confirmed',
                               last_seen_at = NOW()
                        """
                    ),
                    {
                        "eid": entity["id"],
                        "key": vendor_key,
                        "acct": target_account,
                        "dr_cr": dr_cr,
                    },
                )

    return {
        "ok": True,
        "action": "classify_transaction",
        "transaction_id": transaction_id,
        "account_code": target_account,
    }


def _execute_add_note(
    session, *, entity_code, transaction_id, note, clerk_user_id,
) -> dict[str, Any]:
    if not transaction_id or not note:
        return {"ok": False, "error": "transaction_id + note required"}
    session.execute(
        text(
            """
            UPDATE bank_transactions
               SET review_note = COALESCE(review_note || E'\\n', '') || :note,
                   reviewed_by = :who,
                   last_reviewed_at = NOW()
             WHERE id = :tid
               AND entity_id = (SELECT id FROM entities WHERE entity_code = :ec)
            """
        ),
        {
            "tid": transaction_id,
            "ec": entity_code,
            "note": note,
            "who": clerk_user_id or "assistant",
        },
    )
    return {"ok": True, "action": "add_note", "transaction_id": transaction_id}


def _execute_post_pending(
    session, *, entity_code, conversation_id, original_message, amount,
    target_date, description, suggested_account,
) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            INSERT INTO assistant_pending_intents (
                entity_code, conversation_id, original_message,
                parsed_amount, parsed_date, parsed_description,
                suggested_account_code, status
            ) VALUES (
                :ec, :cid, :msg, :amt, :pdate, :desc, :acct, 'pending'
            )
            RETURNING id
            """
        ),
        {
            "ec": entity_code,
            "cid": conversation_id,
            "msg": original_message,
            "amt": amount,
            "pdate": target_date,
            "desc": description,
            "acct": suggested_account,
        },
    ).mappings().first()
    return {
        "ok": True,
        "action": "post_to_pending",
        "pending_intent_id": str(row["id"]) if row else None,
    }


# --------------------------------------------------------------------------
# Payroll executors
#
# Every executor:
#   1. Scopes the lookup by entity_id (we re-resolve from entity_code
#      every time — no trusting the proposal_json blindly).
#   2. Refuses to mutate when the affected pay run sits in a
#      closed_locked / approved_to_close accounting period.
#   3. Returns a JSON-safe dict. Errors are caught and reported as
#      {"ok": false, ...} — the outer try/except in execute_action
#      provides one more safety net.
#   4. Writes a learning entry capturing the old → new transition.
# --------------------------------------------------------------------------


def _payroll_run_is_locked(session, *, payroll_run_id: str) -> bool:
    row = session.execute(
        text(
            """
            SELECT ap.status FROM payroll_runs pr
            LEFT JOIN accounting_periods ap ON ap.id = pr.accounting_period_id
             WHERE pr.id = :rid
            """
        ),
        {"rid": payroll_run_id},
    ).mappings().first()
    if not row:
        return False
    return (row.get("status") or "") in ("closed_locked", "approved_to_close")


def _execute_payroll_action(
    session,
    *,
    entity_code: str,
    action_type: str,
    employee_id: str | None,
    payroll_run_id: str | None,
    payroll_run_line_id: str | None,
    new_value: Any,
    clerk_user_id: str | None,
) -> dict[str, Any]:
    if not employee_id or new_value is None:
        return {"ok": False, "error": "employee_id and new_value are required"}

    # Verify employee belongs to this entity — cross-entity guard.
    entity_row = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity_row:
        return {"ok": False, "error": f"unknown entity {entity_code!r}"}
    emp = session.execute(
        text(
            "SELECT id, entity_id, full_name, hourly_rate, biweekly_salary "
            "  FROM payroll_employees WHERE id = :id AND entity_id = :eid"
        ),
        {"id": employee_id, "eid": entity_row["id"]},
    ).mappings().first()
    if not emp:
        return {
            "ok": False,
            "error": "employee not found for this entity (cross-entity refused)",
        }

    new_val = Decimal(str(new_value))

    if action_type == "update_employee_rate":
        return _execute_update_rate(session, emp=emp, new_rate=new_val,
                                    clerk_user_id=clerk_user_id,
                                    entity_code=entity_code)
    if action_type == "update_employee_salary":
        return _execute_update_salary(session, emp=emp, new_biweekly=new_val,
                                      clerk_user_id=clerk_user_id,
                                      entity_code=entity_code)
    if action_type == "one_time_pay_override":
        if not payroll_run_id:
            return {"ok": False, "error": "payroll_run_id required"}
        if _payroll_run_is_locked(session, payroll_run_id=payroll_run_id):
            return {"ok": False, "error": "pay run sits in a locked period"}
        return _execute_one_time_override(
            session,
            emp=emp,
            payroll_run_id=payroll_run_id,
            payroll_run_line_id=payroll_run_line_id,
            new_gross=new_val,
            clerk_user_id=clerk_user_id,
            entity_code=entity_code,
        )
    if action_type == "add_bonus_line":
        if not payroll_run_id:
            return {"ok": False, "error": "payroll_run_id required"}
        if _payroll_run_is_locked(session, payroll_run_id=payroll_run_id):
            return {"ok": False, "error": "pay run sits in a locked period"}
        return _execute_add_bonus(
            session,
            emp=emp,
            payroll_run_id=payroll_run_id,
            payroll_run_line_id=payroll_run_line_id,
            bonus_amount=new_val,
            clerk_user_id=clerk_user_id,
            entity_code=entity_code,
        )
    return {"ok": False, "error": f"unknown payroll action_type {action_type!r}"}


def _learn_payroll_change(
    session, *, entity_code: str, key: str, summary: str
) -> None:
    """Single-place learning sink for payroll edits. Tolerant —
    failures here are non-fatal."""
    try:
        session.execute(
            text(
                """
                INSERT INTO assistant_entity_memory (
                    id, entity_code, memory_type, memory_key, memory_value,
                    confidence, times_confirmed, times_corrected,
                    last_seen_at, created_at
                ) VALUES (
                    gen_random_uuid(), :ec, 'correction', :k, :v,
                    100, 1, 0, NOW(), NOW()
                )
                ON CONFLICT (entity_code, memory_type, memory_key) DO UPDATE
                   SET memory_value = EXCLUDED.memory_value,
                       times_confirmed = assistant_entity_memory.times_confirmed + 1,
                       last_seen_at = NOW()
                """
            ),
            {"ec": entity_code, "k": key, "v": summary},
        )
    except Exception:
        logger.warning("payroll learning sink failed", exc_info=True)


def _execute_update_rate(
    session, *, emp, new_rate, clerk_user_id, entity_code,
) -> dict[str, Any]:
    old = Decimal(str(emp["hourly_rate"] or "0"))
    session.execute(
        text(
            """
            UPDATE payroll_employees
               SET hourly_rate = :rate, updated_at = NOW()
             WHERE id = :id AND entity_id = :eid
            """
        ),
        {"rate": new_rate, "id": emp["id"], "eid": emp["entity_id"]},
    )
    _learn_payroll_change(
        session,
        entity_code=entity_code,
        key=f"employee_rate_change:{emp['full_name']}",
        summary=f"{emp['full_name']}: ${old} → ${new_rate} hourly (by {clerk_user_id or 'assistant'})",
    )
    return {
        "ok": True,
        "action": "update_employee_rate",
        "employee_id": str(emp["id"]),
        "employee_name": emp["full_name"],
        "old_value": float(old),
        "new_value": float(new_rate),
    }


def _execute_update_salary(
    session, *, emp, new_biweekly, clerk_user_id, entity_code,
) -> dict[str, Any]:
    old = Decimal(str(emp["biweekly_salary"] or "0"))
    session.execute(
        text(
            """
            UPDATE payroll_employees
               SET biweekly_salary = :sal, updated_at = NOW()
             WHERE id = :id AND entity_id = :eid
            """
        ),
        {"sal": new_biweekly, "id": emp["id"], "eid": emp["entity_id"]},
    )
    _learn_payroll_change(
        session,
        entity_code=entity_code,
        key=f"employee_salary_change:{emp['full_name']}",
        summary=(
            f"{emp['full_name']}: ${old} → ${new_biweekly} bi-weekly "
            f"(annual ${(new_biweekly * Decimal('26')).quantize(Decimal('0.01'))}) "
            f"(by {clerk_user_id or 'assistant'})"
        ),
    )
    return {
        "ok": True,
        "action": "update_employee_salary",
        "employee_id": str(emp["id"]),
        "employee_name": emp["full_name"],
        "old_value": float(old),
        "new_value": float(new_biweekly),
    }


def _execute_one_time_override(
    session, *, emp, payroll_run_id, payroll_run_line_id, new_gross,
    clerk_user_id, entity_code,
) -> dict[str, Any]:
    """Set the gross_pay on the existing payroll_run_lines row and
    flag the run for recalc. Doesn't recompute tax — that's the next
    /build-journal's job (the bookkeeper triggers it explicitly)."""
    if not payroll_run_line_id:
        # The line may not exist yet (employee wasn't on the run as
        # built). For now refuse — the bookkeeper should add the
        # employee to the run first.
        return {
            "ok": False,
            "error": "no payroll_run_lines row for this employee on this run",
        }
    res = session.execute(
        text(
            """
            UPDATE payroll_run_lines
               SET gross_pay = :gross,
                   notes = COALESCE(notes, '') ||
                           CASE WHEN COALESCE(notes,'') = '' THEN '' ELSE ' | ' END ||
                           :tag
             WHERE id = :lid AND payroll_run_id = :rid
            """
        ),
        {
            "gross": new_gross,
            "tag": f"manual_override by assistant ({clerk_user_id or 'unknown'}): ${new_gross}",
            "lid": payroll_run_line_id,
            "rid": payroll_run_id,
        },
    )
    if res.rowcount == 0:
        return {"ok": False, "error": "payroll_run_lines row not found"}
    # Flag the run for recalc via summary_json.
    session.execute(
        text(
            """
            UPDATE payroll_runs
               SET summary_json = COALESCE(summary_json, '{}'::jsonb) ||
                                  '{"pending_recalc": true}'::jsonb,
                   updated_at = NOW()
             WHERE id = :rid
            """
        ),
        {"rid": payroll_run_id},
    )
    _learn_payroll_change(
        session,
        entity_code=entity_code,
        key=f"one_time_pay_override:{emp['full_name']}:{payroll_run_id}",
        summary=f"{emp['full_name']}: gross_pay override to ${new_gross} on run {payroll_run_id}",
    )
    return {
        "ok": True,
        "action": "one_time_pay_override",
        "employee_id": str(emp["id"]),
        "employee_name": emp["full_name"],
        "payroll_run_id": payroll_run_id,
        "payroll_run_line_id": payroll_run_line_id,
        "new_gross": float(new_gross),
        "pending_recalc": True,
    }


def _execute_add_bonus(
    session, *, emp, payroll_run_id, payroll_run_line_id, bonus_amount,
    clerk_user_id, entity_code,
) -> dict[str, Any]:
    """Add a bonus to the line's gross_pay. No separate "bonus" column
    exists on payroll_run_lines today — we fold the bonus into gross
    and tag the notes field so the bookkeeper can audit. Recalc flag
    set on the run."""
    if not payroll_run_line_id:
        return {
            "ok": False,
            "error": "no payroll_run_lines row for this employee on this run",
        }
    res = session.execute(
        text(
            """
            UPDATE payroll_run_lines
               SET gross_pay = gross_pay + :bonus,
                   notes = COALESCE(notes, '') ||
                           CASE WHEN COALESCE(notes,'') = '' THEN '' ELSE ' | ' END ||
                           :tag
             WHERE id = :lid AND payroll_run_id = :rid
            """
        ),
        {
            "bonus": bonus_amount,
            "tag": f"bonus by assistant ({clerk_user_id or 'unknown'}): +${bonus_amount}",
            "lid": payroll_run_line_id,
            "rid": payroll_run_id,
        },
    )
    if res.rowcount == 0:
        return {"ok": False, "error": "payroll_run_lines row not found"}
    session.execute(
        text(
            """
            UPDATE payroll_runs
               SET summary_json = COALESCE(summary_json, '{}'::jsonb) ||
                                  '{"pending_recalc": true}'::jsonb,
                   updated_at = NOW()
             WHERE id = :rid
            """
        ),
        {"rid": payroll_run_id},
    )
    _learn_payroll_change(
        session,
        entity_code=entity_code,
        key=f"add_bonus_line:{emp['full_name']}:{payroll_run_id}",
        summary=f"{emp['full_name']}: +${bonus_amount} bonus on run {payroll_run_id}",
    )
    return {
        "ok": True,
        "action": "add_bonus_line",
        "employee_id": str(emp["id"]),
        "employee_name": emp["full_name"],
        "payroll_run_id": payroll_run_id,
        "payroll_run_line_id": payroll_run_line_id,
        "bonus_amount": float(bonus_amount),
        "pending_recalc": True,
    }


# --------------------------------------------------------------------------
# 2F. Learning engine
# --------------------------------------------------------------------------


def learn_from_interaction(
    session,
    *,
    entity_code: str,
    message: str,
    intent: IntentResult,
    action_taken: str | None,
    was_corrected: bool,
    correction_details: str | None = None,
) -> None:
    """Upsert entity memory after every confirm / correct round. Never
    raises — wrapped in try/except by the caller."""
    # Always log the interaction as a 'terminology' clue when an account
    # code was chosen.
    chosen_account = intent.suggested_debit_account or intent.suggested_credit_account
    if chosen_account:
        memory_key = (intent.description or message)[:120].lower().strip()
        if memory_key:
            _upsert_memory(
                session,
                entity_code=entity_code,
                memory_type="terminology",
                memory_key=memory_key,
                memory_value=chosen_account,
                confirmed=not was_corrected,
            )
    if was_corrected and correction_details:
        _upsert_memory(
            session,
            entity_code=entity_code,
            memory_type="correction",
            memory_key=(intent.description or message)[:120].lower().strip()
            or "correction",
            memory_value=correction_details[:500],
            confirmed=True,
        )


def _upsert_memory(
    session,
    *,
    entity_code: str,
    memory_type: str,
    memory_key: str,
    memory_value: str,
    confirmed: bool,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO assistant_entity_memory (
                entity_code, memory_type, memory_key, memory_value,
                confidence, times_confirmed, times_corrected, last_seen_at
            ) VALUES (
                :ec, :mt, :mk, :mv, 100, :tc, :tcc, NOW()
            )
            ON CONFLICT (entity_code, memory_type, memory_key) DO UPDATE
               SET memory_value = EXCLUDED.memory_value,
                   confidence = LEAST(100,
                       GREATEST(0,
                           assistant_entity_memory.confidence
                           + CASE WHEN :confirmed THEN 5 ELSE -10 END
                       )),
                   times_confirmed = assistant_entity_memory.times_confirmed
                       + CASE WHEN :confirmed THEN 1 ELSE 0 END,
                   times_corrected = assistant_entity_memory.times_corrected
                       + CASE WHEN :confirmed THEN 0 ELSE 1 END,
                   last_seen_at = NOW()
            """
        ),
        {
            "ec": entity_code,
            "mt": memory_type,
            "mk": memory_key[:200],
            "mv": memory_value[:500],
            "tc": 1 if confirmed else 0,
            "tcc": 0 if confirmed else 1,
            "confirmed": confirmed,
        },
    )


# --------------------------------------------------------------------------
# Special-case responses for query intents
# --------------------------------------------------------------------------


# --------------------------------------------------------------------------
# 2G. Period-close learning hook
# --------------------------------------------------------------------------


def learn_from_period_close(
    session, *, entity_code: str, period_end: str | DateType
) -> dict[str, Any]:
    """Called from POST /api/period-close/approve right after a period
    flips to closed. Writes a clutch of `assistant_period_observations`
    rows so the conversational assistant can reason about close cadence,
    variances, and recurring journals.

    Failure-isolated: caller wraps in try/except + logger.error so a
    learning hiccup never rolls back the period close.
    """
    if isinstance(period_end, str):
        try:
            period_end_d = DateTimeType.strptime(period_end, "%Y-%m-%d").date()
        except ValueError:
            return {"observations_created": 0, "skipped": "bad date"}
    else:
        period_end_d = period_end

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return {"observations_created": 0, "skipped": "unknown entity"}

    period = session.execute(
        text(
            """
            SELECT id, period_label, period_start, closed_at
              FROM accounting_periods
             WHERE entity_id = :eid AND period_end = :pe
             LIMIT 1
            """
        ),
        {"eid": entity["id"], "pe": period_end_d},
    ).mappings().first()
    if not period:
        return {"observations_created": 0, "skipped": "period not found"}

    observations_created = 0

    def _obs(observation_type: str, observation: str, severity: str = "info",
             account_code: str | None = None, amount: Decimal | None = None) -> None:
        nonlocal observations_created
        try:
            session.execute(
                text(
                    """
                    INSERT INTO assistant_period_observations (
                        entity_code, period_end, observation_type,
                        observation, account_code, amount, severity
                    ) VALUES (
                        :ec, :pe, :ot, :ob, :ac, :am, :sv
                    )
                    """
                ),
                {
                    "ec": entity_code,
                    "pe": period_end_d,
                    "ot": observation_type,
                    "ob": observation[:1000],
                    "ac": account_code,
                    "am": amount,
                    "sv": severity,
                },
            )
            observations_created += 1
        except Exception:
            logger.exception("learn_from_period_close obs insert failed")

    # 1. Close duration — how many days from period_end to closed_at.
    if period["closed_at"]:
        try:
            duration_days = (period["closed_at"].date() - period_end_d).days
            _obs(
                "close_duration_days",
                f"Closed {period['period_label']} {duration_days} days after period end",
                severity="info",
                amount=Decimal(str(duration_days)),
            )
        except Exception:
            pass

    # 2. Journal counts by source_module.
    counts = session.execute(
        text(
            """
            SELECT source_module, COUNT(*) AS c,
                   COALESCE(SUM(total_debits), 0) AS total_dr
              FROM journal_batches
             WHERE entity_id = :eid
               AND accounting_period_id = :pid
               AND status NOT IN ('voided', 'rejected')
             GROUP BY source_module
             ORDER BY c DESC
            """
        ),
        {"eid": entity["id"], "pid": period["id"]},
    ).mappings().all()
    for row in counts:
        _obs(
            "journal_created",
            f"{row['c']} {row['source_module']} journal{'s' if row['c'] != 1 else ''}",
            account_code=None,
            amount=Decimal(str(row["total_dr"])),
        )

    # 3. Unusual amounts — any single batch over $500k stands out.
    big = session.execute(
        text(
            """
            SELECT source_module, batch_label, total_debits
              FROM journal_batches
             WHERE entity_id = :eid
               AND accounting_period_id = :pid
               AND total_debits > 500000
             ORDER BY total_debits DESC LIMIT 5
            """
        ),
        {"eid": entity["id"], "pid": period["id"]},
    ).mappings().all()
    for row in big:
        _obs(
            "unusual_amount",
            f"{row['source_module']} batch {row['batch_label']!r} "
            f"= ${float(row['total_debits']):,.0f}",
            severity="anomaly",
            amount=Decimal(str(row["total_debits"])),
        )

    # 4. New vendors — vendor_classification_memory rows last_seen during
    #    this period (heuristic: created_at within period_start..period_end).
    new_vendors = session.execute(
        text(
            """
            SELECT normalized_vendor_key, account_code
              FROM vendor_classification_memory
             WHERE entity_id = :eid
               AND first_seen_at >= :ps
               AND first_seen_at <= CAST(:pe AS date) + INTERVAL '1 day'
             ORDER BY first_seen_at DESC LIMIT 5
            """
        ),
        {"eid": entity["id"], "ps": period["period_start"], "pe": period_end_d},
    ).mappings().all()
    if new_vendors:
        _obs(
            "new_vendor",
            f"{len(new_vendors)} new vendor pattern{'s' if len(new_vendors) != 1 else ''} learned this period",
        )

    return {
        "observations_created": observations_created,
        "period_label": period["period_label"],
    }


# --------------------------------------------------------------------------
# 2H. Pending-intent matcher (runs after bank-statement upload)
# --------------------------------------------------------------------------


def check_pending_intents(session, entity_code: str) -> dict[str, Any]:
    """Iterate over `pending` assistant_pending_intents and try to match
    each against bank_transactions that have arrived since the intent
    was recorded. On a confident match, flip the intent status to
    'matched' and stamp the matched_transaction_id.

    Heuristic: same amount ± $0.50, transaction_date within ±5 days of
    parsed_date. The assistant later picks up these matched intents
    and finishes the classification.
    """
    pending = session.execute(
        text(
            """
            SELECT id, parsed_amount, parsed_date, parsed_description
              FROM assistant_pending_intents
             WHERE entity_code = :ec
               AND status = 'pending'
               AND expires_at > NOW()
               AND parsed_amount IS NOT NULL
             ORDER BY created_at DESC
             LIMIT 50
            """
        ),
        {"ec": entity_code},
    ).mappings().all()
    if not pending:
        return {"checked": 0, "matched": 0}

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return {"checked": 0, "matched": 0}

    matched = 0
    for intent in pending:
        amt = abs(Decimal(str(intent["parsed_amount"] or 0)))
        if amt <= 0:
            continue
        params: dict[str, Any] = {
            "eid": entity["id"],
            "lo": amt - Decimal("0.50"),
            "hi": amt + Decimal("0.50"),
        }
        where_date = ""
        if intent["parsed_date"]:
            params["d_lo"] = intent["parsed_date"] - timedelta(days=5)
            params["d_hi"] = intent["parsed_date"] + timedelta(days=5)
            where_date = "AND transaction_date BETWEEN :d_lo AND :d_hi"
        candidate = session.execute(
            text(
                f"""
                SELECT id FROM bank_transactions
                 WHERE entity_id = :eid
                   AND ABS(amount) BETWEEN :lo AND :hi
                   AND review_status IN ('new', 'needs_review')
                   {where_date}
                 ORDER BY transaction_date DESC
                 LIMIT 1
                """
            ),
            params,
        ).mappings().first()
        if not candidate:
            continue
        session.execute(
            text(
                """
                UPDATE assistant_pending_intents
                   SET status = 'matched',
                       matched_transaction_id = :tid
                 WHERE id = :id
                """
            ),
            {"id": intent["id"], "tid": candidate["id"]},
        )
        matched += 1

    return {"checked": len(pending), "matched": matched}


# --------------------------------------------------------------------------
# 2I. App-improvement insights
# --------------------------------------------------------------------------


def generate_app_improvement_insights(
    session, entity_code: str
) -> list[dict[str, Any]]:
    """Aggregate signals from observations + memory + classification
    quality and surface the top 3-10 things the dealer can act on.
    Each insight has a type, description, severity, optional action.
    """
    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        return []

    out: list[dict[str, Any]] = []

    # 1. Periods overdue.
    overdue = session.execute(
        text(
            """
            SELECT period_label, period_end FROM accounting_periods
             WHERE entity_id = :eid
               AND period_end < CURRENT_DATE - INTERVAL '14 days'
               AND status NOT IN ('closed')
             ORDER BY period_end DESC LIMIT 1
            """
        ),
        {"eid": entity["id"]},
    ).mappings().first()
    if overdue:
        out.append({
            "type": "period_overdue",
            "description": (
                f"{overdue['period_label']} is more than 2 weeks past "
                f"month-end — close it to keep reporting current"
            ),
            "severity": "action",
            "action": "Open month-end",
            "action_url": "/month-end",
        })

    # 2. Unclassified bank transactions ramp.
    unc = session.execute(
        text(
            """
            SELECT COUNT(*) AS c FROM bank_transactions
             WHERE entity_id = :eid
               AND review_status IN ('new', 'needs_review')
            """
        ),
        {"eid": entity["id"]},
    ).mappings().first()
    if int(unc["c"]) > 50:
        out.append({
            "type": "unclassified_backlog",
            "description": (
                f"{unc['c']} bank transactions unclassified — run the "
                f"auto-classifier or review them in /bank"
            ),
            "severity": "warning",
            "action": "Review bank",
            "action_url": "/bank",
        })

    # 3. Vendor memory size — proxy for "assistant is getting smarter".
    vendors = session.execute(
        text(
            """
            SELECT COUNT(*) AS c FROM vendor_classification_memory
             WHERE entity_id = :eid
            """
        ),
        {"eid": entity["id"]},
    ).mappings().first()
    if int(vendors["c"]) > 0:
        out.append({
            "type": "vendor_memory_size",
            "description": (
                f"AI assistant has learned {vendors['c']} vendor pattern"
                f"{'s' if int(vendors['c']) != 1 else ''} for this store"
            ),
            "severity": "info",
        })

    # 4. Pending intents waiting for a bank match.
    pending = session.execute(
        text(
            """
            SELECT COUNT(*) AS c FROM assistant_pending_intents
             WHERE entity_code = :ec AND status = 'pending'
               AND expires_at > NOW()
            """
        ),
        {"ec": entity_code},
    ).mappings().first()
    if int(pending["c"]) > 0:
        out.append({
            "type": "pending_intents",
            "description": (
                f"{pending['c']} note{'s' if int(pending['c']) != 1 else ''} from "
                f"chat waiting for a matching bank transaction"
            ),
            "severity": "info",
            "action": "View chat",
            "action_url": "/dashboard",
        })

    # 5. Recent anomalies from period observations.
    recent_anomalies = session.execute(
        text(
            """
            SELECT observation, period_end FROM assistant_period_observations
             WHERE entity_code = :ec AND severity = 'anomaly'
             ORDER BY period_end DESC, created_at DESC LIMIT 2
            """
        ),
        {"ec": entity_code},
    ).mappings().all()
    for r in recent_anomalies:
        out.append({
            "type": "anomaly",
            "description": f"{r['observation']} (in {r['period_end']})",
            "severity": "warning",
        })

    return out


def get_balance_summary(session, entity_code: str) -> str | None:
    """Used by /api/assistant/message when intent is query_balance — pulls
    the most recent cash_balancing_days row and renders a one-liner.

    Returns the empty-state message when no cash_balancing_days row
    exists yet. Per Q3 decision we do NOT fall back to a bank-transaction
    running sum — too slow and error-prone; the dealer is better served
    by a clear "enable the nightly sync" prompt than a wrong number.
    """
    row = session.execute(
        text(
            """
            SELECT business_date, opening_cash, closing_cash, total_sales
              FROM cash_balancing_days
             WHERE entity_id = (SELECT id FROM entities WHERE entity_code = :ec)
               AND closing_cash IS NOT NULL
             ORDER BY business_date DESC
             LIMIT 1
            """
        ),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        return (
            "No cash balancing data yet. Enable the nightly sync to track cash."
        )
    return (
        f"Your closing cash on {row['business_date']} was "
        f"${float(row['closing_cash']):,.2f}. That's the most recent record I have."
    )
