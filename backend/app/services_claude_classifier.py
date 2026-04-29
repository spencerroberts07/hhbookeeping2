"""
Claude API classifier for unmatched bank transactions.

Layer 3 of the bank-auto-journal classifier. Called only when:
    1. No hard-coded rule (Layer 1) matches.
    2. No vendor_classification_memory hit (Layer 2) at the
       VENDOR_MEMORY_AUTO_DRAFT_THRESHOLD or above.

Cost control:
    - Model = claude-haiku-4-5-20251001 (cheap, fast; classification
      is easy work).
    - Chart of accounts is sent once via the system prompt with
      cache_control: ephemeral so the cache TTL covers a whole run.
    - Returns 'UNCLASSIFIED' when the model is below
      CLAUDE_MIN_CONFIDENCE; the caller leaves the transaction as
      unmatched in that case.

If ANTHROPIC_API_KEY is not configured, classify_with_claude() returns
None — the auto-journal builder treats that as "no Layer 3 result"
and leaves the transaction as unmatched.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

from .config import settings


logger = logging.getLogger(__name__)


CLAUDE_MODEL_ID = "claude-haiku-4-5-20251001"
CLAUDE_MAX_TOKENS = 600


_SYSTEM_INSTRUCTIONS = """You are a Canadian small-business bookkeeping assistant for a Home Hardware store (Bridlewood Hardware Co. Ltd.).

Given a bank transaction description, decide which GL account from the supplied chart of accounts the transaction should be classified to. The bank account itself (1020 TD Canada Trust) is the OTHER side of the journal entry — your job is to pick the OPPOSITE side.

Rules:
- For an outflow (money leaving the bank), pick a debit account (an expense, asset, or liability-payment account).
- For an inflow (money entering the bank), pick a credit account (revenue, contra-asset, or liability-increase account).
- If the description matches a card-processor settlement (VSA DEP, MC DEP, AMEX 19xxxxxxxx) or a Home Hardware payment (HOME HARDWARE MSP/AP), return UNCLASSIFIED — those are handled by other modules.
- If you are unsure, return UNCLASSIFIED rather than guessing.

Output ONLY valid JSON in this exact shape:
{"account_code": "<4-digit code or UNCLASSIFIED>", "debit_or_credit": "debit" | "credit", "confidence": <0.0 to 1.0>, "reasoning": "<one short sentence>"}
"""


def _build_chart_block(chart_of_accounts: list[dict[str, Any]]) -> str:
    """Format the chart of accounts as a compact lookup the model can scan."""
    lines = []
    for a in chart_of_accounts:
        code = a.get("account_code") or a.get("code")
        name = a.get("account_name") or a.get("name") or ""
        cls = a.get("account_class") or a.get("class") or ""
        if not code:
            continue
        if cls:
            lines.append(f"{code}  {name}  ({cls})")
        else:
            lines.append(f"{code}  {name}")
    return "\n".join(lines)


def _build_user_prompt(
    *,
    description: str,
    amount: Decimal,
    direction: str,
    similar_past: list[dict[str, Any]],
) -> str:
    examples_block = ""
    if similar_past:
        lines = ["Similar past classifications for this entity:"]
        for r in similar_past[:5]:
            ex = ""
            raw = r.get("raw_examples") or []
            if isinstance(raw, list) and raw:
                ex = f' (e.g. "{raw[0][:60]}")'
            lines.append(
                f"  - key={r['normalized_vendor_key']} -> "
                f"{r['account_code']} {r['debit_or_credit']} "
                f"(confidence {r['confidence_score']}, "
                f"seen {r['occurrences_count']}x){ex}"
            )
        examples_block = "\n".join(lines) + "\n\n"

    return (
        f"{examples_block}"
        f"Transaction:\n"
        f'  description = "{description}"\n'
        f"  amount      = {amount}\n"
        f"  direction   = {direction}\n"
        f"\n"
        f"Pick the best account_code from the chart above. "
        f"Output JSON only — no preamble, no code fence."
    )


def is_claude_available() -> bool:
    return bool(getattr(settings, "anthropic_api_key", None))


def classify_with_claude(
    *,
    description: str,
    amount: Decimal,
    direction: str,
    chart_of_accounts: list[dict[str, Any]],
    similar_past: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """
    Returns:
        {"account_code", "debit_or_credit", "confidence", "reasoning",
         "raw_response"}
    or None when the API key isn't configured.

    Raises ValueError on a malformed response that the caller should
    surface as a hard error.
    """
    api_key = getattr(settings, "anthropic_api_key", None)
    if not api_key:
        return None

    try:
        from anthropic import Anthropic  # noqa: WPS433
    except ImportError as exc:
        raise ValueError(
            "anthropic SDK not installed. Add `anthropic` to requirements.txt."
        ) from exc

    client = Anthropic(api_key=api_key)

    chart_block = _build_chart_block(chart_of_accounts)
    user_prompt = _build_user_prompt(
        description=description,
        amount=amount,
        direction=direction,
        similar_past=similar_past or [],
    )

    # Cache the chart-of-accounts block (it's identical for every call in
    # a run). cache_control: ephemeral has a 5-minute TTL — long enough
    # to cover a single auto-journal run.
    system_blocks = [
        {
            "type": "text",
            "text": _SYSTEM_INSTRUCTIONS,
        },
        {
            "type": "text",
            "text": "CHART OF ACCOUNTS (account_code  name  class):\n" + chart_block,
            "cache_control": {"type": "ephemeral"},
        },
    ]

    try:
        msg = client.messages.create(
            model=CLAUDE_MODEL_ID,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=system_blocks,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as exc:
        logger.warning("Claude classify call failed: %s", exc)
        return {
            "account_code": "UNCLASSIFIED",
            "debit_or_credit": "debit" if direction == "outflow" else "credit",
            "confidence": Decimal("0.0"),
            "reasoning": f"Claude API call failed: {exc}",
            "raw_response": {"error": str(exc)},
        }

    # Extract the text content. Anthropic returns a list of content
    # blocks; for our prompt the first text block holds the JSON.
    text_out = ""
    for block in msg.content or []:
        if getattr(block, "type", None) == "text":
            text_out += block.text or ""
    text_out = text_out.strip()

    # Strip code fences if the model added them despite instructions.
    if text_out.startswith("```"):
        text_out = text_out.strip("`")
        # remove leading 'json\n' if present
        if text_out.lower().startswith("json"):
            text_out = text_out[4:].lstrip("\n")

    parsed: dict[str, Any]
    try:
        parsed = json.loads(text_out)
    except json.JSONDecodeError:
        logger.warning("Claude returned non-JSON: %r", text_out[:200])
        return {
            "account_code": "UNCLASSIFIED",
            "debit_or_credit": "debit" if direction == "outflow" else "credit",
            "confidence": Decimal("0.0"),
            "reasoning": "Model returned non-JSON output",
            "raw_response": {"raw_text": text_out[:500]},
        }

    account_code = str(parsed.get("account_code") or "").strip()
    dr_or_cr = (parsed.get("debit_or_credit") or "").strip().lower()
    if dr_or_cr not in ("debit", "credit"):
        dr_or_cr = "debit" if direction == "outflow" else "credit"
    try:
        confidence = Decimal(str(parsed.get("confidence", 0))).quantize(
            Decimal("0.001")
        )
    except Exception:
        confidence = Decimal("0.0")
    reasoning = str(parsed.get("reasoning") or "").strip()[:500]

    return {
        "account_code": account_code or "UNCLASSIFIED",
        "debit_or_credit": dr_or_cr,
        "confidence": confidence,
        "reasoning": reasoning,
        "raw_response": parsed,
        "usage": {
            "input_tokens": getattr(getattr(msg, "usage", None), "input_tokens", None),
            "output_tokens": getattr(getattr(msg, "usage", None), "output_tokens", None),
            "cache_creation_input_tokens": getattr(
                getattr(msg, "usage", None), "cache_creation_input_tokens", None
            ),
            "cache_read_input_tokens": getattr(
                getattr(msg, "usage", None), "cache_read_input_tokens", None
            ),
        },
    }
