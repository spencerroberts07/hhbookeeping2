"""AI-fallback adapter — for messy/unknown statements (especially varied bank
PDFs) where the deterministic adapters fail or return low confidence.

Reuses the proven Claude parse pattern (`services_onboarding._claude_parse_json`:
sonnet-4-6, retries, fence-strip, json + regex fallback). For scanned PDFs it
first OCRs via the POS importer's `_ocr_pdf_text`. NEVER silent-empty: if Claude
returns zero transactions on a non-empty statement, that's a warning + zero-conf
so the caller surfaces it rather than importing nothing.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .base import AdapterResult, NormalizedBankTxn, StatementMeta

_SYSTEM = """You extract transactions from a bank statement. Output ONLY valid JSON:
{"account_number": "<or null>", "opening_balance": <number or null>, "closing_balance": <number or null>,
 "period_start": "YYYY-MM-DD or null", "period_end": "YYYY-MM-DD or null",
 "transactions": [{"date": "YYYY-MM-DD", "amount": <signed number, +deposit / -withdrawal>,
                   "description": "<text>", "reference": "<or null>", "balance": <running balance or null>}]}
Rules: amount is SIGNED (deposits/credits positive, withdrawals/debits negative).
Overdraft balances are negative. Do not invent rows. If the document is not a bank statement, return transactions: []."""


def _text_from_bytes(file_bytes: bytes, filename: str) -> str:
    if file_bytes[:5] == b"%PDF-" or (filename or "").lower().endswith(".pdf"):
        try:
            from ..services_bank_pdf import _extract_pdf_pages
            pages = _extract_pdf_pages(file_bytes)
            text = "\n".join(pages)
        except Exception:
            text = ""
        # OCR if the text extraction is thin (scanned statement)
        money_tokens = text.count(".")
        if len(text.strip()) < 200 or money_tokens < 5:
            try:
                from ..services_pos_import import _ocr_pdf_text
                ocr = _ocr_pdf_text(file_bytes)
                if ocr and len(ocr) > len(text):
                    text = ocr
            except Exception:
                pass
        return text
    return file_bytes.decode("utf-8", errors="replace")


def _d(v) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


def _date(v) -> Any:
    if not v:
        return None
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


class AiFallbackBankAdapter:
    source_system = "statement_ai"

    def supports(self, *, filename: str, content_type: str, sniff: bytes) -> bool:
        # Never auto-selected; the dispatcher invokes this only as a fallback.
        return False

    def parse(self, *, file_bytes: bytes, filename: str, entity_code: str,
              hints: dict[str, Any] | None = None) -> AdapterResult:
        from ..services_onboarding import _claude_parse_json

        text = _text_from_bytes(file_bytes, filename)
        if not text.strip():
            return AdapterResult(meta=StatementMeta(source_system=self.source_system),
                                 transactions=[], parser_confidence=0.0,
                                 warnings=["AI fallback: could not extract any text from the file"])
        parsed = _claude_parse_json(_SYSTEM, text)
        if not parsed:
            return AdapterResult(meta=StatementMeta(source_system=self.source_system),
                                 transactions=[], parser_confidence=0.0,
                                 warnings=["AI fallback: Claude returned no parseable JSON"])

        txns: list[NormalizedBankTxn] = []
        for t in parsed.get("transactions", []) or []:
            amt = _d(t.get("amount"))
            d = _date(t.get("date"))
            if amt is None or d is None:
                continue
            txns.append(NormalizedBankTxn(
                transaction_date=d, amount=amt,
                description=str(t.get("description") or ""),
                reference_number=t.get("reference"),
                running_balance=_d(t.get("balance")),
                raw={"ai": True},
            ))
        warnings = []
        if not txns:
            warnings.append("AI fallback: statement had text but Claude extracted zero transactions — review manually")
        meta = StatementMeta(
            source_account_code=hints.get("source_account_code") if hints else None,
            source_account_name=parsed.get("account_number"),
            opening_balance=_d(parsed.get("opening_balance")),
            closing_balance=_d(parsed.get("closing_balance")),
            period_start=_date(parsed.get("period_start")),
            period_end=_date(parsed.get("period_end")),
            statement_date=_date(parsed.get("period_end")),
            source_system=self.source_system,
        )
        return AdapterResult(meta=meta, transactions=txns,
                             parser_confidence=0.85 if txns else 0.0, warnings=warnings)
