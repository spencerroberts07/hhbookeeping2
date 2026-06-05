"""PDF adapter — wraps the existing deterministic TD-statement parser
(`services_bank_pdf.parse_td_statement_pdf`) into the adapter interface.

The closing balance the rec needs = the last row's running balance (negative
when flagged as overdraft); the opening balance = the BALANCE FORWARD row.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..services_bank_pdf import parse_td_statement_pdf
from .base import AdapterResult, NormalizedBankTxn, StatementMeta


def _D(v) -> Decimal | None:
    if v is None or v == "":
        return None
    return Decimal(str(v))


class PdfBankAdapter:
    source_system = "statement_pdf"

    def supports(self, *, filename: str, content_type: str, sniff: bytes) -> bool:
        return (
            (filename or "").lower().endswith(".pdf")
            or "pdf" in (content_type or "").lower()
            or sniff[:5] == b"%PDF-"
        )

    def parse(self, *, file_bytes: bytes, filename: str, entity_code: str,
              hints: dict[str, Any] | None = None) -> AdapterResult:
        parsed = parse_td_statement_pdf(file_bytes)
        txns_in = parsed.get("transactions", []) or []
        warnings = list(parsed.get("warnings", []) or [])

        txns: list[NormalizedBankTxn] = []
        closing: Decimal | None = None
        opening: Decimal | None = None
        for t in txns_in:
            amt = _D(t.get("amount")) or Decimal("0")
            rb = _D(t.get("running_balance"))
            is_od = bool(t.get("running_balance_is_overdraft"))
            signed_rb = (-rb if (rb is not None and is_od) else rb)
            txns.append(NormalizedBankTxn(
                transaction_date=t.get("transaction_date"),
                amount=amt,
                description=str(t.get("description") or ""),
                reference_number=t.get("reference_number"),
                running_balance=signed_rb,
                running_balance_is_overdraft=is_od,
                normalized_description=t.get("normalized_description"),
                raw=t.get("raw") if isinstance(t.get("raw"), dict) else dict(t),
            ))
            # opening = first seen balance minus its own amount; closing = last seen balance
            if signed_rb is not None:
                if opening is None:
                    opening = signed_rb - amt
                closing = signed_rb

        meta = StatementMeta(
            source_account_code=hints.get("source_account_code") if hints else None,
            source_account_name=parsed.get("account_branch"),
            statement_date=parsed.get("period_end"),
            opening_balance=opening,
            closing_balance=closing,
            period_start=parsed.get("period_start"),
            period_end=parsed.get("period_end"),
            source_system=self.source_system,
        )
        return AdapterResult(meta=meta, transactions=txns, parser_confidence=1.0, warnings=warnings)
