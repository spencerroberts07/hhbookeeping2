"""OFX / QFX adapter — deterministic parse of the SGML/XML <STMTTRN> blocks.

OFX is line-oriented SGML (older) or XML (OFX 2.x). We avoid a heavyweight
dependency and walk the <STMTTRN>…</STMTTRN> records directly, which is robust
across both encodings.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .base import AdapterResult, NormalizedBankTxn, StatementMeta

_TAG = re.compile(r"<([A-Z0-9.]+)>([^<\r\n]*)", re.IGNORECASE)
_STMTTRN = re.compile(r"<STMTTRN>(.*?)</STMTTRN>", re.IGNORECASE | re.DOTALL)


def _field(block: str, tag: str) -> str | None:
    m = re.search(rf"<{tag}>([^<\r\n]*)", block, re.IGNORECASE)
    return m.group(1).strip() if m else None


def _ofx_date(v: str | None) -> date | None:
    if not v:
        return None
    digits = re.sub(r"[^0-9]", "", v)[:8]
    if len(digits) < 8:
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d").date()
    except ValueError:
        return None


def _dec(v: str | None) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(v.replace(",", ""))
    except InvalidOperation:
        return None


class OfxBankAdapter:
    source_system = "statement_ofx"

    def supports(self, *, filename: str, content_type: str, sniff: bytes) -> bool:
        name = (filename or "").lower()
        if name.endswith((".ofx", ".qfx")):
            return True
        head = sniff[:512].upper()
        return b"OFXHEADER" in head or b"<OFX>" in head or b"<STMTTRN>" in head

    def parse(self, *, file_bytes: bytes, filename: str, entity_code: str,
              hints: dict[str, Any] | None = None) -> AdapterResult:
        text = file_bytes.decode("utf-8", errors="replace")
        txns: list[NormalizedBankTxn] = []
        for block in _STMTTRN.findall(text):
            amt = _dec(_field(block, "TRNAMT"))
            if amt is None:
                continue
            d = _ofx_date(_field(block, "DTPOSTED"))
            name = _field(block, "NAME") or _field(block, "MEMO") or ""
            memo = _field(block, "MEMO")
            desc = name if not memo or memo == name else f"{name} {memo}".strip()
            txns.append(NormalizedBankTxn(
                transaction_date=d,
                amount=amt,
                description=desc,
                reference_number=_field(block, "CHECKNUM") or _field(block, "REFNUM"),
                source_transaction_id=_field(block, "FITID"),  # stable provider id
                raw={"fitid": _field(block, "FITID"), "trntype": _field(block, "TRNTYPE")},
            ))

        ledgerbal = _dec(_field(text, "BALAMT"))  # LEDGERBAL/BALAMT = closing
        acct = _field(text, "ACCTID")
        meta = StatementMeta(
            source_account_code=hints.get("source_account_code") if hints else None,
            source_account_name=acct,
            closing_balance=ledgerbal,
            statement_date=_ofx_date(_field(text, "DTEND")),
            period_start=_ofx_date(_field(text, "DTSTART")),
            period_end=_ofx_date(_field(text, "DTEND")),
            source_system=self.source_system,
        )
        warnings = [] if txns else ["OFX parse found no <STMTTRN> records"]
        return AdapterResult(meta=meta, transactions=txns, parser_confidence=1.0, warnings=warnings)
