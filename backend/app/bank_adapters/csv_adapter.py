"""CSV adapter — header-mapped deterministic parse (csv.Sniffer for the
delimiter, fuzzy column mapping for date / description / amount or debit+credit).
Mirrors the Excel adapter's mapping model so one mental model covers both."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .base import AdapterResult, NormalizedBankTxn, StatementMeta

_DATE_KEYS = ("date", "posted", "transaction date", "trans date")
_DESC_KEYS = ("description", "details", "narrative", "memo", "transaction")
_AMOUNT_KEYS = ("amount", "value")
_DEBIT_KEYS = ("debit", "withdrawal", "withdrawals", "paid out")
_CREDIT_KEYS = ("credit", "deposit", "deposits", "paid in")
_BAL_KEYS = ("balance", "running balance")


def _match(h: str, keys) -> bool:
    h = (h or "").strip().lower()
    return any(k in h for k in keys)


def _to_date(v) -> date | None:
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%b %d, %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            continue
    return None


def _to_dec(v) -> Decimal | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        return Decimal(str(v).replace(",", "").replace("$", "").strip())
    except (InvalidOperation, ValueError):
        return None


class CsvBankAdapter:
    source_system = "statement_csv"

    def supports(self, *, filename: str, content_type: str, sniff: bytes) -> bool:
        if (filename or "").lower().endswith(".csv") or "csv" in (content_type or "").lower():
            return True
        # crude text+comma sniff (avoid matching binary)
        try:
            head = sniff.decode("utf-8")
        except UnicodeDecodeError:
            return False
        return "," in head and "\n" in head

    def parse(self, *, file_bytes: bytes, filename: str, entity_code: str,
              hints: dict[str, Any] | None = None) -> AdapterResult:
        text = file_bytes.decode("utf-8", errors="replace")
        try:
            dialect = csv.Sniffer().sniff(text[:2048], delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        reader = list(csv.reader(io.StringIO(text), dialect))
        if not reader:
            return AdapterResult(meta=StatementMeta(source_system=self.source_system),
                                 transactions=[], parser_confidence=0.0, warnings=["CSV: empty file"])

        hdr_idx, colmap = None, {}
        for i, row in enumerate(reader[:15]):
            m: dict[str, int] = {}
            for j, c in enumerate(row):
                if "date" not in m and _match(c, _DATE_KEYS): m["date"] = j
                if "desc" not in m and _match(c, _DESC_KEYS): m["desc"] = j
                if "amount" not in m and _match(c, _AMOUNT_KEYS): m["amount"] = j
                if "debit" not in m and _match(c, _DEBIT_KEYS): m["debit"] = j
                if "credit" not in m and _match(c, _CREDIT_KEYS): m["credit"] = j
                if "balance" not in m and _match(c, _BAL_KEYS): m["balance"] = j
            if "date" in m and ("amount" in m or ("debit" in m and "credit" in m)):
                hdr_idx, colmap = i, m
                break
        if hdr_idx is None:
            return AdapterResult(meta=StatementMeta(source_system=self.source_system),
                                 transactions=[], parser_confidence=0.0,
                                 warnings=["CSV: no recognizable header (date + amount/debit/credit)"])

        txns: list[NormalizedBankTxn] = []
        closing: Decimal | None = None
        for row in reader[hdr_idx + 1:]:
            def cell(key):
                j = colmap.get(key)
                return row[j] if j is not None and j < len(row) else None
            d = _to_date(cell("date"))
            if d is None:
                continue
            if "amount" in colmap:
                amt = _to_dec(cell("amount"))
            else:
                deb, cred = _to_dec(cell("debit")), _to_dec(cell("credit"))
                amt = (cred or Decimal("0")) - (deb or Decimal("0"))
            if amt is None:
                continue
            bal = _to_dec(cell("balance"))
            if bal is not None:
                closing = bal
            txns.append(NormalizedBankTxn(
                transaction_date=d, amount=amt,
                description=str(cell("desc") or "").strip(),
                running_balance=bal, raw={"row": row},
            ))

        meta = StatementMeta(
            source_account_code=hints.get("source_account_code") if hints else None,
            closing_balance=closing,
            statement_date=txns[-1].transaction_date if txns else None,
            source_system=self.source_system,
        )
        warnings = [] if txns else ["CSV: header found but no data rows parsed"]
        return AdapterResult(meta=meta, transactions=txns, parser_confidence=1.0, warnings=warnings)
