"""
Pluggable bank-data adapter interface (Phase 3A).

The reconciliation engine consumes NORMALIZED bank transactions through this
interface. Upload parsers (CSV / Excel / OFX / PDF) implement it now; a
Plaid / Flinks adapter would implement the SAME `BankSourceAdapter` Protocol
later and return the SAME `AdapterResult` — that object is the seam, so the
engine never learns where the data came from.

Nothing here touches the GL. Adapters parse bytes (or, later, pull an API) and
hand back a structured result; `services_bank_ingest` does the DB write.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class NormalizedBankTxn:
    """One bank line, source-agnostic. `amount` is signed: +inflow / -outflow.

    `source_transaction_id` is the dedup seed: either a stable provider id
    (OFX FITID, Plaid transaction_id) or a deterministic hash the ingest layer
    computes from (date, amount, description, occurrence). Adapters may leave it
    empty and let ingest hash it.
    """
    transaction_date: date
    amount: Decimal
    description: str
    direction: str = ""                       # 'inflow' | 'outflow'; derived from sign if blank
    reference_number: str | None = None
    posted_date: date | None = None
    running_balance: Decimal | None = None     # per-line balance if the statement prints one
    running_balance_is_overdraft: bool = False
    counterparty_name: str | None = None
    normalized_description: str | None = None
    source_transaction_id: str | None = None   # provider id; else ingest hashes
    raw: dict[str, Any] = field(default_factory=dict)

    def signed_direction(self) -> str:
        if self.direction:
            return self.direction
        return "inflow" if self.amount >= 0 else "outflow"


@dataclass(frozen=True)
class StatementMeta:
    """Statement-level facts used for balance validation + the rec header."""
    source_account_code: str | None = None     # e.g. '1020'
    source_account_name: str | None = None
    statement_date: date | None = None         # statement ending date
    opening_balance: Decimal | None = None
    closing_balance: Decimal | None = None      # signed (negative = overdraft)
    period_start: date | None = None
    period_end: date | None = None
    r2_object_key: str | None = None
    source_system: str = "upload"              # statement_pdf|statement_csv|statement_ofx|statement_excel|plaid|flinks


@dataclass(frozen=True)
class AdapterResult:
    """What every adapter returns. The engine only ever sees this."""
    meta: StatementMeta
    transactions: list[NormalizedBankTxn]
    parser_confidence: float = 1.0             # 1.0 deterministic; <1 AI-fallback
    warnings: list[str] = field(default_factory=list)

    # ---- balance validation (runs at ingest; non-blocking flag) ----
    def tie_out(self) -> tuple[bool, Decimal | None]:
        """Σ(amount) + opening == closing? Returns (ok, variance).
        ok is None-safe: True when we lack the balances to check (can't fail
        what we can't measure), variance None in that case."""
        op, cl = self.meta.opening_balance, self.meta.closing_balance
        if op is None or cl is None:
            return True, None
        net = sum((t.amount for t in self.transactions), Decimal("0"))
        variance = cl - (op + net)
        return abs(variance) <= Decimal("0.01"), variance


@runtime_checkable
class BankSourceAdapter(Protocol):
    """Implemented by upload parsers now and Plaid/Flinks later.

    `supports()` lets the dispatcher pick the right adapter for a file.
    `parse()` does the work. A live-feed adapter ignores `file_bytes` and pulls
    from its API inside `parse()` (or a sibling fetch), returning the same
    `AdapterResult`.
    """
    source_system: str

    def supports(self, *, filename: str, content_type: str, sniff: bytes) -> bool:
        ...

    def parse(
        self,
        *,
        file_bytes: bytes,
        filename: str,
        entity_code: str,
        hints: dict[str, Any] | None = None,
    ) -> AdapterResult:
        ...
