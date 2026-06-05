"""Pluggable bank-data adapters (Phase 3A)."""
from .base import (
    AdapterResult,
    BankSourceAdapter,
    NormalizedBankTxn,
    StatementMeta,
)

__all__ = [
    "AdapterResult",
    "BankSourceAdapter",
    "NormalizedBankTxn",
    "StatementMeta",
]
