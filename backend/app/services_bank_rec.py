"""
Bank reconciliation statement (Phase 3C). Computes/saves a rec for one cash
account + period. Read/link only (no GL writes).

Tie model (book -> bank):
    expected_closing = book_balance
                       - deposits_in_transit        (book recorded, not yet on bank)
                       - outstanding_cheques         (book recorded, not yet on bank)
                       + bank_only_items             (on bank, not yet in book, signed)
    variance = statement_closing - expected_closing

The deposit stream (cash_balancing lump <-> daily bank deposits) is reconciled
in BULK by the matcher; its residual (lump - stream) is a deposit-in-transit.
Everything else unmatched is surfaced itemized. The rec does not plug: a non-zero
variance is reported, attributed to the listed items.
"""
from __future__ import annotations

import json
import uuid
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .routes.reports import _account_sums
from .services_bank_rec_match import run_match

_POSTED = ("draft", "voided", "rejected")


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


_CENT = Decimal("0.01")


def waterfall(
    *, book_balance: Decimal, statement_closing: Decimal, outstanding_cheques: Decimal,
    bank_only_other: Decimal, payroll_deductions: Decimal,
    confirmed_deposits_in_transit: Decimal | None = None,
) -> dict[str, Any]:
    """Pure book->bank waterfall. Deposits-in-transit clear the NEXT cycle and
    aren't derivable from this period's bank file, so they're the balancing item:
    if not confirmed, we surface the figure the waterfall implies and the
    bookkeeper verifies it on lock.

        expected = book - DIT - outstanding_cheques(signed) + bank_only_other - payroll_deductions
        variance = statement_closing - expected ; ties when |variance| <= 0.01

    outstanding_cheques is signed (a not-yet-cleared payment is negative, so it
    ADDS back). payroll_deductions is the gross-vs-net eNet residual the bank drew
    but the book never credited to 1020 (bank-only, subtracts)."""
    implied_dit = (book_balance - outstanding_cheques + bank_only_other
                   - payroll_deductions - statement_closing)
    deposits_in_transit = (confirmed_deposits_in_transit
                           if confirmed_deposits_in_transit is not None else implied_dit)
    expected = (book_balance - deposits_in_transit - outstanding_cheques
                + bank_only_other - payroll_deductions)
    variance = statement_closing - expected
    return {
        "implied_dit_to_close": implied_dit,
        "deposits_in_transit": deposits_in_transit,
        "expected_closing": expected,
        "variance": variance,
        "ties": abs(variance) <= _CENT,
    }


def _book_balance(session, entity_id, account_code, as_of) -> Decimal:
    rows = _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=as_of)
    for r in rows:
        if r["account_code"] == account_code:
            return _D(r["sum_debit"]) - _D(r["sum_credit"])
    return Decimal("0")


def _unmatched_bank(session, entity_id, account_code, ps, pe) -> list[dict]:
    rows = session.execute(
        text(
            """
            SELECT bt.id, bt.transaction_date, bt.amount, bt.description
              FROM bank_transactions bt
             WHERE bt.entity_id = :e AND bt.source_account_code = :ac
               AND bt.transaction_date BETWEEN :ps AND :pe
               AND NOT EXISTS (SELECT 1 FROM bank_transaction_matches m
                                WHERE m.bank_transaction_id = bt.id AND m.active)
          ORDER BY bt.amount
            """
        ),
        {"e": entity_id, "ac": account_code, "ps": ps, "pe": pe},
    ).mappings().all()
    return [dict(r) for r in rows]


def _unmatched_book(session, entity_id, account_code, ps, pe) -> list[dict]:
    rows = session.execute(
        text(
            f"""
            SELECT jl.id, jb.source_module, (jl.debit_amount - jl.credit_amount) AS signed_amount, jl.memo
              FROM journal_lines jl
              JOIN journal_batches jb ON jb.id = jl.journal_batch_id
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jb.entity_id = :e AND jl.account_code = :ac
               AND jb.status NOT IN {_POSTED}
               AND ap.period_start >= :ps AND ap.period_end <= :pe
               AND NOT EXISTS (SELECT 1 FROM bank_transaction_matches m
                                WHERE m.target_table_name = 'journal_lines'
                                  AND m.target_record_id = jl.id::text AND m.active)
               -- cash_balancing deposit lines are reconciled in bulk by the
               -- deposit stream (their residual is the deposits-in-transit), so
               -- they are not separately "outstanding".
               AND jb.source_module <> 'cash_balancing'
               -- a line carrying a bank_transaction_id is a bank-DERIVED booking
               -- (created from a bank txn) — it is on the bank by construction.
               AND (jl.source_json->>'bank_transaction_id') IS NULL
          ORDER BY signed_amount
            """
        ),
        {"e": entity_id, "ac": account_code, "ps": ps, "pe": pe},
    ).mappings().all()
    return [dict(r) for r in rows]


def compute_reconciliation(
    session, *, entity_code: str, source_account_code: str,
    period_start: date, period_end: date, statement_date: date,
    statement_closing_balance: Decimal, statement_opening_balance: Decimal | None = None,
    confirmed_deposits_in_transit: Decimal | None = None,
    rerun_match: bool = True,
) -> dict[str, Any]:
    entity_id = session.execute(
        text("SELECT id FROM entities WHERE entity_code=:ec"), {"ec": entity_code}
    ).scalar()
    period_id = session.execute(
        text("SELECT id FROM accounting_periods WHERE entity_id=:e AND period_end=:pe"),
        {"e": entity_id, "pe": period_end},
    ).scalar()

    match = run_match(session, entity_id=entity_id, source_account_code=source_account_code,
                      period_start=period_start, period_end=period_end) if rerun_match else {}
    stream = match.get("deposit_stream") or {}

    book_balance = _book_balance(session, entity_id, source_account_code, period_end)
    ub = _unmatched_bank(session, entity_id, source_account_code, period_start, period_end)
    bk = _unmatched_book(session, entity_id, source_account_code, period_start, period_end)

    # Named reconciling items.
    stream_residual = _D(stream.get("residual_deposits_in_transit"))  # lump - stream; + = book ahead (DIT)
    payroll_deductions = _D(match.get("payroll_deduction_residual"))  # bank drew gross; book credited 1020 net only
    book_inflows = [x for x in bk if _D(x["signed_amount"]) > 0]
    book_outflows = [x for x in bk if _D(x["signed_amount"]) < 0]
    outstanding_cheques = sum((_D(x["signed_amount"]) for x in book_outflows), Decimal("0"))  # signed (negative)
    bank_only_other = sum((_D(x["amount"]) for x in ub), Decimal("0"))  # genuinely unmatched bank lines
    book_inflow_dit = sum((_D(x["signed_amount"]) for x in book_inflows), Decimal("0"))

    S = _D(statement_closing_balance)
    net_difference = S - book_balance  # raw book-vs-bank gap (should be tiny)
    auto_dit = stream_residual + book_inflow_dit
    wf = waterfall(
        book_balance=book_balance, statement_closing=S, outstanding_cheques=outstanding_cheques,
        bank_only_other=bank_only_other, payroll_deductions=payroll_deductions,
        confirmed_deposits_in_transit=confirmed_deposits_in_transit,
    )
    implied_dit_to_close = wf["implied_dit_to_close"]
    deposits_in_transit = wf["deposits_in_transit"]
    expected = wf["expected_closing"]
    variance = wf["variance"]
    ties = wf["ties"]
    bank_only_signed = bank_only_other - payroll_deductions

    summary = {
        "deposit_stream": stream,
        "payroll_deduction_residual": float(payroll_deductions),
        "implied_dit_to_close": float(implied_dit_to_close),
        "auto_dit": float(auto_dit),
        "named_items": {
            "outstanding_cheques": float(outstanding_cheques),
            "deposits_in_transit": float(deposits_in_transit),
            "payroll_deductions_bank_only": float(-payroll_deductions),
            "other_bank_only": float(bank_only_other),
        },
        "bank_only_items": [
            {"date": x["transaction_date"].isoformat(), "amount": float(x["amount"]),
             "description": x["description"]} for x in ub
        ],
        "outstanding_book": [
            {"source_module": x["source_module"], "amount": float(x["signed_amount"]),
             "memo": x["memo"]} for x in book_outflows + book_inflows
        ],
        "match": {k: match.get(k) for k in ("bank_count", "book_count", "pre_cleared",
                                            "auto_cleared", "suggested")},
    }

    rec_id = session.execute(
        text(
            """
            INSERT INTO bank_reconciliations (
                entity_id, accounting_period_id, source_account_code, statement_date,
                statement_opening_balance, statement_closing_balance, book_balance,
                outstanding_deposits_total, outstanding_cheques_total, bank_only_items_total,
                variance, ties, status, summary_json
            ) VALUES (
                :e, :pid, :ac, :sd, :so, :sc, :bb, :dep, :chq, :bo, :var, :ties, 'draft', CAST(:sj AS jsonb)
            )
            ON CONFLICT (entity_id, source_account_code, accounting_period_id) DO UPDATE SET
                statement_date=EXCLUDED.statement_date,
                statement_opening_balance=EXCLUDED.statement_opening_balance,
                statement_closing_balance=EXCLUDED.statement_closing_balance,
                book_balance=EXCLUDED.book_balance,
                outstanding_deposits_total=EXCLUDED.outstanding_deposits_total,
                outstanding_cheques_total=EXCLUDED.outstanding_cheques_total,
                bank_only_items_total=EXCLUDED.bank_only_items_total,
                variance=EXCLUDED.variance, ties=EXCLUDED.ties,
                summary_json=EXCLUDED.summary_json, updated_at=NOW()
            RETURNING id
            """
        ),
        {
            "e": entity_id, "pid": period_id, "ac": source_account_code, "sd": statement_date,
            "so": statement_opening_balance, "sc": S, "bb": book_balance,
            "dep": deposits_in_transit, "chq": outstanding_cheques, "bo": bank_only_signed,
            "var": variance, "ties": ties, "sj": json.dumps(summary),
        },
    ).scalar()

    return {
        "id": str(rec_id),
        "statement_closing_balance": float(S),
        "book_balance": float(book_balance),
        "deposit_stream": stream,
        "deposits_in_transit": float(deposits_in_transit),
        "outstanding_cheques": float(outstanding_cheques),
        "bank_only_items_total": float(bank_only_signed),
        "expected_closing": float(expected),
        "net_difference": float(net_difference),
        "implied_dit_to_close": float(implied_dit_to_close),
        "payroll_deductions": float(payroll_deductions),
        "variance": float(variance),
        "ties": ties,
        "summary": summary,
    }
