"""Bank reconciliation tests (Phase 3C).

Pure-function tests assert the book->bank waterfall against hand-computed values,
including the deposits-in-transit balancing behaviour and an off-by-one negative
case. A live-DB integration test reconciles the Bridlewood Feb 2026 account 1020
statement to the numbers confirmed at the checkpoint (ties, variance $0.00).
"""
from datetime import date
from decimal import Decimal

import pytest

from app.services_bank_rec import waterfall


def _D(x):
    return Decimal(str(x))


def test_waterfall_implied_dit_always_ties():
    """With DIT unconfirmed, the waterfall solves for it -> variance must be 0."""
    wf = waterfall(
        book_balance=_D("-616680.01"), statement_closing=_D("-616218.86"),
        outstanding_cheques=_D("-5854.85"),   # term loan LN PYMT, signed (adds back)
        bank_only_other=_D("0"), payroll_deductions=_D("4955.16"),
    )
    assert wf["implied_dit_to_close"] == _D("438.54")
    assert wf["variance"] == pytest.approx(0.0, abs=0.01)
    assert wf["ties"] is True


def test_waterfall_confirmed_dit_matches_implied():
    """Confirming the verified DIT (438.54) reproduces the exact close."""
    wf = waterfall(
        book_balance=_D("-616680.01"), statement_closing=_D("-616218.86"),
        outstanding_cheques=_D("-5854.85"), bank_only_other=_D("0"),
        payroll_deductions=_D("4955.16"), confirmed_deposits_in_transit=_D("438.54"),
    )
    assert wf["deposits_in_transit"] == _D("438.54")
    assert float(wf["expected_closing"]) == pytest.approx(-616218.86, abs=0.01)
    assert float(wf["variance"]) == pytest.approx(0.0, abs=0.01)
    assert wf["ties"] is True


def test_waterfall_wrong_confirmed_dit_breaks_tie():
    """A confirmed DIT that's off by a dollar must NOT tie (no plugging)."""
    wf = waterfall(
        book_balance=_D("-616680.01"), statement_closing=_D("-616218.86"),
        outstanding_cheques=_D("-5854.85"), bank_only_other=_D("0"),
        payroll_deductions=_D("4955.16"), confirmed_deposits_in_transit=_D("1438.54"),
    )
    assert wf["ties"] is False
    # a $1000-too-large DIT lowers expected_closing by 1000, so variance = +1000
    assert float(wf["variance"]) == pytest.approx(1000.0, abs=0.01)


def test_waterfall_signs():
    """Hand-checked: each named item moves expected_closing the documented way."""
    base = dict(book_balance=_D("1000"), statement_closing=_D("1000"),
                outstanding_cheques=_D("0"), bank_only_other=_D("0"),
                payroll_deductions=_D("0"), confirmed_deposits_in_transit=_D("0"))
    assert waterfall(**base)["expected_closing"] == _D("1000")
    # an outstanding cheque (negative signed) adds back -> expected rises
    assert waterfall(**{**base, "outstanding_cheques": _D("-50")})["expected_closing"] == _D("1050")
    # a bank-only deposit raises expected; a payroll deduction lowers it
    assert waterfall(**{**base, "bank_only_other": _D("30")})["expected_closing"] == _D("1030")
    assert waterfall(**{**base, "payroll_deductions": _D("40")})["expected_closing"] == _D("960")


# --------------------------------------------------------------------------
# Live-DB integration — Bridlewood Feb 2026, account 1020. Skips when no DB.
# --------------------------------------------------------------------------

def _has_db() -> bool:
    try:
        from app.db import db_session
        with db_session() as s:
            from sqlalchemy import text
            s.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_db(), reason="no live DB connection")
def test_feb_2026_reconciliation_ties():
    from app.db import db_session
    from app.services_bank_rec import compute_reconciliation

    with db_session() as s:
        rec = compute_reconciliation(
            s, entity_code="1877-8", source_account_code="1020",
            period_start=date(2026, 2, 1), period_end=date(2026, 2, 28),
            statement_date=date(2026, 2, 27),
            statement_closing_balance=Decimal("-616218.86"),
            confirmed_deposits_in_transit=Decimal("438.54"),
        )
    assert rec["ties"] is True
    assert rec["variance"] == pytest.approx(0.0, abs=0.01)
    # the three named reconciling items net to statement - book
    assert rec["outstanding_cheques"] == pytest.approx(-5854.85, abs=0.01)
    assert rec["payroll_deductions"] == pytest.approx(4955.16, abs=0.01)
    assert rec["deposits_in_transit"] == pytest.approx(438.54, abs=0.01)
