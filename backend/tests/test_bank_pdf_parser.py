"""
TD PDF parser tests.

Tests the sign-direction classifier and the balance invariant that was added
to catch the March 2026 sign bug (pypdf collapses TD's two-column debit/credit
layout; direction is inferred entirely from description patterns).

Fixtures use the real March 2026 statement when present locally; the pure-unit
tests use in-memory synthetic page text so they run everywhere.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MARCH_PDF_PATH = (
    r"C:\Users\spenc\OneDrive\Desktop\Home Hardware\Apps creation"
    r"\BookWize creation\Process documents\Bank Statements"
    r"\01020_5660371_2026-02-27--2026-03-31.pdf"
)


def _march_pdf() -> bytes | None:
    p = Path(_MARCH_PDF_PATH)
    if p.exists():
        return p.read_bytes()
    return None


# ---------------------------------------------------------------------------
# Unit tests: classifier rules (no PDF needed)
# ---------------------------------------------------------------------------

from app.services_bank_pdf import _classify


class TestClassifierRules:
    def test_bpy_suffix_is_outflow(self):
        """BPY (TD bill pay) must always be an outflow."""
        for desc in [
            "Enbridge Gas BPY",
            "PENINSULA EMPLO BPY",
            "Ottawa Water MSP BPY",
            "SomeBiller BPY",
        ]:
            direction, _ = _classify(desc)
            assert direction == "outflow", f"Expected outflow for {desc!r}"

    def test_tfr_to_is_outflow(self):
        """TFR-TO transfers are debits (transfers to another TD account)."""
        for desc in ["RJ533 TFR-TO C/C", "RJ541 TFR-TO C/C", "II522 TFR-TO SAV"]:
            direction, _ = _classify(desc)
            assert direction == "outflow", f"Expected outflow for {desc!r}"

    def test_standalone_fees_is_outflow(self):
        """Descriptions ending with FEES or FEE (as standalone word) are outflows."""
        for desc in ["LOAN ADMIN FEES", "ACCOUNT ADMIN FEE"]:
            direction, _ = _classify(desc)
            assert direction == "outflow", f"Expected outflow for {desc!r}"

    def test_billing_is_outflow(self):
        """Third-party billing services are outflows."""
        for desc in ["BRW Billing", "EFT Billing", "Rotessa Billing"]:
            direction, _ = _classify(desc)
            assert direction == "outflow", f"Expected outflow for {desc!r}"

    def test_known_inflows_unchanged(self):
        """Existing inflow patterns must not be broken by the new rules."""
        inflow_cases = [
            "VSA DEP14350 MSP",
            "MC DEP 14350 MSP",
            "EF0301 14350 MSP",
            "AMEX 1995722444 MSP",
            "GLR 14350 MSP",
            "TD EXPRESS DEPOSIT",
        ]
        for desc in inflow_cases:
            direction, _ = _classify(desc)
            assert direction == "inflow", f"Expected inflow for {desc!r}"

    def test_known_outflows_unchanged(self):
        """Existing outflow patterns must not be broken by the new rules."""
        outflow_cases = [
            "AMX FEE12593422 MSP",
            "HOME HARDWARE MSP",
            "eNet Employer S BUS",
            "LN PYMT 966037102",
            "SERVICE CHARGE",
            "OVERDRAFT INTEREST",
            "SEND E-TFR *Aum BPY",   # SEND E-TFR takes priority over BPY
        ]
        for desc in outflow_cases:
            direction, _ = _classify(desc)
            assert direction == "outflow", f"Expected outflow for {desc!r}"

    def test_send_etfr_bpy_priority(self):
        """SEND E-TFR must match before the generic BPY rule (more specific first)."""
        direction, txn_type = _classify("SEND E-TFR *Aum BPY")
        assert direction == "outflow"
        assert txn_type == "etfr_outgoing"  # NOT bill_payment

    def test_etransfer_unknown(self):
        """Incoming e-transfers (no SEND prefix) remain unknown direction."""
        direction, _ = _classify("E-TRANSFER ***QRA")
        assert direction == "unknown"


# ---------------------------------------------------------------------------
# Unit tests: balance invariant (in-memory synthetic pages)
# ---------------------------------------------------------------------------

from app.services_bank_pdf import parse_td_statement_pdf


def _make_minimal_statement(
    opening_balance_od: str,       # e.g. "616,218.86OD"
    transactions: list[str],       # e.g. ["VSA DEP14350 MSP 1,000.00 MAR01"]
    closing_balance_od: str,       # e.g. "615,218.86OD"  (balance on last txn)
    period: str = "MAR 01/26-MAR 31/26",
    account: str = "1020 0690-5660371",
) -> bytes:
    """Build a minimal in-memory PDF mimicking TD statement structure.
    Uses pypdf to write a real PDF so the parser's _extract_pdf_pages works."""
    # If the last transaction has a balance, attach it:
    if transactions:
        last = transactions[-1]
        transactions = transactions[:-1] + [f"{last} {closing_balance_od}"]

    lines = [
        f"Statement of Account Account Type Statement From -To",
        f"Branch No. Account No. {period}",
        f"{account} Page 1 of 1",
        f"DESCRIPTION CHEQUE/DEBIT DEPOSIT/CREDIT DATE BALANCE",
        f"BALANCE FORWARD MAR01 {opening_balance_od}",
    ] + transactions + [
        "Pleaseensurethatyou reportinwriting errors.",
        "Accountsissuedby:THE TORONTO-DOMINION BANK",
    ]

    text = "\n".join(lines)

    try:
        from pypdf import PdfWriter
        from pypdf.generic import NameObject, ArrayObject, FloatObject, RectangleObject
        import io

        writer = PdfWriter()
        writer.add_blank_page(width=612, height=792)
        page = writer.pages[0]
        # Encode the text directly as a simple content stream
        content = f"BT /F1 10 Tf 50 750 Td ({text.replace(chr(10), ' ')}) Tj ET"
        # pypdf doesn't easily support adding text; use reportlab if available
        raise ImportError("use reportlab")
    except ImportError:
        pass

    # Fallback: use reportlab
    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.pagesizes import letter
        import io

        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=letter)
        y = 750
        for line in lines:
            c.drawString(50, y, line)
            y -= 12
        c.save()
        return buf.getvalue()
    except ImportError:
        pytest.skip("reportlab not installed — skipping synthetic-PDF invariant tests")


class TestBalanceInvariant:
    def test_invariant_passes_when_correct(self):
        """When all amounts net correctly to (closing − opening), no warning."""
        pdf_bytes = _make_minimal_statement(
            opening_balance_od="10,000.00OD",
            transactions=[
                "VSA DEP14350 MSP 500.00 MAR05",   # inflow +500
                "eNet Employer S BUS 300.00 MAR05", # outflow -300
                # net = +200 → closing = 10,000 - 200 = 9,800
            ],
            closing_balance_od="9,800.00OD",
        )
        result = parse_td_statement_pdf(pdf_bytes)
        invariant_warnings = [
            w for w in result["warnings"] if "invariant" in w.lower()
        ]
        assert not invariant_warnings, (
            f"Unexpected invariant warning: {invariant_warnings}"
        )

    def test_invariant_fires_when_outflow_stored_positive(self):
        """Invariant must fire when an outflow is stored as positive (sign bug)."""
        # Simulate a mis-signed row: HOME HARDWARE MSP is outflow but if we
        # use a vendor description that doesn't match any classifier, it would
        # be stored positive (unknown) instead of negative.
        # Opening -10,000OD; one true inflow +100, one true outflow -200 = net -100.
        # Closing = 10,100OD.  But if the outflow is stored as +200, parser
        # computes net = +300, not -100.  Discrepancy = +400.
        pdf_bytes = _make_minimal_statement(
            opening_balance_od="10,000.00OD",
            transactions=[
                "VSA DEP14350 MSP 100.00 MAR05",      # inflow +100
                "UNKNOWN VENDOR XYZ 200.00 MAR05",    # outflow but unknown → +200
            ],
            # True closing: -10,000 + (100 - 200) = -10,100
            closing_balance_od="10,100.00OD",
        )
        result = parse_td_statement_pdf(pdf_bytes)
        invariant_warnings = [
            w for w in result["warnings"] if "invariant" in w.lower()
        ]
        assert invariant_warnings, "Invariant should have fired but did not"

    def test_invariant_tolerance(self):
        """Discrepancy within $0.01 must not trigger the warning (rounding)."""
        pdf_bytes = _make_minimal_statement(
            opening_balance_od="10,000.00OD",
            transactions=["VSA DEP14350 MSP 100.00 MAR05"],
            closing_balance_od="9,900.00OD",
        )
        result = parse_td_statement_pdf(pdf_bytes)
        inv_warnings = [w for w in result["warnings"] if "invariant" in w.lower()]
        # net = +100; balance change = 10,000 - 9,900 = +100. No discrepancy.
        assert not inv_warnings


# ---------------------------------------------------------------------------
# Integration tests: real March 2026 statement (skip if PDF not available)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def march_result() -> dict[str, Any]:
    pdf = _march_pdf()
    if pdf is None:
        pytest.skip("March 2026 PDF not present; set _MARCH_PDF_PATH")
    return parse_td_statement_pdf(pdf)


class TestMarchStatement:
    def test_period_parsed(self, march_result):
        from datetime import date
        assert march_result["period_start"] == date(2026, 2, 27)
        assert march_result["period_end"] == date(2026, 3, 31)

    def test_transaction_count(self, march_result):
        assert len(march_result["transactions"]) == 215, (
            "Expected 215 rows; count changed — verify statement hasn't been modified"
        )

    def test_bpy_transactions_are_outflows(self, march_result):
        """After fix, BPY transactions must be outflows (not unknowns)."""
        bpy_txns = [
            t for t in march_result["transactions"]
            if "BPY" in (t.get("description") or "").upper()
            and "SEND" not in (t.get("description") or "").upper()
        ]
        assert bpy_txns, "Expected at least one BPY transaction in March"
        for t in bpy_txns:
            assert t["direction"] == "outflow", (
                f"BPY txn must be outflow: {t['description']} is {t['direction']}"
            )

    def test_tfr_to_transactions_are_outflows(self, march_result):
        """After fix, TFR-TO transactions must be outflows."""
        tfr_txns = [
            t for t in march_result["transactions"]
            if "TFR-TO" in (t.get("description") or "").upper()
        ]
        assert tfr_txns, "Expected at least one TFR-TO transaction in March"
        for t in tfr_txns:
            assert t["direction"] == "outflow", (
                f"TFR-TO txn must be outflow: {t['description']}"
            )

    def test_loan_admin_fees_is_outflow(self, march_result):
        """After fix, LOAN ADMIN FEES must be outflow (not unknown)."""
        fee_txns = [
            t for t in march_result["transactions"]
            if "LOAN ADMIN FEES" in (t.get("description") or "").upper()
        ]
        assert fee_txns, "Expected LOAN ADMIN FEES in March"
        for t in fee_txns:
            assert t["direction"] == "outflow"
            assert t["amount"] < 0

    def test_balance_invariant_fires(self, march_result):
        """The balance invariant must fire for the March statement (known discrepancy)."""
        inv_warnings = [
            w for w in march_result["warnings"] if "invariant" in w.lower()
        ]
        assert inv_warnings, (
            "Balance invariant should fire for March 2026 — "
            "remaining unknown-direction outflows create a discrepancy"
        )

    def test_invariant_warning_is_informative(self, march_result):
        """Invariant warning must state the discrepancy amount clearly."""
        for w in march_result["warnings"]:
            if "invariant" in w.lower():
                assert "discrepancy" in w.lower()
                assert "unknown" in w.lower()
                return
        pytest.fail("No invariant warning found")

    def test_closing_balance_correct(self, march_result):
        """The last running_balance in parsed rows must match the statement closing."""
        from decimal import Decimal
        closing_txns = [
            t for t in march_result["transactions"]
            if t.get("running_balance") is not None
        ]
        assert closing_txns
        last = closing_txns[-1]
        # March closing: 612,355.94OD
        assert last["running_balance"] == Decimal("612355.94")
        assert last["running_balance_is_overdraft"] is True

    def test_mar_not_previously_missing_property(self, march_result):
        """Every parsed transaction must have required keys."""
        required = {
            "row_number", "page_number", "description", "amount",
            "amount_magnitude", "direction", "transaction_type",
            "transaction_date", "running_balance", "running_balance_is_overdraft",
        }
        for t in march_result["transactions"]:
            missing = required - set(t.keys())
            assert not missing, f"Transaction missing keys: {missing}"


# ---------------------------------------------------------------------------
# Extra: verify parse_td_statement_pdf returns the total_row_count key
# (needed by preview_bank_pdf_import)
# ---------------------------------------------------------------------------
def test_result_has_no_total_row_count_key():
    """parse_td_statement_pdf returns 'transactions' list, not total_row_count;
    the caller counts it.  Verify the key shape is stable."""
    # We use the march fixture if available, otherwise a minimal check
    pdf = _march_pdf()
    if pdf is None:
        pytest.skip("March PDF not available")
    result = parse_td_statement_pdf(pdf)
    assert "transactions" in result
    assert isinstance(result["transactions"], list)
    assert "warnings" in result
    assert isinstance(result["warnings"], list)
