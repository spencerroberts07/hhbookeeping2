"""Ratio-engine tests (Phase 2C).

Pure-function tests assert each built-in ratio against hand-computed values
from a synthetic financials context. A live-DB integration test reconciles
the Feb 2026 numbers to the values confirmed at the checkpoint.
"""
from datetime import date

import pytest

from app.services_ratios import compute_builtin_ratios


def _synthetic_context() -> dict:
    """Round numbers chosen so every ratio is trivial to verify by hand."""
    return {
        # balances as-of period_end
        "current_assets": 1000.0, "current_liabilities": 500.0,
        "inventory": 300.0, "cash": 100.0,
        "accounts_receivable": 200.0, "accounts_payable": 150.0,
        "total_debt": 800.0, "total_equity": 400.0, "tangible_net_worth": 400.0,
        "total_assets": 1500.0, "working_capital": 500.0,
        # period flows
        "revenue": 1000.0, "cogs": 600.0, "gross_profit": 400.0,
        "opex": 300.0, "net_income": 80.0,
        # TTM flows
        "ttm_revenue": 12000.0, "ttm_cogs": 7200.0, "ttm_gross_profit": 4800.0,
        "ttm_opex": 3600.0, "ttm_net_income": 960.0, "ttm_interest_expense": 200.0,
        "ttm_ebit": 1160.0, "ttm_ebitda": 1360.0, "ttm_ebitda_excl_dgip": 1200.0,
    }


def test_builtin_ratios_match_hand_calc():
    r = compute_builtin_ratios(_synthetic_context(), inputs={"annual_debt_service": 1000.0})
    approx = lambda x: pytest.approx(x, rel=1e-6)
    # liquidity
    assert r["current_ratio"] == approx(2.0)
    assert r["quick_ratio"] == approx(1.4)            # (1000-300)/500
    assert r["cash_ratio"] == approx(0.2)
    assert r["working_capital"] == approx(500.0)
    # leverage / coverage
    assert r["debt_to_equity"] == approx(2.0)
    assert r["debt_to_tnw"] == approx(2.0)
    assert r["debt_to_ebitda"] == approx(800.0 / 1360.0)
    assert r["debt_to_ebitda_excl_dgip"] == approx(800.0 / 1200.0)
    assert r["interest_coverage"] == approx(5.8)       # 1160/200
    assert r["dscr"] == approx(1.36)                   # 1360/1000
    assert r["dscr_excl_dgip"] == approx(1.2)          # 1200/1000
    # profitability
    assert r["gross_margin_pct"] == approx(40.0)
    assert r["operating_margin_pct"] == approx(10.0)   # (400-300)/1000
    assert r["net_margin_pct"] == approx(8.0)
    assert r["ebitda_margin_pct"] == approx(1360.0 / 12000.0 * 100)
    assert r["roa_pct"] == approx(64.0)                # 960/1500
    assert r["roe_pct"] == approx(240.0)               # 960/400
    # efficiency
    assert r["inventory_turnover"] == approx(24.0)     # 7200/300
    assert r["days_inventory_outstanding"] == approx(365.0 / 24.0)
    assert r["days_sales_outstanding"] == approx(200.0 / 12000.0 * 365.0)
    assert r["days_payable_outstanding"] == approx(150.0 / 7200.0 * 365.0)
    assert r["asset_turnover"] == approx(8.0)          # 12000/1500
    assert r["gmroii"] == approx(16.0)                 # 4800/300
    # cash conversion cycle = DIO + DSO - DPO
    assert r["cash_conversion_cycle"] == approx(
        365.0 / 24.0 + 200.0 / 12000.0 * 365.0 - 150.0 / 7200.0 * 365.0
    )


def test_division_by_zero_returns_none():
    ctx = _synthetic_context()
    ctx["current_liabilities"] = 0.0
    ctx["ttm_ebitda"] = 0.0
    r = compute_builtin_ratios(ctx, inputs={})
    assert r["current_ratio"] is None
    assert r["debt_to_ebitda"] is None
    assert r["dscr"] is None            # no annual_debt_service supplied


def test_dscr_none_without_debt_service():
    r = compute_builtin_ratios(_synthetic_context(), inputs={})
    assert r["dscr"] is None
    assert r["dscr_excl_dgip"] is None


# ---- live DB integration: Feb 2026 reconciliation (values from checkpoint) ----

@pytest.mark.integration
def test_feb_2026_reconciles_to_checkpoint():
    from app.db import db_session
    from sqlalchemy import text
    from app.services_ratios import build_financials_context, get_account_roles

    with db_session() as s:
        eid = s.execute(text("SELECT id FROM entities WHERE entity_code='1877-8'")).scalar()
        roles = get_account_roles(s, eid)
        ctx = build_financials_context(
            s, entity_id=eid, period_start=date(2026, 2, 1), period_end=date(2026, 2, 28),
            roles=roles,
        )
        r = compute_builtin_ratios(ctx, inputs={})

    assert ctx["balances_balanced"] is True
    assert ctx["total_debt"] == pytest.approx(786_241.62, abs=0.5)
    assert ctx["ttm_ebitda"] == pytest.approx(140_785.10, abs=1.0)
    assert ctx["ttm_ebitda_excl_dgip"] == pytest.approx(100_785.14, abs=1.0)
    assert r["current_ratio"] == pytest.approx(1.172, abs=0.002)
    # gross margin is now TTM basis (same window as EBITDA): 1,247,329 / 3,220,470
    assert r["gross_margin_pct"] == pytest.approx(38.73, abs=0.1)
    assert r["debt_to_ebitda"] == pytest.approx(5.58, abs=0.02)
    assert r["debt_to_ebitda_excl_dgip"] == pytest.approx(7.80, abs=0.02)
