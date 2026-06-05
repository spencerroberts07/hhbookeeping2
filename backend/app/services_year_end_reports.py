"""
Year-end statements (Phase 5B): full-FY income statement (4-column, FY vs prior
FY), September-30 balance sheet (current vs prior year), and an indirect-method
statement of cash flows. Fiscal year = Oct 1 -> Sep 30.

Cash flow (D5-4-c) is built from balance changes between the opening (Sep 30 of
the prior FY) and closing (Sep 30) cumulative balances, plus the period flows
(net income, D&A, DGIP forgiveness) from build_financials_context. Accumulated
depreciation (16xx) is intentionally excluded from every bucket — the D&A
add-back stands in for it (standard indirect method). The three sections are
reconciled to the actual change in cash (1020); any residual is surfaced as a
variance, never plugged.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .routes.reports import (
    _account_sums,
    _account_type,
    _bs_subclass,
    get_balance_sheet,
    get_income_statement,
)


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


def _fy_bounds(fy: int) -> tuple[date, date, date]:
    """Returns (fy_start, fy_end, prior_close)."""
    fy_start = date(fy - 1, 10, 1)
    fy_end = date(fy, 9, 30)
    prior_close = fy_start - timedelta(days=1)  # Sep 30 of the prior FY
    return fy_start, fy_end, prior_close


def _raw_balances(session, entity_id: str, as_of: date) -> dict[str, tuple[Decimal, Decimal]]:
    rows = _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=as_of)
    return {r["account_code"]: (_D(r["sum_debit"]), _D(r["sum_credit"])) for r in rows}


def _nat(bals: dict[str, tuple[Decimal, Decimal]], code: str) -> Decimal:
    """Natural-sign balance for a single account (asset/expense debit-natural;
    liability/equity/revenue credit-natural)."""
    d, c = bals.get(code, (Decimal("0"), Decimal("0")))
    t = _account_type(code)
    return (d - c) if t in ("asset", "cogs", "operating_expense") else (c - d)


def _all_codes(*maps: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for m in maps:
        out |= set(m.keys())
    return out


def _cash_flow(session, entity_id: str, fy: int) -> dict[str, Any]:
    from .services_ratios import build_financials_context

    fy_start, fy_end, prior_close = _fy_bounds(fy)
    opening = _raw_balances(session, entity_id, prior_close)
    closing = _raw_balances(session, entity_id, fy_end)
    codes = _all_codes(opening, closing)

    def delta(code: str) -> Decimal:
        return _nat(closing, code) - _nat(opening, code)

    ctx = build_financials_context(session, entity_id=entity_id,
                                   period_start=fy_start, period_end=fy_end)
    ni = _D(ctx.get("net_income"))
    da = _D(ctx.get("depreciation_amortization"))
    dgip = _D(ctx.get("dgip_forgiveness"))

    # ---- Operating (indirect) ----
    d_ar = delta("1090")                       # AR (asset): increase uses cash
    d_inv = delta("1120") + delta("1125")      # inventory (asset)
    d_ap = delta("2020") + delta("2030")       # AP (liability): increase is a source
    d_cra = delta("2320")                      # CRA payable (liability)
    d_hst = delta("2301")                      # HST net (liability)

    itemized_current = {"1090", "1120", "1125", "1020",   # cash itemized separately
                        "2020", "2030", "2320", "2301"}
    other_wc = Decimal("0")
    for code in codes:
        if code in itemized_current:
            continue
        t = _account_type(code)
        sub = _bs_subclass(code)
        if t == "asset" and sub == "current":
            other_wc -= delta(code)            # asset increase uses cash
        elif t == "liability" and sub == "current":
            other_wc += delta(code)            # liability increase is a source

    # DGIP forgiveness is non-cash INCOME sitting in net income (Cr 7000 / Dr 2510),
    # so it is REMOVED from operating cash (subtracted); the matching non-cash
    # reduction of the 2510 loan is added back in financing below.
    operating = (ni + da - dgip - d_ar - d_inv + d_ap + d_cra + d_hst + other_wc)

    # ---- Investing: net capex (gross fixed-asset cost 15xx; accum dep 16xx excluded) ----
    investing = Decimal("0")
    for code in codes:
        if code.startswith("15"):
            investing -= delta(code)           # asset additions use cash

    # ---- Financing ----
    d_2500 = delta("2500")
    d_2510 = delta("2510") + dgip              # neutralize non-cash DGIP forgiveness
    d_2515 = delta("2515")
    d_2520 = delta("2520")
    d_2525 = delta("2525")
    financing_named = d_2500 + d_2510 + d_2515 + d_2520 + d_2525
    # owner equity movements (3xxx), excluding 3900 OBE; RE is computed (not an account)
    equity_other = Decimal("0")
    for code in codes:
        if _account_type(code) == "equity" and code != "3900":
            equity_other += delta(code)
    financing = financing_named + equity_other

    delta_cash = delta("1020")
    total = operating + investing + financing
    variance = delta_cash - total
    ties = abs(variance) <= Decimal("1.00")

    return {
        "fy": fy,
        "method": "indirect",
        "operating": {
            "net_income": float(ni),
            "depreciation_amortization": float(da),
            "less_dgip_forgiveness_noncash": float(-dgip),
            "change_accounts_receivable": float(-d_ar),
            "change_inventory": float(-d_inv),
            "change_accounts_payable": float(d_ap),
            "change_cra_payable": float(d_cra),
            "change_hst_net": float(d_hst),
            "other_working_capital": float(other_wc),
            "total": float(operating),
        },
        "investing": {"net_capex": float(investing), "total": float(investing)},
        "financing": {
            "change_2500_principal": float(d_2500),
            "change_2510_net_of_dgip": float(d_2510),
            "change_2515": float(d_2515),
            "change_2520": float(d_2520),
            "change_2525": float(d_2525),
            "equity_movements": float(equity_other),
            "total": float(financing),
        },
        "net_change_in_cash": float(total),
        "actual_change_in_cash_1020": float(delta_cash),
        "variance": float(variance),
        "ties": ties,
    }


def get_year_end_statements(session, *, entity_code: str, fy: int) -> dict[str, Any]:
    fy_start, fy_end, _ = _fy_bounds(fy)
    entity_id = session.execute(
        text("SELECT id FROM entities WHERE entity_code=:ec"),
        {"ec": entity_code},
    ).scalar()
    if not entity_id:
        raise ValueError(f"entity {entity_code} not found")
    entity_id = str(entity_id)

    # Full-FY income statement (4-column, FY vs prior FY) via the custom preset.
    income_statement = get_income_statement(
        entity_code=entity_code, preset="custom",
        period_end=None, date_from=fy_start.isoformat(), date_to=fy_end.isoformat(),
        _user=None)

    # September-30 balance sheet, current vs prior FY.
    bs_current = get_balance_sheet(entity_code=entity_code, as_of_date=fy_end.isoformat(), _user=None)
    try:
        bs_prior = get_balance_sheet(
            entity_code=entity_code, as_of_date=date(fy - 1, 9, 30).isoformat(), _user=None)
    except Exception:
        bs_prior = None

    cash_flow = _cash_flow(session, entity_id, fy)

    # Which FY periods are closed (label gaps).
    periods = session.execute(
        text(
            """SELECT period_end, status FROM accounting_periods
                WHERE entity_id=:e AND period_end BETWEEN :s AND :en ORDER BY period_end"""
        ),
        {"e": entity_id, "s": fy_start, "en": fy_end},
    ).mappings().all()
    open_periods = [p["period_end"].isoformat() for p in periods if (p["status"] or "") != "closed_locked"]

    return {
        "entity_code": entity_code,
        "fy": fy,
        "fy_start": fy_start.isoformat(),
        "fy_end": fy_end.isoformat(),
        "income_statement": income_statement,
        "balance_sheet": {"current": bs_current, "prior": bs_prior},
        "cash_flow": cash_flow,
        "periods_open_or_unclosed": open_periods,
        "complete": len(open_periods) == 0,
    }
