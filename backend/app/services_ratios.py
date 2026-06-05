"""
Ratio engine (Phase 2C) — account-role mapping + financials context + the
built-in ratio library. Read-only analytics; reuses the report helpers so
every total reconciles to the financial statements.

The prefix convention (1xxx asset, 2xxx liability, …) handles the broad
classes, but several ratios need finer roles the prefix can't infer
(interest-bearing debt vs operating payables, interest expense vs bank
charges, income-tax vs other taxes, inventory, D&A). Those live in
`ratio_account_roles`, auto-seeded from QBO account_type/account_subtype +
name heuristics, with admin override.
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .routes.reports import (
    _account_sums,
    _account_type,
    _is_account_sums,
    _signed_amount_by_type,
    _bs_subclass,
)

# Roles the prefix convention cannot infer. current_portion_ltd has no
# auto-seed rule (no such account at Bridlewood) — admin maps it if it exists.
SEEDABLE_ROLE_RULES: dict[str, str] = {
    "cash": "a.account_type = 'Bank'",
    "accounts_receivable": "a.account_name ILIKE '%receivable%'",
    "inventory": "a.account_subtype = 'Inventory'",
    "accounts_payable": "a.account_type = 'Accounts Payable'",
    "interest_bearing_debt": "a.account_type = 'Long Term Liability'",
    "interest_expense": (
        "a.account_type IN ('Expense','Other Expense') "
        "AND a.account_name ILIKE '%interest%'"
    ),
    "depreciation_amortization": (
        "a.account_type IN ('Expense','Other Expense') "
        "AND (a.account_subtype = 'Depreciation' "
        "OR a.account_name ILIKE '%deprec%' OR a.account_name ILIKE '%amort%')"
    ),
    "income_tax_expense": (
        "a.account_type IN ('Expense','Other Expense') "
        "AND a.account_name ILIKE '%income tax%'"
    ),
}

ALL_ROLES = list(SEEDABLE_ROLE_RULES.keys()) + ["current_portion_ltd"]


def seed_account_roles(session, entity_id: str) -> dict[str, int]:
    """Insert auto-classified role rows for an entity. Idempotent and
    non-destructive: ON CONFLICT DO NOTHING preserves any admin edits.
    Returns {role: rows_inserted}."""
    inserted: dict[str, int] = {}
    for role, rule in SEEDABLE_ROLE_RULES.items():
        res = session.execute(
            text(
                f"""
                INSERT INTO ratio_account_roles (entity_id, role, account_code)
                SELECT a.entity_id, :role, a.account_code
                  FROM accounts a
                 WHERE a.entity_id = :eid AND a.is_active = TRUE AND ({rule})
                ON CONFLICT (entity_id, role, account_code) DO NOTHING
                """
            ),
            {"eid": entity_id, "role": role},
        )
        inserted[role] = res.rowcount or 0
    return inserted


def get_account_roles(session, entity_id: str) -> dict[str, list[str]]:
    """{role: [account_code, ...]} for an entity."""
    rows = session.execute(
        text(
            "SELECT role, account_code FROM ratio_account_roles WHERE entity_id = :eid"
        ),
        {"eid": entity_id},
    ).mappings().all()
    out: dict[str, list[str]] = {r: [] for r in ALL_ROLES}
    for row in rows:
        out.setdefault(row["role"], []).append(row["account_code"])
    return out


def account_roles_detail(session, entity_id: str) -> list[dict[str, Any]]:
    """Role map joined to account names — for the admin review/config UI."""
    rows = session.execute(
        text(
            """
            SELECT r.role, r.account_code,
                   COALESCE(a.account_name, r.account_code) AS account_name,
                   a.account_type, a.account_subtype
              FROM ratio_account_roles r
         LEFT JOIN accounts a
                ON a.entity_id = r.entity_id AND a.account_code = r.account_code
                                              AND a.is_active = TRUE
             WHERE r.entity_id = :eid
          ORDER BY r.role, r.account_code
            """
        ),
        {"eid": entity_id},
    ).mappings().all()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------
# Financials context — reuses the report helpers so totals reconcile to the
# statements, then applies the Bridlewood-approved reclasses:
#   * 2520/2525 related-party loans: move signed balance liabilities -> equity.
#   * 1020 (and any cash account) overdraft: floor cash at 0, add |overdraft|
#     to current liabilities AND interest-bearing debt. Both sides move by the
#     same amount, so the balance sheet still balances.
# P&L uses _is_account_sums (closed periods only) to match the income
# statement. BS uses _account_sums cumulative (cutover-aware) to match the
# balance sheet.
# --------------------------------------------------------------------------


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


# P&L flows for the ratio engine use a STATUS-based filter (not the
# closed-only income statement). This deliberately includes historical_import
# batches so trailing-12-month / prior-year windows reflect best-available
# data (native closes where they exist, QBO GL import for pre-cutover months).
# For a fully-closed month this equals the income statement.
_PNL_STATUSES = ("posted", "approved_to_post", "approved", "closed_locked")


def _pnl_window(session, entity_id, start, end, roles) -> dict[str, float]:
    """P&L aggregates over [start, end] on the posted+historical_import basis."""
    rows = session.execute(
        text(
            """
            SELECT jl.account_code,
                   SUM(jl.debit_amount)  AS sum_debit,
                   SUM(jl.credit_amount) AS sum_credit
              FROM journal_lines jl
              JOIN journal_batches jb ON jb.id = jl.journal_batch_id
              JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
             WHERE jb.entity_id = :e
               AND jb.status = ANY(:statuses)
               AND ap.period_end >= :start AND ap.period_end <= :end
               AND LEFT(jl.account_code, 1) IN ('4','5','6','7','8','9')
          GROUP BY jl.account_code
            """
        ),
        {"e": entity_id, "statuses": list(_PNL_STATUSES), "start": start, "end": end},
    ).mappings().all()
    interest_codes = set(roles.get("interest_expense", []))
    tax_codes = set(roles.get("income_tax_expense", []))
    da_codes = set(roles.get("depreciation_amortization", []))
    revenue = cogs = opex = other = Decimal("0")
    interest = tax = da = dgip = Decimal("0")
    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        dr, cr = _D(r["sum_debit"]), _D(r["sum_credit"])
        amt = _signed_amount_by_type(t, dr, cr)
        if t == "revenue":
            revenue += amt
        elif t == "cogs":
            cogs += amt
        elif t == "operating_expense":
            opex += amt
        elif t == "other_income_expense":
            other += amt
        if code in interest_codes:
            interest += (dr - cr)
        if code in tax_codes:
            tax += (dr - cr)
        if code in da_codes:
            da += (dr - cr)
        if code == "7000":
            dgip += (cr - dr)
    gross_profit = revenue - cogs
    net_income = gross_profit - opex + other
    ebitda = net_income + interest + tax + da
    return {
        "revenue": float(revenue), "cogs": float(cogs), "gross_profit": float(gross_profit),
        "opex": float(opex), "other_income": float(other), "net_income": float(net_income),
        "interest_expense": float(interest), "income_tax_expense": float(tax),
        "depreciation_amortization": float(da), "dgip_forgiveness": float(dgip),
        "ebit": float(ebitda - da), "ebitda": float(ebitda),
        "ebitda_excl_dgip": float(ebitda - dgip),
    }


def _bs_asof(session, entity_id, as_of, roles) -> dict[str, float]:
    """Adjusted balance sheet as-of a date (cutover-aware) with the
    2520/2525 equity reclass and the cash-overdraft floor."""
    cash_codes = set(roles.get("cash", []))
    ar_codes = set(roles.get("accounts_receivable", []))
    inv_codes = set(roles.get("inventory", []))
    ap_codes = set(roles.get("accounts_payable", []))
    debt_codes = set(roles.get("interest_bearing_debt", []))
    reclass_codes = set(roles.get("equity_reclass", []))

    rows = _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=as_of)
    current_assets = fixed_assets = current_liab = lt_liab = equity = Decimal("0")
    ar = inventory = ap = total_debt = reclass_signed = Decimal("0")
    pnl_to_date = cash_floored = overdraft = Decimal("0")
    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        dr, cr = _D(r["sum_debit"]), _D(r["sum_credit"])
        if t == "asset":
            bal = dr - cr
            if _bs_subclass(code) == "fixed":
                fixed_assets += bal
            else:
                current_assets += bal
        elif t == "liability":
            bal = cr - dr
            if _bs_subclass(code) == "long_term":
                lt_liab += bal
            else:
                current_liab += bal
        elif t == "equity":
            equity += (cr - dr)
        if t == "revenue":
            pnl_to_date += (cr - dr)
        elif t in ("cogs", "operating_expense"):
            pnl_to_date -= (dr - cr)
        elif t == "other_income_expense":
            pnl_to_date += (cr - dr)
        if code in cash_codes:
            b = dr - cr
            cash_floored += max(b, Decimal("0"))
            if b < 0:
                overdraft += -b
        if code in ar_codes:
            ar += (dr - cr)
        if code in inv_codes:
            inventory += (dr - cr)
        if code in ap_codes:
            ap += (cr - dr)
        if code in debt_codes:
            total_debt += (cr - dr)
        if code in reclass_codes:
            reclass_signed += (cr - dr)
    equity += pnl_to_date
    # A: 2520/2525 liabilities -> equity (signed)
    lt_liab -= reclass_signed
    equity += reclass_signed
    # B: cash overdraft floor
    current_assets += overdraft
    current_liab += overdraft
    total_debt += overdraft
    total_liabilities = current_liab + lt_liab
    total_assets = current_assets + fixed_assets
    return {
        "cash": float(cash_floored), "accounts_receivable": float(ar),
        "inventory": float(inventory), "accounts_payable": float(ap),
        "current_assets": float(current_assets), "fixed_assets": float(fixed_assets),
        "total_assets": float(total_assets), "current_liabilities": float(current_liab),
        "long_term_liabilities": float(lt_liab), "total_liabilities": float(total_liabilities),
        "total_equity": float(equity), "total_debt": float(total_debt),
        "tangible_net_worth": float(equity),
        "working_capital": float(current_assets - current_liab),
        "overdraft_reclassified": float(overdraft),
        "equity_reclassified": float(reclass_signed),
        "balances_balanced": abs(total_assets - (total_liabilities + equity)) < 0.01,
    }


def derive_annual_debt_service(session, entity_id, period_end: DateType) -> dict[str, float]:
    """Trailing-12 debt service from the GL (posted+historical_import):
    principal repayments on the term loan (debits to 2500) + total interest
    expense (6250+6270+6280+6285). Returns the breakdown + the total."""
    try:
        ttm_start = period_end.replace(year=period_end.year - 1)
    except ValueError:
        ttm_start = period_end.replace(year=period_end.year - 1, day=28)
    from datetime import timedelta
    ttm_start = ttm_start + timedelta(days=1)

    def _sum(expr: str, code: str) -> Decimal:
        row = session.execute(
            text(
                f"""
                SELECT COALESCE(SUM({expr}), 0) AS v
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                  JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                 WHERE jb.entity_id = :e AND jb.status = ANY(:statuses)
                   AND jl.account_code = :code
                   AND ap.period_end >= :start AND ap.period_end <= :end
                """
            ),
            {"e": entity_id, "statuses": list(_PNL_STATUSES), "code": code,
             "start": ttm_start, "end": period_end},
        ).mappings().first()
        return _D((row or {}).get("v"))

    term_principal = _sum("jl.debit_amount", "2500")          # repayments = debits to the loan
    term_interest = _sum("jl.debit_amount - jl.credit_amount", "6280")
    operating_interest = _sum("jl.debit_amount - jl.credit_amount", "6270")
    other_interest = (
        _sum("jl.debit_amount - jl.credit_amount", "6250")
        + _sum("jl.debit_amount - jl.credit_amount", "6285")
    )
    total_interest = term_interest + operating_interest + other_interest
    ads = term_principal + total_interest
    return {
        "ttm_start": ttm_start.isoformat(),
        "ttm_end": period_end.isoformat(),
        "term_principal": float(term_principal),
        "term_interest_6280": float(term_interest),
        "operating_interest_6270": float(operating_interest),
        "other_interest_6250_6285": float(other_interest),
        "total_interest": float(total_interest),
        "annual_debt_service": float(ads),
        "term_piece": float(term_principal + term_interest),  # cross-check ~ $60k/yr
    }


def resolve_annual_debt_service(session, entity_id, period_end: DateType) -> tuple[float, str, dict]:
    """Return (value, source, breakdown). Uses an admin override from
    entity_ratio_inputs if present, else the GL-derived figure."""
    breakdown = derive_annual_debt_service(session, entity_id, period_end)
    override = session.execute(
        text("SELECT value FROM entity_ratio_inputs WHERE entity_id=:e AND key='annual_debt_service'"),
        {"e": entity_id},
    ).scalar()
    if override is not None:
        return float(override), "override", breakdown
    return breakdown["annual_debt_service"], "gl_derived", breakdown


def build_financials_context(
    session, *, entity_id: str, period_start: DateType, period_end: DateType,
    roles: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Period P&L (the month) + TTM P&L (trailing 12 months) + adjusted BS
    as-of period_end. Period flows drive margins; TTM flows drive leverage /
    coverage / return / efficiency ratios."""
    roles = roles or get_account_roles(session, entity_id)
    try:
        ttm_start = period_end.replace(year=period_end.year - 1)
    except ValueError:
        ttm_start = period_end.replace(year=period_end.year - 1, day=28)
    from datetime import timedelta
    ttm_start = ttm_start + timedelta(days=1)

    period = _pnl_window(session, entity_id, period_start, period_end, roles)
    ttm = _pnl_window(session, entity_id, ttm_start, period_end, roles)
    bs = _bs_asof(session, entity_id, period_end, roles)

    ctx: dict[str, Any] = {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "ttm_start": ttm_start.isoformat(),
        "ttm_end": period_end.isoformat(),
        "days": (period_end - period_start).days + 1,
    }
    ctx.update(period)
    ctx.update({f"ttm_{k}": v for k, v in ttm.items()})
    ctx.update(bs)
    return ctx


# --------------------------------------------------------------------------
# Built-in ratio library
# --------------------------------------------------------------------------

RATIO_META: dict[str, dict[str, str]] = {
    # liquidity
    "current_ratio": {"label": "Current ratio", "category": "Liquidity", "format": "ratio"},
    "quick_ratio": {"label": "Quick ratio", "category": "Liquidity", "format": "ratio"},
    "cash_ratio": {"label": "Cash ratio", "category": "Liquidity", "format": "ratio"},
    "working_capital": {"label": "Working capital", "category": "Liquidity", "format": "dollar"},
    # leverage / coverage
    "debt_to_equity": {"label": "Debt to equity", "category": "Leverage", "format": "ratio"},
    "debt_to_tnw": {"label": "Debt to tangible net worth", "category": "Leverage", "format": "ratio"},
    "debt_to_ebitda": {"label": "Total debt / EBITDA (TTM)", "category": "Leverage", "format": "ratio"},
    "debt_to_ebitda_excl_dgip": {"label": "Total debt / EBITDA (TTM, excl DGIP)", "category": "Leverage", "format": "ratio"},
    "interest_coverage": {"label": "Interest coverage (TTM)", "category": "Leverage", "format": "ratio"},
    "dscr": {"label": "DSCR (TTM)", "category": "Leverage", "format": "ratio"},
    "dscr_excl_dgip": {"label": "DSCR (TTM, excl DGIP)", "category": "Leverage", "format": "ratio"},
    "fixed_charge_coverage": {"label": "Fixed charge coverage", "category": "Leverage", "format": "ratio"},
    # profitability
    "gross_margin_pct": {"label": "Gross margin", "category": "Profitability", "format": "percent"},
    "operating_margin_pct": {"label": "Operating margin", "category": "Profitability", "format": "percent"},
    "net_margin_pct": {"label": "Net margin", "category": "Profitability", "format": "percent"},
    "ebitda_ttm": {"label": "EBITDA (TTM)", "category": "Profitability", "format": "dollar"},
    "ebitda_margin_pct": {"label": "EBITDA margin (TTM)", "category": "Profitability", "format": "percent"},
    "roa_pct": {"label": "Return on assets (TTM)", "category": "Profitability", "format": "percent"},
    "roe_pct": {"label": "Return on equity (TTM)", "category": "Profitability", "format": "percent"},
    # efficiency
    "inventory_turnover": {"label": "Inventory turnover (TTM)", "category": "Efficiency", "format": "ratio"},
    "days_inventory_outstanding": {"label": "Days inventory (DIO)", "category": "Efficiency", "format": "days"},
    "days_sales_outstanding": {"label": "Receivable days (DSO)", "category": "Efficiency", "format": "days"},
    "days_payable_outstanding": {"label": "Payable days (DPO)", "category": "Efficiency", "format": "days"},
    "cash_conversion_cycle": {"label": "Cash conversion cycle", "category": "Efficiency", "format": "days"},
    "asset_turnover": {"label": "Asset turnover (TTM)", "category": "Efficiency", "format": "ratio"},
    # retail / HH
    "gmroii": {"label": "GMROII (TTM)", "category": "Retail", "format": "ratio"},
}


def _div(n, d):
    if d in (None, 0) or n is None:
        return None
    return n / d


def compute_builtin_ratios(c: dict[str, Any], inputs: dict[str, float] | None = None) -> dict[str, float | None]:
    """All built-in ratios from a financials context. Balance items as-of
    period_end; margins on the period; leverage/coverage/returns/efficiency on
    TTM flows. Division by zero -> None."""
    inputs = inputs or {}
    ads = inputs.get("annual_debt_service")
    out: dict[str, float | None] = {}
    # liquidity
    out["current_ratio"] = _div(c["current_assets"], c["current_liabilities"])
    out["quick_ratio"] = _div(c["current_assets"] - c["inventory"], c["current_liabilities"])
    out["cash_ratio"] = _div(c["cash"], c["current_liabilities"])
    out["working_capital"] = c["working_capital"]
    # leverage / coverage
    out["debt_to_equity"] = _div(c["total_debt"], c["total_equity"])
    out["debt_to_tnw"] = _div(c["total_debt"], c["tangible_net_worth"])
    out["debt_to_ebitda"] = _div(c["total_debt"], c["ttm_ebitda"])
    out["debt_to_ebitda_excl_dgip"] = _div(c["total_debt"], c["ttm_ebitda_excl_dgip"])
    out["interest_coverage"] = _div(c["ttm_ebit"], c["ttm_interest_expense"])
    out["dscr"] = _div(c["ttm_ebitda"], ads)
    out["dscr_excl_dgip"] = _div(c["ttm_ebitda_excl_dgip"], ads)
    out["fixed_charge_coverage"] = None  # needs a lease/rent input — not yet mapped
    # profitability — TTM basis, the same window as EBITDA (one coherent basis)
    out["gross_margin_pct"] = _pct(_div(c["ttm_gross_profit"], c["ttm_revenue"]))
    out["operating_margin_pct"] = _pct(_div(c["ttm_gross_profit"] - c["ttm_opex"], c["ttm_revenue"]))
    out["net_margin_pct"] = _pct(_div(c["ttm_net_income"], c["ttm_revenue"]))
    out["ebitda_ttm"] = c["ttm_ebitda"]
    out["ebitda_margin_pct"] = _pct(_div(c["ttm_ebitda"], c["ttm_revenue"]))
    out["roa_pct"] = _pct(_div(c["ttm_net_income"], c["total_assets"]))
    out["roe_pct"] = _pct(_div(c["ttm_net_income"], c["total_equity"]))
    # efficiency (TTM flows)
    inv_turn = _div(c["ttm_cogs"], c["inventory"])
    out["inventory_turnover"] = inv_turn
    out["days_inventory_outstanding"] = _div(365.0, inv_turn) if inv_turn else None
    out["days_sales_outstanding"] = _mul(_div(c["accounts_receivable"], c["ttm_revenue"]), 365.0)
    out["days_payable_outstanding"] = _mul(_div(c["accounts_payable"], c["ttm_cogs"]), 365.0)
    dio, dso, dpo = (out["days_inventory_outstanding"], out["days_sales_outstanding"], out["days_payable_outstanding"])
    out["cash_conversion_cycle"] = (dio + dso - dpo) if None not in (dio, dso, dpo) else None
    out["asset_turnover"] = _div(c["ttm_revenue"], c["total_assets"])
    # retail / HH
    out["gmroii"] = _div(c["ttm_gross_profit"], c["inventory"])
    return out


def _pct(v):
    return None if v is None else v * 100.0


def _mul(v, k):
    return None if v is None else v * k
