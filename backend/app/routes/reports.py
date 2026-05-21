"""
Live financial reports — Income Statement, Balance Sheet, Trial Balance.

Data source: journal_lines joined to journal_batches joined to
accounting_periods. Date scoping uses accounting_periods.period_end
because journal_lines has no posting_date column — the canonical period
membership IS the line's date for accounting purposes.

Account names: pulled from gl_account_balances when the entity has at
least one GL import. Falls back to the account_code itself when no QBO
name has been imported.

Account classification (account_type + normal_balance): derived from the
4-digit account-code prefix per the Bridlewood / HH dealer chart
convention:

    1xxx  asset           normal debit
    2xxx  liability       normal credit
    3xxx  equity          normal credit
    4xxx  revenue         normal credit
    5xxx  COGS            normal debit
    6xxx  operating exp.  normal debit
    7xxx  other inc/exp   normal credit  (special-cased: 7000 DGIP
                                          forgiveness is credit-natural;
                                          confirmed by the existing TB
                                          flip-prefix rule in commit
                                          069b12e)

Posted-only filter: we only sum lines from batches that have advanced
past 'draft' (status NOT IN ('draft','voided','rejected')). Anything in
draft hasn't been agreed-to yet and shouldn't move report numbers.
"""
from __future__ import annotations

from datetime import date as DateType, datetime as DateTimeType
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services_auth import require_role

router = APIRouter(prefix="/api/reports", tags=["reports"])


# --------------------------------------------------------------------------
# Account classification
# --------------------------------------------------------------------------


def _prefix(account_code: str) -> str:
    return (account_code or "").strip()[:1]


def _account_type(account_code: str) -> str:
    p = _prefix(account_code)
    return {
        "1": "asset",
        "2": "liability",
        "3": "equity",
        "4": "revenue",
        "5": "cogs",
        "6": "operating_expense",
        "7": "other_income_expense",
        "8": "other_income_expense",
        "9": "other_income_expense",
    }.get(p, "other")


def _normal_balance(account_code: str) -> str:
    """debit or credit per the prefix convention."""
    p = _prefix(account_code)
    if p in {"1", "5", "6"}:
        return "debit"
    if p in {"2", "3", "4"}:
        return "credit"
    if p in {"7", "8", "9"}:
        # 7xxx is mixed — 7000 DGIP forgiveness is credit-natural. We default
        # 7xxx to credit because that's what the TB flip-prefix rule does.
        return "credit"
    return "debit"


def _bs_subclass(account_code: str) -> str:
    """current vs fixed assets / current vs long-term liabilities.
    Convention: 10xx-14xx current, 15xx-19xx fixed; 20xx-24xx current
    liabilities, 25xx-29xx long-term liabilities."""
    try:
        num = int((account_code or "").strip()[:2])
    except ValueError:
        return "current"
    if 10 <= num <= 14:
        return "current"
    if 15 <= num <= 19:
        return "fixed"
    if 20 <= num <= 24:
        return "current"
    if 25 <= num <= 29:
        return "long_term"
    return "current"


# --------------------------------------------------------------------------
# Shared query: per-account sums for a date range
# --------------------------------------------------------------------------


# Lines from any batch that isn't draft/voided/rejected are "real" for
# reporting purposes. This matches what the existing TB-compare and the
# month-end-workflow flow consider "posted" for variance reporting.
_POSTED_BATCH_STATUSES = ("draft", "voided", "rejected")


def _account_sums(
    session,
    *,
    entity_id: str,
    period_end_from: DateType | None,
    period_end_to: DateType,
) -> list[dict[str, Any]]:
    """
    Returns one row per account_code with `sum_debit` and `sum_credit` over
    every journal_line whose parent batch falls in
    `(period_end_from, period_end_to]`. When `period_end_from` is None this
    is a cumulative-to-date query — the natural shape for a balance sheet
    or trial balance.

    Each row also carries the latest known `account_name` from
    gl_account_balances (most recent import), if any.
    """
    where = [
        "jb.entity_id = :entity_id",
        "ap.period_end <= :period_end_to",
        f"jb.status NOT IN {_POSTED_BATCH_STATUSES}",
    ]
    params: dict[str, Any] = {
        "entity_id": entity_id,
        "period_end_to": period_end_to,
    }
    if period_end_from is not None:
        # Inclusive lower bound — IS uses [from, to].
        where.append("ap.period_end >= :period_end_from")
        params["period_end_from"] = period_end_from

    rows = session.execute(
        text(
            f"""
            WITH sums AS (
                SELECT jl.account_code,
                       SUM(jl.debit_amount)  AS sum_debit,
                       SUM(jl.credit_amount) AS sum_credit
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                  JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                 WHERE {' AND '.join(where)}
              GROUP BY jl.account_code
            ),
            names AS (
                SELECT DISTINCT ON (account_code)
                       account_code, account_name
                  FROM gl_account_balances
                 WHERE entity_id = :entity_id
              ORDER BY account_code, created_at DESC
            )
            SELECT s.account_code,
                   s.sum_debit,
                   s.sum_credit,
                   n.account_name
              FROM sums s
         LEFT JOIN names n ON n.account_code = s.account_code
          ORDER BY s.account_code
            """
        ),
        params,
    ).mappings().all()
    return [dict(r) for r in rows]


def _resolve_entity(session, entity_code: str) -> dict[str, Any]:
    entity = session.execute(
        text("SELECT id, entity_code, entity_name FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not entity:
        raise HTTPException(404, f"Entity {entity_code!r} not found")
    return dict(entity)


def _parse_date(name: str, value: str) -> DateType:
    try:
        return DateTimeType.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(400, f"{name} must be YYYY-MM-DD") from exc


# --------------------------------------------------------------------------
# Income Statement
# --------------------------------------------------------------------------


@router.get("/income-statement")
def get_income_statement(
    entity_code: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    compare_to: str | None = Query(default=None, pattern="^(prior_period|prior_year)$"),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    period_start = _parse_date("date_from", date_from)
    period_end = _parse_date("date_to", date_to)
    if period_end < period_start:
        raise HTTPException(400, "date_to must be on or after date_from")

    with db_session() as session:
        entity = _resolve_entity(session, entity_code)
        primary = _build_income_statement(session, entity["id"], period_start, period_end)

        comparison: dict[str, Any] | None = None
        if compare_to == "prior_period":
            span = (period_end - period_start).days + 1
            from datetime import timedelta
            prior_end = period_start - timedelta(days=1)
            prior_start = prior_end - timedelta(days=span - 1)
            comparison = _build_income_statement(session, entity["id"], prior_start, prior_end)
        elif compare_to == "prior_year":
            from datetime import timedelta
            prior_start = period_start.replace(year=period_start.year - 1)
            prior_end = period_end.replace(year=period_end.year - 1)
            comparison = _build_income_statement(session, entity["id"], prior_start, prior_end)

    return {
        "entity_code": entity_code,
        "period": {"from": period_start.isoformat(), "to": period_end.isoformat()},
        **primary,
        "comparison": comparison,
    }


def _build_income_statement(
    session, entity_id: str, period_start: DateType, period_end: DateType,
) -> dict[str, Any]:
    rows = _account_sums(
        session,
        entity_id=entity_id,
        period_end_from=period_start,
        period_end_to=period_end,
    )
    revenue: list[dict[str, Any]] = []
    cogs: list[dict[str, Any]] = []
    operating_expenses: list[dict[str, Any]] = []
    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        debit = Decimal(str(r["sum_debit"] or 0))
        credit = Decimal(str(r["sum_credit"] or 0))
        name = r.get("account_name") or code
        if t == "revenue":
            amount = credit - debit  # credits increase revenue
            if amount != 0:
                revenue.append({"account_code": code, "account_name": name, "amount": float(amount)})
        elif t == "cogs":
            amount = debit - credit  # debits increase COGS
            if amount != 0:
                cogs.append({"account_code": code, "account_name": name, "amount": float(amount)})
        elif t == "operating_expense":
            amount = debit - credit
            if amount != 0:
                operating_expenses.append({"account_code": code, "account_name": name, "amount": float(amount)})

    revenue_total = sum(Decimal(str(r["amount"])) for r in revenue)
    cogs_total = sum(Decimal(str(r["amount"])) for r in cogs)
    gross_profit = revenue_total - cogs_total
    opex_total = sum(Decimal(str(r["amount"])) for r in operating_expenses)
    net_income = gross_profit - opex_total
    gross_margin_pct = (
        float((gross_profit / revenue_total) * 100) if revenue_total else None
    )
    return {
        "revenue": revenue,
        "revenue_total": float(revenue_total),
        "cogs": cogs,
        "cogs_total": float(cogs_total),
        "gross_profit": float(gross_profit),
        "gross_margin_pct": gross_margin_pct,
        "operating_expenses": operating_expenses,
        "operating_expenses_total": float(opex_total),
        "net_income": float(net_income),
    }


# --------------------------------------------------------------------------
# Balance Sheet
# --------------------------------------------------------------------------


@router.get("/balance-sheet")
def get_balance_sheet(
    entity_code: str = Query(...),
    as_of_date: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    as_of = _parse_date("as_of_date", as_of_date)
    with db_session() as session:
        entity = _resolve_entity(session, entity_code)
        rows = _account_sums(
            session,
            entity_id=entity["id"],
            period_end_from=None,
            period_end_to=as_of,
        )

    assets_current: list[dict[str, Any]] = []
    assets_fixed: list[dict[str, Any]] = []
    liab_current: list[dict[str, Any]] = []
    liab_long_term: list[dict[str, Any]] = []
    equity_accounts: list[dict[str, Any]] = []

    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        debit = Decimal(str(r["sum_debit"] or 0))
        credit = Decimal(str(r["sum_credit"] or 0))
        name = r.get("account_name") or code

        if t == "asset":
            balance = debit - credit  # asset = debit-natural
            if balance == 0:
                continue
            entry = {"account_code": code, "account_name": name, "balance": float(balance)}
            (assets_fixed if _bs_subclass(code) == "fixed" else assets_current).append(entry)
        elif t == "liability":
            balance = credit - debit  # liability = credit-natural
            if balance == 0:
                continue
            entry = {"account_code": code, "account_name": name, "balance": float(balance)}
            (liab_long_term if _bs_subclass(code) == "long_term" else liab_current).append(entry)
        elif t == "equity":
            balance = credit - debit
            if balance == 0:
                continue
            equity_accounts.append({"account_code": code, "account_name": name, "balance": float(balance)})

    # Retained earnings: net income to date is part of equity. We add the
    # running net income (revenue - cogs - opex to date) into equity as a
    # synthetic "Retained Earnings (computed)" line. Without this, the BS
    # will be out of balance whenever there are P&L movements — which is
    # always, in practice.
    pnl_net = Decimal("0")
    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        debit = Decimal(str(r["sum_debit"] or 0))
        credit = Decimal(str(r["sum_credit"] or 0))
        if t == "revenue":
            pnl_net += (credit - debit)
        elif t in {"cogs", "operating_expense"}:
            pnl_net -= (debit - credit)
        elif t == "other_income_expense":
            # Default credit-natural per the prefix rule.
            pnl_net += (credit - debit)
    if pnl_net != 0:
        equity_accounts.append({
            "account_code": "RE",
            "account_name": "Retained Earnings (computed)",
            "balance": float(pnl_net),
        })

    assets_current.sort(key=lambda a: a["account_code"])
    assets_fixed.sort(key=lambda a: a["account_code"])
    liab_current.sort(key=lambda a: a["account_code"])
    liab_long_term.sort(key=lambda a: a["account_code"])
    equity_accounts.sort(key=lambda a: a["account_code"])

    assets_current_total = sum(Decimal(str(a["balance"])) for a in assets_current)
    assets_fixed_total = sum(Decimal(str(a["balance"])) for a in assets_fixed)
    assets_total = assets_current_total + assets_fixed_total

    liab_current_total = sum(Decimal(str(a["balance"])) for a in liab_current)
    liab_long_term_total = sum(Decimal(str(a["balance"])) for a in liab_long_term)
    liab_total = liab_current_total + liab_long_term_total

    equity_total = sum(Decimal(str(a["balance"])) for a in equity_accounts)
    le_total = liab_total + equity_total
    variance = assets_total - le_total
    balanced = abs(variance) < Decimal("0.01")

    return {
        "entity_code": entity_code,
        "as_of_date": as_of.isoformat(),
        "assets": {
            "current": assets_current,
            "current_total": float(assets_current_total),
            "fixed": assets_fixed,
            "fixed_total": float(assets_fixed_total),
            "total": float(assets_total),
        },
        "liabilities": {
            "current": liab_current,
            "current_total": float(liab_current_total),
            "long_term": liab_long_term,
            "long_term_total": float(liab_long_term_total),
            "total": float(liab_total),
        },
        "equity": {
            "accounts": equity_accounts,
            "total": float(equity_total),
        },
        "liabilities_and_equity_total": float(le_total),
        "balanced": balanced,
        "variance": float(variance),
    }


# --------------------------------------------------------------------------
# Trial Balance
# --------------------------------------------------------------------------


@router.get("/trial-balance")
def get_trial_balance(
    entity_code: str = Query(...),
    as_of_date: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    as_of = _parse_date("as_of_date", as_of_date)
    with db_session() as session:
        entity = _resolve_entity(session, entity_code)
        rows = _account_sums(
            session,
            entity_id=entity["id"],
            period_end_from=None,
            period_end_to=as_of,
        )

    accounts: list[dict[str, Any]] = []
    total_debits = Decimal("0")
    total_credits = Decimal("0")
    for r in rows:
        code = r["account_code"]
        debit = Decimal(str(r["sum_debit"] or 0))
        credit = Decimal(str(r["sum_credit"] or 0))
        net = debit - credit
        name = r.get("account_name") or code
        t = _account_type(code)
        normal = _normal_balance(code)
        # Net balance is debit-positive. Flag when the sign disagrees with
        # the expected normal balance — that's the "unexpected balance" UI
        # the spec asks for.
        unexpected = False
        if normal == "debit" and net < 0:
            unexpected = True
        elif normal == "credit" and net > 0:
            unexpected = True

        accounts.append({
            "account_code": code,
            "account_name": name,
            "account_type": t,
            "normal_balance": normal,
            "total_debits": float(debit),
            "total_credits": float(credit),
            "net_balance": float(net),
            "unexpected_balance": unexpected,
        })
        total_debits += debit
        total_credits += credit

    difference = total_debits - total_credits
    balanced = abs(difference) < Decimal("0.01")
    return {
        "entity_code": entity_code,
        "as_of_date": as_of.isoformat(),
        "accounts": accounts,
        "totals": {
            "total_debits": float(total_debits),
            "total_credits": float(total_credits),
            "difference": float(difference),
        },
        "balanced": balanced,
    }


# --------------------------------------------------------------------------
# Live General Ledger (G1)
#
# Replaces the gl-import-runs fallback the frontend used to call. Queries
# journal_lines directly, scoped by account_code + date range, and emits
# a running balance per row.
#
# Same posted-only filter as the other reports — only non-draft batches
# count.
# --------------------------------------------------------------------------


@router.get("/general-ledger")
def get_general_ledger_report(
    entity_code: str = Query(...),
    account_code: str = Query(...),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    df = _parse_date("date_from", date_from) if date_from else None
    dt = _parse_date("date_to", date_to) if date_to else None

    with db_session() as session:
        entity = _resolve_entity(session, entity_code)

        # Opening balance = net of all lines on this account in periods
        # whose period_end is strictly before date_from. None when no
        # date_from supplied — opening is just 0.
        opening = Decimal("0")
        if df is not None:
            row = session.execute(
                text(
                    """
                    SELECT COALESCE(
                        SUM(jl.debit_amount - jl.credit_amount), 0
                    ) AS net
                      FROM journal_lines jl
                      JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                      JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                     WHERE jb.entity_id = :eid
                       AND jl.account_code = :code
                       AND jb.status NOT IN ('draft', 'voided', 'rejected')
                       AND ap.period_end < :df
                    """
                ),
                {"eid": entity["id"], "code": account_code, "df": df},
            ).mappings().first()
            opening = Decimal(str((row or {}).get("net") or 0))

        # Pull transactions within the window, ordered by period_end +
        # batch + line_number for a stable running-balance walk.
        rows = session.execute(
            text(
                """
                SELECT jl.id,
                       ap.period_end       AS posting_date,
                       jb.source_module,
                       jb.batch_label,
                       jl.line_number,
                       jl.memo,
                       jl.debit_amount,
                       jl.credit_amount,
                       jl.source_json
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                  JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                 WHERE jb.entity_id = :eid
                   AND jl.account_code = :code
                   AND jb.status NOT IN ('draft', 'voided', 'rejected')
                   AND (:df IS NULL OR ap.period_end >= :df)
                   AND (:dt IS NULL OR ap.period_end <= :dt)
                 ORDER BY ap.period_end, jb.batch_label, jl.line_number
                """
            ),
            {
                "eid": entity["id"],
                "code": account_code,
                "df": df,
                "dt": dt,
            },
        ).mappings().all()

        # Resolve a friendly account name (gl_account_balances if seen).
        name_row = session.execute(
            text(
                """
                SELECT account_name FROM gl_account_balances
                 WHERE entity_id = :eid AND account_code = :code
                 ORDER BY created_at DESC LIMIT 1
                """
            ),
            {"eid": entity["id"], "code": account_code},
        ).mappings().first()
        account_name = (name_row or {}).get("account_name") or account_code

    running = opening
    transactions: list[dict[str, Any]] = []
    for r in rows:
        dr = Decimal(str(r["debit_amount"] or 0))
        cr = Decimal(str(r["credit_amount"] or 0))
        running += dr - cr
        sj = r.get("source_json") or {}
        reference = (
            (sj.get("reference_number") if isinstance(sj, dict) else None)
            or r["batch_label"]
        )
        description = r["memo"] or r["source_module"]
        transactions.append({
            "id": str(r["id"]),
            "posting_date": r["posting_date"].isoformat(),
            "description": description,
            "reference": reference,
            "debit": float(dr),
            "credit": float(cr),
            "balance": float(running),
            "source_module": r["source_module"],
        })

    return {
        "entity_code": entity_code,
        "account_code": account_code,
        "account_name": account_name,
        "date_from": df.isoformat() if df else None,
        "date_to": dt.isoformat() if dt else None,
        "opening_balance": float(opening),
        "closing_balance": float(running),
        "transactions": transactions,
        "transaction_count": len(transactions),
    }
