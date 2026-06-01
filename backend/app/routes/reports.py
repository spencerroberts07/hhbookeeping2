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

from datetime import date as DateType, datetime as DateTimeType, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services_auth import require_role

router = APIRouter(prefix="/api/reports", tags=["reports"])


# Bridlewood fiscal year ends Sep 30 — months Oct-Dec roll into the
# next FY number (e.g., Oct 2025 is FY2026 P01). Hardcoded for now;
# per-entity FY config can come later.
_FY_END_MONTH = 9


def _fy_of(d: DateType) -> int:
    return d.year + 1 if d.month > _FY_END_MONTH else d.year


def _fy_start(fy: int) -> DateType:
    return DateType(fy - 1, _FY_END_MONTH + 1, 1)


def _last_day_of_month(d: DateType) -> DateType:
    nxt = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return nxt - timedelta(days=1)


def _first_day_of_month(d: DateType) -> DateType:
    return d.replace(day=1)


def _shift_by_months(d: DateType, months: int) -> DateType:
    """Add a signed number of months to d, snapping day to month end if
    overflowed (e.g. shifting Feb-28 back 12 months stays Feb-28)."""
    total = d.month - 1 + months
    year = d.year + total // 12
    month = total % 12 + 1
    last = _last_day_of_month(DateType(year, month, 1))
    day = min(d.day, last.day)
    return DateType(year, month, day)


def _fiscal_quarter_start(d: DateType) -> DateType:
    """Bridlewood quarters: Q1 Oct-Dec, Q2 Jan-Mar, Q3 Apr-Jun, Q4 Jul-Sep."""
    m = d.month
    if m in (10, 11, 12):
        return DateType(d.year, 10, 1)
    if m in (1, 2, 3):
        return DateType(d.year, 1, 1)
    if m in (4, 5, 6):
        return DateType(d.year, 4, 1)
    return DateType(d.year, 7, 1)


def _fiscal_quarter_number(d: DateType) -> int:
    m = d.month
    if m in (10, 11, 12):
        return 1
    if m in (1, 2, 3):
        return 2
    if m in (4, 5, 6):
        return 3
    return 4


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

    Account name resolution: the CoA `accounts` table (synced from
    QBO) is the authority. `gl_account_balances` is the fallback for
    codes that have GL activity but were never in a CoA sync. Final
    fallback is the bare code itself so callers never see a null.
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
            gl_names AS (
                SELECT DISTINCT ON (account_code)
                       account_code, account_name
                  FROM gl_account_balances
                 WHERE entity_id = :entity_id
              ORDER BY account_code, created_at DESC
            )
            SELECT s.account_code,
                   s.sum_debit,
                   s.sum_credit,
                   COALESCE(a.account_name, gl.account_name, s.account_code) AS account_name
              FROM sums s
         LEFT JOIN accounts a
                ON a.entity_id = :entity_id
               AND a.account_code = s.account_code
               AND a.is_active = TRUE
         LEFT JOIN gl_names gl
                ON gl.account_code = s.account_code
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
# Income Statement (enhanced — 4-column with preset date ranges)
# --------------------------------------------------------------------------


# Stricter status whitelist used by the enhanced income statement.
# Mirrors the dashboard's POSTED_BATCH_STATUSES — only counts journals
# that have been explicitly posted/approved.
_IS_BATCH_STATUSES = ("posted", "approved_to_post", "approved", "closed_locked")
# Periods must be closed (or in active close approval) to feed the IS.
_IS_PERIOD_STATUSES = ("closed_locked", "approved_to_close")


_VALID_PRESETS = (
    "month",
    "ytd",
    "rolling12",
    "qtd",
    "trailing3",
    "last6",
    "custom",
)


def _resolve_preset_range(
    preset: str,
    period_end: DateType | None,
    date_from: DateType | None,
    date_to: DateType | None,
) -> tuple[DateType, DateType, str, str]:
    """Returns (current_start, current_end, current_label, prior_label).
    Prior range is always the same span shifted back 12 months."""
    if preset == "custom":
        if not date_from or not date_to:
            raise HTTPException(400, "custom preset requires date_from and date_to")
        cur_start, cur_end = date_from, date_to
        label = f"{cur_start.isoformat()} → {cur_end.isoformat()}"
        prior_label = (
            f"{_shift_by_months(cur_start, -12).isoformat()} → "
            f"{_shift_by_months(cur_end, -12).isoformat()}"
        )
        return cur_start, cur_end, label, prior_label

    if period_end is None:
        raise HTTPException(400, f"{preset!r} preset requires period_end")

    pe = _last_day_of_month(period_end)
    month_name = pe.strftime("%b %Y")

    if preset == "month":
        cur_start = _first_day_of_month(pe)
        return cur_start, pe, month_name, _shift_by_months(pe, -12).strftime("%b %Y")

    if preset == "ytd":
        fy = _fy_of(pe)
        cur_start = _fy_start(fy)
        prior_start = _fy_start(fy - 1)
        prior_end = _shift_by_months(pe, -12)
        return (
            cur_start,
            pe,
            f"YTD FY{fy} → {pe.strftime('%b %Y')}",
            f"YTD FY{fy - 1} → {prior_end.strftime('%b %Y')}",
        )

    if preset == "rolling12":
        cur_start = _first_day_of_month(_shift_by_months(pe, -11))
        return (
            cur_start,
            pe,
            f"Rolling 12 mo ending {month_name}",
            f"Rolling 12 mo ending {_shift_by_months(pe, -12).strftime('%b %Y')}",
        )

    if preset == "qtd":
        cur_start = _fiscal_quarter_start(pe)
        q = _fiscal_quarter_number(pe)
        fy = _fy_of(pe)
        return (
            cur_start,
            pe,
            f"QTD Q{q} FY{fy} → {month_name}",
            f"QTD Q{q} FY{fy - 1} → {_shift_by_months(pe, -12).strftime('%b %Y')}",
        )

    if preset == "trailing3":
        cur_start = _first_day_of_month(_shift_by_months(pe, -2))
        return (
            cur_start,
            pe,
            f"Trailing 3 mo ending {month_name}",
            f"Trailing 3 mo ending {_shift_by_months(pe, -12).strftime('%b %Y')}",
        )

    if preset == "last6":
        cur_start = _first_day_of_month(_shift_by_months(pe, -5))
        return (
            cur_start,
            pe,
            f"Trailing 6 mo ending {month_name}",
            f"Trailing 6 mo ending {_shift_by_months(pe, -12).strftime('%b %Y')}",
        )

    raise HTTPException(400, f"unknown preset {preset!r}")


def _is_account_sums(
    session,
    *,
    entity_id: str,
    period_start: DateType,
    period_end: DateType,
) -> list[dict[str, Any]]:
    """Per-account debit/credit sums for the income statement.

    Tighter filter than `_account_sums`: requires periods in
    `_IS_PERIOD_STATUSES` and batches in `_IS_BATCH_STATUSES`.

    Account name resolution: the CoA `accounts` table (synced from
    QBO) is the authority. `gl_account_balances` is the fallback for
    codes that have GL activity but were never in a CoA sync.
    """
    rows = session.execute(
        text(
            """
            WITH sums AS (
                SELECT jl.account_code,
                       SUM(jl.debit_amount)  AS sum_debit,
                       SUM(jl.credit_amount) AS sum_credit
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                  JOIN accounting_periods ap ON ap.id = jb.accounting_period_id
                 WHERE jb.entity_id = :entity_id
                   AND ap.period_start >= :period_start
                   AND ap.period_end <= :period_end
                   AND ap.status = ANY(:period_statuses)
                   AND jb.status = ANY(:batch_statuses)
              GROUP BY jl.account_code
            ),
            gl_names AS (
                SELECT DISTINCT ON (account_code)
                       account_code, account_name
                  FROM gl_account_balances
                 WHERE entity_id = :entity_id
              ORDER BY account_code, created_at DESC
            )
            SELECT s.account_code,
                   s.sum_debit,
                   s.sum_credit,
                   COALESCE(a.account_name, gl.account_name, s.account_code) AS account_name,
                   a.parent_code,
                   a.account_type,
                   a.is_sub_account
              FROM sums s
         LEFT JOIN accounts a
                ON a.entity_id = :entity_id
               AND a.account_code = s.account_code
               AND a.is_active = TRUE
         LEFT JOIN gl_names gl
                ON gl.account_code = s.account_code
          ORDER BY s.account_code
            """
        ),
        {
            "entity_id": entity_id,
            "period_start": period_start,
            "period_end": period_end,
            "period_statuses": list(_IS_PERIOD_STATUSES),
            "batch_statuses": list(_IS_BATCH_STATUSES),
        },
    ).mappings().all()
    return [dict(r) for r in rows]


def _is_account_metadata(
    session,
    *,
    entity_id: str,
) -> list[dict[str, Any]]:
    """All active income-statement accounts for the entity, including
    parent group headers that have no GL activity in the period.

    Group headers (account_type containing the QBO "*Income" /
    "*Expense" / "Cost of Goods Sold" / "Other Income" categories)
    show up even when they have zero direct lines — children roll up
    under them.
    """
    rows = session.execute(
        text(
            """
            SELECT account_code,
                   account_name,
                   parent_code,
                   account_type,
                   account_subtype,
                   is_sub_account,
                   statement_type
              FROM accounts
             WHERE entity_id = :entity_id
               AND is_active = TRUE
               AND statement_type = 'income_statement'
          ORDER BY account_code
            """
        ),
        {"entity_id": entity_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _signed_amount_by_type(t: str, debit: Decimal, credit: Decimal) -> Decimal:
    """Convert raw debit/credit sums into the natural P&L sign.

    Revenue: credit-natural → credit - debit (positive = real revenue,
    negative = contra/refund).
    COGS / Opex: debit-natural → debit - credit (positive = expense).
    """
    if t == "revenue":
        return credit - debit
    if t in {"cogs", "operating_expense"}:
        return debit - credit
    if t == "other_income_expense":
        return credit - debit
    return Decimal("0")


def _pct(amount: Decimal, base: Decimal) -> float | None:
    if base == 0:
        return None
    return round(float(amount / base * 100), 1)


def _build_is_side(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate raw _is_account_sums rows into per-account natural-sign
    amounts grouped by section. No percentages yet — those are applied
    once we know total_revenue.

    Sections:
      revenue            (4xxx — credit-natural)
      cogs               (5xxx — debit-natural)
      operating_expenses (6xxx — debit-natural)
      other_income       (7xxx — credit-natural)

    Each row carries `parent_code` from the accounts CoA so the
    caller can roll children under their group header.
    """
    by_section: dict[str, list[dict[str, Any]]] = {
        "revenue": [],
        "cogs": [],
        "operating_expenses": [],
        "other_income": [],
    }
    for r in rows:
        code = r["account_code"]
        t = _account_type(code)
        if t == "revenue":
            section_key = "revenue"
        elif t == "cogs":
            section_key = "cogs"
        elif t == "operating_expense":
            section_key = "operating_expenses"
        elif t == "other_income_expense":
            section_key = "other_income"
        else:
            continue
        debit = Decimal(str(r["sum_debit"] or 0))
        credit = Decimal(str(r["sum_credit"] or 0))
        amount = _signed_amount_by_type(t, debit, credit)
        if amount == 0:
            continue
        by_section[section_key].append({
            "account_code": code,
            "account_name": r.get("account_name") or code,
            "parent_code": r.get("parent_code"),
            "amount": amount,
        })

    revenue_total = sum((a["amount"] for a in by_section["revenue"]), Decimal("0"))
    cogs_total = sum((a["amount"] for a in by_section["cogs"]), Decimal("0"))
    opex_total = sum((a["amount"] for a in by_section["operating_expenses"]), Decimal("0"))
    other_income_total = sum(
        (a["amount"] for a in by_section["other_income"]), Decimal("0"),
    )
    gross_profit = revenue_total - cogs_total
    net_income = gross_profit - opex_total + other_income_total

    return {
        "by_section": by_section,
        "revenue_total": revenue_total,
        "cogs_total": cogs_total,
        "opex_total": opex_total,
        "other_income_total": other_income_total,
        "gross_profit": gross_profit,
        "net_income": net_income,
    }


@router.get("/income-statement/periods")
def list_income_statement_periods(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Closed periods for the income-statement preset dropdown.

    Returns the periods that are eligible to anchor the report
    (closed_locked or approved_to_close) in descending date order.
    """
    with db_session() as session:
        entity = _resolve_entity(session, entity_code)
        rows = session.execute(
            text(
                """
                SELECT id, period_label, period_start, period_end,
                       status, fiscal_year, fiscal_period_number
                  FROM accounting_periods
                 WHERE entity_id = :eid
                   AND status = ANY(:statuses)
              ORDER BY period_end DESC
                """
            ),
            {"eid": entity["id"], "statuses": list(_IS_PERIOD_STATUSES)},
        ).mappings().all()
    return {
        "entity_code": entity_code,
        "periods": [
            {
                "period_label": r["period_label"],
                "period_start": r["period_start"].isoformat(),
                "period_end": r["period_end"].isoformat(),
                "status": r["status"],
                "fiscal_year": r["fiscal_year"],
                "fiscal_period_number": r["fiscal_period_number"],
            }
            for r in rows
        ],
    }


@router.get("/income-statement")
def get_income_statement(
    entity_code: str = Query(...),
    preset: str = Query(default="month"),
    period_end: str | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    if preset not in _VALID_PRESETS:
        raise HTTPException(400, f"preset must be one of {_VALID_PRESETS}")

    pe = _parse_date("period_end", period_end) if period_end else None
    df = _parse_date("date_from", date_from) if date_from else None
    dt = _parse_date("date_to", date_to) if date_to else None
    if dt and df and dt < df:
        raise HTTPException(400, "date_to must be on or after date_from")

    cur_start, cur_end, period_label, prior_label = _resolve_preset_range(
        preset, pe, df, dt,
    )
    prior_start = _shift_by_months(cur_start, -12)
    prior_end = _shift_by_months(cur_end, -12)

    with db_session() as session:
        entity = _resolve_entity(session, entity_code)
        cur_rows = _is_account_sums(
            session, entity_id=entity["id"],
            period_start=cur_start, period_end=cur_end,
        )
        prior_rows = _is_account_sums(
            session, entity_id=entity["id"],
            period_start=prior_start, period_end=prior_end,
        )
        accounts_meta = _is_account_metadata(session, entity_id=entity["id"])

    cur = _build_is_side(cur_rows)
    prior = _build_is_side(prior_rows)

    cur_rev_total = cur["revenue_total"]
    prior_rev_total = prior["revenue_total"]

    sections = _build_grouped_sections(
        cur=cur,
        prior=prior,
        accounts_meta=accounts_meta,
        cur_rev_total=cur_rev_total,
        prior_rev_total=prior_rev_total,
    )

    return {
        "entity_code": entity_code,
        "preset": preset,
        "period_label": period_label,
        "prior_label": prior_label,
        "period_start": cur_start.isoformat(),
        "period_end": cur_end.isoformat(),
        "prior_start": prior_start.isoformat(),
        "prior_end": prior_end.isoformat(),
        "total_revenue": float(cur_rev_total),
        "prior_revenue": float(prior_rev_total),
        "sections": sections,
    }


# --------------------------------------------------------------------------
# Income statement — group-header rollup
# --------------------------------------------------------------------------


# Section labels match QBO's P&L exactly so the report is recognisable
# side-by-side with the QBO export.
_SECTION_LABELS = {
    "revenue": "INCOME",
    "cogs": "COST OF GOODS SOLD",
    "operating_expenses": "EXPENSES",
    "other_income": "OTHER INCOME",
}


def _build_grouped_sections(
    *,
    cur: dict[str, Any],
    prior: dict[str, Any],
    accounts_meta: list[dict[str, Any]],
    cur_rev_total: Decimal,
    prior_rev_total: Decimal,
) -> list[dict[str, Any]]:
    """Render each IS section as a tree of group-header rows with
    indented children and a per-group subtotal. Group identity comes
    from `accounts.parent_code` — set by the QBO CoA sync. Accounts
    whose parent_code is null (or whose parent doesn't appear on this
    statement) render as standalone lines under the section.
    """
    meta_by_code: dict[str, dict[str, Any]] = {
        a["account_code"]: a for a in accounts_meta
    }

    def index_amounts(section: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {a["account_code"]: a for a in section}

    cur_by_sec = {
        k: index_amounts(cur["by_section"][k])
        for k in ("revenue", "cogs", "operating_expenses", "other_income")
    }
    prior_by_sec = {
        k: index_amounts(prior["by_section"][k])
        for k in ("revenue", "cogs", "operating_expenses", "other_income")
    }

    def build_section(key: str) -> dict[str, Any]:
        cur_idx = cur_by_sec[key]
        prior_idx = prior_by_sec[key]
        # All codes that have activity in either period.
        active_codes = sorted(set(cur_idx.keys()) | set(prior_idx.keys()))

        # Resolve each code's effective parent. A parent is "effective"
        # only when it lives in the meta map AND belongs to the same
        # section — otherwise the child rolls up as standalone.
        section_codes = set(active_codes)
        for code in active_codes:
            m = meta_by_code.get(code) or {}
            pc = m.get("parent_code")
            if pc and pc in meta_by_code:
                section_codes.add(pc)

        def amount_for(code: str, idx: dict[str, dict[str, Any]]) -> Decimal:
            row = idx.get(code)
            return row["amount"] if row else Decimal("0")

        def name_for(code: str) -> str:
            return (
                (meta_by_code.get(code) or {}).get("account_name")
                or (cur_idx.get(code) or {}).get("account_name")
                or (prior_idx.get(code) or {}).get("account_name")
                or code
            )

        # parent_code lookup that only honors same-section parents.
        def parent_of(code: str) -> str | None:
            pc = (meta_by_code.get(code) or {}).get("parent_code")
            if pc and pc in section_codes:
                return pc
            return None

        # Children index: parent_code → list[child_code], sorted.
        children: dict[str, list[str]] = {}
        for code in sorted(section_codes):
            pc = parent_of(code)
            if pc:
                children.setdefault(pc, []).append(code)

        # Top-level codes: accounts with no in-section parent.
        top_codes = sorted(
            code for code in section_codes if parent_of(code) is None
        )

        accounts_out: list[dict[str, Any]] = []

        def emit_node(code: str, depth: int) -> tuple[Decimal, Decimal]:
            """Recursively emit a node and return its (current, prior)
            rolled-up totals (self amount + descendants)."""
            self_cur = amount_for(code, cur_idx)
            self_prior = amount_for(code, prior_idx)
            kids = children.get(code, [])
            is_group = bool(kids)

            if is_group:
                # Group header — no amount on its own line.
                header_idx = len(accounts_out)
                accounts_out.append({
                    "account_code": code,
                    "account_name": name_for(code),
                    "depth": depth,
                    "is_group_header": True,
                    "current_amount": None,
                    "prior_amount": None,
                    "current_pct": None,
                    "prior_pct": None,
                })
                group_cur = self_cur
                group_prior = self_prior
                # Self-amount on a parent that also has direct activity:
                # emit it as a leaf row inside the group so it shows up.
                if self_cur != 0 or self_prior != 0:
                    accounts_out.append({
                        "account_code": code,
                        "account_name": name_for(code),
                        "depth": depth + 1,
                        "is_group_header": False,
                        "current_amount": float(self_cur),
                        "prior_amount": float(self_prior),
                        "current_pct": _pct(self_cur, cur_rev_total),
                        "prior_pct": _pct(self_prior, prior_rev_total),
                    })
                for kid in kids:
                    kc, kp = emit_node(kid, depth + 1)
                    group_cur += kc
                    group_prior += kp
                accounts_out.append({
                    "account_code": code,
                    "account_name": f"Total {code} {name_for(code)}",
                    "depth": depth,
                    "is_group_subtotal": True,
                    "current_amount": float(group_cur),
                    "prior_amount": float(group_prior),
                    "current_pct": _pct(group_cur, cur_rev_total),
                    "prior_pct": _pct(group_prior, prior_rev_total),
                })
                _ = header_idx  # reserved if we later want to attach the subtotal back to the header
                return group_cur, group_prior

            # Leaf account — no children. Skip when it has no activity
            # in either period (a CoA entry with zero movement).
            if self_cur == 0 and self_prior == 0:
                return Decimal("0"), Decimal("0")
            accounts_out.append({
                "account_code": code,
                "account_name": name_for(code),
                "depth": depth,
                "is_group_header": False,
                "current_amount": float(self_cur),
                "prior_amount": float(self_prior),
                "current_pct": _pct(self_cur, cur_rev_total),
                "prior_pct": _pct(self_prior, prior_rev_total),
            })
            return self_cur, self_prior

        section_total = Decimal("0")
        prior_total = Decimal("0")
        for code in top_codes:
            c, p = emit_node(code, 0)
            section_total += c
            prior_total += p

        return {
            "section": _SECTION_LABELS[key],
            "accounts": accounts_out,
            "section_total": float(section_total),
            "prior_total": float(prior_total),
            "section_pct": _pct(section_total, cur_rev_total),
            "prior_pct": _pct(prior_total, prior_rev_total),
        }

    revenue_section = build_section("revenue")
    cogs_section = build_section("cogs")
    opex_section = build_section("operating_expenses")
    other_section = build_section("other_income")

    sections: list[dict[str, Any]] = [
        revenue_section,
        cogs_section,
        {
            "section": "GROSS PROFIT",
            "accounts": [],
            "section_total": float(cur["gross_profit"]),
            "prior_total": float(prior["gross_profit"]),
            "section_pct": _pct(cur["gross_profit"], cur_rev_total),
            "prior_pct": _pct(prior["gross_profit"], prior_rev_total),
        },
        opex_section,
    ]
    # Only show OTHER INCOME when there's activity in either period —
    # it's noise for entities that don't use 7xxx.
    if (
        cur["other_income_total"] != 0
        or prior["other_income_total"] != 0
        or other_section["accounts"]
    ):
        sections.append(other_section)
    sections.append({
        "section": "PROFIT",
        "accounts": [],
        "section_total": float(cur["net_income"]),
        "prior_total": float(prior["net_income"]),
        "section_pct": _pct(cur["net_income"], cur_rev_total),
        "prior_pct": _pct(prior["net_income"], prior_rev_total),
    })
    return sections


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
