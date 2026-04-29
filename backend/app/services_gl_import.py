"""
GL Import + Trial Balance Comparison — service layer.

Reads QuickBooks Online General Ledger exports (xlsx) and stores:
    - gl_import_runs        envelope row per upload
    - gl_account_balances   beginning / activity / ending per account
    - gl_transactions       per-line GL transaction detail
    - gl_trial_balance_comparisons
                            cross-walks GL activity vs the app's
                            journal_lines for the same period

Public functions:
    parse_gl_xlsx(file_bytes)
    import_gl(...)
    build_trial_balance_comparison(...)
    get_trial_balance_comparison(...)
    list_gl_import_runs(...)
    get_gl_import_run_detail(...)
    get_gl_account_transactions(...)
    section_gl_import(...)
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
    get_or_create_accounting_period,
)


REPORT_TYPE_GL = "general_ledger"
VARIANCE_TOLERANCE = Decimal("0.05")  # within 5 cents = no variance flag

# QBO General Ledger reports use a "natural-side positive" sign
# convention for credit-natural account classes: liabilities (2xxx)
# and revenue (4xxx). Activity to those accounts is presented with
# CREDIT → positive, DEBIT → negative.
#
# The app's journal_lines use the standard debit-positive /
# credit-negative convention. So `(debit_amount - credit_amount)`
# applied to a 2xxx or 4xxx account produces the OPPOSITE sign from
# what QBO prints.
#
# Verified against the live Bridlewood Feb 2026 GL:
#   * 4010 Sales — Merchandise: GL period_activity = +144,314.71
#     (revenue earned, a credit). App net would be -144,314.71.
#   * 2500 Term Loan: GL period_activity = -5,109.66 (principal
#     reduction, a debit). App net would be +5,109.66.
#
# To compare apples to apples we negate the app's net activity for
# accounts whose code begins with any prefix in this set before
# computing the variance.
_FLIP_APP_SIGN_PREFIXES = ("2", "4")


# ----------------------------------------------------------------------
# Numeric / date helpers
# ----------------------------------------------------------------------


_RE_ACCOUNT_HEADER = re.compile(r"^\s*(\d{4})\s+(.+?)\s*$")
_RE_PERIOD_TEXT = re.compile(
    r"([A-Za-z]+)\s+(\d{1,2})\s*-\s*(\d{1,2}),?\s*(\d{4})"
)
_RE_DATE_DMY = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return Decimal("0")
    s = str(value).strip().replace(",", "").replace("$", "")
    if not s:
        return Decimal("0")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def _money(value: Any) -> Decimal:
    return _to_decimal(value).quantize(Decimal("0.01"))


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    m = _RE_DATE_DMY.match(s)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # QBO Canadian default = DD/MM/YYYY. If the first field exceeds 12
        # we know it's a day (DD/MM/YYYY); otherwise still assume DD/MM/YYYY
        # because that's how the Bridlewood GL is exported.
        try:
            return date(y, mth, d)
        except ValueError:
            try:
                return date(y, d, mth)
            except ValueError:
                return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _parse_period_text(value: Any) -> tuple[date | None, date | None]:
    if value is None:
        return None, None
    s = str(value).strip()
    m = _RE_PERIOD_TEXT.search(s)
    if not m:
        return None, None
    month_name, day_start, day_end, year = m.groups()
    month_idx = _MONTH_NAMES.get(month_name.lower())
    if not month_idx:
        return None, None
    try:
        return (
            date(int(year), month_idx, int(day_start)),
            date(int(year), month_idx, int(day_end)),
        )
    except ValueError:
        return None, None


# ----------------------------------------------------------------------
# Parser
# ----------------------------------------------------------------------


def parse_gl_xlsx(file_bytes: bytes) -> dict[str, Any]:
    """
    Parse a QBO General Ledger xlsx export.

    Returns:
        {
            "title": str | None,
            "entity_name": str | None,
            "period_text": str | None,
            "period_start": date | None,
            "period_end": date | None,
            "accounts": [
                {
                    "account_code": str,
                    "account_name": str,
                    "beginning_balance": Decimal,
                    "period_activity": Decimal,
                    "ending_balance": Decimal,
                    "transactions": [
                        {
                            "transaction_date": date | None,
                            "transaction_type": str | None,
                            "reference_number": str | None,
                            "name": str | None,
                            "memo": str | None,
                            "split_account": str | None,
                            "amount": Decimal,
                            "running_balance": Decimal,
                        },
                        ...
                    ],
                },
                ...
            ],
            "warnings": [str, ...],
        }
    """
    try:
        import openpyxl  # noqa: WPS433
    except ImportError as exc:
        raise ValueError(
            "openpyxl is required to parse GL xlsx files. "
            "Install it: pip install openpyxl"
        ) from exc

    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(file_bytes), data_only=True, read_only=True
        )
    except Exception as exc:
        raise ValueError(f"Could not open xlsx: {exc}") from exc

    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    title = rows[0][0] if len(rows) > 0 else None
    entity_name = rows[1][0] if len(rows) > 1 else None
    period_text = rows[2][0] if len(rows) > 2 else None
    period_start, period_end = _parse_period_text(period_text)

    warnings: list[str] = []
    accounts: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def _commit_current() -> None:
        if current is None:
            return
        # Compute ending balance if not pinned by header rollup.
        current["ending_balance"] = (
            current["beginning_balance"] + current["period_activity"]
        )
        accounts.append(current)

    for r_idx, row in enumerate(rows):
        if not row:
            continue
        col_a = row[0] if len(row) > 0 else None
        col_b = row[1] if len(row) > 1 else None

        # Account section header — column A like "1010 Cash Float"
        if col_a is not None:
            m_acct = _RE_ACCOUNT_HEADER.match(str(col_a))
            if m_acct:
                _commit_current()
                current = {
                    "account_code": m_acct.group(1).strip(),
                    "account_name": m_acct.group(2).strip(),
                    "beginning_balance": Decimal("0"),
                    "period_activity": Decimal("0"),
                    "ending_balance": Decimal("0"),
                    "transactions": [],
                }
                continue

            stripped_a = str(col_a).strip()
            if stripped_a.lower().startswith("total for"):
                # "Total for 1010 Cash Float" → period activity in col I (idx 8)
                # "Total for 1090 ... with sub-accounts" → parent rollup, skip
                if "with sub-accounts" in stripped_a.lower():
                    continue
                if current is None:
                    continue
                amt = row[8] if len(row) > 8 else None
                current["period_activity"] = _money(amt)
                # Closing balance also appears in col J for some QBO exports;
                # we recompute from beginning + activity in _commit_current.
                continue

        # Beginning balance row — column B == "Beginning Balance"
        if col_b is not None:
            stripped_b = str(col_b).strip()
            if stripped_b.lower() == "beginning balance":
                if current is None:
                    continue
                bal = row[9] if len(row) > 9 else None
                current["beginning_balance"] = _money(bal)
                continue

            # Transaction row
            if current is None:
                continue
            txn_date = _to_date(row[2] if len(row) > 2 else None)
            txn_type = (row[3] if len(row) > 3 else None)
            ref_num = row[4] if len(row) > 4 else None
            name = row[5] if len(row) > 5 else None
            memo = row[6] if len(row) > 6 else None
            split = row[7] if len(row) > 7 else None
            amount = _money(row[8] if len(row) > 8 else None)
            running = _money(row[9] if len(row) > 9 else None)

            current["transactions"].append(
                {
                    "transaction_date": txn_date,
                    "transaction_type": (str(txn_type).strip() if txn_type else None),
                    "reference_number": (str(ref_num).strip() if ref_num else None),
                    "name": (str(name).strip() if name else None),
                    "memo": (str(memo).strip() if memo else None),
                    "split_account": (
                        str(split).strip() if split else (str(col_b).strip() or None)
                    ),
                    "amount": amount,
                    "running_balance": running,
                }
            )

    _commit_current()

    if not accounts:
        warnings.append("No account sections parsed from the GL xlsx")

    return {
        "title": str(title).strip() if title else None,
        "entity_name": str(entity_name).strip() if entity_name else None,
        "period_text": str(period_text).strip() if period_text else None,
        "period_start": period_start,
        "period_end": period_end,
        "accounts": accounts,
        "warnings": warnings,
    }


# ----------------------------------------------------------------------
# Importer
# ----------------------------------------------------------------------


def import_gl(
    session,
    *,
    entity_code: str,
    file_bytes: bytes,
    file_name: str,
    period_start: date | None,
    period_end: date | None,
    actor_email: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    parsed = parse_gl_xlsx(file_bytes)

    # Caller-supplied period overrides the parsed header (the user knows
    # the close period; the file header is informational).
    eff_period_start = period_start or parsed["period_start"]
    eff_period_end = period_end or parsed["period_end"]
    if not eff_period_start or not eff_period_end:
        raise ValueError(
            "Could not determine period_start/period_end from the GL "
            "header; supply them explicitly on the upload form."
        )

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], eff_period_end
    )

    # Sum activity for the run header
    total_debit = Decimal("0")
    total_credit = Decimal("0")
    for acct in parsed["accounts"]:
        for txn in acct["transactions"]:
            amt = txn["amount"]
            if amt > 0:
                total_debit += amt
            else:
                total_credit += abs(amt)

    run_row = session.execute(
        text(
            """
            INSERT INTO gl_import_runs (
                entity_id, accounting_period_id, file_name,
                period_start, period_end, report_type,
                total_accounts, total_debit_activity, total_credit_activity,
                status, actor_email
            ) VALUES (
                :entity_id, :accounting_period_id, :file_name,
                :period_start, :period_end, :report_type,
                :total_accounts, :total_debit, :total_credit,
                'imported', :actor_email
            )
            RETURNING id
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
            "file_name": file_name,
            "period_start": eff_period_start,
            "period_end": eff_period_end,
            "report_type": REPORT_TYPE_GL,
            "total_accounts": len(parsed["accounts"]),
            "total_debit": total_debit,
            "total_credit": total_credit,
            "actor_email": actor_email,
        },
    ).mappings().first()
    run_id: UUID = run_row["id"]

    inserted_balances = 0
    inserted_transactions = 0

    for acct in parsed["accounts"]:
        beginning = acct["beginning_balance"]
        activity = acct["period_activity"]
        ending = beginning + activity

        session.execute(
            text(
                """
                INSERT INTO gl_account_balances (
                    entity_id, import_run_id, accounting_period_id,
                    period_start, period_end,
                    account_code, account_name,
                    beginning_balance, period_activity, ending_balance,
                    transaction_count
                ) VALUES (
                    :entity_id, :import_run_id, :accounting_period_id,
                    :period_start, :period_end,
                    :account_code, :account_name,
                    :beginning_balance, :period_activity, :ending_balance,
                    :transaction_count
                )
                ON CONFLICT (entity_id, import_run_id, account_code)
                DO UPDATE SET
                    beginning_balance = EXCLUDED.beginning_balance,
                    period_activity = EXCLUDED.period_activity,
                    ending_balance = EXCLUDED.ending_balance,
                    transaction_count = EXCLUDED.transaction_count
                """
            ),
            {
                "entity_id": entity["id"],
                "import_run_id": run_id,
                "accounting_period_id": accounting_period_id,
                "period_start": eff_period_start,
                "period_end": eff_period_end,
                "account_code": acct["account_code"],
                "account_name": acct["account_name"],
                "beginning_balance": beginning,
                "period_activity": activity,
                "ending_balance": ending,
                "transaction_count": len(acct["transactions"]),
            },
        )
        inserted_balances += 1

        for txn in acct["transactions"]:
            session.execute(
                text(
                    """
                    INSERT INTO gl_transactions (
                        entity_id, import_run_id, accounting_period_id,
                        account_code, account_name,
                        transaction_date, transaction_type,
                        reference_number, name, memo, split_account,
                        amount, running_balance
                    ) VALUES (
                        :entity_id, :import_run_id, :accounting_period_id,
                        :account_code, :account_name,
                        :transaction_date, :transaction_type,
                        :reference_number, :name, :memo, :split_account,
                        :amount, :running_balance
                    )
                    """
                ),
                {
                    "entity_id": entity["id"],
                    "import_run_id": run_id,
                    "accounting_period_id": accounting_period_id,
                    "account_code": acct["account_code"],
                    "account_name": acct["account_name"],
                    "transaction_date": txn["transaction_date"],
                    "transaction_type": (txn["transaction_type"] or None),
                    "reference_number": (txn["reference_number"] or None),
                    "name": (txn["name"] or None),
                    "memo": (txn["memo"] or None),
                    "split_account": (txn["split_account"] or None),
                    "amount": txn["amount"],
                    "running_balance": txn["running_balance"],
                },
            )
            inserted_transactions += 1

    return {
        "import_run_id": str(run_id),
        "entity_code": entity_code,
        "file_name": file_name,
        "period_start": eff_period_start.isoformat(),
        "period_end": eff_period_end.isoformat(),
        "accounts_imported": inserted_balances,
        "transactions_imported": inserted_transactions,
        "total_debit_activity": str(total_debit),
        "total_credit_activity": str(total_credit),
        "warnings": parsed.get("warnings") or [],
    }


# ----------------------------------------------------------------------
# Trial balance comparison
# ----------------------------------------------------------------------


def build_trial_balance_comparison(
    session,
    *,
    entity_code: str,
    gl_import_run_id: str,
    actor_email: str,
) -> dict[str, Any]:
    """
    For each account in gl_account_balances of this run, sum the app's
    journal_lines for the matching accounting_period_id and compute the
    variance.
    """
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    run_uuid = _parse_uuid(gl_import_run_id, "gl_import_run_id")
    run = session.execute(
        text(
            """
            SELECT id, entity_id, accounting_period_id,
                   period_start, period_end
            FROM gl_import_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"gl_import_run not found: {gl_import_run_id}")

    accounting_period_id = run["accounting_period_id"]

    # App-side per-account totals (debit - credit) across all journal_lines
    # for the period. We include every batch regardless of workflow_status:
    # the comparison is "what does the app think the activity is" — not
    # "what's been approved to post".
    app_totals_rows = session.execute(
        text(
            """
            SELECT jl.account_code,
                   COALESCE(SUM(jl.debit_amount), 0)
                       - COALESCE(SUM(jl.credit_amount), 0) AS net_activity,
                   COUNT(*) AS line_count
            FROM journal_lines jl
            JOIN journal_batches jb ON jb.id = jl.journal_batch_id
            WHERE jb.entity_id = :entity_id
              AND jb.accounting_period_id = :accounting_period_id
            GROUP BY jl.account_code
            """
        ),
        {
            "entity_id": entity["id"],
            "accounting_period_id": accounting_period_id,
        },
    ).mappings().all()
    app_totals = {
        row["account_code"]: _money(row["net_activity"])
        for row in app_totals_rows
    }

    balances = session.execute(
        text(
            """
            SELECT account_code, account_name,
                   beginning_balance, period_activity, ending_balance
            FROM gl_account_balances
            WHERE entity_id = :entity_id
              AND import_run_id = :import_run_id
            ORDER BY account_code
            """
        ),
        {
            "entity_id": entity["id"],
            "import_run_id": run_uuid,
        },
    ).mappings().all()

    # Wipe + rewrite comparisons for this run.
    session.execute(
        text(
            """
            DELETE FROM gl_trial_balance_comparisons
            WHERE entity_id = :entity_id AND gl_import_run_id = :run_id
            """
        ),
        {"entity_id": entity["id"], "run_id": run_uuid},
    )

    accounts_with_variance = 0
    total_variance = Decimal("0")
    rows_written = 0
    seen_codes: set[str] = set()

    for bal in balances:
        code = bal["account_code"]
        seen_codes.add(code)
        gl_period = _money(bal["period_activity"])
        app_total_raw = app_totals.get(code, Decimal("0.00"))
        # Normalize app sign to match QBO's natural-side-positive
        # convention for credit-natural account classes (2xxx, 4xxx).
        if code and code[:1] in _FLIP_APP_SIGN_PREFIXES:
            app_total = -app_total_raw
        else:
            app_total = app_total_raw
        variance = gl_period - app_total
        denom = abs(gl_period) if gl_period != 0 else Decimal("0")
        variance_pct = (
            (variance / denom * Decimal("100")).quantize(Decimal("0.0001"))
            if denom > 0 else None
        )
        has_variance = abs(variance) > VARIANCE_TOLERANCE
        if has_variance:
            accounts_with_variance += 1
            total_variance += abs(variance)

        session.execute(
            text(
                """
                INSERT INTO gl_trial_balance_comparisons (
                    entity_id, accounting_period_id, gl_import_run_id,
                    period_start, period_end,
                    account_code, account_name,
                    gl_beginning_balance, gl_period_activity, gl_ending_balance,
                    app_journal_total, variance, variance_pct, has_variance
                ) VALUES (
                    :entity_id, :accounting_period_id, :gl_import_run_id,
                    :period_start, :period_end,
                    :account_code, :account_name,
                    :gl_beginning_balance, :gl_period_activity, :gl_ending_balance,
                    :app_journal_total, :variance, :variance_pct, :has_variance
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "gl_import_run_id": run_uuid,
                "period_start": run["period_start"],
                "period_end": run["period_end"],
                "account_code": code,
                "account_name": bal["account_name"],
                "gl_beginning_balance": _money(bal["beginning_balance"]),
                "gl_period_activity": gl_period,
                "gl_ending_balance": _money(bal["ending_balance"]),
                "app_journal_total": app_total,
                "variance": variance.quantize(Decimal("0.01")),
                "variance_pct": variance_pct,
                "has_variance": has_variance,
            },
        )
        rows_written += 1

    # Catch app-side accounts that have activity but aren't in the GL.
    for code, app_total_raw in app_totals.items():
        if code in seen_codes:
            continue
        if code and code[:1] in _FLIP_APP_SIGN_PREFIXES:
            app_total = -app_total_raw
        else:
            app_total = app_total_raw
        variance = -app_total
        has_variance = abs(variance) > VARIANCE_TOLERANCE
        if has_variance:
            accounts_with_variance += 1
            total_variance += abs(variance)
        session.execute(
            text(
                """
                INSERT INTO gl_trial_balance_comparisons (
                    entity_id, accounting_period_id, gl_import_run_id,
                    period_start, period_end,
                    account_code, account_name,
                    gl_beginning_balance, gl_period_activity, gl_ending_balance,
                    app_journal_total, variance, variance_pct, has_variance,
                    notes
                ) VALUES (
                    :entity_id, :accounting_period_id, :gl_import_run_id,
                    :period_start, :period_end,
                    :account_code, :account_name,
                    0, 0, 0,
                    :app_journal_total, :variance, NULL, :has_variance,
                    'App posted activity but GL has no row for this account'
                )
                """
            ),
            {
                "entity_id": entity["id"],
                "accounting_period_id": accounting_period_id,
                "gl_import_run_id": run_uuid,
                "period_start": run["period_start"],
                "period_end": run["period_end"],
                "account_code": code,
                "account_name": "(not in GL export)",
                "app_journal_total": app_total,
                "variance": variance.quantize(Decimal("0.01")),
                "has_variance": has_variance,
            },
        )
        rows_written += 1

    return {
        "gl_import_run_id": gl_import_run_id,
        "entity_code": entity_code,
        "total_accounts": rows_written,
        "accounts_with_variance": accounts_with_variance,
        "total_absolute_variance": str(total_variance.quantize(Decimal("0.01"))),
        "tolerance": str(VARIANCE_TOLERANCE),
    }


def get_trial_balance_comparison(
    session,
    *,
    entity_code: str,
    gl_import_run_id: str,
    only_variance: bool = False,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(gl_import_run_id, "gl_import_run_id")

    sql = """
        SELECT account_code, account_name,
               gl_beginning_balance, gl_period_activity, gl_ending_balance,
               app_journal_total, variance, variance_pct, has_variance,
               notes
        FROM gl_trial_balance_comparisons
        WHERE entity_id = :entity_id AND gl_import_run_id = :run_id
    """
    if only_variance:
        sql += " AND has_variance = TRUE"
    sql += " ORDER BY account_code"

    rows = session.execute(
        text(sql),
        {"entity_id": entity["id"], "run_id": run_uuid},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "gl_import_run_id": gl_import_run_id,
        "row_count": len(rows),
        "rows": [
            {
                "account_code": r["account_code"],
                "account_name": r["account_name"],
                "gl_beginning_balance": _decimal_str(r["gl_beginning_balance"]),
                "gl_period_activity": _decimal_str(r["gl_period_activity"]),
                "gl_ending_balance": _decimal_str(r["gl_ending_balance"]),
                "app_journal_total": _decimal_str(r["app_journal_total"]),
                "variance": _decimal_str(r["variance"]),
                "variance_pct": _decimal_str(r["variance_pct"]),
                "has_variance": r["has_variance"],
                "notes": r["notes"],
            }
            for r in rows
        ],
    }


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------


def list_gl_import_runs(
    session, *, entity_code: str, limit: int = 50
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if not _has_table(session, "gl_import_runs"):
        return {"entity_code": entity_code, "count": 0, "runs": []}

    rows = session.execute(
        text(
            """
            SELECT id, file_name, period_start, period_end,
                   total_accounts, total_debit_activity, total_credit_activity,
                   status, actor_email, created_at
            FROM gl_import_runs
            WHERE entity_id = :entity_id
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ),
        {"entity_id": entity["id"], "limit": int(limit)},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "count": len(rows),
        "runs": [
            {
                "id": str(r["id"]),
                "file_name": r["file_name"],
                "period_start": r["period_start"].isoformat() if r["period_start"] else None,
                "period_end": r["period_end"].isoformat() if r["period_end"] else None,
                "total_accounts": r["total_accounts"],
                "total_debit_activity": _decimal_str(r["total_debit_activity"]),
                "total_credit_activity": _decimal_str(r["total_credit_activity"]),
                "status": r["status"],
                "actor_email": r["actor_email"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


def get_gl_import_run_detail(
    session, *, entity_code: str, run_id: str
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(run_id, "run_id")

    run = session.execute(
        text(
            """
            SELECT id, file_name, period_start, period_end,
                   total_accounts, total_debit_activity, total_credit_activity,
                   status, actor_email, created_at
            FROM gl_import_runs
            WHERE id = :id AND entity_id = :entity_id
            """
        ),
        {"id": run_uuid, "entity_id": entity["id"]},
    ).mappings().first()
    if not run:
        raise ValueError(f"GL import run not found: {run_id}")

    balances = session.execute(
        text(
            """
            SELECT account_code, account_name,
                   beginning_balance, period_activity, ending_balance,
                   transaction_count
            FROM gl_account_balances
            WHERE entity_id = :entity_id AND import_run_id = :run_id
            ORDER BY account_code
            """
        ),
        {"entity_id": entity["id"], "run_id": run_uuid},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "run": {
            "id": str(run["id"]),
            "file_name": run["file_name"],
            "period_start": run["period_start"].isoformat() if run["period_start"] else None,
            "period_end": run["period_end"].isoformat() if run["period_end"] else None,
            "total_accounts": run["total_accounts"],
            "total_debit_activity": _decimal_str(run["total_debit_activity"]),
            "total_credit_activity": _decimal_str(run["total_credit_activity"]),
            "status": run["status"],
            "actor_email": run["actor_email"],
            "created_at": run["created_at"].isoformat() if run["created_at"] else None,
        },
        "account_count": len(balances),
        "accounts": [
            {
                "account_code": b["account_code"],
                "account_name": b["account_name"],
                "beginning_balance": _decimal_str(b["beginning_balance"]),
                "period_activity": _decimal_str(b["period_activity"]),
                "ending_balance": _decimal_str(b["ending_balance"]),
                "transaction_count": b["transaction_count"],
            }
            for b in balances
        ],
    }


def get_gl_account_transactions(
    session,
    *,
    entity_code: str,
    run_id: str,
    account_code: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    run_uuid = _parse_uuid(run_id, "run_id")

    rows = session.execute(
        text(
            """
            SELECT account_code, account_name,
                   transaction_date, transaction_type,
                   reference_number, name, memo, split_account,
                   amount, running_balance
            FROM gl_transactions
            WHERE entity_id = :entity_id
              AND import_run_id = :run_id
              AND (:account_code IS NULL OR account_code = :account_code)
            ORDER BY account_code, transaction_date, name
            LIMIT :limit
            """
        ),
        {
            "entity_id": entity["id"],
            "run_id": run_uuid,
            "account_code": account_code,
            "limit": int(limit),
        },
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "run_id": run_id,
        "account_code": account_code,
        "transaction_count": len(rows),
        "transactions": [
            {
                "account_code": r["account_code"],
                "account_name": r["account_name"],
                "transaction_date": r["transaction_date"].isoformat() if r["transaction_date"] else None,
                "transaction_type": r["transaction_type"],
                "reference_number": r["reference_number"],
                "name": r["name"],
                "memo": r["memo"],
                "split_account": r["split_account"],
                "amount": _decimal_str(r["amount"]),
                "running_balance": _decimal_str(r["running_balance"]),
            }
            for r in rows
        ],
    }


# ----------------------------------------------------------------------
# Close-control-center section
# ----------------------------------------------------------------------


def section_gl_import(
    session,
    *,
    entity_id: UUID,
    accounting_period_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    """
    Close control center section. Reports whether a GL import exists for
    this period and surfaces variance counts.
    """
    if not _has_table(session, "gl_import_runs"):
        return {
            "status": "no_data",
            "module_present": False,
            "summary": "gl_import_runs table not present",
        }

    run = session.execute(
        text(
            """
            SELECT id, file_name, total_accounts, created_at
            FROM gl_import_runs
            WHERE entity_id = :entity_id AND period_end = :period_end
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()

    if not run:
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": (
                f"No GL import for period_end={period_end.isoformat()}. "
                "Upload the QBO General Ledger export to enable trial-"
                "balance comparison."
            ),
        }

    variance_row = session.execute(
        text(
            """
            SELECT COUNT(*) FILTER (WHERE has_variance) AS variance_count,
                   COUNT(*) AS total_count,
                   COALESCE(SUM(ABS(variance)) FILTER (WHERE has_variance), 0)
                       AS total_variance
            FROM gl_trial_balance_comparisons
            WHERE entity_id = :entity_id AND gl_import_run_id = :run_id
            """
        ),
        {"entity_id": entity_id, "run_id": run["id"]},
    ).mappings().first()

    variance_count = variance_row["variance_count"] or 0
    total_count = variance_row["total_count"] or 0
    total_variance = _money(variance_row["total_variance"])

    if total_count == 0:
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": (
                "GL imported but no trial-balance comparison built yet. "
                "POST /api/gl-import/runs/{id}/build-comparison to run it."
            ),
            "gl_import_run_id": str(run["id"]),
            "file_name": run["file_name"],
        }

    if variance_count > 0:
        return {
            "status": "needs_review",
            "module_present": True,
            "summary": (
                f"{variance_count} of {total_count} accounts have GL-vs-app "
                f"variance (total ${total_variance})."
            ),
            "gl_import_run_id": str(run["id"]),
            "file_name": run["file_name"],
            "variance_count": variance_count,
            "total_count": total_count,
            "total_variance": str(total_variance),
        }

    return {
        "status": "ready",
        "module_present": True,
        "summary": f"GL imported and all {total_count} accounts reconcile to the app.",
        "gl_import_run_id": str(run["id"]),
        "file_name": run["file_name"],
        "variance_count": 0,
        "total_count": total_count,
    }


def _decimal_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
