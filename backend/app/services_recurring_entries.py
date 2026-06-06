"""
Recurring journal-entry engine — service layer (Module A).

Manages recurring_entry_templates, recurring_entry_lines, and
recurring_entry_postings. Provides:
    • Template CRUD (list, upsert, deactivate)
    • Per-period posting (with auto-post vs draft branching)
    • Month-end close status section
    • Standard templates seeded per entity (all OFF by default)

Amount sources (calc_type):
    fixed    — constant amount every period; auto_post=TRUE by default
    formula  — safe_eval expression over GL account tokens; auto_post=FALSE
    schedule — feeder module supplies amounts at post time; auto_post=FALSE

Safety rails (all unconditional):
    • NEVER post to locked period (closed_locked / approved_to_close)
    • NEVER include account 3900 in any journal line
    • Balance guard: Dr total must equal Cr total before any INSERT
    • Idempotency: UNIQUE(entity_id, template_id, posted_period_end) on
      recurring_entry_postings prevents double-posting
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import get_entity_by_code, get_or_create_accounting_period
from .services_period_close import PeriodLockedError, assert_period_not_locked

SOURCE_MODULE = "recurring_entry"

# Standard template keys — these ship OFF (is_active=FALSE) for all entities.
STANDARD_KEYS = {
    "dgip_forgiveness",
    "percentage_rent",
    "interest_accrual",
    "depreciation",
}

# Seed definitions for standard templates. Accounts verified against
# Bridlewood CoA (2026-06-06). Amounts are defaults; bookkeeper can edit.
_STANDARD_SEED = [
    {
        "standard_key": "dgip_forgiveness",
        "name": "DGIP Forgiveness",
        "description": (
            "Monthly DGIP loan forgiveness: Dr 2510 DGIP Loan / Cr 7000 DGIP "
            "Forgiveness. $3,333.33 per month (Government of Canada program). "
            "Fixed amount; auto-posts directly when enabled."
        ),
        "calc_type": "fixed",
        "fixed_amount": Decimal("3333.33"),
        "auto_post": True,
        "cadence": "monthly",
        "lines": [
            {"line_number": 1, "account_code": "2510", "direction": "debit",
             "memo": "DGIP loan forgiveness — reduce liability"},
            {"line_number": 2, "account_code": "7000", "direction": "credit",
             "memo": "DGIP loan forgiveness income"},
        ],
    },
    {
        "standard_key": "percentage_rent",
        "name": "Percentage Rent Accrual",
        "description": (
            "Monthly accrual of percentage rent payable: 4.5% of sales "
            "(account 4000). Dr 6010 Rent / Cr 2201 Accrued Rent. "
            "Amount computed each period from the formula — requires "
            "one-click approval before posting."
        ),
        "calc_type": "formula",
        "formula_expr": "acct_4000 * 0.045",
        "auto_post": False,
        "cadence": "monthly",
        "lines": [
            {"line_number": 1, "account_code": "6010", "direction": "debit",
             "memo": "Percentage rent expense — 4.5% of sales"},
            {"line_number": 2, "account_code": "2201", "direction": "credit",
             "memo": "Accrued rent payable"},
        ],
    },
    {
        "standard_key": "interest_accrual",
        "name": "Interest Accrual",
        "description": (
            "Monthly interest expense accrual: Dr 6280 Interest on Term Loan "
            "/ Cr 2203 Accrued Interest. Amount is fixed but starts at $0 — "
            "update the amount in settings before enabling. Note: Bridlewood's "
            "interest hits the bank directly at month-end so this template "
            "should remain OFF for Bridlewood unless accruals are needed."
        ),
        "calc_type": "fixed",
        "fixed_amount": Decimal("0.00"),
        "auto_post": True,
        "cadence": "monthly",
        "lines": [
            {"line_number": 1, "account_code": "6280", "direction": "debit",
             "memo": "Monthly interest expense accrual"},
            {"line_number": 2, "account_code": "2203", "direction": "credit",
             "memo": "Accrued interest payable"},
        ],
    },
    {
        "standard_key": "depreciation",
        "name": "Monthly Depreciation",
        "description": (
            "Monthly CCA declining-balance depreciation, sourced from the "
            "Fixed Asset module. Per-class journal lines are generated at "
            "post time (one Dr expense / Cr accum-depr pair per class). "
            "Requires one-click approval before posting because the amount "
            "changes when assets are added or disposed. Run a dry-run first "
            "to verify figures match the fixed-asset schedule."
        ),
        "calc_type": "schedule",
        "schedule_source": "fixed_asset_depreciation",
        "auto_post": False,
        "cadence": "monthly",
        "lines": [],  # lines are dynamic; generated by Module B at post time
    },
]


def _money(v: Any) -> Decimal:
    return Decimal(str(v or 0)).quantize(Decimal("0.01"))


# -------------------------------------------------------------------------
# Template CRUD
# -------------------------------------------------------------------------


def seed_standard_templates(session, *, entity_code: str) -> dict[str, Any]:
    """Seed the 4 standard recurring templates for an entity.
    All start with is_active=FALSE. Idempotent — skips existing standard_key rows.
    """
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    inserted = 0
    skipped = 0
    for tmpl in _STANDARD_SEED:
        existing = session.execute(
            text(
                "SELECT id FROM recurring_entry_templates "
                "WHERE entity_id = :eid AND standard_key = :sk"
            ),
            {"eid": entity["id"], "sk": tmpl["standard_key"]},
        ).mappings().first()
        if existing:
            skipped += 1
            continue

        row = session.execute(
            text(
                """
                INSERT INTO recurring_entry_templates (
                    entity_id, name, description, standard_key,
                    calc_type, fixed_amount, formula_expr, schedule_source,
                    cadence, is_active, auto_post, source_module
                ) VALUES (
                    :eid, :name, :desc, :sk,
                    :ct, :fa, :fe, :ss,
                    :cad, FALSE, :ap, 'recurring_entry'
                ) RETURNING id
                """
            ),
            {
                "eid": entity["id"],
                "name": tmpl["name"],
                "desc": tmpl["description"],
                "sk": tmpl["standard_key"],
                "ct": tmpl["calc_type"],
                "fa": tmpl.get("fixed_amount"),
                "fe": tmpl.get("formula_expr"),
                "ss": tmpl.get("schedule_source"),
                "cad": tmpl["cadence"],
                "ap": tmpl["auto_post"],
            },
        ).mappings().first()
        template_id = row["id"]

        for ln in tmpl.get("lines", []):
            session.execute(
                text(
                    """
                    INSERT INTO recurring_entry_lines (
                        template_id, line_number, account_code, direction, memo
                    ) VALUES (:tid, :ln, :ac, :dir, :memo)
                    """
                ),
                {
                    "tid": template_id,
                    "ln": ln["line_number"],
                    "ac": ln["account_code"],
                    "dir": ln["direction"],
                    "memo": ln.get("memo"),
                },
            )
        inserted += 1

    return {
        "entity_code": entity_code,
        "inserted": inserted,
        "skipped": skipped,
    }


def list_templates(session, *, entity_code: str) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    templates = session.execute(
        text(
            """
            SELECT t.id, t.name, t.description, t.standard_key,
                   t.calc_type, t.fixed_amount, t.formula_expr, t.schedule_source,
                   t.cadence, t.posting_day, t.is_active, t.auto_post,
                   t.last_posted_at, t.last_posted_period_end, t.notes
            FROM recurring_entry_templates t
            WHERE t.entity_id = :eid
            ORDER BY
                CASE WHEN t.standard_key IS NOT NULL THEN 0 ELSE 1 END,
                t.name
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()

    lines_rows = session.execute(
        text(
            """
            SELECT l.template_id, l.line_number, l.account_code, l.direction, l.memo
            FROM recurring_entry_lines l
            JOIN recurring_entry_templates t ON t.id = l.template_id
            WHERE t.entity_id = :eid
            ORDER BY l.template_id, l.line_number
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()

    lines_by_tmpl: dict[str, list] = {}
    for ln in lines_rows:
        k = str(ln["template_id"])
        lines_by_tmpl.setdefault(k, []).append({
            "line_number": ln["line_number"],
            "account_code": ln["account_code"],
            "direction": ln["direction"],
            "memo": ln["memo"],
        })

    # Last posting info per template
    postings = session.execute(
        text(
            """
            SELECT p.template_id,
                   MAX(p.posted_period_end) AS last_period_end,
                   COUNT(*) AS total_postings
            FROM recurring_entry_postings p
            WHERE p.entity_id = :eid
            GROUP BY p.template_id
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()
    posting_map = {str(p["template_id"]): p for p in postings}

    result = []
    for t in templates:
        tid = str(t["id"])
        pm = posting_map.get(tid, {})
        result.append({
            "id": tid,
            "name": t["name"],
            "description": t["description"],
            "standard_key": t["standard_key"],
            "calc_type": t["calc_type"],
            "fixed_amount": str(t["fixed_amount"]) if t["fixed_amount"] is not None else None,
            "formula_expr": t["formula_expr"],
            "schedule_source": t["schedule_source"],
            "cadence": t["cadence"],
            "posting_day": t["posting_day"],
            "is_active": t["is_active"],
            "auto_post": t["auto_post"],
            "last_posted_at": t["last_posted_at"].isoformat() if t["last_posted_at"] else None,
            "last_posted_period_end": (
                t["last_posted_period_end"].isoformat()
                if t["last_posted_period_end"] else None
            ),
            "total_postings": int(pm.get("total_postings") or 0),
            "notes": t["notes"],
            "lines": lines_by_tmpl.get(tid, []),
        })

    return {"entity_code": entity_code, "count": len(result), "templates": result}


def upsert_template(
    session,
    *,
    entity_code: str,
    template_id: str | None = None,
    name: str,
    description: str | None = None,
    standard_key: str | None = None,
    calc_type: str,
    fixed_amount: Decimal | None = None,
    formula_expr: str | None = None,
    schedule_source: str | None = None,
    cadence: str = "monthly",
    posting_day: int = 1,
    is_active: bool = False,
    auto_post: bool | None = None,
    notes: str | None = None,
    lines: list[dict] | None = None,
    actor_email: str,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if calc_type not in ("fixed", "formula", "schedule"):
        raise ValueError(f"calc_type must be fixed|formula|schedule; got {calc_type!r}")
    if cadence not in ("monthly", "on_close", "annual"):
        raise ValueError(f"cadence must be monthly|on_close|annual; got {cadence!r}")

    # Validate formula with safe_eval
    if calc_type == "formula" and formula_expr:
        from .services_ratios import safe_eval as _safe_eval
        test = _safe_eval(formula_expr, {"acct_4000": 100000.0, "revenue": 100000.0})
        # test may return None (division-by-zero or namespace miss — still valid expression)

    # Default auto_post: True for fixed, False for formula/schedule
    if auto_post is None:
        auto_post = calc_type == "fixed"

    # Validate lines: no account 3900
    for ln in (lines or []):
        if ln.get("account_code", "").strip() == "3900":
            raise ValueError("Journal lines may not target account 3900 (Opening Balance Equity)")

    if template_id:
        # Update
        session.execute(
            text(
                """
                UPDATE recurring_entry_templates SET
                    name=:name, description=:desc, calc_type=:ct,
                    fixed_amount=:fa, formula_expr=:fe, schedule_source=:ss,
                    cadence=:cad, posting_day=:pd, is_active=:active,
                    auto_post=:ap, notes=:notes, updated_at=NOW()
                WHERE id=:id AND entity_id=:eid
                """
            ),
            {
                "name": name, "desc": description, "ct": calc_type,
                "fa": fixed_amount, "fe": formula_expr, "ss": schedule_source,
                "cad": cadence, "pd": posting_day, "active": is_active,
                "ap": auto_post, "notes": notes,
                "id": template_id, "eid": entity["id"],
            },
        )
        tid = template_id
    else:
        # Insert
        row = session.execute(
            text(
                """
                INSERT INTO recurring_entry_templates (
                    entity_id, name, description, standard_key,
                    calc_type, fixed_amount, formula_expr, schedule_source,
                    cadence, posting_day, is_active, auto_post, notes,
                    source_module
                ) VALUES (
                    :eid, :name, :desc, :sk,
                    :ct, :fa, :fe, :ss,
                    :cad, :pd, :active, :ap, :notes,
                    'recurring_entry'
                ) RETURNING id
                """
            ),
            {
                "eid": entity["id"], "name": name, "desc": description, "sk": standard_key,
                "ct": calc_type, "fa": fixed_amount, "fe": formula_expr, "ss": schedule_source,
                "cad": cadence, "pd": posting_day, "active": is_active,
                "ap": auto_post, "notes": notes,
            },
        ).mappings().first()
        tid = str(row["id"])

    # Replace lines
    if lines is not None:
        session.execute(
            text("DELETE FROM recurring_entry_lines WHERE template_id = :tid"),
            {"tid": tid},
        )
        for ln in lines:
            if ln.get("account_code", "").strip() == "3900":
                raise ValueError("Journal lines may not target account 3900")
            session.execute(
                text(
                    """
                    INSERT INTO recurring_entry_lines (
                        template_id, line_number, account_code, direction, memo
                    ) VALUES (:tid, :ln, :ac, :dir, :memo)
                    """
                ),
                {
                    "tid": tid,
                    "ln": ln["line_number"],
                    "ac": ln["account_code"],
                    "dir": ln["direction"],
                    "memo": ln.get("memo"),
                },
            )

    return {"id": tid, "entity_code": entity_code}


def set_template_active(
    session, *, entity_code: str, template_id: str, is_active: bool
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    session.execute(
        text(
            "UPDATE recurring_entry_templates SET is_active=:a, updated_at=NOW() "
            "WHERE id=:id AND entity_id=:eid"
        ),
        {"a": is_active, "id": template_id, "eid": entity["id"]},
    )
    return {"id": template_id, "is_active": is_active}


# -------------------------------------------------------------------------
# Amount computation
# -------------------------------------------------------------------------


def compute_template_amount(
    session,
    *,
    entity_id: UUID,
    template: dict[str, Any],
    period_end: date,
) -> Decimal | list[dict[str, Any]]:
    """Compute the posting amount for a template.

    Returns:
        Decimal — for fixed and formula types
        list[{expense_account, accum_account, amount, ...}] — for schedule type
            (per-class items from Module B)
    """
    calc_type = template["calc_type"]

    if calc_type == "fixed":
        return _money(template.get("fixed_amount") or 0)

    if calc_type == "formula":
        expr = template.get("formula_expr") or ""
        if not expr:
            raise ValueError(
                f"Template '{template['name']}' has calc_type='formula' but no formula_expr"
            )
        from .services_ratios import build_token_namespace, safe_eval as _safe_eval
        ns = build_token_namespace(session, entity_id, ctx=None, period_end=period_end, inputs={})
        result = _safe_eval(expr, ns)
        if result is None:
            raise ValueError(
                f"Formula evaluation returned None for '{template['name']}' "
                f"(division by zero or missing token). Formula: {expr!r}"
            )
        return _money(result)

    if calc_type == "schedule":
        source = template.get("schedule_source", "")
        if source == "fixed_asset_depreciation":
            from .services_depreciation import compute_monthly_depreciation_by_class
            return compute_monthly_depreciation_by_class(
                session, entity_id=entity_id, period_end=period_end
            )
        raise ValueError(
            f"Unknown schedule_source '{source}' for template '{template['name']}'"
        )

    raise ValueError(f"Unknown calc_type: {calc_type!r}")


# -------------------------------------------------------------------------
# Posting
# -------------------------------------------------------------------------


_GUARD_3900 = "3900"


def _build_journal_lines(
    template: dict[str, Any],
    amount_or_classes: Decimal | list[dict],
    *,
    memo_prefix: str,
) -> list[dict[str, Any]]:
    """Build balanced journal line dicts from a template + computed amount."""
    lines = []
    line_num = 0

    if isinstance(amount_or_classes, list):
        # Schedule type: per-class Dr/Cr pairs
        for cls_item in amount_or_classes:
            if _money(cls_item["amount"]) == Decimal("0"):
                continue
            amt = _money(cls_item["amount"])
            for acct, is_debit in [
                (cls_item["expense_account"], True),
                (cls_item["accum_account"], False),
            ]:
                if acct.strip() == _GUARD_3900:
                    raise ValueError(f"Account 3900 is forbidden in recurring entries")
                line_num += 1
                lines.append({
                    "line_number": line_num,
                    "account_code": acct,
                    "debit_amount": amt if is_debit else Decimal("0"),
                    "credit_amount": Decimal("0") if is_debit else amt,
                    "memo": f"{memo_prefix} — {cls_item['class_name']}",
                })
    else:
        # Fixed/formula: use stored line definitions
        amount = amount_or_classes
        for ln in template.get("lines", []):
            if ln["account_code"].strip() == _GUARD_3900:
                raise ValueError("Account 3900 is forbidden in recurring entries")
            line_num += 1
            lines.append({
                "line_number": line_num,
                "account_code": ln["account_code"],
                "debit_amount": amount if ln["direction"] == "debit" else Decimal("0"),
                "credit_amount": amount if ln["direction"] == "credit" else Decimal("0"),
                "memo": ln.get("memo") or memo_prefix,
            })

    return lines


def post_template(
    session,
    *,
    entity_code: str,
    template_id: str,
    period_end: date,
    actor_email: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Post a single recurring template for the given period.

    Branching:
        auto_post=True  → status='posted',   workflow_status='posted'  (direct)
        auto_post=False → status='draft',    workflow_status='draft_ready'  (needs approval)

    Idempotency: returns 409 if already posted for this template+period.
    Locked period: returns 409 via PeriodLockedError.
    """
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    # Fetch template + lines
    tmpl_row = session.execute(
        text(
            """
            SELECT t.id, t.name, t.calc_type, t.fixed_amount, t.formula_expr,
                   t.schedule_source, t.auto_post, t.is_active, t.source_module
            FROM recurring_entry_templates t
            WHERE t.id = :tid AND t.entity_id = :eid
            """
        ),
        {"tid": template_id, "eid": entity["id"]},
    ).mappings().first()
    if not tmpl_row:
        raise ValueError(f"Template not found: {template_id}")

    line_rows = session.execute(
        text(
            "SELECT line_number, account_code, direction, memo "
            "FROM recurring_entry_lines WHERE template_id = :tid ORDER BY line_number"
        ),
        {"tid": template_id},
    ).mappings().all()

    template = dict(tmpl_row)
    template["lines"] = [dict(r) for r in line_rows]

    # Period lock guard
    assert_period_not_locked(session, entity["id"], period_end)

    # Idempotency guard — check existing posting
    existing_posting = session.execute(
        text(
            """
            SELECT id, journal_batch_id
            FROM recurring_entry_postings
            WHERE entity_id = :eid AND template_id = :tid AND posted_period_end = :pe
            """
        ),
        {"eid": entity["id"], "tid": template_id, "pe": period_end},
    ).mappings().first()
    if existing_posting:
        raise ValueError(
            f"Template '{template['name']}' already posted for period ending "
            f"{period_end.isoformat()}. batch_id={existing_posting['journal_batch_id']}"
        )

    # For DRAFT, also check for unapproved pending draft
    if not template["auto_post"]:
        pending_draft = session.execute(
            text(
                """
                SELECT jb.id FROM journal_batches jb
                WHERE jb.entity_id = :eid
                  AND jb.source_module = 'recurring_entry'
                  AND jb.batch_label = :label
                  AND jb.status = 'draft'
                  AND jb.workflow_status IN ('draft_ready', 'submitted_for_review')
                """
            ),
            {
                "eid": entity["id"],
                "label": f"{template['name']}_{period_end.isoformat()}",
            },
        ).mappings().first()
        if pending_draft:
            raise ValueError(
                f"An unapproved draft already exists for '{template['name']}' "
                f"period {period_end.isoformat()} (batch {pending_draft['id']}). "
                "Approve or reject that batch before creating a new one."
            )

    # Compute the amount
    amount_or_classes = compute_template_amount(
        session, entity_id=entity["id"], template=template, period_end=period_end
    )

    # Build journal lines
    memo_prefix = f"{template['name']} — {period_end.strftime('%b %Y')}"
    journal_lines = _build_journal_lines(
        template, amount_or_classes, memo_prefix=memo_prefix
    )
    if not journal_lines:
        raise ValueError(
            f"Template '{template['name']}' produced zero journal lines for "
            f"{period_end.isoformat()}. Nothing to post."
        )

    total_dr = sum(ln["debit_amount"] for ln in journal_lines)
    total_cr = sum(ln["credit_amount"] for ln in journal_lines)
    if abs(total_dr - total_cr) > Decimal("0.01"):
        raise ValueError(
            f"Balance guard failed: Dr={total_dr} Cr={total_cr} for template "
            f"'{template['name']}'. Journal lines must balance."
        )

    # Grand total amount for postings log
    grand_total = total_dr  # == total_cr

    preview = {
        "template_id": template_id,
        "template_name": template["name"],
        "period_end": period_end.isoformat(),
        "calc_type": template["calc_type"],
        "grand_total": str(grand_total),
        "auto_post": template["auto_post"],
        "journal_lines": [
            {
                "line_number": ln["line_number"],
                "account_code": ln["account_code"],
                "debit_amount": str(ln["debit_amount"]),
                "credit_amount": str(ln["credit_amount"]),
                "memo": ln["memo"],
            }
            for ln in journal_lines
        ],
        "total_debits": str(total_dr),
        "total_credits": str(total_cr),
    }

    if dry_run:
        return {"dry_run": True, **preview}

    # Post
    auto_post = template["auto_post"]
    batch_status = "posted" if auto_post else "draft"
    batch_workflow = "posted" if auto_post else "draft_ready"

    accounting_period_id = get_or_create_accounting_period(session, entity["id"], period_end)
    batch_label = f"{template['name']}_{period_end.isoformat()}"

    batch = session.execute(
        text(
            """
            INSERT INTO journal_batches (
                entity_id, accounting_period_id, source_module, batch_label,
                status, workflow_status, total_debits, total_credits, summary_json
            ) VALUES (
                :eid, :apid, :sm, :bl,
                :st, :wf, :dr, :cr, CAST(:sj AS jsonb)
            ) RETURNING id
            """
        ),
        {
            "eid": entity["id"],
            "apid": accounting_period_id,
            "sm": SOURCE_MODULE,
            "bl": batch_label,
            "st": batch_status,
            "wf": batch_workflow,
            "dr": total_dr,
            "cr": total_cr,
            "sj": json.dumps(preview),
        },
    ).mappings().first()
    batch_id = batch["id"]

    for ln in journal_lines:
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES (
                    :bid, :ln, :ac, :dr, :cr, :memo, CAST(:sj AS jsonb)
                )
                """
            ),
            {
                "bid": batch_id,
                "ln": ln["line_number"],
                "ac": ln["account_code"],
                "dr": ln["debit_amount"],
                "cr": ln["credit_amount"],
                "memo": ln["memo"],
                "sj": json.dumps({
                    "source_module": SOURCE_MODULE,
                    "template_id": template_id,
                }),
            },
        )

    # Log the posting
    period_start = period_end.replace(day=1)
    session.execute(
        text(
            """
            INSERT INTO recurring_entry_postings (
                entity_id, template_id, accounting_period_id, journal_batch_id,
                posted_period_start, posted_period_end, amount, auto_posted, actor_email
            ) VALUES (
                :eid, :tid, :apid, :bid,
                :ps, :pe, :amt, :ap, :ae
            )
            """
        ),
        {
            "eid": entity["id"],
            "tid": template_id,
            "apid": accounting_period_id,
            "bid": batch_id,
            "ps": period_start,
            "pe": period_end,
            "amt": grand_total,
            "ap": auto_post,
            "ae": actor_email,
        },
    )

    # Update last_posted tracking
    session.execute(
        text(
            """
            UPDATE recurring_entry_templates
            SET last_posted_at = NOW(), last_posted_period_end = :pe, updated_at = NOW()
            WHERE id = :tid
            """
        ),
        {"pe": period_end, "tid": template_id},
    )

    return {
        "dry_run": False,
        "journal_batch_id": str(batch_id),
        "batch_status": batch_status,
        **preview,
    }


def post_due_templates(
    session,
    *,
    entity_code: str,
    period_end: date,
    actor_email: str,
) -> dict[str, Any]:
    """Post all active templates that have not yet been posted for this period.
    Returns a summary of posted, skipped, and failed templates.
    """
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    templates = session.execute(
        text(
            """
            SELECT id FROM recurring_entry_templates
            WHERE entity_id = :eid AND is_active = TRUE
            """
        ),
        {"eid": entity["id"]},
    ).mappings().all()

    posted = []
    skipped = []
    failed = []

    for tmpl in templates:
        try:
            result = post_template(
                session,
                entity_code=entity_code,
                template_id=str(tmpl["id"]),
                period_end=period_end,
                actor_email=actor_email,
                dry_run=False,
            )
            posted.append({
                "template_id": str(tmpl["id"]),
                "journal_batch_id": result.get("journal_batch_id"),
                "batch_status": result.get("batch_status"),
                "grand_total": result.get("grand_total"),
            })
        except PeriodLockedError:
            raise
        except ValueError as exc:
            msg = str(exc)
            if "already posted" in msg or "unapproved draft" in msg:
                skipped.append({"template_id": str(tmpl["id"]), "reason": msg})
            else:
                failed.append({"template_id": str(tmpl["id"]), "error": msg})

    return {
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "posted_count": len(posted),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "posted": posted,
        "skipped": skipped,
        "failed": failed,
    }


# -------------------------------------------------------------------------
# Month-end close section gatherer
# -------------------------------------------------------------------------


def get_month_end_status(
    session,
    *,
    entity_id: UUID,
    period_end: date,
) -> dict[str, Any]:
    """Return recurring-entry status for the month-end close-readiness panel."""
    templates = session.execute(
        text(
            """
            SELECT t.id, t.name, t.calc_type, t.auto_post, t.is_active,
                   t.last_posted_period_end
            FROM recurring_entry_templates t
            WHERE t.entity_id = :eid AND t.is_active = TRUE
            ORDER BY t.name
            """
        ),
        {"eid": entity_id},
    ).mappings().all()

    if not templates:
        return {
            "status": "not_configured",
            "module_present": True,
            "summary": "No recurring entries configured.",
            "items": [],
        }

    # Check which are posted for this period
    postings = session.execute(
        text(
            """
            SELECT template_id, journal_batch_id, amount, auto_posted
            FROM recurring_entry_postings
            WHERE entity_id = :eid AND posted_period_end = :pe
            """
        ),
        {"eid": entity_id, "pe": period_end},
    ).mappings().all()
    posted_ids = {str(p["template_id"]): p for p in postings}

    items = []
    all_posted = True
    for t in templates:
        tid = str(t["id"])
        posting = posted_ids.get(tid)
        if posting:
            items.append({
                "template_id": tid,
                "name": t["name"],
                "status": "posted",
                "amount": str(posting["amount"]),
                "journal_batch_id": str(posting["journal_batch_id"]),
                "auto_posted": posting["auto_posted"],
            })
        else:
            all_posted = False
            items.append({
                "template_id": tid,
                "name": t["name"],
                "status": "pending",
                "amount": None,
                "journal_batch_id": None,
            })

    overall = "ready" if all_posted else "needs_review"
    pending_names = [i["name"] for i in items if i["status"] == "pending"]
    return {
        "status": overall,
        "module_present": True,
        "summary": (
            f"{len(templates)} active recurring entries; "
            f"{len(postings)} posted for {period_end.isoformat()}."
            + (f" Pending: {', '.join(pending_names)}." if pending_names else "")
        ),
        "items": items,
        "all_posted": all_posted,
    }
