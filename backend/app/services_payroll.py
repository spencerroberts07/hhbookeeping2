"""
Payroll control module — service layer.

This is NOT a payroll processor. It's the control layer that records what
the external payroll processor produced (gross/deductions/remittance) so
the bookkeeper can:
    1. Approve the journal entry derived from the run.
    2. Mark the net-pay bank withdrawal as cleared.
    3. Mark the CRA remittance bank withdrawal as cleared.

A payroll_run row is unique on (entity_id, payroll_reference). Repeat
upserts update the same row.

Workflow:
    draft -> submitted -> approved -> posted
    rejected, reopened are off-paths
    bank_cleared and remittance_cleared flip independently of workflow.
"""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import (
    _has_table,
    _parse_uuid,
    get_entity_by_code,
    get_or_create_accounting_period,
)


VALID_STATUSES = {"draft", "reviewed", "approved", "posted"}
VALID_WORKFLOW_STATUSES = {
    "draft",
    "submitted",
    "approved",
    "posted",
    "rejected",
    "reopened",
}

EVENT_CREATED = "created"
EVENT_UPDATED = "updated"
EVENT_SUBMITTED = "submitted"
EVENT_APPROVED = "approved"
EVENT_REJECTED = "rejected"
EVENT_REOPENED = "reopened"
EVENT_BANK_CLEARED = "bank_cleared"
EVENT_BANK_UNCLEARED = "bank_uncleared"
EVENT_REMITTANCE_CLEARED = "remittance_cleared"
EVENT_REMITTANCE_UNCLEARED = "remittance_uncleared"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _money(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _money_or_zero(value: Any) -> Decimal:
    m = _money(value)
    return m if m is not None else Decimal("0.00")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date,)):
        return value.isoformat()
    return str(value)


def _row_to_dict(row) -> dict[str, Any]:
    if row is None:
        return None  # type: ignore[return-value]
    d = dict(row)
    # Stringify UUIDs and ISO-format dates for JSON friendliness
    for key in ("id", "entity_id", "accounting_period_id",
                "bank_transaction_id", "remittance_bank_transaction_id"):
        if key in d and d[key] is not None:
            d[key] = str(d[key])
    for key in ("pay_period_start", "pay_period_end", "pay_date"):
        if key in d and d[key] is not None:
            d[key] = _iso(d[key])
    for key in ("created_at", "updated_at",
                "bank_cleared_at", "remittance_cleared_at"):
        if key in d and d[key] is not None:
            d[key] = d[key].isoformat()
    if "raw_import_json" in d and isinstance(d["raw_import_json"], str):
        try:
            d["raw_import_json"] = json.loads(d["raw_import_json"])
        except Exception:
            pass
    # Decimals -> str for JSON
    for key in (
        "gross_wages", "employer_cpp", "employer_ei", "employer_benefits",
        "employee_cpp", "employee_ei", "employee_tax", "employee_benefits",
        "net_pay", "remittance_amount", "total_employer_cost",
    ):
        if key in d and d[key] is not None:
            d[key] = str(d[key])
    return d


def _log_event(
    session,
    *,
    entity_id: UUID,
    payroll_run_id: UUID,
    event_type: str,
    actor_email: str | None,
    from_status: str | None = None,
    to_status: str | None = None,
    notes: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO payroll_run_events (
                entity_id, payroll_run_id, event_type,
                from_status, to_status, actor_email, notes, payload_json
            ) VALUES (
                :entity_id, :payroll_run_id, :event_type,
                :from_status, :to_status, :actor_email, :notes,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": entity_id,
            "payroll_run_id": payroll_run_id,
            "event_type": event_type,
            "from_status": from_status,
            "to_status": to_status,
            "actor_email": actor_email,
            "notes": notes,
            "payload_json": json.dumps(payload or {}, default=str),
        },
    )


def _get_run_by_reference(
    session, entity_id: UUID, payroll_reference: str
) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT * FROM payroll_runs
             WHERE entity_id = :entity_id
               AND payroll_reference = :payroll_reference
             LIMIT 1
            """
        ),
        {"entity_id": entity_id, "payroll_reference": payroll_reference},
    ).mappings().first()
    return dict(row) if row else None


def _get_run_by_id(session, run_id: UUID) -> dict[str, Any] | None:
    row = session.execute(
        text("SELECT * FROM payroll_runs WHERE id = :id LIMIT 1"),
        {"id": run_id},
    ).mappings().first()
    return dict(row) if row else None


# --------------------------------------------------------------------------
# Upsert
# --------------------------------------------------------------------------


def upsert_payroll_run(
    session,
    *,
    entity_code: str,
    payroll_reference: str,
    pay_period_start: date,
    pay_period_end: date,
    pay_date: date,
    processor: str | None = None,
    gross_wages: Any = None,
    employer_cpp: Any = None,
    employer_ei: Any = None,
    employer_benefits: Any = None,
    employee_cpp: Any = None,
    employee_ei: Any = None,
    employee_tax: Any = None,
    employee_benefits: Any = None,
    net_pay: Any = None,
    remittance_amount: Any = None,
    total_employer_cost: Any = None,
    notes: str | None = None,
    raw_import_json: dict[str, Any] | None = None,
    actor_email: str = "",
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    payroll_reference = (payroll_reference or "").strip()
    if not payroll_reference:
        raise ValueError("payroll_reference is required")
    if pay_period_end < pay_period_start:
        raise ValueError("pay_period_end must be >= pay_period_start")

    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    accounting_period_id = get_or_create_accounting_period(
        session, entity["id"], pay_date
    )

    existing = _get_run_by_reference(session, entity["id"], payroll_reference)
    payload = {
        "entity_id": entity["id"],
        "accounting_period_id": accounting_period_id,
        "payroll_reference": payroll_reference,
        "pay_period_start": pay_period_start,
        "pay_period_end": pay_period_end,
        "pay_date": pay_date,
        "processor": processor,
        "gross_wages": _money(gross_wages),
        "employer_cpp": _money(employer_cpp),
        "employer_ei": _money(employer_ei),
        "employer_benefits": _money(employer_benefits),
        "employee_cpp": _money(employee_cpp),
        "employee_ei": _money(employee_ei),
        "employee_tax": _money(employee_tax),
        "employee_benefits": _money(employee_benefits),
        "net_pay": _money(net_pay),
        "remittance_amount": _money(remittance_amount),
        "total_employer_cost": _money(total_employer_cost),
        "notes": notes,
        "raw_import_json": json.dumps(raw_import_json or {}, default=str),
        "actor_email": actor_email,
    }

    if existing:
        # Refuse to update if posted (terminal)
        if existing["status"] == "posted":
            raise ValueError(
                "This payroll run is posted and cannot be updated. "
                "Reopen it first if a correction is needed."
            )
        session.execute(
            text(
                """
                UPDATE payroll_runs
                   SET accounting_period_id = :accounting_period_id,
                       pay_period_start = :pay_period_start,
                       pay_period_end = :pay_period_end,
                       pay_date = :pay_date,
                       processor = :processor,
                       gross_wages = :gross_wages,
                       employer_cpp = :employer_cpp,
                       employer_ei = :employer_ei,
                       employer_benefits = :employer_benefits,
                       employee_cpp = :employee_cpp,
                       employee_ei = :employee_ei,
                       employee_tax = :employee_tax,
                       employee_benefits = :employee_benefits,
                       net_pay = :net_pay,
                       remittance_amount = :remittance_amount,
                       total_employer_cost = :total_employer_cost,
                       notes = :notes,
                       raw_import_json = CAST(:raw_import_json AS jsonb),
                       actor_email = :actor_email,
                       updated_at = NOW()
                 WHERE id = :id
                """
            ),
            {**payload, "id": existing["id"]},
        )
        run_id = existing["id"]
        _log_event(
            session,
            entity_id=entity["id"],
            payroll_run_id=run_id,
            event_type=EVENT_UPDATED,
            actor_email=actor_email,
            payload={"changed_via": "upsert"},
        )
    else:
        row = session.execute(
            text(
                """
                INSERT INTO payroll_runs (
                    entity_id, accounting_period_id, payroll_reference,
                    pay_period_start, pay_period_end, pay_date, processor,
                    gross_wages, employer_cpp, employer_ei, employer_benefits,
                    employee_cpp, employee_ei, employee_tax, employee_benefits,
                    net_pay, remittance_amount, total_employer_cost,
                    notes, raw_import_json, actor_email
                ) VALUES (
                    :entity_id, :accounting_period_id, :payroll_reference,
                    :pay_period_start, :pay_period_end, :pay_date, :processor,
                    :gross_wages, :employer_cpp, :employer_ei, :employer_benefits,
                    :employee_cpp, :employee_ei, :employee_tax, :employee_benefits,
                    :net_pay, :remittance_amount, :total_employer_cost,
                    :notes, CAST(:raw_import_json AS jsonb), :actor_email
                )
                RETURNING id
                """
            ),
            payload,
        ).mappings().first()
        run_id = row["id"]
        _log_event(
            session,
            entity_id=entity["id"],
            payroll_run_id=run_id,
            event_type=EVENT_CREATED,
            actor_email=actor_email,
            to_status="draft",
            payload={"payroll_reference": payroll_reference},
        )

    return get_payroll_run(session, entity_code=entity_code, payroll_reference=payroll_reference)


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------


def get_payroll_run(
    session, *, entity_code: str, payroll_reference: str
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    row = _get_run_by_reference(session, entity["id"], payroll_reference)
    if not row:
        raise ValueError(f"No payroll run with reference '{payroll_reference}'")
    return _row_to_dict(row)


def list_payroll_runs(
    session,
    *,
    entity_code: str,
    period_start: date | None = None,
    period_end: date | None = None,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    where = ["entity_id = :entity_id"]
    params: dict[str, Any] = {"entity_id": entity["id"]}
    if period_start is not None:
        where.append("pay_date >= :period_start")
        params["period_start"] = period_start
    if period_end is not None:
        where.append("pay_date <= :period_end")
        params["period_end"] = period_end

    rows = session.execute(
        text(
            f"""
            SELECT * FROM payroll_runs
             WHERE {" AND ".join(where)}
             ORDER BY pay_date DESC, payroll_reference
            """
        ),
        params,
    ).mappings().all()

    runs = [_row_to_dict(dict(r)) for r in rows]
    return {
        "entity_code": entity_code,
        "period_start": period_start.isoformat() if period_start else None,
        "period_end": period_end.isoformat() if period_end else None,
        "count": len(runs),
        "runs": runs,
    }


# --------------------------------------------------------------------------
# Workflow transitions
# --------------------------------------------------------------------------


def _transition(
    session,
    *,
    entity_code: str,
    payroll_reference: str,
    actor_email: str,
    notes: str | None,
    target_workflow: str,
    valid_from: set[str],
    event_type: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    existing = _get_run_by_reference(session, entity["id"], payroll_reference)
    if not existing:
        raise ValueError(f"No payroll run '{payroll_reference}'")
    current = existing["workflow_status"]
    if current not in valid_from:
        raise ValueError(
            f"Cannot {event_type} from workflow_status '{current}'. "
            f"Allowed: {sorted(valid_from)}"
        )
    new_status = (
        "approved" if target_workflow in {"approved", "posted"} else existing["status"]
    )
    if target_workflow == "posted":
        new_status = "posted"
    elif target_workflow == "submitted":
        new_status = "reviewed"
    session.execute(
        text(
            """
            UPDATE payroll_runs
               SET workflow_status = :workflow_status,
                   status = :status,
                   updated_at = NOW()
             WHERE id = :id
            """
        ),
        {
            "id": existing["id"],
            "workflow_status": target_workflow,
            "status": new_status,
        },
    )
    _log_event(
        session,
        entity_id=entity["id"],
        payroll_run_id=existing["id"],
        event_type=event_type,
        actor_email=actor_email,
        from_status=current,
        to_status=target_workflow,
        notes=notes,
    )
    return get_payroll_run(session, entity_code=entity_code, payroll_reference=payroll_reference)


def submit_payroll_run(
    session, *, entity_code: str, payroll_reference: str,
    actor_email: str, notes: str | None = None,
) -> dict[str, Any]:
    return _transition(
        session,
        entity_code=entity_code,
        payroll_reference=payroll_reference,
        actor_email=actor_email,
        notes=notes,
        target_workflow="submitted",
        valid_from={"draft", "rejected", "reopened"},
        event_type=EVENT_SUBMITTED,
    )


def approve_payroll_run(
    session, *, entity_code: str, payroll_reference: str,
    actor_email: str, notes: str | None = None,
) -> dict[str, Any]:
    return _transition(
        session,
        entity_code=entity_code,
        payroll_reference=payroll_reference,
        actor_email=actor_email,
        notes=notes,
        target_workflow="approved",
        valid_from={"submitted"},
        event_type=EVENT_APPROVED,
    )


# --------------------------------------------------------------------------
# Bank-clear flips
# --------------------------------------------------------------------------


def _flip_clearing(
    session,
    *,
    entity_code: str,
    payroll_reference: str,
    bank_transaction_id: str | None,
    actor_email: str,
    column: str,
    bank_column: str,
    cleared_at_column: str,
    cleared_by_column: str,
    event_type: str,
    flag_value: bool,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    existing = _get_run_by_reference(session, entity["id"], payroll_reference)
    if not existing:
        raise ValueError(f"No payroll run '{payroll_reference}'")

    bank_uuid: UUID | None = None
    if flag_value and bank_transaction_id:
        bank_uuid = _parse_uuid(bank_transaction_id, "bank_transaction_id")
        # Verify the bank txn exists and belongs to this entity
        bank_row = session.execute(
            text(
                """
                SELECT id FROM bank_transactions
                 WHERE id = :id AND entity_id = :entity_id
                 LIMIT 1
                """
            ),
            {"id": bank_uuid, "entity_id": entity["id"]},
        ).mappings().first()
        if not bank_row:
            raise ValueError(
                f"bank_transaction_id {bank_transaction_id} not found for entity {entity_code}"
            )

    sets = [
        f"{column} = :flag",
        f"{bank_column} = :bank_id",
        f"{cleared_at_column} = " + ("NOW()" if flag_value else "NULL"),
        f"{cleared_by_column} = :actor_email" if flag_value else f"{cleared_by_column} = NULL",
        "updated_at = NOW()",
    ]

    session.execute(
        text(f"UPDATE payroll_runs SET {', '.join(sets)} WHERE id = :id"),
        {
            "id": existing["id"],
            "flag": flag_value,
            "bank_id": bank_uuid if flag_value else None,
            "actor_email": actor_email,
        },
    )

    _log_event(
        session,
        entity_id=entity["id"],
        payroll_run_id=existing["id"],
        event_type=event_type,
        actor_email=actor_email,
        payload={"bank_transaction_id": str(bank_uuid) if bank_uuid else None},
    )
    return get_payroll_run(
        session, entity_code=entity_code, payroll_reference=payroll_reference
    )


def mark_bank_cleared(
    session, *, entity_code: str, payroll_reference: str,
    bank_transaction_id: str, actor_email: str,
) -> dict[str, Any]:
    return _flip_clearing(
        session,
        entity_code=entity_code,
        payroll_reference=payroll_reference,
        bank_transaction_id=bank_transaction_id,
        actor_email=actor_email,
        column="bank_cleared",
        bank_column="bank_transaction_id",
        cleared_at_column="bank_cleared_at",
        cleared_by_column="bank_cleared_by",
        event_type=EVENT_BANK_CLEARED,
        flag_value=True,
    )


def mark_remittance_cleared(
    session, *, entity_code: str, payroll_reference: str,
    bank_transaction_id: str, actor_email: str,
) -> dict[str, Any]:
    return _flip_clearing(
        session,
        entity_code=entity_code,
        payroll_reference=payroll_reference,
        bank_transaction_id=bank_transaction_id,
        actor_email=actor_email,
        column="remittance_cleared",
        bank_column="remittance_bank_transaction_id",
        cleared_at_column="remittance_cleared_at",
        cleared_by_column="remittance_cleared_by",
        event_type=EVENT_REMITTANCE_CLEARED,
        flag_value=True,
    )


# --------------------------------------------------------------------------
# Summary (used by month-end-close control center)
# --------------------------------------------------------------------------


def get_payroll_summary(
    session,
    *,
    entity_code: str,
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")

    if not _has_table(session, "payroll_runs"):
        return {
            "entity_code": entity_code,
            "module_present": False,
            "summary": "payroll_runs table not present",
        }

    row = session.execute(
        text(
            """
            SELECT
                COUNT(*)                                                 AS total_runs,
                COALESCE(SUM(gross_wages), 0)                            AS total_gross_wages,
                COALESCE(SUM(total_employer_cost), 0)                    AS total_employer_cost,
                COALESCE(SUM(net_pay) FILTER (WHERE bank_cleared = FALSE), 0)
                                                                          AS uncleared_net_pay,
                COALESCE(SUM(remittance_amount) FILTER (WHERE remittance_cleared = FALSE), 0)
                                                                          AS uncleared_remittances,
                COUNT(*) FILTER (WHERE bank_cleared = FALSE)              AS uncleared_runs_count,
                COUNT(*) FILTER (WHERE remittance_cleared = FALSE)        AS uncleared_remittance_count,
                COUNT(*) FILTER (WHERE workflow_status NOT IN ('approved','posted'))
                                                                          AS pending_approval_count
              FROM payroll_runs
             WHERE entity_id = :entity_id
               AND pay_date BETWEEN :period_start AND :period_end
            """
        ),
        {
            "entity_id": entity["id"],
            "period_start": period_start,
            "period_end": period_end,
        },
    ).mappings().first() or {}

    total_runs = row.get("total_runs", 0) or 0
    pending_approval_count = row.get("pending_approval_count", 0) or 0
    uncleared_runs_count = row.get("uncleared_runs_count", 0) or 0
    uncleared_remittance_count = row.get("uncleared_remittance_count", 0) or 0

    if total_runs == 0:
        status = "no_data"
        summary = "No payroll runs in this period"
    elif pending_approval_count > 0:
        status = "blocked"
        summary = f"{pending_approval_count} payroll run(s) not yet approved"
    elif uncleared_runs_count > 0 or uncleared_remittance_count > 0:
        status = "needs_review"
        summary = (
            f"{uncleared_runs_count} run(s) with uncleared net pay; "
            f"{uncleared_remittance_count} run(s) with uncleared CRA remittance"
        )
    else:
        status = "ready"
        summary = f"All {total_runs} payroll run(s) approved and cleared"

    return {
        "entity_code": entity_code,
        "module_present": True,
        "status": status,
        "summary": summary,
        "total_runs": total_runs,
        "total_gross_wages": str(row.get("total_gross_wages", 0)),
        "total_employer_cost": str(row.get("total_employer_cost", 0)),
        "uncleared_net_pay": str(row.get("uncleared_net_pay", 0)),
        "uncleared_remittances": str(row.get("uncleared_remittances", 0)),
        "uncleared_runs_count": uncleared_runs_count,
        "uncleared_remittance_count": uncleared_remittance_count,
        "pending_approval_count": pending_approval_count,
    }
