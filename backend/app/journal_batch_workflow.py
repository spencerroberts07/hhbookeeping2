from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping

from fastapi import HTTPException
from sqlalchemy import text

WORKFLOW_STATUS_DRAFT_EXCEPTION = "draft_exception"
WORKFLOW_STATUS_DRAFT_READY = "draft_ready"
WORKFLOW_STATUS_SUBMITTED_FOR_REVIEW = "submitted_for_review"
WORKFLOW_STATUS_APPROVED_TO_POST = "approved_to_post"
WORKFLOW_STATUS_REJECTED = "rejected"
WORKFLOW_STATUS_REOPENED = "reopened"
WORKFLOW_STATUS_POSTED = "posted"

LOCKED_WORKFLOW_STATUSES = {
    WORKFLOW_STATUS_APPROVED_TO_POST,
    WORKFLOW_STATUS_POSTED,
}

SUBMIT_ALLOWED_FROM = {
    WORKFLOW_STATUS_DRAFT_EXCEPTION,
    WORKFLOW_STATUS_DRAFT_READY,
    WORKFLOW_STATUS_REJECTED,
    WORKFLOW_STATUS_REOPENED,
}

REOPEN_ALLOWED_FROM = {
    WORKFLOW_STATUS_APPROVED_TO_POST,
    WORKFLOW_STATUS_REJECTED,
    WORKFLOW_STATUS_SUBMITTED_FOR_REVIEW,
}


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def normalize_actor_email(value: Any) -> str | None:
    cleaned = normalize_text(value)
    if not cleaned:
        return None
    return cleaned.lower()


def money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def money_float(value: Any) -> float:
    return float(money(value))


def default_workflow_status_from_batch_status(batch_status: str | None) -> str:
    normalized = (normalize_text(batch_status) or "").lower()
    if normalized in {"draft_exception", "draft_unbalanced"}:
        return WORKFLOW_STATUS_DRAFT_EXCEPTION
    return WORKFLOW_STATUS_DRAFT_READY


def resolve_workflow_status(batch_row: Mapping[str, Any] | None) -> str:
    if not batch_row:
        return WORKFLOW_STATUS_DRAFT_READY
    explicit = normalize_text(batch_row.get("workflow_status"))
    if explicit:
        return explicit
    return default_workflow_status_from_batch_status(batch_row.get("status"))


def workflow_is_locked(workflow_status: str | None) -> bool:
    normalized = (normalize_text(workflow_status) or "").lower()
    return normalized in LOCKED_WORKFLOW_STATUSES


def parse_summary_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def get_hard_stop_failures(summary_json: dict[str, Any]) -> list[dict[str, Any]]:
    controls = ((summary_json.get("controls") or {}).get("hard_stop_controls") or [])
    return [control for control in controls if isinstance(control, dict) and control.get("status") == "exception"]


def batch_is_balanced(batch_row: Mapping[str, Any]) -> bool:
    summary_json = parse_summary_json(batch_row.get("summary_json"))
    if "is_balanced" in summary_json:
        return bool(summary_json.get("is_balanced"))

    total_debits = money(batch_row.get("total_debits"))
    total_credits = money(batch_row.get("total_credits"))
    return total_debits == total_credits


def ensure_batch_can_be_rebuilt(batch_row: Mapping[str, Any]):
    effective_status = resolve_workflow_status(batch_row)
    if workflow_is_locked(effective_status) or batch_row.get("locked_at") is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This journal batch is locked and cannot be rebuilt until it is reopened.",
                "workflow_status": effective_status,
            },
        )


def require_workflow_note(note: str | None, *, action: str) -> str:
    cleaned = normalize_text(note)
    if not cleaned:
        raise HTTPException(status_code=400, detail=f"A note is required to {action} this batch")
    return cleaned


def require_actor_email(actor_email: str | None) -> str:
    cleaned = normalize_actor_email(actor_email)
    if not cleaned:
        raise HTTPException(status_code=400, detail="actor_email is required")
    return cleaned


def validate_batch_controls_for_workflow(batch_row: Mapping[str, Any], *, action_label: str):
    if not batch_is_balanced(batch_row):
        raise HTTPException(status_code=400, detail=f"Unbalanced journal batches cannot be {action_label}")

    summary_json = parse_summary_json(batch_row.get("summary_json"))
    hard_stop_failures = get_hard_stop_failures(summary_json)
    if hard_stop_failures:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"This journal batch still has hard-stop control failures and cannot be {action_label}.",
                "hard_stop_failures": hard_stop_failures,
            },
        )


def validate_batch_ready_for_submission(batch_row: Mapping[str, Any]):
    effective_status = resolve_workflow_status(batch_row)
    if effective_status not in SUBMIT_ALLOWED_FROM:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This journal batch cannot be submitted from its current workflow status.",
                "workflow_status": effective_status,
            },
        )

    validate_batch_controls_for_workflow(batch_row, action_label="submitted for review")


def validate_batch_ready_for_approval(batch_row: Mapping[str, Any], note: str | None):
    effective_status = resolve_workflow_status(batch_row)
    if effective_status != WORKFLOW_STATUS_SUBMITTED_FOR_REVIEW:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Only batches that are submitted_for_review can be approved.",
                "workflow_status": effective_status,
            },
        )

    validate_batch_controls_for_workflow(batch_row, action_label="approved")

    summary_json = parse_summary_json(batch_row.get("summary_json"))
    if summary_json.get("has_review_exception"):
        cleaned_note = normalize_text(note)
        if not cleaned_note:
            raise HTTPException(
                status_code=400,
                detail="approval note is required when review exceptions remain open",
            )


def get_journal_batch(
    session,
    *,
    entity_id: str,
    accounting_period_id: str,
    source_module: str,
    batch_label: str,
):
    return session.execute(
        text(
            """
            SELECT
                id,
                entity_id,
                accounting_period_id,
                source_module,
                batch_label,
                status,
                workflow_status,
                total_debits,
                total_credits,
                summary_json,
                submitted_by,
                submitted_at,
                reviewed_by,
                reviewed_at,
                approved_by,
                approved_at,
                approval_note,
                rejection_note,
                locked_by,
                locked_at,
                created_at,
                updated_at
            FROM journal_batches
            WHERE entity_id = :entity_id
              AND accounting_period_id = :accounting_period_id
              AND source_module = :source_module
              AND batch_label = :batch_label
            LIMIT 1
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "source_module": source_module,
            "batch_label": batch_label,
        },
    ).mappings().first()


def get_journal_batch_by_id(session, journal_batch_id: str):
    return session.execute(
        text(
            """
            SELECT
                id,
                entity_id,
                accounting_period_id,
                source_module,
                batch_label,
                status,
                workflow_status,
                total_debits,
                total_credits,
                summary_json,
                submitted_by,
                submitted_at,
                reviewed_by,
                reviewed_at,
                approved_by,
                approved_at,
                approval_note,
                rejection_note,
                locked_by,
                locked_at,
                created_at,
                updated_at
            FROM journal_batches
            WHERE id = :journal_batch_id
            LIMIT 1
            """
        ),
        {"journal_batch_id": journal_batch_id},
    ).mappings().first()


def get_workflow_events(session, journal_batch_id: str) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT
                id,
                action,
                from_workflow_status,
                to_workflow_status,
                actor_email,
                note,
                payload_json,
                created_at
            FROM journal_batch_workflow_events
            WHERE journal_batch_id = :journal_batch_id
            ORDER BY created_at, id
            """
        ),
        {"journal_batch_id": journal_batch_id},
    ).mappings().all()

    return [
        {
            "id": str(row["id"]),
            "action": row["action"],
            "from_workflow_status": row["from_workflow_status"],
            "to_workflow_status": row["to_workflow_status"],
            "actor_email": row["actor_email"],
            "note": row["note"],
            "payload_json": parse_summary_json(row["payload_json"]),
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        }
        for row in rows
    ]


def serialize_workflow(batch_row: Mapping[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    effective_status = resolve_workflow_status(batch_row)
    return {
        "workflow_status": effective_status,
        "is_locked": workflow_is_locked(effective_status) or batch_row.get("locked_at") is not None,
        "submitted_by": batch_row.get("submitted_by"),
        "submitted_at": batch_row.get("submitted_at").isoformat() if batch_row.get("submitted_at") else None,
        "reviewed_by": batch_row.get("reviewed_by"),
        "reviewed_at": batch_row.get("reviewed_at").isoformat() if batch_row.get("reviewed_at") else None,
        "approved_by": batch_row.get("approved_by"),
        "approved_at": batch_row.get("approved_at").isoformat() if batch_row.get("approved_at") else None,
        "approval_note": batch_row.get("approval_note"),
        "rejection_note": batch_row.get("rejection_note"),
        "locked_by": batch_row.get("locked_by"),
        "locked_at": batch_row.get("locked_at").isoformat() if batch_row.get("locked_at") else None,
        "history": history or [],
    }


def insert_workflow_event(
    session,
    *,
    batch_row: Mapping[str, Any],
    action: str,
    actor_email: str | None,
    note: str | None,
    from_workflow_status: str | None,
    to_workflow_status: str | None,
    payload_json: dict[str, Any] | None = None,
):
    session.execute(
        text(
            """
            INSERT INTO journal_batch_workflow_events (
                journal_batch_id,
                entity_id,
                accounting_period_id,
                source_module,
                batch_label,
                action,
                from_workflow_status,
                to_workflow_status,
                actor_email,
                note,
                payload_json
            ) VALUES (
                :journal_batch_id,
                :entity_id,
                :accounting_period_id,
                :source_module,
                :batch_label,
                :action,
                :from_workflow_status,
                :to_workflow_status,
                :actor_email,
                :note,
                CAST(:payload_json AS jsonb)
            )
            """
        ),
        {
            "journal_batch_id": batch_row["id"],
            "entity_id": batch_row["entity_id"],
            "accounting_period_id": batch_row.get("accounting_period_id"),
            "source_module": batch_row["source_module"],
            "batch_label": batch_row["batch_label"],
            "action": action,
            "from_workflow_status": from_workflow_status,
            "to_workflow_status": to_workflow_status,
            "actor_email": normalize_actor_email(actor_email),
            "note": normalize_text(note),
            "payload_json": json.dumps(payload_json or {}),
        },
    )


def transition_journal_batch_workflow(
    session,
    *,
    batch_row: Mapping[str, Any],
    action: str,
    actor_email: str,
    note: str | None = None,
):
    actor_email = require_actor_email(actor_email)
    current_status = resolve_workflow_status(batch_row)
    cleaned_note = normalize_text(note)
    summary_json = parse_summary_json(batch_row.get("summary_json"))

    if action == "submit":
        validate_batch_ready_for_submission(batch_row)
        next_status = WORKFLOW_STATUS_SUBMITTED_FOR_REVIEW
        update_sql = """
            UPDATE journal_batches
            SET workflow_status = :workflow_status,
                submitted_by = :actor_email,
                submitted_at = NOW(),
                reviewed_by = NULL,
                reviewed_at = NULL,
                approved_by = NULL,
                approved_at = NULL,
                approval_note = NULL,
                rejection_note = NULL,
                locked_by = NULL,
                locked_at = NULL,
                updated_at = NOW()
            WHERE id = :journal_batch_id
        """
    elif action == "approve":
        validate_batch_ready_for_approval(batch_row, note)
        next_status = WORKFLOW_STATUS_APPROVED_TO_POST
        update_sql = """
            UPDATE journal_batches
            SET workflow_status = :workflow_status,
                reviewed_by = :actor_email,
                reviewed_at = NOW(),
                approved_by = :actor_email,
                approved_at = NOW(),
                approval_note = :note,
                rejection_note = NULL,
                locked_by = :actor_email,
                locked_at = NOW(),
                updated_at = NOW()
            WHERE id = :journal_batch_id
        """
    elif action == "reject":
        if current_status != WORKFLOW_STATUS_SUBMITTED_FOR_REVIEW:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Only batches that are submitted_for_review can be rejected.",
                    "workflow_status": current_status,
                },
            )
        cleaned_note = require_workflow_note(cleaned_note, action="reject")
        next_status = WORKFLOW_STATUS_REJECTED
        update_sql = """
            UPDATE journal_batches
            SET workflow_status = :workflow_status,
                reviewed_by = :actor_email,
                reviewed_at = NOW(),
                approved_by = NULL,
                approved_at = NULL,
                approval_note = NULL,
                rejection_note = :note,
                locked_by = NULL,
                locked_at = NULL,
                updated_at = NOW()
            WHERE id = :journal_batch_id
        """
    elif action == "reopen":
        if current_status not in REOPEN_ALLOWED_FROM:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "This journal batch cannot be reopened from its current workflow status.",
                    "workflow_status": current_status,
                },
            )
        cleaned_note = require_workflow_note(cleaned_note, action="reopen")
        next_status = WORKFLOW_STATUS_REOPENED
        update_sql = """
            UPDATE journal_batches
            SET workflow_status = :workflow_status,
                submitted_by = NULL,
                submitted_at = NULL,
                reviewed_by = NULL,
                reviewed_at = NULL,
                approved_by = NULL,
                approved_at = NULL,
                approval_note = NULL,
                rejection_note = NULL,
                locked_by = NULL,
                locked_at = NULL,
                updated_at = NOW()
            WHERE id = :journal_batch_id
        """
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported workflow action: {action}")

    session.execute(
        text(update_sql),
        {
            "journal_batch_id": batch_row["id"],
            "workflow_status": next_status,
            "actor_email": actor_email,
            "note": cleaned_note,
        },
    )

    event_payload = {
        "batch_status": batch_row.get("status"),
        "is_balanced": batch_is_balanced(batch_row),
        "has_review_exception": bool(summary_json.get("has_review_exception")),
    }
    insert_workflow_event(
        session,
        batch_row=batch_row,
        action=action,
        actor_email=actor_email,
        note=cleaned_note,
        from_workflow_status=current_status,
        to_workflow_status=next_status,
        payload_json=event_payload,
    )

    updated_batch = get_journal_batch_by_id(session, str(batch_row["id"]))
    if not updated_batch:
        raise HTTPException(status_code=500, detail="Failed to reload updated journal batch")
    return updated_batch
