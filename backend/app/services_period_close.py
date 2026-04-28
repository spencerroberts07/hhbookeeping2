"""
Period close lock workflow — service layer.

Lifecycle:
    open -> submitted_for_close -> approved_to_close -> closed_locked
    closed_locked -> reopened (-> writes allowed again)

Once a period is closed_locked, other modules call is_period_locked() before
any write that would mutate data dated inside the period. If locked, they
raise PeriodLockedError, which the route layer catches and turns into 409.

Public surface used by routes/period_close.py:
    submit_period_for_close(...)
    approve_period_close(...)
    reopen_period(...)
    get_period_close_status(...)
    list_period_close_history(...)

Public surface used by other modules' write paths:
    is_period_locked(session, entity_id, accounting_period_id) -> bool
    is_date_in_locked_period(session, entity_id, when: date) -> bool
    PeriodLockedError    (raise from write paths; route layer converts)
    effective_period_status(row) -> str   ('draft' is normalized to 'open')
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text

from .services import _has_table, _parse_uuid, get_entity_by_code
from .services_month_end_close import get_month_end_close_status


# --------------------------------------------------------------------------
# Status constants
# --------------------------------------------------------------------------

STATUS_OPEN = "open"
STATUS_SUBMITTED_FOR_CLOSE = "submitted_for_close"
STATUS_APPROVED_TO_CLOSE = "approved_to_close"
STATUS_CLOSED_LOCKED = "closed_locked"
STATUS_REOPENED = "reopened"

# Statuses that block writes (terminal locked states + the transitional
# approved_to_close which exists between approve and close).
LOCKED_STATUSES = {STATUS_APPROVED_TO_CLOSE, STATUS_CLOSED_LOCKED}

# Statuses that count as "open for writes". Includes 'draft' (legacy default
# from the original schema) and 'reopened' (post-reopen).
OPEN_STATUSES = {STATUS_OPEN, "draft", STATUS_REOPENED}

EVENT_SUBMITTED = "submitted_for_close"
EVENT_APPROVED = "approved_to_close"
EVENT_CLOSED = "closed_locked"
EVENT_REOPENED = "reopened"
EVENT_REJECTED = "close_rejected"


class PeriodLockedError(Exception):
    """Raised by other modules' write paths when the target period is locked."""

    def __init__(self, period: dict[str, Any], message: str | None = None):
        self.period = period
        super().__init__(
            message
            or (
                f"Accounting period {period.get('period_label')} "
                f"({period.get('period_end')}) is closed_locked; "
                f"writes are blocked. Reopen it first."
            )
        )


# --------------------------------------------------------------------------
# Status normalization
# --------------------------------------------------------------------------


def effective_period_status(row: dict[str, Any] | None) -> str:
    """Map legacy 'draft' to 'open' so callers only need to think about
    the new state machine."""
    if not row:
        return STATUS_OPEN
    raw = (row.get("status") or "").strip().lower()
    if raw in {"", "draft"}:
        return STATUS_OPEN
    return raw


# --------------------------------------------------------------------------
# Period lookup / lock checks
# --------------------------------------------------------------------------


def _get_period_by_id(session, accounting_period_id: UUID) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT id, entity_id, period_label, period_start, period_end,
                   status, closed_at, closed_by, reopened_at, reopened_by,
                   close_notes, reopen_notes
              FROM accounting_periods
             WHERE id = :id
             LIMIT 1
            """
        ),
        {"id": accounting_period_id},
    ).mappings().first()
    return dict(row) if row else None


def _get_period_by_end_date(
    session, entity_id: UUID, period_end: date
) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT id, entity_id, period_label, period_start, period_end,
                   status, closed_at, closed_by, reopened_at, reopened_by,
                   close_notes, reopen_notes
              FROM accounting_periods
             WHERE entity_id = :entity_id
               AND period_end = :period_end
             LIMIT 1
            """
        ),
        {"entity_id": entity_id, "period_end": period_end},
    ).mappings().first()
    return dict(row) if row else None


def _get_period_for_date(
    session, entity_id: UUID, when: date
) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT id, entity_id, period_label, period_start, period_end,
                   status, closed_at, closed_by, reopened_at, reopened_by,
                   close_notes, reopen_notes
              FROM accounting_periods
             WHERE entity_id = :entity_id
               AND :when BETWEEN period_start AND period_end
             ORDER BY period_start DESC
             LIMIT 1
            """
        ),
        {"entity_id": entity_id, "when": when},
    ).mappings().first()
    return dict(row) if row else None


def is_period_locked(
    session, *, entity_id: str | UUID | None = None, accounting_period_id: str | UUID
) -> bool:
    """True if the given accounting period is in a write-blocked status."""
    if accounting_period_id is None:
        return False
    period_uuid = _parse_uuid(str(accounting_period_id), "accounting_period_id")
    period = _get_period_by_id(session, period_uuid)
    if not period:
        return False
    return effective_period_status(period) in LOCKED_STATUSES


def is_date_in_locked_period(
    session, *, entity_id: str | UUID, when: date | None
) -> tuple[bool, dict[str, Any] | None]:
    """Resolve which period contains `when` and return (is_locked, period_row)."""
    if when is None:
        return False, None
    entity_uuid = _parse_uuid(str(entity_id), "entity_id")
    period = _get_period_for_date(session, entity_uuid, when)
    if not period:
        return False, None
    return effective_period_status(period) in LOCKED_STATUSES, period


def assert_period_not_locked(
    session, *, entity_id: str | UUID, when: date | None
) -> None:
    """Raise PeriodLockedError if `when` falls in a locked period."""
    locked, period = is_date_in_locked_period(session, entity_id=entity_id, when=when)
    if locked and period:
        raise PeriodLockedError(period)


# --------------------------------------------------------------------------
# Event log
# --------------------------------------------------------------------------


def _log_event(
    session,
    *,
    entity_id: UUID,
    accounting_period_id: UUID,
    event_type: str,
    from_status: str | None,
    to_status: str | None,
    actor_email: str | None,
    notes: str | None,
    blocking_items: list[dict[str, Any]] | None = None,
    warning_items: list[dict[str, Any]] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO period_close_events (
                entity_id, accounting_period_id, event_type,
                from_status, to_status, actor_email, notes,
                blocking_items_json, warning_items_json
            ) VALUES (
                :entity_id, :accounting_period_id, :event_type,
                :from_status, :to_status, :actor_email, :notes,
                CAST(:blocking_items_json AS jsonb),
                CAST(:warning_items_json AS jsonb)
            )
            """
        ),
        {
            "entity_id": entity_id,
            "accounting_period_id": accounting_period_id,
            "event_type": event_type,
            "from_status": from_status,
            "to_status": to_status,
            "actor_email": actor_email,
            "notes": notes,
            "blocking_items_json": json.dumps(blocking_items or [], default=str),
            "warning_items_json": json.dumps(warning_items or [], default=str),
        },
    )


def _set_period_status(
    session,
    *,
    period_id: UUID,
    new_status: str,
    closed_at: datetime | None = None,
    closed_by: str | None = None,
    reopened_at: datetime | None = None,
    reopened_by: str | None = None,
    close_notes: str | None = None,
    reopen_notes: str | None = None,
) -> None:
    sets = ["status = :status"]
    params: dict[str, Any] = {"id": period_id, "status": new_status}
    if closed_at is not None or closed_by is not None or close_notes is not None:
        if closed_at is not None:
            sets.append("closed_at = :closed_at")
            params["closed_at"] = closed_at
        if closed_by is not None:
            sets.append("closed_by = :closed_by")
            params["closed_by"] = closed_by
        if close_notes is not None:
            sets.append("close_notes = :close_notes")
            params["close_notes"] = close_notes
    if reopened_at is not None or reopened_by is not None or reopen_notes is not None:
        if reopened_at is not None:
            sets.append("reopened_at = :reopened_at")
            params["reopened_at"] = reopened_at
        if reopened_by is not None:
            sets.append("reopened_by = :reopened_by")
            params["reopened_by"] = reopened_by
        if reopen_notes is not None:
            sets.append("reopen_notes = :reopen_notes")
            params["reopen_notes"] = reopen_notes

    sql = f"UPDATE accounting_periods SET {', '.join(sets)} WHERE id = :id"
    session.execute(text(sql), params)


# --------------------------------------------------------------------------
# Public workflow
# --------------------------------------------------------------------------


def _resolve_entity_and_period(
    session, *, entity_code: str, period_end: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise ValueError(f"Unknown entity code: {entity_code}")
    try:
        period_end_date = date.fromisoformat(period_end)
    except ValueError as exc:
        raise ValueError(f"period_end must be YYYY-MM-DD, got {period_end}") from exc
    period = _get_period_by_end_date(session, entity["id"], period_end_date)
    if not period:
        raise ValueError(
            f"No accounting_periods row for entity {entity_code} period_end {period_end}"
        )
    return dict(entity), period


def submit_period_for_close(
    session,
    *,
    entity_code: str,
    period_end: str,
    actor_email: str,
    notes: str | None = None,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity, period = _resolve_entity_and_period(
        session, entity_code=entity_code, period_end=period_end
    )

    current = effective_period_status(period)
    if current in LOCKED_STATUSES:
        raise ValueError(f"Period is already {current}; cannot submit again")
    if current == STATUS_SUBMITTED_FOR_CLOSE:
        raise ValueError("Period is already submitted_for_close")

    snapshot = get_month_end_close_status(
        session, entity_code=entity_code, period_end=period_end
    )
    blocking_items = snapshot.get("blocking_items") or []
    warning_items = snapshot.get("warning_items") or []
    if blocking_items:
        # Log a "rejected" event so history reflects the failed submission attempt.
        _log_event(
            session,
            entity_id=entity["id"],
            accounting_period_id=period["id"],
            event_type=EVENT_REJECTED,
            from_status=current,
            to_status=current,
            actor_email=actor_email,
            notes=notes,
            blocking_items=blocking_items,
            warning_items=warning_items,
        )
        # Surface a structured 400 to the route layer
        raise BlockingItemsError(
            "Cannot submit period for close: blocking items remain.",
            blocking_items=blocking_items,
            warning_items=warning_items,
        )

    _set_period_status(session, period_id=period["id"], new_status=STATUS_SUBMITTED_FOR_CLOSE)
    _log_event(
        session,
        entity_id=entity["id"],
        accounting_period_id=period["id"],
        event_type=EVENT_SUBMITTED,
        from_status=current,
        to_status=STATUS_SUBMITTED_FOR_CLOSE,
        actor_email=actor_email,
        notes=notes,
        blocking_items=blocking_items,
        warning_items=warning_items,
    )

    return _build_status_payload(session, entity, period["id"])


def approve_period_close(
    session,
    *,
    entity_code: str,
    period_end: str,
    actor_email: str,
    notes: str | None = None,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    entity, period = _resolve_entity_and_period(
        session, entity_code=entity_code, period_end=period_end
    )

    current = effective_period_status(period)
    if current != STATUS_SUBMITTED_FOR_CLOSE:
        raise ValueError(
            f"Cannot approve close from status '{current}'. "
            f"Period must be submitted_for_close first."
        )

    # Re-check blocking items at approval time.
    snapshot = get_month_end_close_status(
        session, entity_code=entity_code, period_end=period_end
    )
    blocking_items = snapshot.get("blocking_items") or []
    warning_items = snapshot.get("warning_items") or []
    if blocking_items:
        raise BlockingItemsError(
            "Cannot approve close: blocking items have appeared since submission.",
            blocking_items=blocking_items,
            warning_items=warning_items,
        )

    now = datetime.now(timezone.utc)
    # transition to approved_to_close, log it, then immediately to closed_locked
    _set_period_status(
        session, period_id=period["id"], new_status=STATUS_APPROVED_TO_CLOSE
    )
    _log_event(
        session,
        entity_id=entity["id"],
        accounting_period_id=period["id"],
        event_type=EVENT_APPROVED,
        from_status=current,
        to_status=STATUS_APPROVED_TO_CLOSE,
        actor_email=actor_email,
        notes=notes,
        blocking_items=blocking_items,
        warning_items=warning_items,
    )

    _set_period_status(
        session,
        period_id=period["id"],
        new_status=STATUS_CLOSED_LOCKED,
        closed_at=now,
        closed_by=actor_email,
        close_notes=notes,
    )
    _log_event(
        session,
        entity_id=entity["id"],
        accounting_period_id=period["id"],
        event_type=EVENT_CLOSED,
        from_status=STATUS_APPROVED_TO_CLOSE,
        to_status=STATUS_CLOSED_LOCKED,
        actor_email=actor_email,
        notes=notes,
        blocking_items=blocking_items,
        warning_items=warning_items,
    )

    return _build_status_payload(session, entity, period["id"])


def reopen_period(
    session,
    *,
    entity_code: str,
    period_end: str,
    actor_email: str,
    notes: str,
) -> dict[str, Any]:
    if not actor_email:
        raise ValueError("actor_email is required")
    cleaned_notes = (notes or "").strip()
    if not cleaned_notes:
        raise ValueError("notes are required to reopen a closed period")

    entity, period = _resolve_entity_and_period(
        session, entity_code=entity_code, period_end=period_end
    )
    current = effective_period_status(period)
    if current != STATUS_CLOSED_LOCKED:
        raise ValueError(
            f"Cannot reopen from status '{current}'. Period must be closed_locked."
        )

    now = datetime.now(timezone.utc)
    _set_period_status(
        session,
        period_id=period["id"],
        new_status=STATUS_REOPENED,
        reopened_at=now,
        reopened_by=actor_email,
        reopen_notes=cleaned_notes,
    )
    _log_event(
        session,
        entity_id=entity["id"],
        accounting_period_id=period["id"],
        event_type=EVENT_REOPENED,
        from_status=current,
        to_status=STATUS_REOPENED,
        actor_email=actor_email,
        notes=cleaned_notes,
    )
    return _build_status_payload(session, entity, period["id"])


def get_period_close_status_payload(
    session, *, entity_code: str, period_end: str
) -> dict[str, Any]:
    entity, period = _resolve_entity_and_period(
        session, entity_code=entity_code, period_end=period_end
    )
    return _build_status_payload(session, entity, period["id"])


def list_period_close_history(
    session, *, entity_code: str, period_end: str
) -> dict[str, Any]:
    entity, period = _resolve_entity_and_period(
        session, entity_code=entity_code, period_end=period_end
    )
    rows = session.execute(
        text(
            """
            SELECT id, event_type, from_status, to_status, actor_email,
                   notes, blocking_items_json, warning_items_json, created_at
              FROM period_close_events
             WHERE accounting_period_id = :period_id
             ORDER BY created_at DESC, id DESC
            """
        ),
        {"period_id": period["id"]},
    ).mappings().all()

    return {
        "entity_code": entity_code,
        "period": _period_to_dict(period),
        "count": len(rows),
        "events": [
            {
                "id": str(r["id"]),
                "event_type": r["event_type"],
                "from_status": r["from_status"],
                "to_status": r["to_status"],
                "actor_email": r["actor_email"],
                "notes": r["notes"],
                "blocking_items": _safe_json(r["blocking_items_json"]),
                "warning_items": _safe_json(r["warning_items_json"]),
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _period_to_dict(period: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(period["id"]),
        "entity_id": str(period["entity_id"]),
        "period_label": period.get("period_label"),
        "period_start": period["period_start"].isoformat() if period.get("period_start") else None,
        "period_end": period["period_end"].isoformat() if period.get("period_end") else None,
        "status": period.get("status"),
        "effective_status": effective_period_status(period),
        "is_locked": effective_period_status(period) in LOCKED_STATUSES,
        "closed_at": period["closed_at"].isoformat() if period.get("closed_at") else None,
        "closed_by": period.get("closed_by"),
        "reopened_at": period["reopened_at"].isoformat() if period.get("reopened_at") else None,
        "reopened_by": period.get("reopened_by"),
        "close_notes": period.get("close_notes"),
        "reopen_notes": period.get("reopen_notes"),
    }


def _build_status_payload(
    session, entity: dict[str, Any], period_id: UUID
) -> dict[str, Any]:
    period = _get_period_by_id(session, period_id)
    return {
        "entity_code": entity["entity_code"],
        "entity_name": entity.get("entity_name"),
        "as_of": datetime.now(timezone.utc).isoformat(),
        "period": _period_to_dict(period),
    }


class BlockingItemsError(Exception):
    """Raised when submit/approve attempts run into outstanding blocking items."""

    def __init__(
        self,
        message: str,
        *,
        blocking_items: list[dict[str, Any]] | None = None,
        warning_items: list[dict[str, Any]] | None = None,
    ):
        super().__init__(message)
        self.blocking_items = blocking_items or []
        self.warning_items = warning_items or []
