"""
Period close lock workflow — HTTP routes.

Endpoints:
    POST /api/period-close/submit
    POST /api/period-close/approve
    POST /api/period-close/reopen
    GET  /api/period-close/status?entity_code=...&period_end=...
    GET  /api/period-close/history?entity_code=...&period_end=...
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_period_close import (
    BlockingItemsError,
    PeriodLockedError,
    approve_period_close,
    get_period_close_status_payload,
    list_period_close_history,
    reopen_period,
    submit_period_for_close,
)


router = APIRouter(prefix="/api/period-close", tags=["period-close"])


class SubmitRequest(BaseModel):
    entity_code: str
    period_end: str = Field(examples=["2026-02-28"])
    actor_email: str
    notes: str | None = None


class ApproveRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str
    notes: str | None = None


class ReopenRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str
    notes: str = Field(min_length=1)


def _blocking_to_http(exc: BlockingItemsError) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "message": str(exc),
            "blocking_items": exc.blocking_items,
            "warning_items": exc.warning_items,
        },
    )


@router.post("/submit")
def post_submit(
    body: SubmitRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return submit_period_for_close(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except BlockingItemsError as exc:
        raise _blocking_to_http(exc) from exc
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/approve")
def post_approve(
    body: ApproveRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    import logging as _logging
    _log = _logging.getLogger(__name__)
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            result = approve_period_close(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except BlockingItemsError as exc:
        raise _blocking_to_http(exc) from exc
    except PeriodLockedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Period-close learning hook (D1). Runs OUTSIDE the close transaction
    # — failure here writes a log line and is never allowed to roll
    # back the period close itself.
    try:
        from ..services_assistant import learn_from_period_close as _learn
        with db_session() as session:
            _learn(session, entity_code=body.entity_code, period_end=body.period_end)
    except Exception:
        _log.exception(
            "learn_from_period_close failed for %s/%s — non-fatal",
            body.entity_code, body.period_end,
        )

    return result


@router.post("/reopen")
def post_reopen(
    body: ReopenRequest,
    _user: dict = Depends(require_role("approver")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            return reopen_period(
                session,
                entity_code=body.entity_code,
                period_end=body.period_end,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class CreatePeriodRequest(BaseModel):
    entity_code: str
    period_start: str = Field(examples=["2026-02-01"])
    period_end: str = Field(examples=["2026-02-28"])
    period_label: str | None = None
    actor_email: str


@router.post("/periods", status_code=201)
def create_accounting_period(
    body: CreatePeriodRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Create a new accounting_periods row. Idempotent on (entity_id, period_end).
    Enforces one OPEN period per entity by rejecting if another period for
    the same entity already has status='open'.
    """
    from sqlalchemy import text as _text
    from .. import services_auth as _svc_auth

    _svc_auth.enforce_entity_code(_user, body.entity_code)

    with db_session() as session:
        entity = session.execute(
            _text("SELECT id FROM entities WHERE entity_code = :ec"),
            {"ec": body.entity_code},
        ).mappings().first()
        if not entity:
            raise HTTPException(404, f"Entity {body.entity_code!r} not found")

        # Reject if there's already an open period (excluding one for the
        # exact same period_end — that's idempotent re-creation).
        other_open = session.execute(
            _text(
                """
                SELECT period_label, period_end FROM accounting_periods
                 WHERE entity_id = :eid
                   AND status = 'open'
                   AND period_end <> :pe
                 ORDER BY period_end DESC
                 LIMIT 1
                """
            ),
            {"eid": entity["id"], "pe": body.period_end},
        ).mappings().first()
        if other_open:
            raise HTTPException(
                409,
                f"Another period is already open: {other_open['period_label']} "
                f"({other_open['period_end']}). Close it before opening a new one.",
            )

        label = body.period_label or body.period_end[:7]
        row = session.execute(
            _text(
                """
                INSERT INTO accounting_periods (
                    entity_id, period_label, period_start, period_end, status
                ) VALUES (
                    :eid, :label, :ps, :pe, 'open'
                )
                ON CONFLICT DO NOTHING
                RETURNING id, period_label, period_start, period_end, status
                """
            ),
            {
                "eid": entity["id"],
                "label": label,
                "ps": body.period_start,
                "pe": body.period_end,
            },
        ).mappings().first()
        if not row:
            # Already exists at that period_end — fetch and return it (idempotent).
            row = session.execute(
                _text(
                    """
                    SELECT id, period_label, period_start, period_end, status
                      FROM accounting_periods
                     WHERE entity_id = :eid AND period_end = :pe
                     LIMIT 1
                    """
                ),
                {"eid": entity["id"], "pe": body.period_end},
            ).mappings().first()
    if not row:
        raise HTTPException(500, "Could not create or locate the period")
    return {
        "id": str(row["id"]),
        "period_label": row["period_label"],
        "period_start": row["period_start"].isoformat() if hasattr(row["period_start"], "isoformat") else str(row["period_start"]),
        "period_end": row["period_end"].isoformat() if hasattr(row["period_end"], "isoformat") else str(row["period_end"]),
        "status": row["status"],
    }


@router.get("/current")
def get_current_period(
    entity_code: str = Query(...),
) -> dict[str, Any]:
    """
    Return the period the dashboard should land on. Tiered resolution
    (oldest-first to drive chronological close discipline):

      1. Oldest past non-closed period that has at least one
         approved_to_post batch — i.e. work has actually landed and is
         awaiting close.
      2. Oldest past non-closed period regardless of batches — surfaces
         the first month chronologically that hasn't been closed.
      3. Fallback to the most recent closed period so the dealer still
         has context.
      4. 404 if no accounting_periods rows exist at all.

    Returns: {period_end: 'YYYY-MM-DD', period_label, status}.
    """
    from sqlalchemy import text as _text

    with db_session() as session:
        # Tier 1: oldest with ≥1 approved_to_post batch.
        row = session.execute(
            _text(
                """
                SELECT ap.period_end, ap.period_label, ap.status
                  FROM accounting_periods ap
                  JOIN entities e ON e.id = ap.entity_id
                 WHERE e.entity_code = :entity_code
                   AND ap.period_end <= CURRENT_DATE
                   AND ap.status NOT IN ('closed_locked', 'approved_to_close')
                   AND EXISTS (
                       SELECT 1 FROM journal_batches jb
                        WHERE jb.accounting_period_id = ap.id
                          AND jb.status = 'approved_to_post'
                   )
                 ORDER BY ap.period_end ASC
                 LIMIT 1
                """
            ),
            {"entity_code": entity_code},
        ).mappings().first()

        # Tier 2: oldest past non-closed, regardless of batch state.
        if not row:
            row = session.execute(
                _text(
                    """
                    SELECT ap.period_end, ap.period_label, ap.status
                      FROM accounting_periods ap
                      JOIN entities e ON e.id = ap.entity_id
                     WHERE e.entity_code = :entity_code
                       AND ap.period_end <= CURRENT_DATE
                       AND ap.status NOT IN ('closed_locked', 'approved_to_close')
                     ORDER BY ap.period_end ASC
                     LIMIT 1
                    """
                ),
                {"entity_code": entity_code},
            ).mappings().first()

        # Tier 3: everything past is closed — most recent closed period.
        if not row:
            row = session.execute(
                _text(
                    """
                    SELECT ap.period_end, ap.period_label, ap.status
                      FROM accounting_periods ap
                      JOIN entities e ON e.id = ap.entity_id
                     WHERE e.entity_code = :entity_code
                       AND ap.period_end <= CURRENT_DATE
                     ORDER BY ap.period_end DESC
                     LIMIT 1
                    """
                ),
                {"entity_code": entity_code},
            ).mappings().first()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No accounting periods exist for entity {entity_code!r}",
        )
    return {
        "period_end": row["period_end"].isoformat()
        if hasattr(row["period_end"], "isoformat")
        else str(row["period_end"]),
        "period_label": row["period_label"],
        "status": row["status"],
    }


@router.get("/status")
def get_status(
    entity_code: str = Query(...),
    period_end: str = Query(..., examples=["2026-02-28"]),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return get_period_close_status_payload(
                session, entity_code=entity_code, period_end=period_end
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/history")
def get_history(
    entity_code: str = Query(...),
    period_end: str = Query(..., examples=["2026-02-28"]),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_period_close_history(
                session, entity_code=entity_code, period_end=period_end
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
