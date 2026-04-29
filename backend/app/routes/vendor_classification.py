"""
Self-improving vendor classification — HTTP routes.

Endpoints:
    POST /api/vendor-classification/learn-from-gl
         body: {entity_code, gl_import_run_id, actor_email}

    GET  /api/vendor-classification/memory
         ?entity_code=...&source=gl_history|user_confirmed|ai_seeded

    POST /api/vendor-classification/memory/upsert
         body: {entity_code, normalized_vendor_key, account_code,
                debit_or_credit, actor_email, notes?}

    GET  /api/vendor-classification/suggestions
         ?entity_code=...&status=pending|accepted|overridden|rejected

    POST /api/vendor-classification/suggestions/{id}/accept
         body: {entity_code, actor_email,
                final_account_code? (default: suggested),
                final_debit_or_credit?}

    POST /api/vendor-classification/suggestions/{id}/override
         body: {entity_code, actor_email, final_account_code,
                final_debit_or_credit?}
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_vendor_classification import (
    learn_from_gl_history,
    list_pending_suggestions,
    list_vendor_memory,
    record_user_feedback,
    upsert_vendor_memory,
)


router = APIRouter(
    prefix="/api/vendor-classification", tags=["vendor-classification"]
)


class LearnFromGLRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    gl_import_run_id: str
    actor_email: str


@router.post("/learn-from-gl")
def post_learn_from_gl(
    body: LearnFromGLRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return learn_from_gl_history(
                session,
                entity_code=body.entity_code,
                gl_import_run_id=body.gl_import_run_id,
                actor_email=body.actor_email,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/memory")
def get_memory(
    entity_code: str = Query(...),
    source: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_vendor_memory(
                session,
                entity_code=entity_code,
                source=source,
                limit=limit,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class UpsertMemoryRequest(BaseModel):
    entity_code: str
    normalized_vendor_key: str
    account_code: str
    debit_or_credit: str = Field(..., examples=["debit"])
    actor_email: str
    notes: str | None = None


@router.post("/memory/upsert")
def post_upsert_memory(
    body: UpsertMemoryRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return upsert_vendor_memory(
                session,
                entity_code=body.entity_code,
                normalized_vendor_key=body.normalized_vendor_key,
                account_code=body.account_code,
                debit_or_credit=body.debit_or_credit,
                actor_email=body.actor_email,
                notes=body.notes,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/suggestions")
def get_suggestions(
    entity_code: str = Query(...),
    status: str = Query(default="pending"),
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        with db_session() as session:
            return list_pending_suggestions(
                session,
                entity_code=entity_code,
                status=status,
                limit=limit,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class FeedbackRequest(BaseModel):
    entity_code: str
    actor_email: str
    final_account_code: str | None = None
    final_debit_or_credit: str | None = None


@router.post("/suggestions/{suggestion_id}/accept")
def post_accept_suggestion(
    suggestion_id: str = Path(...),
    body: FeedbackRequest = ...,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Accept the suggestion as-is. final_account_code defaults to the
    model's suggested_account_code; explicitly provide it only when
    you want to confirm a different choice.
    """
    try:
        with db_session() as session:
            # If caller didn't supply final_account_code, look up the
            # suggestion to use the suggested code.
            from sqlalchemy import text

            from ..services import _parse_uuid, get_entity_by_code

            entity = get_entity_by_code(session, body.entity_code)
            if not entity:
                raise HTTPException(status_code=400, detail="Unknown entity")

            sug_uuid = _parse_uuid(suggestion_id, "suggestion_id")
            sug = session.execute(
                text(
                    """
                    SELECT suggested_account_code, suggested_debit_or_credit
                    FROM bank_classification_suggestions
                    WHERE id = :id AND entity_id = :entity_id
                    """
                ),
                {"id": sug_uuid, "entity_id": entity["id"]},
            ).mappings().first()
            if not sug:
                raise HTTPException(
                    status_code=404, detail=f"Suggestion not found: {suggestion_id}"
                )
            final_acct = body.final_account_code or sug["suggested_account_code"]
            if not final_acct or final_acct == "UNCLASSIFIED":
                raise HTTPException(
                    status_code=400,
                    detail="Suggestion has no account_code — supply final_account_code",
                )
            return record_user_feedback(
                session,
                entity_code=body.entity_code,
                suggestion_id=suggestion_id,
                final_account_code=final_acct,
                final_debit_or_credit=(
                    body.final_debit_or_credit or sug["suggested_debit_or_credit"]
                ),
                actor_email=body.actor_email,
                accepted=True,
            )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/suggestions/{suggestion_id}/override")
def post_override_suggestion(
    suggestion_id: str = Path(...),
    body: FeedbackRequest = ...,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    if not body.final_account_code:
        raise HTTPException(
            status_code=400,
            detail="final_account_code is required when overriding a suggestion",
        )
    try:
        with db_session() as session:
            return record_user_feedback(
                session,
                entity_code=body.entity_code,
                suggestion_id=suggestion_id,
                final_account_code=body.final_account_code,
                final_debit_or_credit=body.final_debit_or_credit,
                actor_email=body.actor_email,
                accepted=False,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
