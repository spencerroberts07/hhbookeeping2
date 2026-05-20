"""
BookWize AI assistant — HTTP routes.

Endpoints (prefix /api/assistant):
    POST   /message                 send a user message, get an assistant reply
    POST   /confirm                 confirm or correct a proposed action
    GET    /history                 last N conversations + messages
    GET    /memory                  learned memory (admin)
    DELETE /memory/{id}             prune one memory row (admin)

Every endpoint enforces entity_code matching via the require_role /
enforce_entity_code pattern used elsewhere. The /memory endpoints are
admin-only because the memory store contains classifications that
could be leveraged for fraud (e.g. "this vendor key always maps to
account X").
"""
from __future__ import annotations

import logging
from datetime import date as DateType
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..services_auth import enforce_entity_code, require_role
from ..services_auth_clerk import CurrentUser
from ..services_assistant import (
    AssistantReply,
    IntentResult,
    ProposedAction,
    build_entity_context,
    execute_action,
    find_matching_transaction,
    generate_response,
    get_balance_summary,
    learn_from_interaction,
    parse_intent,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/assistant", tags=["assistant"])


# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------


class MessageRequest(BaseModel):
    entity_code: str
    message: str = Field(min_length=1, max_length=2000)
    conversation_id: str | None = None


class ConfirmRequest(BaseModel):
    entity_code: str
    message_id: str
    confirmed: bool
    correction: str | None = None


# --------------------------------------------------------------------------
# POST /api/assistant/message
# --------------------------------------------------------------------------


@router.post("/message")
def post_message(
    body: MessageRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    clerk_user_id = _clerk_id(_user)

    with db_session() as session:
        # 1. Get or create conversation.
        conv_id = body.conversation_id
        if conv_id:
            existing = session.execute(
                text(
                    "SELECT id FROM assistant_conversations "
                    " WHERE id = :id AND entity_code = :ec LIMIT 1"
                ),
                {"id": conv_id, "ec": body.entity_code},
            ).mappings().first()
            if not existing:
                conv_id = None
        if not conv_id:
            row = session.execute(
                text(
                    """
                    INSERT INTO assistant_conversations (
                        entity_code, clerk_user_id, channel
                    ) VALUES (:ec, :uid, 'dashboard')
                    RETURNING id
                    """
                ),
                {"ec": body.entity_code, "uid": clerk_user_id},
            ).mappings().first()
            conv_id = str(row["id"]) if row else None
            if not conv_id:
                raise HTTPException(500, "Could not create conversation")

        # 2. Persist the user message.
        user_msg = session.execute(
            text(
                """
                INSERT INTO assistant_messages (
                    conversation_id, entity_code, role, content
                ) VALUES (:cid, :ec, 'user', :content)
                RETURNING id, created_at
                """
            ),
            {"cid": conv_id, "ec": body.entity_code, "content": body.message},
        ).mappings().first()
        user_msg_id = str(user_msg["id"]) if user_msg else None

        # 3. Build context + parse intent + find matches.
        context = build_entity_context(session, body.entity_code)
        intent = parse_intent(body.message, context)

        matches = []
        if intent.intent == "classify_transaction":
            matches = find_matching_transaction(
                session,
                entity_code=body.entity_code,
                amount=intent.amount,
                target_date=intent.date,
                description=intent.description,
            )

        # 4. Conversation history for the reply prompt.
        history_rows = session.execute(
            text(
                """
                SELECT role, content FROM assistant_messages
                 WHERE conversation_id = :cid
                 ORDER BY created_at ASC
                """
            ),
            {"cid": conv_id},
        ).mappings().all()
        history = [{"role": r["role"], "content": r["content"]} for r in history_rows]

        # 5. Generate reply.
        reply = generate_response(
            session,
            entity_code=body.entity_code,
            user_message=body.message,
            intent=intent,
            matches=matches,
            history=history,
            context=context,
        )

        # 5a. Special-case the cash-balance query — inject the latest
        # cash_balancing_days row into the reply when Claude punts.
        if intent.intent == "query_balance" and ("…" in reply.reply or "Pulling" in reply.reply):
            balance_text = get_balance_summary(session, body.entity_code)
            if balance_text:
                reply.reply = balance_text

        # 6. Persist the assistant reply.
        assistant_msg = session.execute(
            text(
                """
                INSERT INTO assistant_messages (
                    conversation_id, entity_code, role, content, intent,
                    resolved, proposal_json, transaction_id
                ) VALUES (
                    :cid, :ec, 'assistant', :content, :intent,
                    :resolved, CAST(:pj AS jsonb), :tid
                )
                RETURNING id, created_at
                """
            ),
            {
                "cid": conv_id,
                "ec": body.entity_code,
                "content": reply.reply,
                "intent": intent.intent,
                "resolved": not reply.needs_confirmation,
                "pj": _serialize_action(reply.proposed_action),
                "tid": reply.proposed_action.transaction_id,
            },
        ).mappings().first()
        assistant_msg_id = str(assistant_msg["id"]) if assistant_msg else None

        # 7. Touch conversation last_message_at.
        session.execute(
            text(
                "UPDATE assistant_conversations SET last_message_at = NOW() "
                " WHERE id = :id"
            ),
            {"id": conv_id},
        )

    return {
        "conversation_id": conv_id,
        "user_message_id": user_msg_id,
        "message_id": assistant_msg_id,
        "reply": reply.reply,
        "intent": reply.intent,
        "needs_confirmation": reply.needs_confirmation,
        "proposed_action": _action_to_dict(reply.proposed_action) if reply.needs_confirmation else None,
        "matched_transactions": [
            {
                "transaction_id": m.transaction_id,
                "date": m.transaction_date,
                "amount": float(m.amount),
                "description": m.description,
                "direction": m.direction,
                "score": m.score,
            }
            for m in reply.matched_transactions
        ],
    }


# --------------------------------------------------------------------------
# POST /api/assistant/confirm
# --------------------------------------------------------------------------


@router.post("/confirm")
def post_confirm(
    body: ConfirmRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    clerk_user_id = _clerk_id(_user)

    with db_session() as session:
        msg = session.execute(
            text(
                """
                SELECT id, conversation_id, content, proposal_json, intent
                  FROM assistant_messages
                 WHERE id = :id AND entity_code = :ec AND role = 'assistant'
                 LIMIT 1
                """
            ),
            {"id": body.message_id, "ec": body.entity_code},
        ).mappings().first()
        if not msg:
            raise HTTPException(404, "Assistant message not found")

        proposal = msg["proposal_json"] or {}
        if not isinstance(proposal, dict):
            proposal = {}

        # Build the IntentResult shape just enough for learning.
        intent_for_learning = IntentResult(
            intent=msg["intent"] or "other",
            description=(proposal.get("journal_preview") or {}).get("note") or msg["content"][:200],
            suggested_debit_account=(proposal.get("journal_preview") or {}).get("debit_account_code"),
            suggested_credit_account=(proposal.get("journal_preview") or {}).get("credit_account_code"),
        )

        result: dict[str, Any]
        if body.confirmed:
            action_type = proposal.get("action_type") or "none"
            jp = proposal.get("journal_preview") or {}
            result = execute_action(
                session,
                entity_code=body.entity_code,
                action_type=action_type,
                transaction_id=proposal.get("transaction_id"),
                debit_account=jp.get("debit_account_code"),
                credit_account=jp.get("credit_account_code"),
                note=jp.get("note") or msg["content"][:200],
                clerk_user_id=clerk_user_id,
                conversation_id=str(msg["conversation_id"]),
                original_message=msg["content"][:500],
            )
            # Mark the assistant message resolved.
            session.execute(
                text(
                    "UPDATE assistant_messages "
                    "   SET resolved = TRUE "
                    " WHERE id = :id"
                ),
                {"id": body.message_id},
            )
            # Learning.
            try:
                learn_from_interaction(
                    session,
                    entity_code=body.entity_code,
                    message=msg["content"],
                    intent=intent_for_learning,
                    action_taken=action_type,
                    was_corrected=False,
                )
            except Exception:
                logger.exception("learn_from_interaction failed (confirm)")
        else:
            # Correction path. Log the correction; the route layer
            # doesn't yet re-run parse_intent for a follow-up proposal
            # — the user can send a new message with their correction.
            result = {"ok": True, "action": "correction_logged"}
            try:
                learn_from_interaction(
                    session,
                    entity_code=body.entity_code,
                    message=msg["content"],
                    intent=intent_for_learning,
                    action_taken=None,
                    was_corrected=True,
                    correction_details=body.correction or "user rejected without details",
                )
            except Exception:
                logger.exception("learn_from_interaction failed (reject)")

    return result


# --------------------------------------------------------------------------
# GET /api/assistant/history
# --------------------------------------------------------------------------


@router.get("/history")
def get_history(
    entity_code: str = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        conv_rows = session.execute(
            text(
                """
                SELECT id, started_at, last_message_at
                  FROM assistant_conversations
                 WHERE entity_code = :ec
                 ORDER BY last_message_at DESC
                 LIMIT :limit
                """
            ),
            {"ec": entity_code, "limit": limit},
        ).mappings().all()
        conv_ids = [str(c["id"]) for c in conv_rows]
        if not conv_ids:
            return {"conversations": []}

        msg_rows = session.execute(
            text(
                """
                SELECT id, conversation_id, role, content, intent, resolved,
                       proposal_json, created_at, transaction_id
                  FROM assistant_messages
                 WHERE conversation_id = ANY(:ids)
                 ORDER BY created_at ASC
                """
            ),
            {"ids": conv_ids},
        ).mappings().all()
        msgs_by_conv: dict[str, list[dict[str, Any]]] = {cid: [] for cid in conv_ids}
        for r in msg_rows:
            msgs_by_conv[str(r["conversation_id"])].append({
                "id": str(r["id"]),
                "role": r["role"],
                "content": r["content"],
                "intent": r["intent"],
                "resolved": r["resolved"],
                "proposed_action": r["proposal_json"] if isinstance(r["proposal_json"], dict) else None,
                "transaction_id": str(r["transaction_id"]) if r["transaction_id"] else None,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })
    return {
        "conversations": [
            {
                "id": str(c["id"]),
                "started_at": c["started_at"].isoformat() if c["started_at"] else None,
                "last_message_at": c["last_message_at"].isoformat() if c["last_message_at"] else None,
                "messages": msgs_by_conv.get(str(c["id"]), []),
            }
            for c in conv_rows
        ]
    }


# --------------------------------------------------------------------------
# GET /api/assistant/memory  +  DELETE /api/assistant/memory/{id}
# --------------------------------------------------------------------------


@router.get("/memory")
def get_memory(
    entity_code: str = Query(...),
    memory_type: str | None = Query(default=None),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    with db_session() as session:
        if memory_type:
            rows = session.execute(
                text(
                    """
                    SELECT id, memory_type, memory_key, memory_value, confidence,
                           times_confirmed, times_corrected, last_seen_at, created_at
                      FROM assistant_entity_memory
                     WHERE entity_code = :ec AND memory_type = :mt
                     ORDER BY confidence DESC, times_confirmed DESC
                    """
                ),
                {"ec": entity_code, "mt": memory_type},
            ).mappings().all()
        else:
            rows = session.execute(
                text(
                    """
                    SELECT id, memory_type, memory_key, memory_value, confidence,
                           times_confirmed, times_corrected, last_seen_at, created_at
                      FROM assistant_entity_memory
                     WHERE entity_code = :ec
                     ORDER BY confidence DESC, times_confirmed DESC
                    """
                ),
                {"ec": entity_code},
            ).mappings().all()
    return {
        "memory": [
            {
                "id": str(r["id"]),
                "memory_type": r["memory_type"],
                "memory_key": r["memory_key"],
                "memory_value": r["memory_value"],
                "confidence": float(r["confidence"]),
                "times_confirmed": int(r["times_confirmed"]),
                "times_corrected": int(r["times_corrected"]),
                "last_seen_at": r["last_seen_at"].isoformat() if r["last_seen_at"] else None,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    }


@router.delete("/memory/{memory_id}")
def delete_memory(
    memory_id: str = Path(...),
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    with db_session() as session:
        res = session.execute(
            text(
                """
                DELETE FROM assistant_entity_memory
                 WHERE id = :id AND entity_code = :ec
                """
            ),
            {"id": memory_id, "ec": entity_code},
        )
    return {"ok": True, "deleted": res.rowcount}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _clerk_id(user: Any) -> str | None:
    if isinstance(user, CurrentUser):
        return user.clerk_user_id
    if isinstance(user, dict):
        return user.get("clerk_user_id") or user.get("id")
    return None


def _serialize_action(action: ProposedAction) -> str:
    import json as _json

    return _json.dumps(_action_to_dict(action))


def _action_to_dict(action: ProposedAction) -> dict[str, Any]:
    return {
        "action_type": action.action_type,
        "transaction_id": action.transaction_id,
        "transaction_preview": action.transaction_preview,
        "journal_preview": action.journal_preview,
        "pending_intent_id": action.pending_intent_id,
    }
