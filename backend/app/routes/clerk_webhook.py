"""
Clerk -> BookWize webhook receiver.

Endpoint:
    POST /api/webhooks/clerk

Authentication:
    No Bearer token. Clerk signs every payload using svix; we verify the
    signature against CLERK_WEBHOOK_SECRET. Any request that doesn't carry
    a valid svix signature is rejected with 401.

Events handled:
    user.created / user.updated / user.deleted
    organizationMembership.created / .updated / .deleted
    organization.created / .updated

Any other event type is logged at INFO and acknowledged with 200 so Clerk
doesn't keep retrying — the SDK fires every event type that's enabled in
the dashboard, and we'd rather drop unknown ones than 500.

This endpoint must be exempt from any auth middleware. main.py registers
it before any global auth dependencies; the route itself reads no Bearer
token.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from ..db import db_session
from ..services_auth_clerk import (
    sync_clerk_membership_from_webhook,
    sync_clerk_org_from_webhook,
    sync_clerk_user_from_webhook,
    verify_clerk_webhook,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


@router.post("/clerk")
async def clerk_webhook(request: Request) -> dict[str, str]:
    body_bytes = await request.body()
    # svix expects lowercased header names. FastAPI gives us a case-insensitive
    # multi-dict; flatten to a plain dict for the SDK.
    headers = {k.lower(): v for k, v in request.headers.items()}

    event = verify_clerk_webhook(body_bytes, headers)

    event_type = event.get("type") or ""
    data = event.get("data") or {}

    try:
        with db_session() as session:
            if event_type.startswith("user."):
                sync_clerk_user_from_webhook(
                    session, event_type=event_type, data=data
                )
            elif event_type.startswith("organizationMembership."):
                sync_clerk_membership_from_webhook(
                    session, event_type=event_type, data=data
                )
            elif event_type.startswith("organization."):
                sync_clerk_org_from_webhook(
                    session, event_type=event_type, data=data
                )
            else:
                logger.info("Clerk webhook: unhandled event_type=%s", event_type)
                return {"ok": "ignored", "event_type": event_type}
    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Clerk webhook handler failed for event_type=%s", event_type
        )
        # Return 500 so Clerk retries — the alternative (swallow + 200) hides
        # sync failures and lets the DB drift from Clerk silently.
        raise HTTPException(status_code=500, detail="webhook handler failed")

    return {"ok": "processed", "event_type": event_type}
