"""
Stripe billing — HTTP routes.

Endpoints:
    POST /api/billing/checkout-session    create a Checkout Session
    POST /api/billing/portal-session      create a Customer Portal Session
    GET  /api/billing/subscription        current subscription for an entity
    POST /api/webhooks/stripe             Stripe-signed events
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from ..db import db_session
from ..services_auth import require_role
from ..services_auth_clerk import CurrentUser
from ..services_billing import (
    create_checkout_session,
    create_portal_session,
    get_subscription_for_entity,
    upsert_subscription_from_webhook,
    verify_stripe_webhook,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/billing", tags=["billing"])
webhook_router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------


class CheckoutSessionRequest(BaseModel):
    entity_code: str
    plan_tier: str = Field(pattern="^(starter|professional)$")
    success_url: str
    cancel_url: str


class PortalSessionRequest(BaseModel):
    entity_code: str
    return_url: str


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------


@router.post("/checkout-session")
def post_checkout_session(
    body: CheckoutSessionRequest,
    user: Any = Depends(require_role("admin")),
) -> dict[str, str]:
    clerk_user_id, email, name = _identity_from_user(user)
    with db_session() as session:
        return create_checkout_session(
            session,
            clerk_user_id=clerk_user_id,
            email=email,
            name=name,
            entity_code=body.entity_code,
            plan_tier=body.plan_tier,
            success_url=body.success_url,
            cancel_url=body.cancel_url,
        )


@router.post("/portal-session")
def post_portal_session(
    body: PortalSessionRequest,
    user: Any = Depends(require_role("admin")),
) -> dict[str, str]:
    clerk_user_id, email, name = _identity_from_user(user)
    with db_session() as session:
        return create_portal_session(
            session,
            clerk_user_id=clerk_user_id,
            email=email,
            name=name,
            return_url=body.return_url,
        )


@router.get("/subscription")
def get_billing_subscription(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    with db_session() as session:
        return get_subscription_for_entity(session, entity_code)


# --------------------------------------------------------------------------
# Stripe webhook
# --------------------------------------------------------------------------


@webhook_router.post("/stripe")
async def stripe_webhook(request: Request) -> dict[str, str]:
    body = await request.body()
    sig = request.headers.get("stripe-signature")
    event = verify_stripe_webhook(body, sig)
    event_type = event.get("type", "")
    data = (event.get("data") or {}).get("object") or {}

    try:
        with db_session() as session:
            if event_type in {
                "customer.subscription.created",
                "customer.subscription.updated",
                "customer.subscription.deleted",
            }:
                upsert_subscription_from_webhook(session, subscription=data)
            else:
                logger.info("Stripe webhook: unhandled event_type=%s", event_type)
                return {"ok": "ignored", "event_type": event_type}
    except HTTPException:
        raise
    except Exception:
        logger.exception("Stripe webhook handler failed for %s", event_type)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="webhook handler failed",
        )

    return {"ok": "processed", "event_type": event_type}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _identity_from_user(user: Any) -> tuple[str, str | None, str | None]:
    """Pull the Clerk user id + email + name out of whatever the auth
    dispatcher returned. Under Clerk this is a CurrentUser; under legacy
    JWT the route is rarely useful (legacy users aren't tied to Stripe
    customers) but we degrade gracefully."""
    if isinstance(user, CurrentUser):
        return user.clerk_user_id, user.email, None
    if isinstance(user, dict):
        clerk_user_id = user.get("clerk_user_id") or user.get("id")
        if not clerk_user_id:
            raise HTTPException(
                status_code=400,
                detail="Caller is not a Clerk user — Stripe billing is Clerk-only",
            )
        return str(clerk_user_id), user.get("email"), user.get("full_name")
    raise HTTPException(status_code=400, detail="Unknown caller identity shape")
