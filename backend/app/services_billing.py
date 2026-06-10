"""
Stripe billing service layer.

Billing model (per q27 + q28):
  - One Stripe **customer** per dealer (keyed by their Clerk user id).
  - One Stripe **subscription** per entity (one store = one subscription).
  - Subscription line items: 1× plan-tier price (starter or professional)
    + 0..N× additional-store price for any entities beyond the first.

This file owns:
  - Customer create-or-fetch
  - Checkout session creation (subscription mode)
  - Customer portal session creation (self-serve management)
  - Subscription read + persistence on webhook events

The dependency on the `stripe` package is loaded lazily so the rest of the
app still imports when STRIPE_SECRET_KEY is not configured.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import text

from .config import settings

logger = logging.getLogger(__name__)


def _stripe():
    """Lazy import + key application — never at module load."""
    if not settings.stripe_secret_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="STRIPE_SECRET_KEY is not configured",
        )
    try:
        import stripe  # type: ignore
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="stripe is not installed (pip install stripe)",
        ) from exc
    stripe.api_key = settings.stripe_secret_key
    return stripe


# --------------------------------------------------------------------------
# Plan-tier predicates + internal-account helpers
# --------------------------------------------------------------------------


def is_internal(plan_tier: str | None) -> bool:
    return plan_tier == "internal"


def is_starter(plan_tier: str | None) -> bool:
    # Internal accounts get Professional feature parity, so they are
    # NOT starter — every starter-only feature gate should fail for them.
    return plan_tier == "starter"


def is_professional(plan_tier: str | None) -> bool:
    # Internal accounts get every Professional feature — treat them as
    # professional for every entitlement check.
    return plan_tier in {"professional", "internal"}


def _is_internal_by_code(session, entity_code: str | None) -> bool:
    """Check entities.is_internal from DB + DEMO-* prefix guard.
    An entity with is_internal=TRUE bypasses Stripe. DEMO-* prefix
    is a secondary guard for ad-hoc demo entities without a DB row.
    """
    if not entity_code:
        return False
    if entity_code.upper().startswith("DEMO-"):
        return True
    row = session.execute(
        text("SELECT is_internal FROM entities WHERE entity_code = :code"),
        {"code": entity_code},
    ).mappings().first()
    return bool(row and row["is_internal"])


def _internal_subscription_payload(entity_code: str) -> dict[str, Any]:
    """Shape returned to callers when an entity is internal-tier.
    Mirrors get_subscription_for_entity's fields so frontend code never
    has to special-case missing values.

    TODO: Replace with real Stripe subscription when owner is ready to
    be billed. Simply delete the billing_subscriptions row with
    plan_tier='internal' and run through /settings/billing checkout
    flow.
    """
    return {
        "status": "active",
        "plan_tier": "internal",
        "current_period_end": None,
        "trial_end": None,
        "cancel_at_period_end": False,
        "store_count": 1,
        "customer_id": None,
        "message": "Owner account — no billing required",
    }


# --------------------------------------------------------------------------
# Customer lookup / create
# --------------------------------------------------------------------------


def get_or_create_customer(
    session,
    *,
    clerk_user_id: str,
    email: str | None,
    name: str | None,
) -> dict[str, Any]:
    """Return the billing_customers row for this Clerk user, creating it
    (and the matching Stripe customer) on first call."""
    row = session.execute(
        text(
            """
            SELECT id, stripe_customer_id, email, name
              FROM billing_customers
             WHERE clerk_user_id = :uid
            """
        ),
        {"uid": clerk_user_id},
    ).mappings().first()
    if row:
        return dict(row)

    stripe = _stripe()
    customer = stripe.Customer.create(
        email=email,
        name=name,
        metadata={"clerk_user_id": clerk_user_id},
    )

    row = session.execute(
        text(
            """
            INSERT INTO billing_customers (
                clerk_user_id, stripe_customer_id, email, name
            ) VALUES (:uid, :sid, :email, :name)
            RETURNING id, stripe_customer_id, email, name
            """
        ),
        {
            "uid": clerk_user_id,
            "sid": customer["id"],
            "email": email,
            "name": name,
        },
    ).mappings().first()
    return dict(row)


# --------------------------------------------------------------------------
# Checkout
# --------------------------------------------------------------------------


def _price_id_for_tier(tier: str) -> str:
    if tier == "starter":
        if not settings.stripe_starter_price_id:
            raise HTTPException(500, "STRIPE_STARTER_PRICE_ID is not configured")
        return settings.stripe_starter_price_id
    if tier == "professional":
        if not settings.stripe_professional_price_id:
            raise HTTPException(500, "STRIPE_PROFESSIONAL_PRICE_ID is not configured")
        return settings.stripe_professional_price_id
    raise HTTPException(400, f"Unknown plan_tier {tier!r}")


def _additional_store_price_id() -> str:
    if not settings.stripe_additional_store_price_id:
        raise HTTPException(500, "STRIPE_ADDITIONAL_STORE_PRICE_ID is not configured")
    return settings.stripe_additional_store_price_id


def create_checkout_session(
    session,
    *,
    clerk_user_id: str,
    email: str | None,
    name: str | None,
    entity_code: str,
    plan_tier: str,
    success_url: str,
    cancel_url: str,
) -> dict[str, str]:
    """
    Build a Stripe Checkout Session for subscribing the dealer's entity to
    a plan. The first store creates the subscription with the base plan
    price. Subsequent stores append an `additional_store` line item to the
    existing subscription via the portal (handled out-of-band).
    """
    stripe = _stripe()
    entity = session.execute(
        text("SELECT id, entity_name FROM entities WHERE entity_code = :code"),
        {"code": entity_code},
    ).mappings().first()
    if not entity:
        raise HTTPException(404, f"Entity {entity_code!r} not found")

    customer = get_or_create_customer(
        session,
        clerk_user_id=clerk_user_id,
        email=email,
        name=name,
    )

    checkout = stripe.checkout.Session.create(
        mode="subscription",
        customer=customer["stripe_customer_id"],
        line_items=[
            {"price": _price_id_for_tier(plan_tier), "quantity": 1},
        ],
        subscription_data={
            "trial_period_days": 30,  # q9
            "metadata": {
                "entity_code": entity_code,
                "entity_id": str(entity["id"]),
                "plan_tier": plan_tier,
                "clerk_user_id": clerk_user_id,
            },
        },
        success_url=success_url,
        cancel_url=cancel_url,
        client_reference_id=entity_code,
        allow_promotion_codes=True,
    )

    return {"url": checkout["url"], "session_id": checkout["id"]}


# --------------------------------------------------------------------------
# Portal
# --------------------------------------------------------------------------


def create_portal_session(
    session,
    *,
    clerk_user_id: str,
    email: str | None,
    name: str | None,
    return_url: str,
) -> dict[str, str]:
    stripe = _stripe()
    customer = get_or_create_customer(
        session,
        clerk_user_id=clerk_user_id,
        email=email,
        name=name,
    )
    portal = stripe.billing_portal.Session.create(
        customer=customer["stripe_customer_id"],
        return_url=return_url,
    )
    return {"url": portal["url"]}


# --------------------------------------------------------------------------
# Subscription read
# --------------------------------------------------------------------------


def get_subscription_for_entity(
    session, entity_code: str
) -> dict[str, Any]:
    """
    Returns the subscription row for an entity. Empty values when the entity
    has no subscription yet (e.g. mid-trial-signup or post-cancel).

    Internal-tier entities short-circuit before any Stripe state is
    consulted: entities with is_internal=TRUE in the DB (or a 'DEMO-' prefix)
    get a synthetic internal payload even if no billing_subscriptions row
    exists. This is the safety net for owner + demo stores that should never
    hit Stripe.
    """
    # Safety-net fallback — checked first so a misconfigured entity can't
    # accidentally be billed as a real dealer.
    if _is_internal_by_code(session, entity_code):
        return _internal_subscription_payload(entity_code)

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :code"),
        {"code": entity_code},
    ).mappings().first()
    if not entity:
        raise HTTPException(404, f"Entity {entity_code!r} not found")

    row = session.execute(
        text(
            """
            SELECT s.plan_tier, s.status, s.current_period_end, s.trial_end,
                   s.cancel_at_period_end, c.stripe_customer_id,
                   (SELECT COUNT(*) FROM billing_subscriptions s2
                     WHERE s2.billing_customer_id = s.billing_customer_id
                       AND s2.status NOT IN ('canceled','incomplete')) AS store_count
              FROM billing_subscriptions s
              JOIN billing_customers c ON c.id = s.billing_customer_id
             WHERE s.entity_id = :eid
            """
        ),
        {"eid": entity["id"]},
    ).mappings().first()

    if not row:
        return {
            "status": None,
            "plan_tier": None,
            "current_period_end": None,
            "trial_end": None,
            "cancel_at_period_end": False,
            "store_count": 0,
            "customer_id": None,
        }

    # Explicit internal subscription in the DB — short-circuit Stripe
    # values too (current_period_end is set to 100yrs out by migration
    # 031; we hide it to avoid confusion).
    if is_internal(row["plan_tier"]):
        return _internal_subscription_payload(entity_code)

    return {
        "status": row["status"],
        "plan_tier": row["plan_tier"],
        "current_period_end": (
            row["current_period_end"].isoformat()
            if row["current_period_end"]
            else None
        ),
        "trial_end": row["trial_end"].isoformat() if row["trial_end"] else None,
        "cancel_at_period_end": row["cancel_at_period_end"],
        "store_count": int(row["store_count"]),
        "customer_id": row["stripe_customer_id"],
    }


def ensure_internal_subscription(
    session, *, entity_code: str
) -> None:
    """Create a billing_subscriptions row at plan_tier='internal' for
    the given entity if one doesn't already exist. Used when POST
    /api/entities receives a DEMO-* code or an explicit internal flag.

    Idempotent. No-op when the entity already has any subscription row.
    """
    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :code"),
        {"code": entity_code},
    ).mappings().first()
    if not entity:
        return

    existing = session.execute(
        text(
            "SELECT 1 FROM billing_subscriptions WHERE entity_id = :eid LIMIT 1"
        ),
        {"eid": entity["id"]},
    ).first()
    if existing:
        return

    # Re-use the singleton internal billing_customers row created by
    # migration 031. Create-if-missing keeps this resilient.
    customer = session.execute(
        text(
            """
            INSERT INTO billing_customers (clerk_user_id, stripe_customer_id, name)
            VALUES ('internal_owner', 'internal_owner',
                    'BookWize Internal — Owner & Demo Accounts')
            ON CONFLICT (clerk_user_id) DO UPDATE
               SET updated_at = NOW()
            RETURNING id
            """
        )
    ).mappings().first()
    if not customer:
        return

    session.execute(
        text(
            """
            INSERT INTO billing_subscriptions (
                entity_id, billing_customer_id, stripe_subscription_id,
                plan_tier, status, current_period_end, cancel_at_period_end
            ) VALUES (
                :eid, :cid, :sid, 'internal', 'active',
                NOW() + INTERVAL '100 years', FALSE
            )
            ON CONFLICT (entity_id) DO NOTHING
            """
        ),
        {
            "eid": entity["id"],
            "cid": customer["id"],
            "sid": f"internal_owner:{entity_code}",
        },
    )


def upsert_subscription_from_webhook(
    session, *, subscription: dict[str, Any]
) -> None:
    """
    Persist (or update) a billing_subscriptions row from a Stripe
    `customer.subscription.*` webhook payload. Idempotent.
    """
    sub_id = subscription.get("id")
    customer_id = subscription.get("customer")
    status_value = subscription.get("status")
    metadata = subscription.get("metadata") or {}
    entity_code = metadata.get("entity_code")
    plan_tier = metadata.get("plan_tier")
    cur_period_end = subscription.get("current_period_end")
    trial_end = subscription.get("trial_end")
    cancel_at_period_end = bool(subscription.get("cancel_at_period_end"))

    if not (sub_id and customer_id and entity_code and plan_tier and status_value):
        logger.warning(
            "Stripe sub webhook missing required fields: sub_id=%s "
            "customer_id=%s entity_code=%s plan_tier=%s status=%s",
            sub_id, customer_id, entity_code, plan_tier, status_value,
        )
        return

    customer_row = session.execute(
        text(
            "SELECT id FROM billing_customers WHERE stripe_customer_id = :sid"
        ),
        {"sid": customer_id},
    ).mappings().first()
    if not customer_row:
        logger.warning(
            "Stripe sub webhook for unknown customer %s — ignoring", customer_id
        )
        return

    entity = session.execute(
        text("SELECT id FROM entities WHERE entity_code = :code"),
        {"code": entity_code},
    ).mappings().first()
    if not entity:
        logger.warning(
            "Stripe sub webhook for unknown entity %s — ignoring", entity_code
        )
        return

    from datetime import datetime, timezone

    def _ts(epoch: int | None):
        if not epoch:
            return None
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc)

    session.execute(
        text(
            """
            INSERT INTO billing_subscriptions (
                entity_id, billing_customer_id, stripe_subscription_id,
                plan_tier, status, current_period_end, trial_end,
                cancel_at_period_end, updated_at
            ) VALUES (
                :entity_id, :customer_id, :sub_id,
                :plan_tier, :status, :cur_end, :trial_end,
                :cancel_at_pe, NOW()
            )
            ON CONFLICT (stripe_subscription_id) DO UPDATE
               SET plan_tier = EXCLUDED.plan_tier,
                   status = EXCLUDED.status,
                   current_period_end = EXCLUDED.current_period_end,
                   trial_end = EXCLUDED.trial_end,
                   cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                   updated_at = NOW()
            """
        ),
        {
            "entity_id": entity["id"],
            "customer_id": customer_row["id"],
            "sub_id": sub_id,
            "plan_tier": plan_tier,
            "status": status_value,
            "cur_end": _ts(cur_period_end),
            "trial_end": _ts(trial_end),
            "cancel_at_pe": cancel_at_period_end,
        },
    )


def verify_stripe_webhook(payload: bytes, sig_header: str | None) -> dict[str, Any]:
    if not settings.stripe_webhook_secret:
        raise HTTPException(500, "STRIPE_WEBHOOK_SECRET is not configured")
    if not sig_header:
        raise HTTPException(400, "Missing Stripe-Signature header")
    stripe = _stripe()
    try:
        return stripe.Webhook.construct_event(
            payload, sig_header, settings.stripe_webhook_secret
        )
    except Exception as exc:
        logger.warning("Stripe webhook verification failed: %r", exc)
        raise HTTPException(401, "Invalid Stripe webhook signature") from exc
