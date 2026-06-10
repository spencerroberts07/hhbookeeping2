"""
Entity creation + per-user entity lookup.

Endpoints:
    POST   /api/entities                     create a new entity (admin)
    PATCH  /api/entities/{entity_code}       partial update (admin)
    GET    /api/me/entities                  entities mapped to the caller's
                                             Clerk org memberships

The frontend onboarding wizard calls POST /api/entities at step 1, then the
entity switcher and every protected page reads /api/me/entities on mount.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..config import settings
from ..db import db_session
from ..services_auth import require_role
from ..services_auth_clerk import (
    CurrentUser,
    ORG_ROLE_TO_APP_ROLE,
    _verify_clerk_token,
    _extract_org_id,
    _extract_org_role,
)
from ..services_billing import ensure_internal_subscription

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/api/entities", tags=["entities"])
me_router = APIRouter(prefix="/api/me", tags=["me"])


# --------------------------------------------------------------------------
# POST /api/entities — create a new entity
# --------------------------------------------------------------------------


class CreateEntityRequest(BaseModel):
    entity_code: str = Field(min_length=2, max_length=64)
    entity_name: str = Field(min_length=1)
    fiscal_year_end_month: int = Field(ge=1, le=12)
    fiscal_year_end_day: int = Field(ge=1, le=31)
    province: str = Field(min_length=2, max_length=4)
    base_currency: str = Field(default="CAD", max_length=3)
    clerk_org_id: str | None = None
    # When true, the new entity is auto-seeded with an internal-tier
    # billing_subscriptions row (no Stripe). Set this for owner stores.
    # 'DEMO-*' prefixed entity_codes auto-enable internal regardless.
    internal: bool = False


@router.post("", status_code=201)
def create_entity(
    body: CreateEntityRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """
    Create a new entity. Caller must be admin on at least one existing entity
    (legacy JWT) or hold org:admin in any Clerk org (Clerk mode). The created
    entity is wired to the supplied clerk_org_id if present — otherwise it
    starts unlinked and the dealer must complete the Clerk org-link step.
    """
    with db_session() as session:
        existing = session.execute(
            text("SELECT id FROM entities WHERE entity_code = :code"),
            {"code": body.entity_code},
        ).mappings().first()
        if existing:
            raise HTTPException(
                status_code=409,
                detail=f"Entity {body.entity_code!r} already exists",
            )

        # Every entity belongs to an `organizations` row (legacy multi-tenant
        # concept that pre-dates Clerk). For new dealer signups we mint a
        # fresh organization row tied to the new entity.
        organization_id = uuid4()
        session.execute(
            text(
                """
                INSERT INTO organizations (id, name, created_at)
                VALUES (:id, :name, NOW())
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"id": organization_id, "name": body.entity_name},
        )

        # Pull the creator's Clerk user id from request.state.user (the Clerk
        # dependency stamped it) — falls back to None under legacy JWT.
        creator_id: str | None = None
        if isinstance(_user, CurrentUser):
            creator_id = _user.clerk_user_id
        elif isinstance(_user, dict) and _user.get("clerk_user_id"):
            creator_id = _user["clerk_user_id"]

        row = session.execute(
            text(
                """
                INSERT INTO entities (
                    id, organization_id, entity_code, entity_name,
                    fiscal_year_end_month, fiscal_year_end_day,
                    base_currency, province, clerk_org_id,
                    created_by_clerk_user_id, created_at
                ) VALUES (
                    uuid_generate_v4(), :organization_id, :entity_code, :entity_name,
                    :fiscal_year_end_month, :fiscal_year_end_day,
                    :base_currency, :province, :clerk_org_id,
                    :created_by_clerk_user_id, NOW()
                )
                RETURNING id, organization_id, entity_code, entity_name,
                          fiscal_year_end_month, fiscal_year_end_day,
                          base_currency, province, clerk_org_id
                """
            ),
            {
                "organization_id": organization_id,
                "entity_code": body.entity_code,
                "entity_name": body.entity_name,
                "fiscal_year_end_month": body.fiscal_year_end_month,
                "fiscal_year_end_day": body.fiscal_year_end_day,
                "base_currency": body.base_currency,
                "province": body.province,
                "clerk_org_id": body.clerk_org_id,
                "created_by_clerk_user_id": creator_id,
            },
        ).mappings().first()

        # DEMO-* entities and any caller that explicitly asks for it
        # get an internal-tier subscription seeded immediately. This
        # keeps demo stores out of Stripe and the billing UI.
        # TODO: Replace with real Stripe subscription when an internal
        # entity is ready to be billed. Delete its billing_subscriptions
        # row with plan_tier='internal' and run through
        # /settings/billing checkout flow.
        if body.internal or body.entity_code.upper().startswith("DEMO-"):
            try:
                ensure_internal_subscription(session, entity_code=body.entity_code)
            except Exception:
                logger.exception(
                    "ensure_internal_subscription failed for %s — non-fatal",
                    body.entity_code,
                )

        return _entity_to_dict(row)


# --------------------------------------------------------------------------
# PATCH /api/entities/{entity_code}
# --------------------------------------------------------------------------


class UpdateEntityRequest(BaseModel):
    entity_name: str | None = None
    fiscal_year_end_month: int | None = Field(default=None, ge=1, le=12)
    fiscal_year_end_day: int | None = Field(default=None, ge=1, le=31)
    province: str | None = None
    clerk_org_id: str | None = None


@router.patch("/{entity_code}")
def update_entity(
    body: UpdateEntityRequest,
    entity_code: str = Path(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    updates: dict[str, Any] = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No updatable fields supplied")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["entity_code"] = entity_code

    with db_session() as session:
        row = session.execute(
            text(
                f"""
                UPDATE entities
                   SET {set_clause}
                 WHERE entity_code = :entity_code
                RETURNING id, organization_id, entity_code, entity_name,
                          fiscal_year_end_month, fiscal_year_end_day,
                          base_currency, province, clerk_org_id
                """
            ),
            updates,
        ).mappings().first()
        if not row:
            raise HTTPException(
                status_code=404, detail=f"Entity {entity_code!r} not found"
            )
        return _entity_to_dict(row)


# --------------------------------------------------------------------------
# Notification preferences (per-entity JSONB on entities)
# --------------------------------------------------------------------------


_DEFAULT_NOTIFICATION_PREFS = {
    "month_end_reminders": True,
    "variance_alerts": True,
    "approval_requests": True,
    "payment_receipts": True,
}


@router.get("/{entity_code}/notifications")
def get_notification_preferences(
    entity_code: str = Path(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text(
                """
                SELECT notification_preferences
                  FROM entities
                 WHERE entity_code = :code
                """
            ),
            {"code": entity_code},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        prefs = dict(_DEFAULT_NOTIFICATION_PREFS)
        prefs.update(row.get("notification_preferences") or {})
        return {"entity_code": entity_code, "notification_preferences": prefs}


class NotificationPrefsRequest(BaseModel):
    month_end_reminders: bool | None = None
    variance_alerts: bool | None = None
    approval_requests: bool | None = None
    payment_receipts: bool | None = None
    # AP alert sub-object: {email_enabled, remittance_advice_enabled, thresholds}
    ap_alerts: dict | None = None


@router.patch("/{entity_code}/notifications")
def update_notification_preferences(
    body: NotificationPrefsRequest,
    entity_code: str = Path(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Merge-update — keys not in the body are left untouched. Returns
    the full effective prefs after merging."""
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(400, "No preferences supplied")

    import json as _json

    with db_session() as session:
        row = session.execute(
            text(
                """
                UPDATE entities
                   SET notification_preferences =
                       COALESCE(notification_preferences, '{}'::jsonb) ||
                       CAST(:patch AS jsonb)
                 WHERE entity_code = :code
                RETURNING notification_preferences
                """
            ),
            {"code": entity_code, "patch": _json.dumps(updates)},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Unknown entity: {entity_code}")
        prefs = dict(_DEFAULT_NOTIFICATION_PREFS)
        prefs.update(row["notification_preferences"] or {})
        return {"entity_code": entity_code, "notification_preferences": prefs}


# --------------------------------------------------------------------------
# GET /api/me/entities
# --------------------------------------------------------------------------
#
# This endpoint is unusual: it must work even when the caller's active Clerk
# org isn't mapped to any entity yet (e.g. a freshly signed-up dealer who is
# still on the onboarding wizard). That means require_role(...) would 403 on
# the missing entity mapping. We resolve manually instead.


@me_router.get("/entities")
def list_my_entities(request: Request) -> dict[str, Any]:
    """
    Returns every entity the caller's Clerk *organization memberships* map to.
    Reads `org_id` and `org_role` directly from the verified token claims,
    walks Clerk's user-memberships list if available, then looks up the
    matching entities.

    Under the legacy JWT path this endpoint returns an empty list — callers
    on the JWT path use the legacy /api/auth/me roles array instead.
    """
    if not settings.use_clerk_auth:
        return {"entities": []}

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )
    token = auth_header.split(" ", 1)[1].strip()
    claims = _verify_clerk_token(request, token)

    # The session token only carries the *active* org. The complete
    # memberships list lives in Clerk's REST API — but for the v1 build the
    # frontend resolves that via Clerk's `useOrganizationList()` hook and
    # passes us org IDs to look up. So this endpoint returns the entity for
    # the active org plus, optionally, any orgs the client asks us to
    # resolve via the `org_ids` query param.
    active_org_id = _extract_org_id(claims)
    org_ids: list[str] = []
    if active_org_id:
        org_ids.append(active_org_id)

    extra = request.query_params.getlist("org_ids") if hasattr(
        request.query_params, "getlist"
    ) else []
    if not extra:
        # Starlette returns a multidict; fall back to comma-separated parse.
        joined = request.query_params.get("org_ids")
        if joined:
            extra = [s.strip() for s in joined.split(",") if s.strip()]
    org_ids = list(dict.fromkeys([*org_ids, *extra]))  # dedupe, preserve order

    if not org_ids:
        return {"entities": []}

    active_role = ORG_ROLE_TO_APP_ROLE.get(_extract_org_role(claims) or "", None)

    with db_session() as session:
        rows = session.execute(
            text(
                """
                SELECT entity_code, entity_name, clerk_org_id
                  FROM entities
                 WHERE clerk_org_id = ANY(:org_ids)
                """
            ),
            {"org_ids": org_ids},
        ).mappings().all()

    entities = []
    for r in rows:
        entities.append(
            {
                "entity_code": r["entity_code"],
                "entity_name": r["entity_name"],
                "clerk_org_id": r["clerk_org_id"],
                # The role we have for sure is the one in the active token.
                # For other orgs the frontend should pass the role from the
                # Clerk membership object; this endpoint doesn't call Clerk's
                # REST API.
                "role": active_role if r["clerk_org_id"] == active_org_id else None,
            }
        )
    return {"entities": entities}


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _entity_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "organization_id": str(row["organization_id"]),
        "entity_code": row["entity_code"],
        "entity_name": row["entity_name"],
        "fiscal_year_end_month": row["fiscal_year_end_month"],
        "fiscal_year_end_day": row["fiscal_year_end_day"],
        "base_currency": row["base_currency"],
        "province": row.get("province") if isinstance(row, dict) else row["province"],
        "clerk_org_id": (
            row.get("clerk_org_id")
            if isinstance(row, dict)
            else row["clerk_org_id"]
        ),
    }
