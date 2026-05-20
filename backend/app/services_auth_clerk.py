"""
Clerk-based authentication and per-entity RBAC.

Sits alongside services_auth.py (the legacy JWT module). The dispatcher in
services_auth.require_role picks between the two based on
settings.use_clerk_auth.

Token verification uses PyJWT against Clerk's JWKS endpoint. The JWKS URL
is either explicitly set via CLERK_JWKS_URL, or derived from
CLERK_PUBLISHABLE_KEY (Clerk publishable keys encode the instance host in
base64 after the pk_test_/pk_live_ prefix). The JWKS keys are cached for
10 minutes by PyJWT — Clerk rotates them rarely, this is plenty.

Public surface (used by routes and by the dispatcher):

    CurrentUser           dataclass returned by every role dependency
    require_owner         only 'admin' (top role) passes
    require_admin         alias for require_owner
    require_approver      admin or approver
    require_bookkeeper    admin, approver, bookkeeper
    require_viewer        any authenticated user with a mapped entity
    require_entity_access alias for require_viewer
    enforce_entity_code(user, entity_code)
                          body-aware check: 403 if the request's
                          entity_code does not match the user's
                          Clerk-org-mapped entity_code.

    sync_clerk_user_from_webhook(...)
    sync_clerk_membership_from_webhook(...)
    sync_clerk_org_from_webhook(...)
                          DB writers used by the webhook route.

    verify_clerk_webhook(body_bytes, headers)
                          svix signature check, returns parsed event dict.

Test note: tests monkeypatch _verify_clerk_token so no network or live
Clerk instance is touched.
"""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from typing import Any

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text

from .config import settings
from .db import db_session

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Constants — duplicated from services_auth to avoid an import cycle.
# Keep in sync with that module's ROLE_HIERARCHY.
# --------------------------------------------------------------------------

ROLE_VIEWER = "viewer"
ROLE_BOOKKEEPER = "bookkeeper"
ROLE_APPROVER = "approver"
ROLE_ADMIN = "admin"

ROLE_HIERARCHY: dict[str, int] = {
    ROLE_VIEWER: 10,
    ROLE_BOOKKEEPER: 20,
    ROLE_APPROVER: 30,
    ROLE_ADMIN: 40,
}

# Clerk org role string -> app role. The Clerk dashboard for BookWize must
# define org:viewer / org:bookkeeper / org:approver / org:admin. 'org:owner'
# is accepted as an alias for admin so an environment that only configured
# the three roles in the original spec keeps working.
ORG_ROLE_TO_APP_ROLE: dict[str, str] = {
    "org:viewer": ROLE_VIEWER,
    "org:bookkeeper": ROLE_BOOKKEEPER,
    "org:approver": ROLE_APPROVER,
    "org:admin": ROLE_ADMIN,
    "org:owner": ROLE_ADMIN,
}


_bearer = HTTPBearer(
    auto_error=False,
    scheme_name="ClerkBearer",
    bearerFormat="JWT",
    description="Clerk session token from the frontend (Authorization: Bearer ...).",
)


# --------------------------------------------------------------------------
# CurrentUser
# --------------------------------------------------------------------------


@dataclass
class CurrentUser:
    """
    What a Clerk-authenticated request resolves to. Mapping the Clerk session
    token to an app entity + role is the whole job of the auth layer; this
    object is what the route handler reads.

    For backward compatibility with code that historically did
    `current_user["id"]` against a legacy users row, __getitem__ and .get
    are implemented so most call sites keep working — but the 'id' returned
    is the Clerk user id (a string like 'user_2ab...'), NOT a UUID. Code that
    needs a UUID FK to the legacy users table cannot use this object directly
    and must look up by email or migrate the FK to clerk_user_id text.
    """
    clerk_user_id: str
    entity_code: str
    role: str
    email: str | None = None
    clerk_org_id: str | None = None
    raw_claims: dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        if key == "id":
            return self.clerk_user_id
        if key == "is_superadmin":
            return False
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except (AttributeError, KeyError):
            return default


# --------------------------------------------------------------------------
# Token verification
# --------------------------------------------------------------------------


_jwks_client_cache: dict[str, Any] = {}


def _derive_jwks_url() -> str:
    """
    Return Clerk's JWKS URL. Prefers settings.clerk_jwks_url. Otherwise
    derives from settings.clerk_publishable_key — the host is base64-encoded
    after the 'pk_test_'/'pk_live_' prefix, with a trailing '$' marker.
    """
    if settings.clerk_jwks_url:
        return settings.clerk_jwks_url

    pub = settings.clerk_publishable_key or ""
    if not pub:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Clerk auth is enabled but neither CLERK_JWKS_URL nor "
                "CLERK_PUBLISHABLE_KEY is set"
            ),
        )
    for prefix in ("pk_test_", "pk_live_"):
        if pub.startswith(prefix):
            encoded = pub[len(prefix):].encode("ascii")
            padding = b"=" * (-len(encoded) % 4)
            try:
                decoded = base64.b64decode(encoded + padding).decode("utf-8")
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to decode CLERK_PUBLISHABLE_KEY: {exc}",
                ) from exc
            host = decoded.rstrip("$")
            return f"https://{host}/.well-known/jwks.json"

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="CLERK_PUBLISHABLE_KEY must start with pk_test_ or pk_live_",
    )


def _get_jwks_client():
    """Cache the PyJWKClient by URL. PyJWT caches the signing keys itself
    with a 10-minute TTL, so we don't need a separate cache layer."""
    from jwt import PyJWKClient

    url = _derive_jwks_url()
    client = _jwks_client_cache.get(url)
    if client is None:
        client = PyJWKClient(url, cache_keys=True, lifespan=600)
        _jwks_client_cache[url] = client
    return client


def _verify_clerk_token(request: Request, token: str) -> dict[str, Any]:
    """
    Verify a Clerk session JWT against the instance's JWKS and return the
    claims dict. Raises 401 on any verification failure. The `request`
    argument is accepted for parity with the SDK signature; tests use it.

    Tests monkeypatch this function to inject fake claims without hitting
    the network.
    """
    import jwt

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key
    except Exception as exc:
        logger.warning("Clerk JWKS lookup failed: %r", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not verify Clerk session signing key",
        ) from exc

    try:
        return jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            # Clerk tokens carry 'azp' (authorized party) rather than 'aud';
            # PyJWT's audience check doesn't apply. Issuer is the instance
            # host, which the signing-key lookup already binds, so explicit
            # iss verification would be redundant.
            options={"verify_aud": False},
            leeway=5,  # absorb minor clock skew between Clerk and the API
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clerk session token has expired",
        ) from exc
    except jwt.InvalidTokenError as exc:
        logger.warning("Clerk token decode failed: %r", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Clerk session token",
        ) from exc


# --------------------------------------------------------------------------
# Claim extraction + DB lookup
# --------------------------------------------------------------------------


def _extract_org_role(claims: dict[str, Any]) -> str | None:
    """
    Clerk puts the active org role in 'org_role' (e.g. 'org:bookkeeper') for
    sessions with org_id set. Some templates put it under 'o.rol' or
    'organization.role'. We look in all known places, returning None if the
    session has no active org.
    """
    if claims.get("org_role"):
        return str(claims["org_role"])
    org_obj = claims.get("o") or claims.get("organization")
    if isinstance(org_obj, dict):
        for key in ("rol", "role"):
            if org_obj.get(key):
                return str(org_obj[key])
    return None


def _extract_org_id(claims: dict[str, Any]) -> str | None:
    if claims.get("org_id"):
        return str(claims["org_id"])
    org_obj = claims.get("o") or claims.get("organization")
    if isinstance(org_obj, dict):
        for key in ("id", "org_id"):
            if org_obj.get(key):
                return str(org_obj[key])
    return None


def _lookup_entity_for_org(session, clerk_org_id: str) -> dict[str, Any] | None:
    row = session.execute(
        text(
            """
            SELECT id, entity_code, entity_name
              FROM entities
             WHERE clerk_org_id = :org_id
             LIMIT 1
            """
        ),
        {"org_id": clerk_org_id},
    ).mappings().first()
    return dict(row) if row else None


# --------------------------------------------------------------------------
# Core dependency: resolve a CurrentUser
# --------------------------------------------------------------------------


def _resolve_current_user(request: Request, token: str) -> CurrentUser:
    claims = _verify_clerk_token(request, token)

    clerk_user_id = claims.get("sub")
    if not clerk_user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clerk token missing 'sub' claim",
        )

    clerk_org_id = _extract_org_id(claims)
    if not clerk_org_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "No active Clerk organization on this session. "
                "Switch into an organization in the app before retrying."
            ),
        )

    org_role = _extract_org_role(claims)
    app_role = ORG_ROLE_TO_APP_ROLE.get(org_role or "", None)
    if not app_role:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Clerk org role {org_role!r} does not map to an app role",
        )

    with db_session() as session:
        entity = _lookup_entity_for_org(session, clerk_org_id)
    if not entity:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Clerk org {clerk_org_id!r} is not linked to any entity. "
                "Ask a superadmin to set entities.clerk_org_id."
            ),
        )

    email = claims.get("email") or claims.get("email_address")
    return CurrentUser(
        clerk_user_id=str(clerk_user_id),
        entity_code=entity["entity_code"],
        role=app_role,
        email=str(email) if email else None,
        clerk_org_id=clerk_org_id,
        raw_claims=claims,
    )


def _require_min_role(min_role: str):
    """Factory: returns a FastAPI dep that enforces `min_role` after verifying
    the Clerk session and mapping its org to an entity."""
    if min_role not in ROLE_HIERARCHY:
        raise ValueError(f"Unknown min_role: {min_role!r}")

    def dependency(
        request: Request,
        creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
    ) -> CurrentUser:
        if creds is None or creds.scheme.lower() != "bearer" or not creds.credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        user = _resolve_current_user(request, creds.credentials)
        if ROLE_HIERARCHY.get(user.role, 0) < ROLE_HIERARCHY[min_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Role {user.role!r} on entity {user.entity_code!r} is below "
                    f"required {min_role!r}"
                ),
            )

        # Query-string entity_code check: if the client passed ?entity_code=
        # and it doesn't match the user's Clerk-mapped entity, reject. Form
        # / JSON-body entity_code is checked by the handler via
        # enforce_entity_code(...) because the body is not yet parsed here
        # and consuming the stream would break downstream parsing.
        qp_entity = request.query_params.get("entity_code")
        if qp_entity and qp_entity.strip() != user.entity_code:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Query entity_code {qp_entity!r} does not match the "
                    f"caller's Clerk-mapped entity {user.entity_code!r}"
                ),
            )

        # Mirror legacy services_auth: stamp the request state so downstream
        # middleware/handlers can read it without re-verifying the token.
        request.state.user = user
        request.state.entity_code = user.entity_code
        request.state.user_role = user.role
        return user

    return dependency


# Public dependencies (matching the spec's naming, plus require_admin as an
# explicit synonym for require_owner that better reflects the actual app role).
def require_owner() -> CurrentUser:  # type: ignore[empty-body]
    """Only the top role passes (app role 'admin', i.e. org:admin/org:owner)."""
    raise RuntimeError("Use Depends(require_owner) — do not call directly.")


def require_admin() -> CurrentUser:  # type: ignore[empty-body]
    raise RuntimeError("Use Depends(require_admin) — do not call directly.")


def require_approver() -> CurrentUser:  # type: ignore[empty-body]
    raise RuntimeError("Use Depends(require_approver) — do not call directly.")


def require_bookkeeper() -> CurrentUser:  # type: ignore[empty-body]
    raise RuntimeError("Use Depends(require_bookkeeper) — do not call directly.")


def require_viewer() -> CurrentUser:  # type: ignore[empty-body]
    raise RuntimeError("Use Depends(require_viewer) — do not call directly.")


def require_entity_access() -> CurrentUser:  # type: ignore[empty-body]
    raise RuntimeError("Use Depends(require_entity_access) — do not call directly.")


# Replace each stub above with its dependency. We do this dance so the names
# can be imported and used in `Depends(require_owner)` (FastAPI calls the
# resolved callable per request) without the wrapper-factory boilerplate
# appearing at every call site.
require_owner = _require_min_role(ROLE_ADMIN)               # noqa: F811
require_admin = _require_min_role(ROLE_ADMIN)               # noqa: F811
require_approver = _require_min_role(ROLE_APPROVER)         # noqa: F811
require_bookkeeper = _require_min_role(ROLE_BOOKKEEPER)     # noqa: F811
require_viewer = _require_min_role(ROLE_VIEWER)             # noqa: F811
require_entity_access = _require_min_role(ROLE_VIEWER)      # noqa: F811


# --------------------------------------------------------------------------
# Body-aware org-match enforcement
# --------------------------------------------------------------------------


def enforce_entity_code(user: CurrentUser, entity_code: str | None) -> None:
    """
    Routes that accept entity_code in the request body/form/query must call
    this before doing anything entity-scoped. It ensures the body's
    entity_code matches the entity that this user's Clerk org is bound to.

    Why this matters: under JWT, require_role read entity_code from the
    request and granted access if the user had a role on it. Under Clerk,
    the entity is fixed by the user's active org membership — so any
    discrepancy is the client trying to act outside its scope.
    """
    if entity_code is None:
        return  # nothing to check; downstream will validate presence
    if not user.entity_code:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No entity mapping for current Clerk session",
        )
    if entity_code.strip() != user.entity_code:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Request entity_code {entity_code!r} does not match the "
                f"caller's Clerk-mapped entity {user.entity_code!r}"
            ),
        )


# --------------------------------------------------------------------------
# Webhook sync helpers
# --------------------------------------------------------------------------


def sync_clerk_user_from_webhook(session, *, event_type: str, data: dict[str, Any]) -> None:
    """Handle user.created / user.updated / user.deleted events."""
    clerk_user_id = data.get("id")
    if not clerk_user_id:
        logger.warning("Clerk user webhook missing 'id': %s", event_type)
        return

    primary_email = None
    for entry in (data.get("email_addresses") or []):
        if entry.get("id") == data.get("primary_email_address_id"):
            primary_email = entry.get("email_address")
            break
    if primary_email is None and (data.get("email_addresses") or []):
        primary_email = data["email_addresses"][0].get("email_address")

    if event_type == "user.deleted":
        session.execute(
            text(
                """
                UPDATE clerk_users
                   SET is_active = FALSE,
                       updated_at = NOW()
                 WHERE clerk_user_id = :uid
                """
            ),
            {"uid": clerk_user_id},
        )
        return

    # user.created / user.updated
    session.execute(
        text(
            """
            INSERT INTO clerk_users (clerk_user_id, email, is_active, updated_at)
            VALUES (:uid, :email, TRUE, NOW())
            ON CONFLICT (clerk_user_id) DO UPDATE
               SET email = EXCLUDED.email,
                   is_active = TRUE,
                   updated_at = NOW()
            """
        ),
        {"uid": clerk_user_id, "email": primary_email},
    )


def sync_clerk_membership_from_webhook(
    session, *, event_type: str, data: dict[str, Any]
) -> None:
    """Handle organizationMembership.created / .updated / .deleted events."""
    public_user = data.get("public_user_data") or {}
    clerk_user_id = data.get("user_id") or public_user.get("user_id")
    organization = data.get("organization") or {}
    clerk_org_id = organization.get("id") or data.get("organization_id")
    org_role = data.get("role") or organization.get("role")  # e.g. 'org:bookkeeper'
    email = public_user.get("identifier")

    if not clerk_user_id or not clerk_org_id:
        logger.warning(
            "Clerk membership webhook missing user_id or org_id: %s payload=%s",
            event_type,
            list(data.keys()),
        )
        return

    entity_row = session.execute(
        text("SELECT entity_code FROM entities WHERE clerk_org_id = :org_id LIMIT 1"),
        {"org_id": clerk_org_id},
    ).mappings().first()
    entity_code = entity_row["entity_code"] if entity_row else None
    if entity_code is None:
        logger.warning(
            "Clerk org %s has no entities.clerk_org_id mapping yet; "
            "membership recorded with NULL entity_code",
            clerk_org_id,
        )

    app_role = ORG_ROLE_TO_APP_ROLE.get(org_role or "")

    if event_type.endswith(".deleted"):
        session.execute(
            text(
                """
                UPDATE clerk_users
                   SET entity_code = NULL,
                       role = NULL,
                       updated_at = NOW()
                 WHERE clerk_user_id = :uid
                """
            ),
            {"uid": clerk_user_id},
        )
        return

    session.execute(
        text(
            """
            INSERT INTO clerk_users (clerk_user_id, entity_code, role, email, is_active, updated_at)
            VALUES (:uid, :entity_code, :role, :email, TRUE, NOW())
            ON CONFLICT (clerk_user_id) DO UPDATE
               SET entity_code = EXCLUDED.entity_code,
                   role = EXCLUDED.role,
                   email = COALESCE(EXCLUDED.email, clerk_users.email),
                   is_active = TRUE,
                   updated_at = NOW()
            """
        ),
        {
            "uid": clerk_user_id,
            "entity_code": entity_code,
            "role": app_role,
            "email": email,
        },
    )


def sync_clerk_org_from_webhook(
    session, *, event_type: str, data: dict[str, Any]
) -> None:
    """
    Handle organization.created / .updated.

    We do NOT create new rows in entities here — entity creation is a heavy
    operation (fiscal year, base currency, QBO realm linkage) that must
    happen via the admin flow. Instead, if the Clerk org metadata names an
    existing entity_code, we wire entities.clerk_org_id to that row.
    """
    clerk_org_id = data.get("id")
    if not clerk_org_id:
        logger.warning("Clerk org webhook missing 'id': %s", event_type)
        return

    private_meta = data.get("private_metadata") or {}
    public_meta = data.get("public_metadata") or {}
    entity_code = (
        private_meta.get("entity_code")
        or public_meta.get("entity_code")
        or data.get("slug")  # last-resort: slug equals entity_code
    )
    if not entity_code:
        logger.info(
            "Clerk org %s has no entity_code in metadata; skipping link",
            clerk_org_id,
        )
        return

    res = session.execute(
        text(
            """
            UPDATE entities
               SET clerk_org_id = :org_id
             WHERE entity_code = :entity_code
               AND (clerk_org_id IS NULL OR clerk_org_id = :org_id)
            """
        ),
        {"org_id": clerk_org_id, "entity_code": entity_code},
    )
    if res.rowcount == 0:
        logger.warning(
            "Clerk org %s metadata.entity_code=%s but no matching entity "
            "row was updated (entity missing or already bound to a different org)",
            clerk_org_id,
            entity_code,
        )


# --------------------------------------------------------------------------
# Webhook signature verification (svix)
# --------------------------------------------------------------------------


def verify_clerk_webhook(body_bytes: bytes, headers: dict[str, str]) -> dict[str, Any]:
    """
    Verify a Clerk webhook payload using svix. Returns the parsed event dict.

    Raises HTTPException 401 on invalid signature, 500 if the secret is
    unset. Body MUST be the raw request body, not the parsed JSON.
    """
    if not settings.clerk_webhook_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CLERK_WEBHOOK_SECRET is not configured",
        )

    try:
        from svix.webhooks import Webhook, WebhookVerificationError
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="svix is not installed",
        ) from exc

    wh = Webhook(settings.clerk_webhook_secret)
    try:
        # svix.verify accepts the raw payload as str or bytes; pass str for
        # widest version compatibility.
        payload_text = body_bytes.decode("utf-8")
        event = wh.verify(payload_text, headers)
    except WebhookVerificationError as exc:
        logger.warning("Clerk webhook signature verification failed: %r", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook signature",
        ) from exc

    if isinstance(event, (str, bytes)):
        event = json.loads(event)
    return event
