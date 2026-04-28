"""
Authentication + multi-tenant role management service layer.

Why this is its own file:
    services.py is already ~3.8k lines. New modules go in their own
    services_<module>.py file. This module owns everything about users,
    sessions, JWT issue/verify, and per-entity RBAC.

Public surface (used by routes/auth.py and by other modules' write endpoints
via the require_role(...) dependency):

    Password helpers:
        hash_password(plain_password) -> str
        verify_password(plain_password, hashed) -> bool

    User CRUD:
        create_user(...)
        get_user_by_email(...)
        get_user_by_id(...)
        update_user(...)
        list_users(...)

    Roles:
        grant_user_entity_role(...)
        revoke_user_entity_role(...)
        get_user_entity_role(user_id, entity_id) -> str | None
        list_user_entity_roles(user_id) -> list[dict]
        ROLE_HIERARCHY (constant dict, role -> rank int)

    JWT:
        create_jwt_token(user) -> (token, expires_at)
        verify_jwt_token(token) -> claims dict (raises HTTPException 401 on bad)
        revoke_session(token_hash) -> None

    FastAPI dependencies:
        require_role(min_role) -> dependency callable
            Reads Bearer token from Authorization header, verifies JWT,
            looks up the user's role for the entity_code in the request
            (query string or form), and rejects with 403 if below min_role.
            Superadmins always pass.

    Audit:
        log_auth_event(session, *, user_id, event_type, ...)

Conventions:
    - All money is irrelevant here.
    - actor_email is captured on every write that the API forwards.
    - Token hashes are SHA-256 of the JWT string itself, stored so we can
      revoke without keeping the token plaintext.
"""
from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text

from .config import settings
from .db import db_session
from .services import _parse_uuid, get_entity_by_code


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

ROLE_VIEWER = "viewer"
ROLE_BOOKKEEPER = "bookkeeper"
ROLE_APPROVER = "approver"
ROLE_ADMIN = "admin"
ROLE_SUPERADMIN = "superadmin"

# Higher number = more privileged. superadmin is implicit (column on users)
# and beats every entity-scoped role.
ROLE_HIERARCHY: dict[str, int] = {
    ROLE_VIEWER: 10,
    ROLE_BOOKKEEPER: 20,
    ROLE_APPROVER: 30,
    ROLE_ADMIN: 40,
    ROLE_SUPERADMIN: 100,
}

VALID_ENTITY_ROLES = {ROLE_VIEWER, ROLE_BOOKKEEPER, ROLE_APPROVER, ROLE_ADMIN}

EVENT_LOGIN = "login"
EVENT_LOGOUT = "logout"
EVENT_FAILED_LOGIN = "failed_login"
EVENT_PASSWORD_CHANGE = "password_change"
EVENT_USER_CREATED = "user_created"
EVENT_ROLE_GRANTED = "role_granted"
EVENT_ROLE_REVOKED = "role_revoked"


# --------------------------------------------------------------------------
# Password hashing
# --------------------------------------------------------------------------
#
# We call bcrypt directly rather than going through passlib. passlib 1.7.4
# probes bcrypt.__about__.__version__ at import time, which was removed in
# bcrypt 4.1+, and the resulting AttributeError surfaces as
# "trapped error reading bcrypt version" plus a 500 on /api/auth/register
# under Python 3.13. Direct bcrypt avoids the whole probe.


def hash_password(plain_password: str) -> str:
    if not plain_password or len(plain_password) < 8:
        raise ValueError("password must be at least 8 characters long")
    return bcrypt.hashpw(
        plain_password.encode("utf-8"),
        bcrypt.gensalt(rounds=12),
    ).decode("utf-8")


def verify_password(plain_password: str, hashed: str) -> bool:
    if not plain_password or not hashed:
        return False
    try:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed.encode("utf-8"),
        )
    except Exception:
        return False


# --------------------------------------------------------------------------
# JWT
# --------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_jwt_token(user: dict) -> tuple[str, datetime]:
    """
    Build and sign a JWT for the given user row. Returns (token, expires_at).
    The session is NOT persisted by this call — callers should pass the
    returned token through register_session(...) when they want it tracked
    for revocation.
    """
    issued_at = _utcnow()
    expires_at = issued_at + timedelta(hours=int(settings.jwt_expiry_hours))
    payload = {
        "sub": str(user["id"]),
        "email": user["email"],
        "is_superadmin": bool(user.get("is_superadmin")),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": secrets.token_hex(16),
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, expires_at


def verify_jwt_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
        ) from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc


def register_session(
    session,
    *,
    user_id: UUID,
    token: str,
    expires_at: datetime,
    ip_address: str | None,
    user_agent: str | None,
) -> UUID:
    row = session.execute(
        text(
            """
            INSERT INTO user_sessions (
                user_id, token_hash, issued_at, expires_at, ip_address, user_agent
            ) VALUES (
                :user_id, :token_hash, NOW(), :expires_at, :ip_address, :user_agent
            )
            RETURNING id
            """
        ),
        {
            "user_id": user_id,
            "token_hash": _hash_token(token),
            "expires_at": expires_at,
            "ip_address": ip_address,
            "user_agent": user_agent,
        },
    ).mappings().first()
    return row["id"]


def revoke_session(session, token: str) -> bool:
    """Mark the session row for this exact token as revoked. Returns True
    if a session row was updated, False if the token was unknown."""
    row = session.execute(
        text(
            """
            UPDATE user_sessions
               SET revoked_at = NOW()
             WHERE token_hash = :token_hash
               AND revoked_at IS NULL
            RETURNING id
            """
        ),
        {"token_hash": _hash_token(token)},
    ).mappings().first()
    return row is not None


def is_session_revoked(session, token: str) -> bool:
    row = session.execute(
        text(
            """
            SELECT revoked_at, expires_at
              FROM user_sessions
             WHERE token_hash = :token_hash
             LIMIT 1
            """
        ),
        {"token_hash": _hash_token(token)},
    ).mappings().first()
    if row is None:
        # Unknown token (e.g. issued before sessions tracked) — let JWT
        # validation gate it. We don't force revocation lookup to succeed.
        return False
    return row["revoked_at"] is not None


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------


def log_auth_event(
    session,
    *,
    user_id: UUID | None,
    event_type: str,
    entity_id: UUID | None = None,
    actor_email: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    session.execute(
        text(
            """
            INSERT INTO auth_events (
                user_id, event_type, entity_id, actor_email,
                ip_address, user_agent, detail_json
            ) VALUES (
                :user_id, :event_type, :entity_id, :actor_email,
                :ip_address, :user_agent, CAST(:detail_json AS jsonb)
            )
            """
        ),
        {
            "user_id": user_id,
            "event_type": event_type,
            "entity_id": entity_id,
            "actor_email": actor_email,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "detail_json": json.dumps(detail or {}, default=str),
        },
    )


# --------------------------------------------------------------------------
# Users
# --------------------------------------------------------------------------


def _row_to_user_dict(row) -> dict[str, Any]:
    if row is None:
        return None  # type: ignore[return-value]
    return {
        "id": str(row["id"]),
        "email": row["email"],
        "full_name": row["full_name"],
        "is_active": row["is_active"],
        "is_superadmin": row["is_superadmin"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        "last_login_at": (
            row["last_login_at"].isoformat() if row.get("last_login_at") else None
        ),
    }


def get_user_by_email(session, email: str) -> dict[str, Any] | None:
    if not email:
        return None
    row = session.execute(
        text(
            """
            SELECT id, email, hashed_password, full_name, is_active, is_superadmin,
                   created_at, updated_at, last_login_at
              FROM users
             WHERE LOWER(email) = LOWER(:email)
             LIMIT 1
            """
        ),
        {"email": email.strip()},
    ).mappings().first()
    return dict(row) if row else None


def get_user_by_id(session, user_id: str | UUID) -> dict[str, Any] | None:
    user_uuid = _parse_uuid(str(user_id), "user_id")
    row = session.execute(
        text(
            """
            SELECT id, email, hashed_password, full_name, is_active, is_superadmin,
                   created_at, updated_at, last_login_at
              FROM users
             WHERE id = :user_id
             LIMIT 1
            """
        ),
        {"user_id": user_uuid},
    ).mappings().first()
    return dict(row) if row else None


def count_users(session) -> int:
    row = session.execute(text("SELECT COUNT(*) AS c FROM users")).mappings().first()
    return int(row["c"] or 0) if row else 0


def create_user(
    session,
    *,
    email: str,
    password: str,
    full_name: str | None,
    is_superadmin: bool = False,
    is_active: bool = True,
) -> dict[str, Any]:
    cleaned_email = (email or "").strip()
    if not cleaned_email or "@" not in cleaned_email:
        raise HTTPException(status_code=400, detail="email is required and must be valid")

    existing = get_user_by_email(session, cleaned_email)
    if existing:
        raise HTTPException(status_code=409, detail="A user with this email already exists")

    hashed = hash_password(password)
    row = session.execute(
        text(
            """
            INSERT INTO users (
                email, hashed_password, full_name, is_active, is_superadmin
            ) VALUES (
                :email, :hashed_password, :full_name, :is_active, :is_superadmin
            )
            RETURNING id, email, full_name, is_active, is_superadmin,
                      created_at, updated_at, last_login_at
            """
        ),
        {
            "email": cleaned_email,
            "hashed_password": hashed,
            "full_name": (full_name or "").strip() or None,
            "is_active": is_active,
            "is_superadmin": is_superadmin,
        },
    ).mappings().first()
    return _row_to_user_dict(row)


def update_user(
    session,
    *,
    user_id: str | UUID,
    full_name: str | None = None,
    is_active: bool | None = None,
    is_superadmin: bool | None = None,
    new_password: str | None = None,
) -> dict[str, Any]:
    user_uuid = _parse_uuid(str(user_id), "user_id")
    sets = ["updated_at = NOW()"]
    params: dict[str, Any] = {"user_id": user_uuid}
    if full_name is not None:
        sets.append("full_name = :full_name")
        params["full_name"] = full_name.strip() or None
    if is_active is not None:
        sets.append("is_active = :is_active")
        params["is_active"] = is_active
    if is_superadmin is not None:
        sets.append("is_superadmin = :is_superadmin")
        params["is_superadmin"] = is_superadmin
    if new_password is not None:
        sets.append("hashed_password = :hashed_password")
        params["hashed_password"] = hash_password(new_password)

    sql = f"""
        UPDATE users
           SET {", ".join(sets)}
         WHERE id = :user_id
        RETURNING id, email, full_name, is_active, is_superadmin,
                  created_at, updated_at, last_login_at
    """
    row = session.execute(text(sql), params).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return _row_to_user_dict(row)


def stamp_last_login(session, user_id: UUID) -> None:
    session.execute(
        text("UPDATE users SET last_login_at = NOW() WHERE id = :user_id"),
        {"user_id": user_id},
    )


def list_users(session) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT id, email, full_name, is_active, is_superadmin,
                   created_at, updated_at, last_login_at
              FROM users
             ORDER BY created_at DESC
            """
        )
    ).mappings().all()
    return [_row_to_user_dict(r) for r in rows]


# --------------------------------------------------------------------------
# Roles
# --------------------------------------------------------------------------


def get_user_entity_role(
    session, user_id: str | UUID, entity_id: str | UUID
) -> str | None:
    user_uuid = _parse_uuid(str(user_id), "user_id")
    entity_uuid = _parse_uuid(str(entity_id), "entity_id")
    row = session.execute(
        text(
            """
            SELECT role
              FROM user_entity_roles
             WHERE user_id = :user_id
               AND entity_id = :entity_id
               AND is_active = TRUE
             LIMIT 1
            """
        ),
        {"user_id": user_uuid, "entity_id": entity_uuid},
    ).mappings().first()
    return row["role"] if row else None


def list_user_entity_roles(session, user_id: str | UUID) -> list[dict[str, Any]]:
    user_uuid = _parse_uuid(str(user_id), "user_id")
    rows = session.execute(
        text(
            """
            SELECT r.id, r.entity_id, e.entity_code, e.entity_name,
                   r.role, r.granted_at, r.is_active
              FROM user_entity_roles r
              JOIN entities e ON e.id = r.entity_id
             WHERE r.user_id = :user_id
               AND r.is_active = TRUE
             ORDER BY e.entity_code
            """
        ),
        {"user_id": user_uuid},
    ).mappings().all()
    return [
        {
            "id": str(r["id"]),
            "entity_id": str(r["entity_id"]),
            "entity_code": r["entity_code"],
            "entity_name": r["entity_name"],
            "role": r["role"],
            "granted_at": r["granted_at"].isoformat() if r["granted_at"] else None,
            "is_active": r["is_active"],
        }
        for r in rows
    ]


def grant_user_entity_role(
    session,
    *,
    user_id: str | UUID,
    entity_id: str | UUID,
    role: str,
    granted_by_user_id: UUID | None,
) -> dict[str, Any]:
    if role not in VALID_ENTITY_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role '{role}'. Valid roles: {sorted(VALID_ENTITY_ROLES)}",
        )

    user_uuid = _parse_uuid(str(user_id), "user_id")
    entity_uuid = _parse_uuid(str(entity_id), "entity_id")

    # Revoke any existing active role for this (user, entity) so the new
    # grant is the only active one.
    session.execute(
        text(
            """
            UPDATE user_entity_roles
               SET is_active = FALSE,
                   revoked_at = NOW()
             WHERE user_id = :user_id
               AND entity_id = :entity_id
               AND is_active = TRUE
            """
        ),
        {"user_id": user_uuid, "entity_id": entity_uuid},
    )

    row = session.execute(
        text(
            """
            INSERT INTO user_entity_roles (
                user_id, entity_id, role, granted_by, is_active
            ) VALUES (
                :user_id, :entity_id, :role, :granted_by, TRUE
            )
            RETURNING id, user_id, entity_id, role, granted_at, is_active
            """
        ),
        {
            "user_id": user_uuid,
            "entity_id": entity_uuid,
            "role": role,
            "granted_by": granted_by_user_id,
        },
    ).mappings().first()

    return {
        "id": str(row["id"]),
        "user_id": str(row["user_id"]),
        "entity_id": str(row["entity_id"]),
        "role": row["role"],
        "granted_at": row["granted_at"].isoformat() if row["granted_at"] else None,
        "is_active": row["is_active"],
    }


def revoke_user_entity_role(
    session,
    *,
    user_id: str | UUID,
    entity_id: str | UUID,
) -> bool:
    user_uuid = _parse_uuid(str(user_id), "user_id")
    entity_uuid = _parse_uuid(str(entity_id), "entity_id")
    row = session.execute(
        text(
            """
            UPDATE user_entity_roles
               SET is_active = FALSE,
                   revoked_at = NOW()
             WHERE user_id = :user_id
               AND entity_id = :entity_id
               AND is_active = TRUE
            RETURNING id
            """
        ),
        {"user_id": user_uuid, "entity_id": entity_uuid},
    ).mappings().first()
    return row is not None


# --------------------------------------------------------------------------
# FastAPI dependency
# --------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


def _meets_role(user_role: str | None, is_superadmin: bool, min_role: str) -> bool:
    if is_superadmin:
        return True
    if user_role is None:
        return False
    return ROLE_HIERARCHY.get(user_role, 0) >= ROLE_HIERARCHY.get(min_role, 0)


def _extract_entity_code_from_request(request: Request) -> str | None:
    qp = request.query_params.get("entity_code")
    if qp:
        return qp.strip() or None
    # Many endpoints take entity_code in the body; lazily peek without
    # consuming the stream by relying on FastAPI's already-parsed body cache.
    # Since we can't reliably read the body here without consuming it, the
    # routes that use require_role will pass entity_code via dependencies
    # (Query/Form/Path). Falls through to None — caller must use
    # require_role_with_entity_code(...) form for body params.
    return None


def require_role(min_role: str):
    """
    FastAPI dependency: enforces that the caller has at least `min_role`
    on the entity referenced by the request.

    Looks for entity_code in:
      1. Query string (?entity_code=...)
      2. Path/state attribute set by the route (request.state.entity_code)

    For endpoints that take entity_code only in a JSON body, the route
    must set request.state.entity_code = body.entity_code BEFORE calling
    Depends(require_role(...)) — or use the explicit
    enforce_role(session, user, entity_code, min_role) function from a
    body-aware handler.

    Returns the authenticated user dict.
    """
    if min_role not in ROLE_HIERARCHY:
        raise ValueError(f"Unknown min_role: {min_role}")

    def dependency(
        request: Request,
        creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    ) -> dict[str, Any]:
        if creds is None or creds.scheme.lower() != "bearer" or not creds.credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = creds.credentials
        claims = verify_jwt_token(token)

        with db_session() as session:
            if is_session_revoked(session, token):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Session has been revoked",
                )
            user = get_user_by_id(session, claims["sub"])
            if user is None or not user["is_active"]:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="User no longer exists or is inactive",
                )

            # Superadmin bypasses entity-role checks.
            if user["is_superadmin"]:
                request.state.user = user
                return user

            entity_code = _extract_entity_code_from_request(request)
            if not entity_code:
                # Try request.state if the route set it manually before this
                # dependency was resolved (won't happen with Depends ordering,
                # but the handler can fall back to enforce_role()).
                entity_code = getattr(request.state, "entity_code", None)
            if not entity_code:
                raise HTTPException(
                    status_code=400,
                    detail="entity_code is required to authorize this request",
                )

            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(
                    status_code=404, detail=f"Unknown entity_code: {entity_code}"
                )

            role = get_user_entity_role(session, user["id"], str(entity["id"]))
            if not _meets_role(role, False, min_role):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"Role '{role}' on entity '{entity_code}' is below "
                        f"required '{min_role}'"
                    ),
                )

            request.state.user = user
            request.state.entity_id = str(entity["id"])
            request.state.entity_code = entity_code
            request.state.user_role = role
            return user

    return dependency


def enforce_role(
    session,
    *,
    user: dict[str, Any],
    entity_code: str,
    min_role: str,
) -> dict[str, Any]:
    """
    Body-aware role enforcement for endpoints that read entity_code from
    JSON. Call this from inside the route handler after parsing the body.
    Returns the entity dict for downstream use.
    """
    if min_role not in ROLE_HIERARCHY:
        raise ValueError(f"Unknown min_role: {min_role}")

    if user["is_superadmin"]:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(
                status_code=404, detail=f"Unknown entity_code: {entity_code}"
            )
        return dict(entity)

    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise HTTPException(
            status_code=404, detail=f"Unknown entity_code: {entity_code}"
        )
    role = get_user_entity_role(session, user["id"], str(entity["id"]))
    if not _meets_role(role, False, min_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Role '{role}' on entity '{entity_code}' is below "
                f"required '{min_role}'"
            ),
        )
    return dict(entity)


def get_current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """
    Lighter dependency that authenticates the user but doesn't check
    any entity role. Use for /me, /logout, etc.
    """
    if creds is None or creds.scheme.lower() != "bearer" or not creds.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = creds.credentials
    claims = verify_jwt_token(token)
    with db_session() as session:
        if is_session_revoked(session, token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session has been revoked",
            )
        user = get_user_by_id(session, claims["sub"])
        if user is None or not user["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User no longer exists or is inactive",
            )
    user["_token"] = token
    return user
