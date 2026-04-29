"""
User authentication + role management routes.

Endpoints (prefix /api/auth):
    POST   /register
        Create the first superadmin (allowed only when no users exist),
        OR create any user when called by an authenticated superadmin.
    POST   /login                   email + password -> {access_token, ...}
    POST   /logout                  revokes the current bearer token
    GET    /me                      returns the current user + their roles
    POST   /users/{user_id}/roles   grant a role on an entity (admin/superadmin)
    DELETE /users/{user_id}/roles/{entity_id}
                                    revoke a user's role on an entity
                                    (admin on that entity, or superadmin)
    GET    /users                   list all users (superadmin only)

QBO OAuth lives in routes/qbo_auth.py with prefix /api/auth/quickbooks —
that path was preserved so existing QBO redirects keep working.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from pydantic import BaseModel, EmailStr, Field

import jwt as _jwt

from ..config import settings
from ..db import db_session
from ..services_auth import (
    EVENT_FAILED_LOGIN,
    EVENT_LOGIN,
    EVENT_LOGOUT,
    EVENT_ROLE_GRANTED,
    EVENT_ROLE_REVOKED,
    EVENT_USER_CREATED,
    ROLE_ADMIN,
    ROLE_SUPERADMIN,
    VALID_ENTITY_ROLES,
    count_users,
    create_jwt_token,
    create_user,
    enforce_role,
    get_current_user,
    get_user_by_email,
    get_user_by_id,
    get_user_entity_role,
    grant_user_entity_role,
    list_user_entity_roles,
    list_users,
    log_auth_event,
    register_session,
    revoke_session,
    revoke_user_entity_role,
    stamp_last_login,
    verify_password,
)
from ..services import _parse_uuid, get_entity_by_code

router = APIRouter(prefix="/api/auth", tags=["auth"])


# --------------------------------------------------------------------------
# Schemas (route-local; not adding to schemas.py to keep that file tight)
# --------------------------------------------------------------------------


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    full_name: str | None = None
    is_superadmin: bool = False


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: str
    user: dict[str, Any]


class GrantRoleRequest(BaseModel):
    entity_code: str
    role: str
    actor_email: str | None = None


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _client_meta(request: Request) -> tuple[str | None, str | None]:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    return ip, user_agent


def _user_to_response(user: dict[str, Any], roles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "id": str(user["id"]),
        "email": user["email"],
        "full_name": user.get("full_name"),
        "is_active": user.get("is_active"),
        "is_superadmin": user.get("is_superadmin"),
        "created_at": user.get("created_at"),
        "last_login_at": user.get("last_login_at"),
        "roles": roles or [],
    }


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------


@router.post("/register", status_code=201)
def register(
    body: RegisterRequest,
    request: Request,
):
    """
    Create a user. Two modes:
      - Bootstrap: when zero users exist, anyone can register and the first
        user is forced to is_superadmin=TRUE regardless of body flag.
      - Normal: requires a logged-in superadmin (Authorization: Bearer ...).
    """
    ip, user_agent = _client_meta(request)

    with db_session() as session:
        existing_count = count_users(session)

        if existing_count == 0:
            # Bootstrap path — no auth required, but we always make this user a superadmin.
            new_user = create_user(
                session,
                email=str(body.email),
                password=body.password,
                full_name=body.full_name,
                is_superadmin=True,
            )
            log_auth_event(
                session,
                user_id=_parse_uuid(new_user["id"], "user_id"),
                event_type=EVENT_USER_CREATED,
                actor_email=str(body.email),
                ip_address=ip,
                user_agent=user_agent,
                detail={"bootstrap": True, "is_superadmin": True},
            )
            return _user_to_response(new_user)

        # Normal path — require superadmin. Inline auth check (we can't use
        # Depends(get_current_user) because the bootstrap path above must
        # work without a token).
        from ..services_auth import is_session_revoked, verify_jwt_token

        auth_header = request.headers.get("authorization", "")
        if not auth_header.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Bearer token required")
        token = auth_header.split(" ", 1)[1].strip()
        claims = verify_jwt_token(token)
        if is_session_revoked(session, token):
            raise HTTPException(status_code=401, detail="Session revoked")
        actor = get_user_by_id(session, claims["sub"])
        if not actor or not actor["is_superadmin"]:
            raise HTTPException(status_code=403, detail="Only superadmins can create users")

        new_user = create_user(
            session,
            email=str(body.email),
            password=body.password,
            full_name=body.full_name,
            is_superadmin=bool(body.is_superadmin),
        )
        log_auth_event(
            session,
            user_id=_parse_uuid(new_user["id"], "user_id"),
            event_type=EVENT_USER_CREATED,
            actor_email=actor["email"],
            ip_address=ip,
            user_agent=user_agent,
            detail={"created_by": actor["email"], "is_superadmin": new_user["is_superadmin"]},
        )
        return _user_to_response(new_user)


@router.post("/login", response_model=LoginResponse)
def login(body: LoginRequest, request: Request):
    ip, user_agent = _client_meta(request)
    with db_session() as session:
        user = get_user_by_email(session, str(body.email))
        if not user or not user.get("is_active"):
            log_auth_event(
                session,
                user_id=user["id"] if user else None,
                event_type=EVENT_FAILED_LOGIN,
                actor_email=str(body.email),
                ip_address=ip,
                user_agent=user_agent,
                detail={"reason": "no_user_or_inactive"},
            )
            raise HTTPException(status_code=401, detail="Invalid email or password")

        if not verify_password(body.password, user["hashed_password"]):
            log_auth_event(
                session,
                user_id=user["id"],
                event_type=EVENT_FAILED_LOGIN,
                actor_email=user["email"],
                ip_address=ip,
                user_agent=user_agent,
                detail={"reason": "bad_password"},
            )
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token, expires_at = create_jwt_token(user)
        register_session(
            session,
            user_id=user["id"],
            token=token,
            expires_at=expires_at,
            ip_address=ip,
            user_agent=user_agent,
        )
        stamp_last_login(session, user["id"])
        log_auth_event(
            session,
            user_id=user["id"],
            event_type=EVENT_LOGIN,
            actor_email=user["email"],
            ip_address=ip,
            user_agent=user_agent,
        )
        roles = list_user_entity_roles(session, user["id"])

        return LoginResponse(
            access_token=token,
            expires_at=expires_at.isoformat(),
            user=_user_to_response(user, roles),
        )


@router.post("/logout")
def logout(request: Request, current_user: dict = Depends(get_current_user)):
    ip, user_agent = _client_meta(request)
    token = current_user["_token"]
    with db_session() as session:
        revoked = revoke_session(session, token)
        log_auth_event(
            session,
            user_id=_parse_uuid(current_user["id"], "user_id"),
            event_type=EVENT_LOGOUT,
            actor_email=current_user["email"],
            ip_address=ip,
            user_agent=user_agent,
            detail={"already_revoked": not revoked},
        )
    return {"ok": True}


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    with db_session() as session:
        roles = list_user_entity_roles(session, current_user["id"])
    return _user_to_response(current_user, roles)


@router.post("/users/{user_id}/roles")
def grant_role(
    body: GrantRoleRequest,
    request: Request,
    user_id: str = Path(...),
    current_user: dict = Depends(get_current_user),
):
    """
    Grant `role` to `user_id` on the entity identified by `entity_code`.
    Caller must be:
      - superadmin, OR
      - admin on that same entity_code
    """
    if body.role not in VALID_ENTITY_ROLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role. Valid roles: {sorted(VALID_ENTITY_ROLES)}",
        )

    ip, user_agent = _client_meta(request)

    with db_session() as session:
        target_user = get_user_by_id(session, user_id)
        if not target_user:
            raise HTTPException(status_code=404, detail="Target user not found")

        # Authorize the caller
        entity = enforce_role(
            session,
            user=current_user,
            entity_code=body.entity_code,
            min_role=ROLE_ADMIN,
        )

        granted = grant_user_entity_role(
            session,
            user_id=target_user["id"],
            entity_id=entity["id"],
            role=body.role,
            granted_by_user_id=_parse_uuid(current_user["id"], "user_id"),
        )

        log_auth_event(
            session,
            user_id=_parse_uuid(target_user["id"], "user_id"),
            event_type=EVENT_ROLE_GRANTED,
            entity_id=entity["id"],
            actor_email=current_user["email"],
            ip_address=ip,
            user_agent=user_agent,
            detail={
                "role": body.role,
                "entity_code": body.entity_code,
                "granted_to": target_user["email"],
            },
        )
        return granted


@router.delete("/users/{user_id}/roles/{entity_id}")
def revoke_role(
    request: Request,
    user_id: str = Path(...),
    entity_id: str = Path(...),
    current_user: dict = Depends(get_current_user),
):
    ip, user_agent = _client_meta(request)
    with db_session() as session:
        target_user = get_user_by_id(session, user_id)
        if not target_user:
            raise HTTPException(status_code=404, detail="Target user not found")

        # Look up the entity so we can authorize via enforce_role
        entity_uuid = _parse_uuid(entity_id, "entity_id")
        entity_row = session.execute(
            __import__("sqlalchemy").text(
                "SELECT id, entity_code FROM entities WHERE id = :id"
            ),
            {"id": entity_uuid},
        ).mappings().first()
        if not entity_row:
            raise HTTPException(status_code=404, detail="Entity not found")

        # Authorize
        if not current_user["is_superadmin"]:
            caller_role = get_user_entity_role(session, current_user["id"], entity_uuid)
            if caller_role != ROLE_ADMIN:
                raise HTTPException(
                    status_code=403,
                    detail="Caller must be admin on this entity (or superadmin)",
                )

        ok = revoke_user_entity_role(
            session,
            user_id=target_user["id"],
            entity_id=entity_uuid,
        )
        if not ok:
            raise HTTPException(
                status_code=404, detail="No active role to revoke for this user/entity"
            )

        log_auth_event(
            session,
            user_id=_parse_uuid(target_user["id"], "user_id"),
            event_type=EVENT_ROLE_REVOKED,
            entity_id=entity_uuid,
            actor_email=current_user["email"],
            ip_address=ip,
            user_agent=user_agent,
            detail={"entity_code": entity_row["entity_code"]},
        )
        return {"ok": True}


# --------------------------------------------------------------------------
# DEBUG endpoint — REMOVE after JWT_SECRET mismatch is resolved.
# Exposes the first 8 chars of the loaded JWT_SECRET so we can confirm at a
# glance whether the token-issuing instance and the token-verifying instance
# share the same key. Returns whatever payload it can decode (signed or not)
# plus the verification outcome.
# --------------------------------------------------------------------------


@router.get("/debug-token")
def debug_token(request: Request):
    secret_prefix = settings.jwt_secret[:8]
    secret_length = len(settings.jwt_secret)

    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        return {
            "DEBUG": "REMOVE THIS ENDPOINT AFTER USE",
            "received_authorization_header": bool(auth_header),
            "jwt_secret_prefix": secret_prefix,
            "jwt_secret_length": secret_length,
            "jwt_algorithm": settings.jwt_algorithm,
            "verification_succeeded": False,
            "error": "No Bearer token in Authorization header",
        }

    token = auth_header.split(" ", 1)[1].strip()

    unverified_payload: Any = None
    unverified_header: Any = None
    decode_unverified_error: str | None = None
    try:
        unverified_payload = _jwt.decode(token, options={"verify_signature": False})
    except Exception as exc:
        decode_unverified_error = repr(exc)
    try:
        unverified_header = _jwt.get_unverified_header(token)
    except Exception as exc:
        if decode_unverified_error is None:
            decode_unverified_error = repr(exc)

    verified_payload: Any = None
    verification_error: str | None = None
    verification_succeeded = False
    try:
        verified_payload = _jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
        verification_succeeded = True
    except Exception as exc:
        verification_error = repr(exc)

    return {
        "DEBUG": "REMOVE THIS ENDPOINT AFTER USE",
        "jwt_secret_prefix": secret_prefix,
        "jwt_secret_length": secret_length,
        "jwt_algorithm": settings.jwt_algorithm,
        "token_length": len(token),
        "unverified_header": unverified_header,
        "unverified_payload": unverified_payload,
        "decode_unverified_error": decode_unverified_error,
        "verification_succeeded": verification_succeeded,
        "verified_payload": verified_payload,
        "verification_error": verification_error,
    }


@router.get("/users")
def list_all_users(current_user: dict = Depends(get_current_user)):
    if not current_user["is_superadmin"]:
        raise HTTPException(status_code=403, detail="Superadmin only")
    with db_session() as session:
        users = list_users(session)
        # Hydrate roles for each
        for u in users:
            u["roles"] = list_user_entity_roles(session, u["id"])
    return {"count": len(users), "users": users}
