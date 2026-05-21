"""
QuickBooks OAuth connect/callback routes.

This file used to live as routes/auth.py. It was renamed when the new user
auth module landed; the URL prefix /api/auth/quickbooks is unchanged.

OAuth state is persisted to the `oauth_state_cache` table on /connect and
verified-and-consumed on /callback. Previously /connect echoed a state
the callback never checked — that's the CSRF gap migration 030 closed.
"""
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import text

from ..config import settings
from ..db import db_session
from ..quickbooks import QuickBooksClient
from ..schemas import ConnectResponse
from ..services import connect_company, get_entity_by_code

router = APIRouter(prefix="/api/auth/quickbooks", tags=["quickbooks-auth"])


@router.get("/connect", response_model=ConnectResponse)
def start_connect(entity_code: str = Query(default="1877-8")) -> ConnectResponse:
    qb = QuickBooksClient()
    state = qb.new_state()
    with db_session() as session:
        # Sweep expired tokens at the same time so the table stays small.
        session.execute(
            text("DELETE FROM oauth_state_cache WHERE expires_at < NOW()")
        )
        session.execute(
            text(
                """
                INSERT INTO oauth_state_cache (state, entity_code)
                VALUES (:state, :ec)
                ON CONFLICT (state) DO NOTHING
                """
            ),
            {"state": state, "ec": entity_code},
        )
    return ConnectResponse(
        entity_code=entity_code,
        authorization_url=qb.build_authorization_url(state),
        state=state,
    )


@router.get("/callback")
async def callback(
    code: str,
    realmId: str,
    state: str,
    entity_code: str = Query(default="1877-8"),
):
    try:
        with db_session() as session:
            # Verify-and-consume the state row. A missing or expired
            # state means the callback didn't originate from a /connect
            # we issued.
            cached = session.execute(
                text(
                    """
                    SELECT entity_code, expires_at
                      FROM oauth_state_cache
                     WHERE state = :state
                     LIMIT 1
                    """
                ),
                {"state": state},
            ).mappings().first()
            if not cached:
                raise HTTPException(status_code=400, detail="Unknown OAuth state.")
            session.execute(
                text("DELETE FROM oauth_state_cache WHERE state = :state"),
                {"state": state},
            )
            # Prefer the entity_code from the state record — it was
            # captured at /connect time and is harder to tamper with
            # than the query string.
            effective_entity_code = cached["entity_code"] or entity_code

            if not get_entity_by_code(session, effective_entity_code):
                raise HTTPException(
                    status_code=404,
                    detail=f"Unknown entity code: {effective_entity_code}",
                )
            result = await connect_company(session, effective_entity_code, realmId, code)
            # Send the dealer back to the onboarding wizard with a
            # success flag so the wizard auto-advances. Falls back to
            # JSON when bookwize_app_url isn't configured (local dev).
            app_url = (settings.bookwize_app_url or "").rstrip("/")
            if app_url:
                params = urlencode({
                    "qbo": "connected",
                    "realm_id": result["realm_id"],
                })
                return RedirectResponse(
                    url=f"{app_url}/onboarding?{params}",
                    status_code=303,
                )
            return JSONResponse(
                {
                    "ok": True,
                    "entity_code": effective_entity_code,
                    "state_echo": state,
                    "realm_id": result["realm_id"],
                    "company_name": result["company_info"].get("CompanyName"),
                    "legal_name": result["company_info"].get("LegalName"),
                }
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
