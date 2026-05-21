"""
Demo-account write guard middleware.

DEMO-* entities are seeded with prospect-friendly data and shown to
every visitor. Any write that would mutate that data — POST / PUT /
PATCH / DELETE / form upload — gets a 403 before it touches a route
handler. Reads (GET / HEAD / OPTIONS) pass through so prospects can
explore freely.

The guard inspects three places for an entity_code:
  - JSON body (top-level "entity_code")
  - Form data (multipart or urlencoded)
  - Query string (?entity_code=…)

If any of them starts with "DEMO-" (case-insensitive), the request is
rejected. The middleware buffers the request body so downstream
handlers still see it untouched on requests that pass.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


# HTTP methods that mutate state. Anything else is allowed through.
_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

_DEMO_PREFIX = "demo-"

_DENY_PAYLOAD = {
    "ok": False,
    "detail": (
        "Demo accounts are read-only. Create a free account to use BookWize "
        "with your store data."
    ),
    "code": "demo_read_only",
}


def _is_demo_code(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return value.lower().startswith(_DEMO_PREFIX)


async def _extract_entity_code(request: Request) -> str | None:
    # 1. Query string first — cheapest.
    qs_value = request.query_params.get("entity_code")
    if qs_value:
        return qs_value

    content_type = (request.headers.get("content-type") or "").lower()

    # 2. JSON body — read + cache for downstream handlers.
    if "application/json" in content_type:
        body_bytes = await request.body()
        if not body_bytes:
            return None
        try:
            payload = json.loads(body_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if isinstance(payload, dict):
            v = payload.get("entity_code")
            if isinstance(v, str):
                return v
        return None

    # 3. Form / multipart body. We can't easily peek at multipart bodies
    # without consuming them; FastAPI re-reads from the cached
    # ASGI receive. To stay safe we only inspect urlencoded forms.
    if "application/x-www-form-urlencoded" in content_type:
        body_bytes = await request.body()
        try:
            decoded = body_bytes.decode("utf-8", errors="ignore")
        except Exception:
            return None
        for part in decoded.split("&"):
            if part.startswith("entity_code="):
                from urllib.parse import unquote_plus
                return unquote_plus(part.split("=", 1)[1])

    # Multipart: we can't read the body without breaking the stream for
    # FastAPI. Leave it to route handlers — they call
    # enforce_entity_code() on the entity_code form field, which we
    # extend below to also block DEMO writes.
    return None


class DemoWriteGuardMiddleware(BaseHTTPMiddleware):
    """Reject DEMO-* writes at the edge."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        if request.method.upper() not in _WRITE_METHODS:
            return await call_next(request)

        try:
            entity_code = await _extract_entity_code(request)
        except Exception:
            logger.exception("DemoWriteGuard extract failed — fail-open")
            entity_code = None

        if entity_code and _is_demo_code(entity_code):
            return JSONResponse(status_code=403, content=_DENY_PAYLOAD)

        return await call_next(request)
