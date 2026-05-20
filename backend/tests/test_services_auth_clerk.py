"""
Unit tests for the Clerk auth layer.

Stdlib-only (unittest + FastAPI TestClient). The Clerk SDK is never called
because we monkeypatch _verify_clerk_token to return canned claims, and the
DB lookup is monkeypatched to return a canned entity row — so the tests
do not need a Clerk API key, a network, or a live Postgres.

Run from the backend directory:
    python -m unittest tests.test_services_auth_clerk -v
"""
from __future__ import annotations

import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

# Make `app` importable without installing the package.
_BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BACKEND_DIR))

# Settings need these vars to import. Use throwaway values — the JWT secret
# only has to be >= 32 chars and not a placeholder.
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("QBO_CLIENT_ID", "test-client-id")
os.environ.setdefault("QBO_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("QBO_REDIRECT_URI", "http://localhost:8000/cb")
os.environ.setdefault(
    "JWT_SECRET", "test-only-secret-do-not-use-anywhere-real-32chars"
)
os.environ.setdefault("CLERK_SECRET_KEY", "sk_test_unit_tests_only_never_real")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app import services_auth_clerk
from app.services_auth_clerk import (
    CurrentUser,
    enforce_entity_code,
    require_admin,
    require_approver,
    require_bookkeeper,
    require_viewer,
)


# --------------------------------------------------------------------------
# Test helpers — monkeypatchable verify + DB lookup
# --------------------------------------------------------------------------


@contextmanager
def stubbed_clerk(
    *,
    org_role: str = "org:bookkeeper",
    org_id: str = "org_test_001",
    entity_row: dict[str, Any] | None = {"id": "uuid-1", "entity_code": "1877-8", "entity_name": "BridleA"},
    sub: str = "user_test_abc",
    email: str = "tester@bookwize.ca",
):
    """Patch token verification + entity lookup with canned values for one test."""
    fake_claims = {
        "sub": sub,
        "org_id": org_id,
        "org_role": org_role,
        "email": email,
    }

    def _fake_verify(request, token):
        if not token or token == "bad":
            from fastapi import HTTPException

            raise HTTPException(status_code=401, detail="Invalid Clerk session token")
        return fake_claims

    def _fake_lookup(session, clerk_org_id):
        if entity_row is None:
            return None
        return entity_row

    with patch.object(services_auth_clerk, "_verify_clerk_token", _fake_verify), \
         patch.object(services_auth_clerk, "_lookup_entity_for_org", _fake_lookup), \
         patch.object(services_auth_clerk, "db_session", _DummySession):
        yield


@contextmanager
def _DummySession():
    """db_session is used only to wrap the entity lookup; the lookup is
    already patched so we just need something that supports `with`."""
    yield None


# --------------------------------------------------------------------------
# Test app — minimal FastAPI app exercising each role dependency
# --------------------------------------------------------------------------


def _build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/admin")
    def admin_route(user: CurrentUser = Depends(require_admin)):
        return {"role": user.role, "entity_code": user.entity_code, "id": user["id"]}

    @app.get("/approver")
    def approver_route(user: CurrentUser = Depends(require_approver)):
        return {"role": user.role}

    @app.get("/bookkeeper")
    def bookkeeper_route(user: CurrentUser = Depends(require_bookkeeper)):
        return {"role": user.role}

    @app.get("/viewer")
    def viewer_route(user: CurrentUser = Depends(require_viewer)):
        return {"role": user.role}

    @app.post("/body-scoped")
    def body_route(
        payload: dict[str, Any], user: CurrentUser = Depends(require_bookkeeper)
    ):
        enforce_entity_code(user, payload.get("entity_code"))
        return {"ok": True, "entity_code": user.entity_code}

    return app


# --------------------------------------------------------------------------
# The actual tests
# --------------------------------------------------------------------------


class ClerkAuthDependencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = _build_app()
        self.client = TestClient(self.app, raise_server_exceptions=False)

    # ------ token / entity lookup ------

    def test_valid_token_returns_entity_code(self):
        with stubbed_clerk(org_role="org:bookkeeper"):
            r = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["role"], "bookkeeper")

    def test_invalid_token_returns_401(self):
        with stubbed_clerk():
            r = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer bad"}
            )
        self.assertEqual(r.status_code, 401)

    def test_missing_token_returns_401(self):
        with stubbed_clerk():
            r = self.client.get("/bookkeeper")
        self.assertEqual(r.status_code, 401)

    def test_token_for_unmapped_org_returns_403(self):
        with stubbed_clerk(entity_row=None):
            r = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(r.status_code, 403)
        self.assertIn("not linked", r.json()["detail"].lower())

    def test_token_with_no_org_returns_403(self):
        with stubbed_clerk(org_id=""):
            r = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(r.status_code, 403)

    # ------ role hierarchy ------

    def test_admin_role_passes_every_dep(self):
        with stubbed_clerk(org_role="org:admin"):
            for path in ("/admin", "/approver", "/bookkeeper", "/viewer"):
                r = self.client.get(path, headers={"Authorization": "Bearer good"})
                self.assertEqual(r.status_code, 200, f"{path} should pass for admin")

    def test_owner_role_passes_admin(self):
        # 'org:owner' is mapped to admin for backward compat with the
        # 3-role variant of the spec.
        with stubbed_clerk(org_role="org:owner"):
            r = self.client.get("/admin", headers={"Authorization": "Bearer good"})
        self.assertEqual(r.status_code, 200)

    def test_bookkeeper_role_passes_bookkeeper_fails_approver(self):
        with stubbed_clerk(org_role="org:bookkeeper"):
            ok = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer good"}
            )
            blocked = self.client.get(
                "/approver", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(blocked.status_code, 403)

    def test_viewer_role_fails_bookkeeper_passes_viewer(self):
        with stubbed_clerk(org_role="org:viewer"):
            blocked = self.client.get(
                "/bookkeeper", headers={"Authorization": "Bearer good"}
            )
            ok = self.client.get(
                "/viewer", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(blocked.status_code, 403)
        self.assertEqual(ok.status_code, 200)

    def test_approver_passes_approver_fails_admin(self):
        with stubbed_clerk(org_role="org:approver"):
            ok = self.client.get(
                "/approver", headers={"Authorization": "Bearer good"}
            )
            blocked = self.client.get(
                "/admin", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(blocked.status_code, 403)

    def test_unknown_org_role_returns_403(self):
        with stubbed_clerk(org_role="org:wat"):
            r = self.client.get(
                "/viewer", headers={"Authorization": "Bearer good"}
            )
        self.assertEqual(r.status_code, 403)

    # ------ query / body entity_code mismatch ------

    def test_query_entity_code_mismatch_returns_403(self):
        with stubbed_clerk():
            r = self.client.get(
                "/bookkeeper?entity_code=NOT-MINE",
                headers={"Authorization": "Bearer good"},
            )
        self.assertEqual(r.status_code, 403)
        self.assertIn("does not match", r.json()["detail"])

    def test_query_entity_code_match_passes(self):
        with stubbed_clerk():
            r = self.client.get(
                "/bookkeeper?entity_code=1877-8",
                headers={"Authorization": "Bearer good"},
            )
        self.assertEqual(r.status_code, 200)

    def test_body_entity_code_mismatch_returns_403(self):
        with stubbed_clerk():
            r = self.client.post(
                "/body-scoped",
                headers={"Authorization": "Bearer good"},
                json={"entity_code": "EVIL-INC"},
            )
        self.assertEqual(r.status_code, 403)

    def test_body_entity_code_match_returns_200(self):
        with stubbed_clerk():
            r = self.client.post(
                "/body-scoped",
                headers={"Authorization": "Bearer good"},
                json={"entity_code": "1877-8"},
            )
        self.assertEqual(r.status_code, 200)


class CurrentUserMappingShimTests(unittest.TestCase):
    """The legacy route code does `current_user["id"]` etc. The Clerk
    CurrentUser must support that for the dispatcher to be a drop-in."""

    def test_getitem_id_returns_clerk_user_id(self):
        u = CurrentUser(
            clerk_user_id="user_xyz", entity_code="1877-8", role="bookkeeper"
        )
        self.assertEqual(u["id"], "user_xyz")

    def test_getitem_is_superadmin_returns_false(self):
        u = CurrentUser(
            clerk_user_id="user_xyz", entity_code="1877-8", role="bookkeeper"
        )
        self.assertFalse(u["is_superadmin"])

    def test_get_with_default(self):
        u = CurrentUser(
            clerk_user_id="user_xyz", entity_code="1877-8", role="bookkeeper"
        )
        self.assertEqual(u.get("missing", "fallback"), "fallback")
        self.assertEqual(u.get("email"), None)


if __name__ == "__main__":
    unittest.main()
