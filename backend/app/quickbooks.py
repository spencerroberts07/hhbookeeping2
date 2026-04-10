import base64
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx
from sqlalchemy import text

from .config import settings


class QuickBooksClient:
    def __init__(self) -> None:
        self.auth_url = settings.qbo_auth_url
        self.token_url = settings.qbo_token_url
        self.api_base_url = settings.qbo_api_base_url.rstrip("/")
        self.scope = settings.qbo_scope
        self.redirect_uri = settings.qbo_redirect_uri
        self.minor_version = settings.qbo_minor_version

    @staticmethod
    def new_state() -> str:
        return secrets.token_urlsafe(24)

    def build_authorization_url(self, state: str) -> str:
        query = urlencode(
            {
                "client_id": settings.qbo_client_id,
                "response_type": "code",
                "scope": self.scope,
                "redirect_uri": self.redirect_uri,
                "state": state,
            }
        )
        return f"{self.auth_url}?{query}"

    def _basic_auth_header(self) -> str:
        raw = f"{settings.qbo_client_id}:{settings.qbo_client_secret}".encode("utf-8")
        return "Basic " + base64.b64encode(raw).decode("utf-8")

    async def exchange_code(self, code: str) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
        }
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(self.token_url, headers=headers, data=data)
            response.raise_for_status()
            return response.json()

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(self.token_url, headers=headers, data=data)
            response.raise_for_status()
            return response.json()

    async def get_company_info(self, realm_id: str, access_token: str) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/companyinfo/{realm_id}"
        params = {"minorversion": self.minor_version}
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    async def query(self, realm_id: str, access_token: str, query: str) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/query"
        params = {"query": query, "minorversion": self.minor_version}
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

    async def cdc(self, realm_id: str, access_token: str, changed_since_iso: str, entities: list[str]) -> dict[str, Any]:
        url = f"{self.api_base_url}/v3/company/{realm_id}/cdc"
        params = {
            "changedSince": changed_since_iso,
            "entities": ",".join(entities),
            "minorversion": self.minor_version,
        }
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()


def token_expiry_from_seconds(seconds: int | None) -> datetime | None:
    if not seconds:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def upsert_connection(session, entity_id: str, realm_id: str, token_payload: dict[str, Any]) -> None:
    session.execute(
        text(
            """
            INSERT INTO quickbooks_connections (
                entity_id, realm_id, access_token, refresh_token,
                access_token_expires_at, refresh_token_expires_at, is_active
            )
            VALUES (
                :entity_id, :realm_id, :access_token, :refresh_token,
                :access_expiry, :refresh_expiry, TRUE
            )
            ON CONFLICT (entity_id, realm_id)
            DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                access_token_expires_at = EXCLUDED.access_token_expires_at,
                refresh_token_expires_at = EXCLUDED.refresh_token_expires_at,
                disconnected_at = NULL,
                is_active = TRUE
            """
        ),
        {
            "entity_id": entity_id,
            "realm_id": realm_id,
            "access_token": token_payload["access_token"],
            "refresh_token": token_payload["refresh_token"],
            "access_expiry": token_expiry_from_seconds(token_payload.get("expires_in")),
            "refresh_expiry": token_expiry_from_seconds(token_payload.get("x_refresh_token_expires_in")),
        },
    )
