from __future__ import annotations

import os
from typing import Any

import httpx


class QuickBooksClient:
    """
    Lightweight QuickBooks Online client used by the bookkeeping control layer.

    Notes:
    - Uses POST /query with the SQL-like query in the request body.
    - Handles pagination with STARTPOSITION / MAXRESULTS.
    - Keeps methods small and reusable so routes/services stay clean.
    """

    def __init__(self) -> None:
        env_base = os.getenv("QBO_API_BASE_URL", "").strip()
        self.base_url = env_base or "https://sandbox-quickbooks.api.intuit.com"
        self.minor_version = os.getenv("QBO_MINOR_VERSION", "75")
        self.client_id = os.getenv("QBO_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("QBO_CLIENT_SECRET", "").strip()

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        """
        Exchange a refresh token for a new access token.
        """
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing QBO_CLIENT_ID or QBO_CLIENT_SECRET")

        token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        basic_auth = httpx.BasicAuth(self.client_id, self.client_secret)

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                token_url,
                data=data,
                headers=headers,
                auth=basic_auth,
            )
            response.raise_for_status()
            return response.json()

    async def get_company_info(self, realm_id: str, access_token: str) -> dict[str, Any]:
        """
        Fetch basic company info from QBO.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{self.base_url}/v3/company/{realm_id}/companyinfo/{realm_id}",
                params={"minorversion": self.minor_version},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()

    async def query(
        self,
        realm_id: str,
        access_token: str,
        query: str,
    ) -> dict[str, Any]:
        """
        Run a single QBO query using POST /query.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{self.base_url}/v3/company/{realm_id}/query",
                params={"minorversion": self.minor_version},
                headers=headers,
                content=query,
            )
            response.raise_for_status()
            return response.json()

    async def query_all(
        self,
        realm_id: str,
        access_token: str,
        base_query: str,
        object_name: str,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Run a paginated QBO query and return all rows for one object type.

        Example:
            base_query = "SELECT * FROM Check WHERE TxnDate >= '2026-02-01' AND TxnDate <= '2026-02-28'"
            object_name = "Check"
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        }

        all_rows: list[dict[str, Any]] = []
        start_pos = 1

        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                query = f"{base_query} STARTPOSITION {start_pos} MAXRESULTS {page_size}"

                response = await client.post(
                    f"{self.base_url}/v3/company/{realm_id}/query",
                    params={"minorversion": self.minor_version},
                    headers=headers,
                    content=query,
                )
                response.raise_for_status()

                data = response.json()
                query_response = data.get("QueryResponse", {})

                rows = query_response.get(object_name, []) or []

                # QBO sometimes returns a single object instead of a list
                if isinstance(rows, dict):
                    rows = [rows]

                if not rows:
                    break

                all_rows.extend(rows)

                if len(rows) < page_size:
                    break

                start_pos += page_size

        return all_rows

    async def get_account(self, realm_id: str, access_token: str, account_id: str) -> dict[str, Any]:
        """
        Fetch a single account by ID.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{self.base_url}/v3/company/{realm_id}/account/{account_id}",
                params={"minorversion": self.minor_version},
                headers=headers,
            )
            response.raise_for_status()
            return response.json()

    async def get_accounts(self, realm_id: str, access_token: str) -> list[dict[str, Any]]:
        """
        Fetch all accounts through the query endpoint.
        """
        data = await self.query_all(
            realm_id=realm_id,
            access_token=access_token,
            base_query="SELECT * FROM Account",
            object_name="Account",
            page_size=1000,
        )
        return data
