from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    ok: bool = True
    environment: str


class ConnectResponse(BaseModel):
    entity_code: str
    authorization_url: str
    state: str


class SyncRequest(BaseModel):
    entity_code: str = Field(default="1877-8")
    date_from: date
    date_to: date


class SyncResponse(BaseModel):
    entity_code: str
    sync_type: str
    imported_count: int
    summary: dict[str, Any]


class DashboardResponse(BaseModel):
    entity_code: str
    has_quickbooks_connection: bool
    company_realm_id: str | None = None
    imported_accounts: int = 0
    imported_transactions: int = 0
    last_sync_at: datetime | None = None
