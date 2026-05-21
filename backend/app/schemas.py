from datetime import date, datetime
from decimal import Decimal
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
    # Frontend `QuickbooksStatus` interface expects these names.
    # Returning both shapes lets the existing frontend work unchanged
    # while keeping the original payload backwards-compatible.
    is_connected: bool = False
    realm_id: str | None = None
    company_name: str | None = None
    last_synced_at: datetime | None = None


class BankSyncRequest(BaseModel):
    entity_code: str = Field(default="1877-8")
    date_from: date
    date_to: date


class BankSyncResponse(BaseModel):
    entity_code: str
    sync_type: str
    imported_count: int
    updated_count: int
    summary: dict[str, Any]


class BankTransactionListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    review_status: str | None = None
    count: int
    summary: dict[str, Any] = Field(default_factory=dict)
    transactions: list[dict[str, Any]] = Field(default_factory=list)


class BankTransactionDetailResponse(BaseModel):
    entity_code: str
    transaction: dict[str, Any]
    matches: list[dict[str, Any]] = Field(default_factory=list)
    review_history: list[dict[str, Any]] = Field(default_factory=list)


class BankTransactionReviewStatusRequest(BaseModel):
    actor_email: str
    review_status: str
    note: str | None = None


class BankTransactionMatchRequest(BaseModel):
    actor_email: str
    match_type: str
    target_label: str
    target_table: str | None = None
    target_record_id: str | None = None
    amount_matched: Decimal | None = None
    note: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class BankTransactionUnmatchRequest(BaseModel):
    actor_email: str
    note: str | None = None


class BankTransactionActionResponse(BaseModel):
    ok: bool = True
    transaction_id: str
    summary: dict[str, Any]


class HHAPRemittanceBankMatchListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    bank_match_status: str | None = None
    count: int
    summary: dict[str, Any] = Field(default_factory=dict)
    remittances: list[dict[str, Any]] = Field(default_factory=list)


class HHAPRemittanceBankMatchDetailResponse(BaseModel):
    entity_code: str
    remittance: dict[str, Any]
    lines: list[dict[str, Any]] = Field(default_factory=list)
    suggestions: list[dict[str, Any]] = Field(default_factory=list)
    match_history: list[dict[str, Any]] = Field(default_factory=list)


class HHAPRemittanceBankMatchRequest(BaseModel):
    actor_email: str
    bank_transaction_id: str
    amount_matched: Decimal | None = None
    note: str | None = None


class HHAPRemittanceBankUnmatchRequest(BaseModel):
    actor_email: str
    note: str | None = None


class HHAPRemittanceBankActionResponse(BaseModel):
    ok: bool = True
    remittance_id: str
    summary: dict[str, Any]


class HHAPRemittanceBankAutoMatchRequest(BaseModel):
    entity_code: str = Field(default="1877-8")
    date_from: date
    date_to: date
    actor_email: str
    date_window_days: int = Field(default=5, ge=0, le=31)
    amount_tolerance: Decimal = Field(default=Decimal("0.05"))
    max_to_apply: int = Field(default=100, ge=1, le=1000)
    note: str | None = None


class HHAPRemittanceBankSummaryResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    summary: dict[str, Any] = Field(default_factory=dict)


class DirectVendorAPInvoiceUpsertRequest(BaseModel):
    actor_email: str
    vendor_name: str
    invoice_number: str
    invoice_date: date
    total_amount: Decimal
    due_date: date | None = None
    received_date: date | None = None
    vendor_code: str | None = None
    subtotal_amount: Decimal | None = None
    tax_amount: Decimal | None = None
    currency_code: str | None = "CAD"
    priority: str = "normal"
    status: str = "open"
    payment_status: str = "unpaid"
    source_document_name: str | None = None
    note: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class DirectVendorAPInvoiceStatusRequest(BaseModel):
    actor_email: str
    status: str
    note: str | None = None


class DirectVendorAPInvoiceMatchRequest(BaseModel):
    actor_email: str
    bank_transaction_id: str
    amount_matched: Decimal | None = None
    note: str | None = None


class DirectVendorAPInvoiceUnmatchRequest(BaseModel):
    actor_email: str
    note: str | None = None


class DirectVendorAPSummaryResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    summary: dict[str, Any] = Field(default_factory=dict)


class DirectVendorAPListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    status: str | None = None
    payment_status: str | None = None
    due_state: str | None = None
    match_state: str | None = None
    count: int
    summary: dict[str, Any] = Field(default_factory=dict)
    invoices: list[dict[str, Any]] = Field(default_factory=list)


class DirectVendorAPDetailResponse(BaseModel):
    entity_code: str
    invoice: dict[str, Any]
    matches: list[dict[str, Any]] = Field(default_factory=list)
    history: list[dict[str, Any]] = Field(default_factory=list)
    suggestions: list[dict[str, Any]] = Field(default_factory=list)


class DirectVendorAPActionResponse(BaseModel):
    ok: bool = True
    invoice_id: str
    summary: dict[str, Any]


class CardSettlementBatchUpsertRequest(BaseModel):
    actor_email: str
    processor_name: str
    business_date: date
    net_deposit_amount: Decimal
    deposit_date: date | None = None
    merchant_account: str | None = None
    settlement_reference: str | None = None
    currency_code: str | None = "CAD"
    gross_sales_amount: Decimal | None = None
    refunds_amount: Decimal | None = None
    chargebacks_amount: Decimal | None = None
    fees_amount: Decimal | None = None
    tax_on_fees_amount: Decimal | None = None
    reconciliation_status: str = "new"
    source_file_name: str | None = None
    note: str | None = None
    payload_json: dict[str, Any] = Field(default_factory=dict)


class CardSettlementStatusRequest(BaseModel):
    actor_email: str
    reconciliation_status: str
    note: str | None = None


class CardSettlementMatchRequest(BaseModel):
    actor_email: str
    bank_transaction_id: str
    amount_matched: Decimal | None = None
    note: str | None = None


class CardSettlementUnmatchRequest(BaseModel):
    actor_email: str
    note: str | None = None


class CardSettlementSummaryResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    summary: dict[str, Any] = Field(default_factory=dict)


class CardSettlementListResponse(BaseModel):
    entity_code: str
    date_from: str
    date_to: str
    reconciliation_status: str | None = None
    bank_match_state: str | None = None
    count: int
    summary: dict[str, Any] = Field(default_factory=dict)
    batches: list[dict[str, Any]] = Field(default_factory=list)


class CardSettlementDetailResponse(BaseModel):
    entity_code: str
    batch: dict[str, Any]
    matches: list[dict[str, Any]] = Field(default_factory=list)
    history: list[dict[str, Any]] = Field(default_factory=list)
    suggestions: list[dict[str, Any]] = Field(default_factory=list)


class CardSettlementActionResponse(BaseModel):
    ok: bool = True
    batch_id: str
    summary: dict[str, Any]
