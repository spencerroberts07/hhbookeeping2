# Backend Endpoint Catalog

Reference material for the Next.js frontend team. Built by reading every file under `backend/app/routes/` directly; do not treat this as guesswork.

Notes on role columns:
- `bookkeeper` / `approver` / `admin` / `superadmin` come from `Depends(require_role("..."))` or inline `enforce_role(...)`. `require_role` reads `entity_code` from the **query string** only on the legacy JWT path (under Clerk, it reads it from the caller's active org).
- `enforce_entity_code(user, entity_code)` is a flag-aware secondary check: no-op under JWT, enforces caller's Clerk org under Clerk. Its presence is called out per endpoint.
- `none` = no auth dep on the route. `none — PUBLIC` is reserved for endpoints truly not gated (e.g. health, webhook).
- `user-context` = `Depends(get_current_user)` (any logged-in user, no entity-role check).

All endpoints return JSON unless noted.

---

## Section A — Endpoint catalog

### routes/auth.py — prefix `/api/auth`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/auth/register` | bootstrap path: none; otherwise inline `superadmin` check | none | body `RegisterRequest` {`email: EmailStr`, `password: str (min 8)`, `full_name?: str`, `is_superadmin: bool = False`} | user dict {`id`, `email`, `full_name`, `is_active`, `is_superadmin`, `created_at`, `last_login_at`, `roles: []`}; 201 |
| POST | `/api/auth/login` | none — PUBLIC | none | body `LoginRequest` {`email: EmailStr`, `password: str`} | `LoginResponse` {`access_token`, `token_type="bearer"`, `expires_at: str`, `user: {…, roles[]}`} |
| POST | `/api/auth/logout` | user-context (`get_current_user`) | none | — | `{"ok": True}` |
| GET | `/api/auth/me` | user-context | none | — | user dict + `roles[]` |
| POST | `/api/auth/users/{user_id}/roles` | user-context + inline `enforce_role(..., min_role=admin)` on body.entity_code | body.entity_code | path `user_id`; body `GrantRoleRequest` {`entity_code: str`, `role: str`, `actor_email?: str`} | granted role row |
| DELETE | `/api/auth/users/{user_id}/roles/{entity_id}` | user-context + inline (admin on entity OR superadmin) | path `entity_id` | path `user_id`, `entity_id` | `{"ok": True}` |
| GET | `/api/auth/users` | user-context + inline `is_superadmin` | none | — | `{count, users[]}` (users each have `roles[]`) |

### routes/accruals.py — prefix `/api/accruals`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/accruals/seed-templates` | bookkeeper + `enforce_entity_code` | body | body `SeedTemplatesRequest` {`entity_code`, `actor_email`} | dict (seed result) |
| GET | `/api/accruals/templates` | none | query | query `entity_code` | dict (templates list) |
| POST | `/api/accruals/templates/upsert` | bookkeeper + `enforce_entity_code` | body | body `UpsertTemplateRequest` {`entity_code`, `actor_email`, `accrual_code`, `description?`, `debit_account`, `credit_account`, `default_amount?: float`, `frequency?: str = "monthly"`, `is_active: bool = True`, `notes?`} | upserted template |
| POST | `/api/accruals/build-journal` | bookkeeper + `enforce_entity_code` | body | body `BuildJournalRequest` {`entity_code`, `period_end: YYYY-MM-DD`, `accrual_codes: str[]`, `amounts_override?: {code: float}`, `actor_email`} | journal_batch + lines |
| GET | `/api/accruals/journals` | none | query | query `entity_code`, `period_end` | journals for that period |

### routes/auto_match.py — prefix `/api/auto-match`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/auto-match/run` | bookkeeper + `enforce_entity_code` | body | body `RunRequest` {`entity_code`, `period_start: date`, `period_end: date`, `actor_email`, `triggered_by="manual"`, `trigger_source_id?`, `date_window_days: int = 7 (0..31)`, `amount_tolerance: Decimal = 0.05`, `max_to_apply: int = 100 (1..1000)`} | auto-match run summary |
| GET | `/api/auto-match/runs` | none | query | query `entity_code`, `limit: int = 50` | runs list |
| GET | `/api/auto-match/runs/{run_id}` | none | query | path `run_id`; query `entity_code` | run detail |

### routes/bank_auto_journal.py — prefix `/api/bank-auto-journal`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/bank-auto-journal/seed-rules` | bookkeeper + `enforce_entity_code` | body | `SeedRulesRequest` {`entity_code`, `actor_email`} | seeded rules dict |
| GET | `/api/bank-auto-journal/rules` | none | query | query `entity_code` | rules list |
| POST | `/api/bank-auto-journal/run` | bookkeeper + `enforce_entity_code` | body | `RunRequest` {`entity_code`, `period_start: YYYY-MM-DD`, `period_end: YYYY-MM-DD`, `actor_email`} | run summary |
| GET | `/api/bank-auto-journal/runs` | none | query | query `entity_code`, `limit: int = 50` | runs list |
| GET | `/api/bank-auto-journal/runs/{run_id}` | none | query | path `run_id`; query `entity_code` | run detail |
| GET | `/api/bank-auto-journal/unmatched` | none | query | query `entity_code`, `period_start`, `period_end` | unmatched txns list |

### routes/bank_csv.py — prefix `/api/bank-csv`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/bank-csv/mapping-profiles` | none | none | — | built-in mapping profiles |
| POST | `/api/bank-csv/preview` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `file: UploadFile`, `mapping_profile: str = "generic"`, `source_account_code?`, `source_account_name?`, `column_map_json?`, `sample_limit: int = 20` | preview summary |
| POST | `/api/bank-csv/upload` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file: UploadFile`, `mapping_profile`, `source_account_code?`, `source_account_name?`, `column_map_json?`, `note?` | import run dict; 409 on `PeriodLockedError` |
| GET | `/api/bank-csv/import-runs` | none | query | query `entity_code`, `limit: int = 50`, `source_account_code?` | runs list |
| GET | `/api/bank-csv/import-runs/{run_id}` | none | query | path `run_id`; query `entity_code` | run detail |

### routes/bank_pdf.py — prefix `/api/bank-pdf`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/bank-pdf/preview` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `file: UploadFile`, `source_account_code?`, `source_account_name?`, `sample_limit: int = 25` | preview dict |
| POST | `/api/bank-pdf/upload` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file: UploadFile`, `source_account_code?`, `source_account_name?`, `note?` | run dict; 409 on `PeriodLockedError` |

### routes/bank_review.py — prefix `/api/bank-review`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/bank-review/summary` | none | query | query `entity_code`, `date_from`, `date_to` | `BankReviewSummaryResponse` |
| GET | `/api/bank-review/transactions` | none | query | query `entity_code`, `date_from`, `date_to`, `review_status?`, `match_state?` | `BankReviewTransactionListResponse` |
| GET | `/api/bank-review/transactions/{transaction_id}` | none | none | path `transaction_id` | `BankTransactionDetailResponse` |
| POST | `/api/bank-review/transactions/{transaction_id}/set-review-status` | bookkeeper (no `enforce_entity_code`) | none on dep — entity inferred via transaction_id | path `transaction_id`; body `BankTransactionReviewStatusRequest` {`actor_email`, `review_status`, `note?`} | `BankTransactionDetailResponse` |
| POST | `/api/bank-review/transactions/{transaction_id}/match` | bookkeeper | none on dep | path `transaction_id`; body `BankTransactionMatchRequest` {`actor_email`, `match_type`, `note?`, `matched_amount?: float`, `target_table_name?`, `target_record_id?`, `raw_json: {}`} | `BankTransactionDetailResponse` |
| POST | `/api/bank-review/transactions/{transaction_id}/unmatch` | bookkeeper | none on dep | path `transaction_id`; body `BankTransactionUnmatchRequest` {`actor_email`, `note?`} | `BankTransactionDetailResponse` |

> Note: routes/bank_review.py is **not** wired into `main.py` (not included via `include_router`). The endpoints exist in source but are not currently exposed by the deployed app — see Gaps section.

### routes/bank_review.py wiring check

This router is **not** included in `app/main.py`. The same matching functionality is also exposed under `/api/qbo-bank-sync` (see below) which is wired in. This is in the Gaps section.

### routes/card_settlement.py — prefix `/api/card-settlement`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/card-settlement/summary` | none | query | query `entity_code`, `date_from`, `date_to`, `reconciliation_status?`, `bank_match_state?` | `CardSettlementSummaryResponse` |
| GET | `/api/card-settlement/batches` | none | query | same query params as `/summary` | `CardSettlementListResponse` |
| POST | `/api/card-settlement/batches/upsert` | bookkeeper (no `enforce_entity_code`) | query | query `entity_code`; body `CardSettlementBatchUpsertRequest` (see schemas.py) | `CardSettlementActionResponse` |
| GET | `/api/card-settlement/batches/{batch_id}` | none | query | path `batch_id`; query `entity_code`, `suggestion_date_window_days: int = 7`, `amount_tolerance: Decimal = 0.05` | `CardSettlementDetailResponse` |
| POST | `/api/card-settlement/batches/{batch_id}/set-status` | bookkeeper | query | path `batch_id`; query `entity_code`; body `CardSettlementStatusRequest` | `CardSettlementActionResponse` |
| POST | `/api/card-settlement/batches/{batch_id}/match` | bookkeeper | query | path `batch_id`; query `entity_code`; body `CardSettlementMatchRequest` | `CardSettlementActionResponse` |
| POST | `/api/card-settlement/batches/{batch_id}/unmatch/{match_id}` | bookkeeper | query | path `batch_id`, `match_id`; query `entity_code`; body `CardSettlementUnmatchRequest` | `CardSettlementActionResponse` |

### routes/cash_balancing.py — prefix `/api/cash-balancing`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/cash-balancing/sync` | none — PUBLIC (no auth dep) | body | body `CashBalancingSyncRequest` {`entity_code`, `sheet_tabs: str[] = []`, `lookback_days: int = 56 (1..365)`} | sync summary dict (`raw_inserted_count`, `day_upserted_count`, `summary: {tabs, tabs_source, …}`) |
| GET | `/api/cash-balancing/status` | none — PUBLIC | query | query `entity_code` | status summary {`row_count`, `day_count`, `line_count`, `mapped_line_count`, …, `latest_run`} |

### routes/clerk_webhook.py — prefix `/api/webhooks`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/webhooks/clerk` | none — PUBLIC (svix signature verified inline) | none | Clerk svix-signed webhook event | `{"ok": "processed"|"ignored", "event_type": str}` |

### routes/cogs.py — prefix `/api/cogs`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/cogs/build-journal` | bookkeeper + `enforce_entity_code` | body | `BuildCogsJournalRequest` {`entity_code`, `period_end: YYYY-MM-DD`, `actor_email`, `dating_new_amount: Decimal`, `dating_reversal_amount?: Decimal`, `other_adjustment_amount?: Decimal`, `other_adjustment_memo?`, `shrinkage_included: bool = True`} | journal_batch + lines |
| GET | `/api/cogs/status` | none | query | query `entity_code`, `period_end: YYYY-MM-DD` | status dict |
| GET | `/api/cogs/suggested-dating` | none | query | query `entity_code`, `period_end` | suggested reversal amount + carry-forward |

### routes/dashboard.py — prefix `/api/dashboard`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/dashboard/quickbooks-status` | none — PUBLIC | query (default `"1877-8"`) | query `entity_code` | `DashboardResponse` {`entity_code`, `has_quickbooks_connection: bool`, `company_realm_id?`, `imported_accounts: int`, `imported_transactions: int`, `last_sync_at?: datetime`} |

### routes/depreciation.py — prefix `/api/depreciation`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/depreciation/seed-assets` | bookkeeper + `enforce_entity_code` | body | `SeedAssetsRequest` {`entity_code`, `actor_email`} | seed result |
| GET | `/api/depreciation/assets` | none | query | query `entity_code` | asset list |
| POST | `/api/depreciation/generate-schedule` | bookkeeper + `enforce_entity_code` | body | `GenerateScheduleRequest` {`entity_code`, `fiscal_year: int`, `actor_email`, `half_year_asset_codes?: str[]`} | schedule rows |
| GET | `/api/depreciation/schedule` | none | query | query `entity_code`, `fiscal_year: int` | schedule rows |
| POST | `/api/depreciation/build-journal` | bookkeeper + `enforce_entity_code` | body | `BuildJournalRequest` {`entity_code`, `period_end: YYYY-MM-DD`, `actor_email`} | journal_batch + lines |
| GET | `/api/depreciation/summary` | none | query | query `entity_code`, `period_end` | summary dict |

### routes/direct_vendor_ap.py — prefix `/api/direct-vendor-ap`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/direct-vendor-ap/summary` | none | query | query `entity_code`, `date_from`, `date_to`, `status?`, `payment_status?`, `due_state?`, `match_state?` | `DirectVendorAPSummaryResponse` |
| GET | `/api/direct-vendor-ap/invoices` | none | query | same as `/summary` | `DirectVendorAPListResponse` |
| POST | `/api/direct-vendor-ap/invoices/upsert` | bookkeeper (no `enforce_entity_code`) | query | query `entity_code`; body `DirectVendorAPInvoiceUpsertRequest` (see schemas.py) | `DirectVendorAPActionResponse` |
| GET | `/api/direct-vendor-ap/invoices/{invoice_id}` | none | query | path `invoice_id`; query `entity_code`, `suggestion_date_window_days: int = 14`, `amount_tolerance: Decimal = 0.05` | `DirectVendorAPDetailResponse` |
| POST | `/api/direct-vendor-ap/invoices/{invoice_id}/set-status` | bookkeeper | query | path `invoice_id`; query `entity_code`; body `DirectVendorAPInvoiceStatusRequest` | `DirectVendorAPActionResponse` |
| POST | `/api/direct-vendor-ap/invoices/{invoice_id}/match` | bookkeeper | query | path `invoice_id`; query `entity_code`; body `DirectVendorAPInvoiceMatchRequest` | `DirectVendorAPActionResponse` |
| POST | `/api/direct-vendor-ap/invoices/{invoice_id}/unmatch/{match_id}` | bookkeeper | query | path `invoice_id`, `match_id`; query `entity_code`; body `DirectVendorAPInvoiceUnmatchRequest` | `DirectVendorAPActionResponse` |

### routes/gl_import.py — prefix `/api/gl-import`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/gl-import/upload` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file: UploadFile`, `period_start?: YYYY-MM-DD`, `period_end?` | gl_import_run dict |
| POST | `/api/gl-import/runs/{run_id}/build-comparison` | bookkeeper + `enforce_entity_code` | body | path `run_id`; body `BuildComparisonRequest` {`entity_code`, `actor_email`} | trial-balance comparison |
| GET | `/api/gl-import/runs` | none | query | query `entity_code`, `limit: int = 50` | runs list |
| GET | `/api/gl-import/runs/{run_id}` | none | query | path `run_id`; query `entity_code` | run detail |
| GET | `/api/gl-import/runs/{run_id}/trial-balance` | none | query | path `run_id`; query `entity_code`, `only_variance: bool = False` | comparison rows |
| GET | `/api/gl-import/runs/{run_id}/transactions` | none | query | path `run_id`; query `entity_code`, `account_code?`, `limit: int = 1000` | transactions list |

### routes/hh_ap.py — prefix `/api/hh-ap`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/hh-ap/upload-documents` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `document_type`, `document_date?`, `files: UploadFile[]` | `{inserted_count, updated_count, duplicate_count, inserted_documents[], updated_documents[], duplicate_documents[]}` |
| POST | `/api/hh-ap/invoices/upload-and-parse-batch` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `document_date?`, `files: UploadFile[]` | `{parsed_count, failed_count, parsed_files[], failed_files[]}` |
| POST | `/api/hh-ap/invoices/upsert` | bookkeeper (no `enforce_entity_code`; entity_code in body but no Clerk guard) | body | body `HHAPInvoiceUpsertRequest` {`entity_code`, `document_id?`, `invoices: HHAPInvoiceInput[]`}; each invoice has {`invoice_number`, `invoice_type`, `vendor_name?`, `vendor_invoice_number?`, `po_number?`, `invoice_date?`, `due_date?`, `remittance_due_date?`, `currency_code="CAD"`, `subtotal?`, `hst_amount?`, `surcharge_amount?`, `advertising_amount?`, `subscribed_shares_amount?`, `five_year_note_amount?`, `total_amount?`, `is_statement_only=false`, `notes?`, `raw_json={}`} | `{upserted_count, upserted_invoices[]}`; 409 with `period_label` on locked period |
| POST | `/api/hh-ap/remittances/upsert` | bookkeeper | body | `HHAPRemittanceUpsertRequest` {`entity_code`, `document_id?`, `remittance_reference?`, `remittance_date?`, `withdrawal_date?`, `total_amount?: Decimal`, `raw_json={}`, `lines: HHAPRemittanceLineInput[]`}; each line has {`invoice_number?`, `line_description?`, `due_date?`, `line_amount: Decimal`, `raw_json={}`} | `{remittance_id, remittance_line_count}` |
| POST | `/api/hh-ap/statements/upsert` | bookkeeper | body | `HHAPStatementUpsertRequest` {`entity_code`, `document_id?`, `statement_date?`, `statement_month_end: date`, `total_open_balance?`, `raw_json={}`, `lines: HHAPStatementLineInput[]`} | `{statement_id, statement_line_count, statement_month_end}` |
| POST | `/api/hh-ap/statements/parse-document` | bookkeeper | body | `HHAPParseStatementDocumentRequest` {`entity_code`, `document_id?`} | parsed statement detail + `summary_balances`, `summary_components` |
| POST | `/api/hh-ap/invoices/parse-document` | bookkeeper | body | `HHAPParseInvoiceDocumentRequest` {`entity_code`, `document_id?`} | parsed invoice fields |
| POST | `/api/hh-ap/remittances/parse-document` | bookkeeper | body | `HHAPParseRemittanceDocumentRequest` {`entity_code`, `document_id?`} | parsed remittance + lines |
| POST | `/api/hh-ap/match/run` | bookkeeper | body | `HHAPMatchRunRequest` {`entity_code`, `statement_month_end?: date`} | `{matched_statement_line_count, missing_download_count, matched_remittance_line_count, unmatched_remittance_line_count, matched_invoice_count}` |
| GET | `/api/hh-ap/status` | none | query | query `entity_code` | dashboard {`document_counts_by_type`, `invoice_summary`, `remittance_summary`, `statement_summary`, `latest_statement`, `latest_documents[]`} |
| GET | `/api/hh-ap/exceptions` | none | query | query `entity_code`, `statement_month_end?`, `loaded_period_start?`, `loaded_period_end?`, `limit: int = 500` | `{missing_download_summary, missing_download_statement_lines[], unmatched_remittance_lines[], unmatched_invoices[]}` |
| GET | `/api/hh-ap/reconciliation` | none | query | query `entity_code`, `statement_month_end?` | statement reconciliation summary with component totals |

### routes/hh_ap_overrides.py — prefix `/api/hh-ap` (shared with hh_ap.py)

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/hh-ap/invoice-overrides/upsert` | none (NO `require_role`) | body | `HHAPInvoiceOverrideUpsertRequest` {`entity_code`, `invoice_number`, `invoice_type`, `override_invoice_date?`, `override_due_date?`, `override_subtotal?`, `override_hst_amount?`, `override_total_amount?`, `override_special_shares_amount?`, `override_five_year_note_amount?`, `override_advertising_amount?`, `reason`, `review_status="approved"`, `reviewed_by?`, `is_active=true`, `raw_json={}`} | override + `effective_values` |
| GET | `/api/hh-ap/invoice-overrides` | none | query | query `entity_code`, `invoice_date_from?`, `invoice_date_to?`, `invoice_type?` | `{override_count, overrides[]}` |
| GET | `/api/hh-ap/review-queue` | none | query | query `entity_code`, `invoice_date_from?`, `invoice_date_to?`, `invoice_type?`, `only_warning_rows: bool = True`, `only_without_override: bool = False` | `{review_row_count, rows[]}` with parsed_values + effective_values + parser_warnings |

### routes/hh_ap_remittance_bank_match.py

This file declares **two routers**.

**Router A — prefix `/api/hh-ap/remittance-bank-match`**

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/hh-ap/remittance-bank-match/summary` | none | query | query `entity_code`, `date_from`, `date_to`, `bank_match_status?`, `suggestion_date_window_days: int = 7`, `amount_tolerance: Decimal = 0.05` | `HHAPRemittanceBankSummaryResponse` |
| GET | `/api/hh-ap/remittance-bank-match/remittances` | none | query | same as `/summary` | `HHAPRemittanceBankMatchListResponse` |
| GET | `/api/hh-ap/remittance-bank-match/remittances/{remittance_id}` | none | query | path `remittance_id`; query `entity_code`, `suggestion_date_window_days`, `amount_tolerance` | `HHAPRemittanceBankMatchDetailResponse` |
| POST | `/api/hh-ap/remittance-bank-match/remittances/{remittance_id}/match` | bookkeeper (no `enforce_entity_code`) | query | path `remittance_id`; query `entity_code`; body `HHAPRemittanceBankMatchRequest` | `HHAPRemittanceBankActionResponse` |
| POST | `/api/hh-ap/remittance-bank-match/remittances/{remittance_id}/unmatch/{match_id}` | bookkeeper | query | path `remittance_id`, `match_id`; query `entity_code`; body `HHAPRemittanceBankUnmatchRequest` | `HHAPRemittanceBankActionResponse` |
| POST | `/api/hh-ap/remittance-bank-match/auto-match` | bookkeeper (no `enforce_entity_code`) | body | body `HHAPRemittanceBankAutoMatchRequest` (entity_code in body; see schemas.py) | `HHAPRemittanceBankActionResponse` |

**Router B — `clearing_router`, prefix `/api/hh-ap-remittance`**

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/hh-ap-remittance/build-clearing-journal` | bookkeeper (no `enforce_entity_code`) | body | body `BuildClearingJournalRequest` {`entity_code`, `period_start: YYYY-MM-DD`, `period_end: YYYY-MM-DD`, `actor_email`} | clearing journal_batch |

### routes/month_end.py — prefix `/api/month-end`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/month-end/cash-balancing/build` | none — PUBLIC | body | `BuildCashBalancingJournalRequest` {`entity_code`, `period_end: YYYY-MM-DD`} | cash-balancing journal_batch + lines |
| POST | `/api/month-end/manual/build` | none — PUBLIC | body | `BuildManualMonthEndJournalRequest` {`entity_code`, `period_end`, `lines: ManualMonthEndLineInput[]`, `batch_label="manual_month_end"`, `batch_memo?`} | manual journal_batch + lines |
| GET | `/api/month-end/cash-balancing/review` | none — PUBLIC | query | query `entity_code`, `period_end` | journal_batch + workflow + lines |
| GET | `/api/month-end/manual/review` | none — PUBLIC | query | query `entity_code`, `period_end`, `batch_label="manual_month_end"` | journal_batch + workflow + lines |
| GET | `/api/month-end/combined/review` | none — PUBLIC | query | query `entity_code`, `period_end`, `manual_batch_label="manual_month_end"` | combined cash+manual review |

### routes/month_end_close.py — prefix `/api/month-end-close`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/month-end-close/status` | none | query | query `entity_code`, `period_end: YYYY-MM-DD` | full close-center status (per-module summaries + `blocking_items` + `warning_items` + `overall_close_readiness`) |

### routes/month_end_hh_ap.py — prefix `/api/month-end/hh-ap`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/month-end/hh-ap/build` | none — PUBLIC | body | `BuildHHAPMonthEndJournalRequest` {`entity_code`, `period_end: YYYY-MM-DD`, `statement_month_end?`, `batch_label="hh_ap_month_end"`, `batch_memo="HHSL Statement"`, `control_tolerance: Decimal = 0.05`} | journal_batch + `summary_json` with controls, payable_tie_out, variance_support |
| GET | `/api/month-end/hh-ap/review` | none — PUBLIC | query | query `entity_code`, `period_end`, `batch_label="hh_ap_month_end"` | journal_batch + lines (with GL-export signed amounts) |

### routes/month_end_workflow.py — prefix `/api/month-end/workflow`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/month-end/workflow/batch` | none — PUBLIC | query | query `entity_code`, `period_end`, `source_module`, `batch_label` | journal_batch + workflow + summary_json |
| POST | `/api/month-end/workflow/submit` | approver + `enforce_entity_code` | body | `JournalBatchWorkflowActionRequest` {`entity_code`, `period_end`, `source_module`, `batch_label`, `actor_email`, `note?`} | post-transition workflow response |
| POST | `/api/month-end/workflow/approve` | approver + `enforce_entity_code` | body | same as `/submit` | post-transition workflow response |
| POST | `/api/month-end/workflow/reject` | approver + `enforce_entity_code` | body | same as `/submit` | post-transition workflow response |
| POST | `/api/month-end/workflow/reopen` | approver + `enforce_entity_code` | body | same as `/submit` | post-transition workflow response |

### routes/payroll.py — prefix `/api/payroll`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/payroll/employees/seed` | bookkeeper + `enforce_entity_code` | body | `SeedEmployeesRequest` {`entity_code`, `actor_email`} | seeded employees |
| GET | `/api/payroll/employees` | none | query | query `entity_code` | employee list |
| POST | `/api/payroll/employees/upsert` | bookkeeper + `enforce_entity_code` | body | `UpsertEmployeeRequest` {`entity_code`, `employee_number`, `actor_email`, `first_name?`, `last_name?`, `employment_type?`, `hourly_rate?: Decimal`, `biweekly_salary?: Decimal`, `vacation_rate?: Decimal`, `has_life_insurance?: bool`, `life_insurance_biweekly?: Decimal`, `is_active?: bool`, `ods_name_key?`, `notes?`, `bank_transit?`, `bank_institution?`, `bank_account?`} | upserted employee |
| POST | `/api/payroll/runs/upload-hours` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `pay_run_number`, `period_number: int`, `period_start`, `period_end`, `pay_date`, `actor_email`, `file: UploadFile`, `stat_pay_overrides?` (JSON string), `vacation_paid_overrides?` (JSON string) | payroll_run + lines |
| POST | `/api/payroll/runs/manual-hours` | bookkeeper + `enforce_entity_code` | body | `ManualHoursRequest` {`entity_code`, `pay_run_number`, `period_number: int`, `period_start: str`, `period_end`, `pay_date`, `actor_email`, `hours: ManualHoursRow[]`, `stat_pay_overrides?: {str: Decimal}`, `vacation_paid_overrides?`} | payroll_run + lines |
| POST | `/api/payroll/runs/upload-register` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file: UploadFile` (PDF), `pay_run_number?`, `period_number?: int`, `pay_date?` | payroll_run + per-line ENetEmployer-exact deductions |
| POST | `/api/payroll/runs/{payroll_run_id}/build-journal` | bookkeeper + `enforce_entity_code` | body | path `payroll_run_id`; body `BuildJournalRequest` {`entity_code`, `actor_email`} | journal_batch + lines |
| GET | `/api/payroll/runs` | none | query | query `entity_code`, `period_end?`, `limit: int = 50` | runs list |
| GET | `/api/payroll/runs/{payroll_run_id}` | none | query | path `payroll_run_id`; query `entity_code` | run detail with lines |
| GET | `/api/payroll/runs/{payroll_run_id}/summary` | none | query | path `payroll_run_id`; query `entity_code` | summary totals |
| POST | `/api/payroll/runs/{payroll_run_id}/submit` | bookkeeper + `enforce_entity_code` | body | path; body `WorkflowRequest` {`entity_code`, `actor_email`} | run after state transition |
| POST | `/api/payroll/runs/{payroll_run_id}/approve` | bookkeeper + `enforce_entity_code` | body | same | run after state transition |
| POST | `/api/payroll/runs/{payroll_run_id}/schedule-withdrawals` | bookkeeper + `enforce_entity_code` | body | same | scheduled withdrawal rows |
| GET | `/api/payroll/validate-feb-2026` | none — PUBLIC | query (default `"1877-8"`) | query `entity_code` | per-run variance against hard-coded Feb 2026 targets |

### routes/period_close.py — prefix `/api/period-close`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/period-close/submit` | approver + `enforce_entity_code` | body | `SubmitRequest` {`entity_code`, `period_end: YYYY-MM-DD`, `actor_email`, `notes?`} | submit result (or 400 with `blocking_items`/`warning_items`; 409 on `PeriodLockedError`) |
| POST | `/api/period-close/approve` | approver + `enforce_entity_code` | body | `ApproveRequest` (same shape as Submit) | approval result |
| POST | `/api/period-close/reopen` | approver + `enforce_entity_code` | body | `ReopenRequest` {`entity_code`, `period_end`, `actor_email`, `notes: str (required, min_length=1)`} | reopen result |
| GET | `/api/period-close/status` | none | query | query `entity_code`, `period_end` | full status payload |
| GET | `/api/period-close/history` | none | query | query `entity_code`, `period_end` | history events |

### routes/pos_import.py — prefix `/api/pos-import`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/pos-import/inventory-adjustment` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file: UploadFile` (txt/PDF; PDF auto-OCRs) | import run + parsed lines |
| POST | `/api/pos-import/pos-financial` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file` | import run + parsed totals |
| POST | `/api/pos-import/inventory-value` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file`, `snapshot_date?: YYYY-MM-DD` | import run + snapshot |
| POST | `/api/pos-import/aged-ar` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file`, `snapshot_date?` | import run + buckets |
| POST | `/api/pos-import/ar-adjustment` | bookkeeper + `enforce_entity_code` | **form** | multipart: `entity_code`, `actor_email`, `file` | import run + lines |
| POST | `/api/pos-import/build-store-use-journal` | bookkeeper + `enforce_entity_code` | body | `BuildJournalRequest` {`entity_code`, `import_run_id`, `actor_email`, `expense_account_code?`, `inventory_account_code?`, `override_total?: float`} | journal_batch + lines |
| POST | `/api/pos-import/build-donation-journal` | bookkeeper + `enforce_entity_code` | body | same `BuildJournalRequest` | journal_batch + lines |
| POST | `/api/pos-import/build-ar-adjustment-journal` | bookkeeper + `enforce_entity_code` | body | `BuildArAdjustmentJournalRequest` {`entity_code`, `import_run_id`, `actor_email`, `bad_debt_account_code?`, `ar_account_code?`} | journal_batch + lines |
| POST | `/api/pos-import/validate-pos-financial` | bookkeeper + `enforce_entity_code` | body | `ValidatePosFinancialRequest` {`entity_code`, `import_run_id`} | per-GL-account variance (read-only) |
| GET | `/api/pos-import/runs` | none | query | query `entity_code`, `period_start?`, `period_end?`, `report_type?`, `limit: int = 100` | runs list |
| GET | `/api/pos-import/runs/{run_id}` | none | query | path `run_id`; query `entity_code` | run detail |
| GET | `/api/pos-import/inventory-value/latest` | none | query | query `entity_code` | `{entity_code, snapshot}` |
| GET | `/api/pos-import/aged-ar/latest` | none | query | query `entity_code` | `{entity_code, snapshot}` |
| GET | `/api/pos-import/pos-financial/latest` | none | query | query `entity_code` | `{entity_code, snapshot}` |

### routes/qbo_auth.py — prefix `/api/auth/quickbooks`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/auth/quickbooks/connect` | none — PUBLIC | query (default `"1877-8"`) | query `entity_code` | `ConnectResponse` {`entity_code`, `authorization_url`, `state`} |
| GET | `/api/auth/quickbooks/callback` | none — PUBLIC (OAuth landing) | query (default `"1877-8"`) | query `code`, `realmId`, `state`, `entity_code` | `{ok, entity_code, state_echo, realm_id, company_name, legal_name}` |

### routes/qbo_bank_sync.py — prefix `/api/qbo-bank-sync`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/qbo-bank-sync/sync` | none — PUBLIC | body | `BankSyncRequest` {`entity_code`, `date_from`, `date_to`} | `BankSyncResponse` |
| GET | `/api/qbo-bank-sync/transactions` | none — PUBLIC | query | query `entity_code`, `date_from`, `date_to`, `review_status?` | `BankTransactionListResponse` |
| GET | `/api/qbo-bank-sync/transactions/{transaction_id}` | none — PUBLIC | query | path `transaction_id`; query `entity_code` | `BankTransactionDetailResponse` |
| POST | `/api/qbo-bank-sync/transactions/{transaction_id}/review-status` | none — PUBLIC (NO `require_role`) | query | path `transaction_id`; query `entity_code`; body `BankTransactionReviewStatusRequest` | `BankTransactionActionResponse` |
| POST | `/api/qbo-bank-sync/transactions/{transaction_id}/match` | none — PUBLIC | query | path `transaction_id`; query `entity_code`; body `BankTransactionMatchRequest` | `BankTransactionActionResponse` |
| POST | `/api/qbo-bank-sync/transactions/{transaction_id}/unmatch/{match_id}` | none — PUBLIC | query | path `transaction_id`, `match_id`; query `entity_code`; body `BankTransactionUnmatchRequest` | `BankTransactionActionResponse` |

### routes/sync.py — prefix `/api/sync`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/sync/chart-of-accounts` | none — PUBLIC | body | `SyncRequest` {`entity_code`, `date_from?`, `date_to?`} | `SyncResponse` |
| POST | `/api/sync/transactions` | none — PUBLIC | body | `SyncRequest` | `SyncResponse` |

### routes/vendor_classification.py — prefix `/api/vendor-classification`

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| POST | `/api/vendor-classification/learn-from-gl` | bookkeeper + `enforce_entity_code` | body | `LearnFromGLRequest` {`entity_code`, `gl_import_run_id`, `actor_email`} | learning result |
| GET | `/api/vendor-classification/memory` | none | query | query `entity_code`, `source?` (`gl_history|user_confirmed|ai_seeded`), `limit: int = 500` | memory rows |
| POST | `/api/vendor-classification/memory/upsert` | bookkeeper + `enforce_entity_code` | body | `UpsertMemoryRequest` {`entity_code`, `normalized_vendor_key`, `account_code`, `debit_or_credit`, `actor_email`, `notes?`} | upserted memory row |
| GET | `/api/vendor-classification/suggestions` | none | query | query `entity_code`, `status: str = "pending"`, `limit: int = 200` | suggestions list |
| POST | `/api/vendor-classification/suggestions/{suggestion_id}/accept` | bookkeeper + `enforce_entity_code` | body | path `suggestion_id`; body `FeedbackRequest` {`entity_code`, `actor_email`, `final_account_code?`, `final_debit_or_credit?`} | acceptance result |
| POST | `/api/vendor-classification/suggestions/{suggestion_id}/override` | bookkeeper + `enforce_entity_code` | body | path; body `FeedbackRequest` (`final_account_code` required) | override result |

### Top-level routes (defined in main.py)

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/health` | none — PUBLIC | none | — | `HealthResponse` {`environment`} |

---

## Section B — Services + SQL inventory

### Services (`backend/app/services_*.py`)

- **services_accruals.py** — Monthly accruals: seed Bridlewood templates (rent / accounting / interest), upsert templates, build per-period accrual `journal_batch`, list past journals.
- **services_auth.py** — Legacy JWT auth + per-entity RBAC. Owns user creation, password hashing/verify, session register/revoke, `require_role` FastAPI dependency (reads `entity_code` from **query string** only, plus `request.state.entity_code` fallback), `enforce_role` (body-aware), `enforce_entity_code` (flag-aware Clerk guard).
- **services_auth_clerk.py** — Clerk session-token verification (JWKS), Clerk org -> entity mapping, webhook event handlers (`sync_clerk_user_from_webhook`, `sync_clerk_org_from_webhook`, `sync_clerk_membership_from_webhook`).
- **services_auto_match.py** — Post-import auto-match runner across HH remittance + card settlement + direct vendor AP modules; records one `auto_match_runs` row per call.
- **services_bank_auto_journal.py** — Walks `bank_transactions` over a period, runs each row against `bank_transaction_rules`, writes one journal_batch (Dr expense / Cr 1020 etc.). Skips HH AP / card-settlement categories that other modules own.
- **services_bank_csv.py** — Bank CSV upload fallback: built-in mapping profiles, preview + idempotent import into `bank_transactions` (source_system='statement_csv').
- **services_bank_pdf.py** — TD Canada Trust PDF statement parser; same idempotent SHA-256-keyed pattern as the CSV importer.
- **services_claude_classifier.py** — Layer 3 of the bank-auto-journal classifier — Claude API (`claude-haiku-4-5`) called only when hard-coded rules and vendor memory both miss.
- **services_cogs.py** — Monthly COGS journal builder: POS COGS + dating reversal + new month-end dating, all posted as a single balanced journal_batch.
- **services_depreciation.py** — Canadian CCA declining-balance with half-year rule; seeds Bridlewood asset classes (Class 8 equipment/computers, Class 10 vehicles); generates fiscal-year schedule + month-end journal.
- **services_gl_import.py** — Parses QBO General Ledger xlsx, persists `gl_import_runs` / `gl_account_balances` / `gl_transactions`, builds trial-balance comparison vs the app's `journal_lines`.
- **services_month_end_close.py** — Read-only aggregator: pulls a status snapshot from every module that matters for close and computes `overall_close_readiness` with `blocking_items[]` and `warning_items[]`.
- **services_payroll.py** — Payroll-control layer: employee seed/upsert, payroll run construction from manual hours / ODS hours upload / ENetEmployer register PDF, journal builder, workflow transitions, scheduled withdrawals.
- **services_payroll_calc.py** — Canadian 2026 tax calculation engine (ON province; CPP/EI/Fed/Prov approximation via annualize-and-bracket; NOT a CRA PDOC clone).
- **services_period_close.py** — Period close lifecycle (`open -> submitted_for_close -> approved_to_close -> closed_locked -> reopened`); other modules call `is_period_locked()` before writes and raise `PeriodLockedError` (→ HTTP 409).
- **services_pos_import.py** — Parses five POS month-end reports (inventory_adjustment, pos_financial, inventory_value, aged_ar, ar_adjustment), persists to `pos_import_runs` + line tables; builds store-use / donation / AR-adjustment journals.
- **services_vendor_classification.py** — Layer 2 of the classifier: vendor memory lookup, `learn_from_gl_history`, and `record_user_feedback` (upgrades a suggestion to a confirmed memory entry).

> Note: `services.py` (un-suffixed; ~3.8k lines) holds the older surface (QBO sync, HH remittance bank match, card settlement, direct vendor AP, bank-transaction matching). New modules go in dedicated `services_*.py` files.

### SQL migrations (`backend/sql/*.sql`)

- **000_baseline_schema.sql** — Baseline schema snapshot (do not run on existing DB; reference only).
- **001_quickbooks_connection.sql** — `quickbooks_connections` table (OAuth tokens, realm_id).
- **002_google_cash_balancing.sql** — Cash balancing source tables (Google Sheets ingest): `cash_balancing_sources`, `_rows`, `_days`, `_lines`, `_import_runs`.
- **003_month_end_workflow.sql** — Adds `workflow_status` columns + supporting columns to `journal_batches` (submitted_by/at, reviewed_by/at, approved_by/at, locked_by/at, notes).
- **004_qbo_bank_sync.sql** — Initial `bank_transactions` table.
- **005_bank_review_and_matching.sql** — Adds review status / matching to bank_transactions (`bank_transaction_matches` + match-event audit).
- **005_bank_transaction_review_layer.sql** — DEPRECATED (duplicate; do not run).
- **006_hh_ap_remittance_bank_matching.sql** — `hh_ap_remittance_bank_match_events` audit table + indexes.
- **007_direct_vendor_ap_tracker.sql** — `direct_vendor_ap_invoices` table.
- **008_card_settlement_reconciliation.sql** — `card_settlement_batches` table.
- **009_bank_csv_upload.sql** — Bank CSV upload fallback module: `bank_csv_import_runs`, `bank_transactions.source_import_run_id`.
- **010_auth.sql** — Authentication + multi-tenant user management: `users`, `user_entity_roles`, `auth_sessions`, `auth_events`.
- **011_period_close.sql** — Period close lock workflow: `open -> submitted_for_close -> approved_to_close -> closed_locked` lifecycle on `accounting_periods` plus `is_period_locked()` helper.
- **012_auto_match_runner.sql** — `auto_match_runs` + per-run line table; one row per `run_auto_match()` call.
- **013_payroll_control.sql** — v0.7 stub payroll control tables (replaced by 024).
- **014_inventory_month_end.sql** — Month-end POS imports: `pos_import_runs` + line tables for inventory_adjustment / pos_financial / inventory_value / aged_ar.
- **015_ar_adjustment.sql** — Adds `ar_adjustment` to `pos_import_runs.report_type` CHECK + AR transaction-list line table.
- **016_gl_import.sql** — GL Import + Trial Balance Comparison schema (`gl_import_runs`, `gl_account_balances`, `gl_transactions`, `gl_trial_balance_comparisons`).
- **017_depreciation.sql** — Fixed asset / depreciation tables; three Bridlewood asset classes seeded.
- **018_accruals.sql** — Monthly accrual templates + posted journals.
- **019_bank_auto_journal.sql** — Bank auto-journal builder schema (`bank_auto_journal_runs`, `bank_transaction_rules`).
- **020_vendor_classification.sql** — `vendor_classification_memory` + `bank_classification_suggestions` (Layers 2 + 3 of the classifier).
- **021_bank_rules_hard_skip.sql** — `hard_skip` flag on `bank_transaction_rules` so a DB rule can declare "another module owns this row".
- **022_cogs_journal.sql** — Monthly COGS journal builder schema (`cogs_journal_runs`, support for carry-forward).
- **023_cogs_shrinkage.sql** — Adds inventory shrinkage rollup from `inventory_adjustment_lines` into the COGS journal.
- **024_payroll_module.sql** — Full payroll calculation module (replaces 013 stub): `payroll_employees`, `payroll_runs`, `payroll_run_lines`, scheduled withdrawals.
- **025_clerk_auth.sql** — Clerk integration: `entities.clerk_org_id`, `clerk_users` mapping, webhook tracking.

---

## Section C — Gaps and assumptions

### 1. Auth wiring gaps — entity-scoped routes with no `require_role`

These endpoints accept `entity_code` (query or body) but have NO `require_role` dependency on the route signature. Anyone with network access can hit them.

| Module | Endpoint | Severity |
|---|---|---|
| routes/sync.py | `POST /api/sync/chart-of-accounts` | **HIGH** — writes accounts to a tenant DB |
| routes/sync.py | `POST /api/sync/transactions` | **HIGH** — writes QBO transactions |
| routes/qbo_bank_sync.py | `POST /api/qbo-bank-sync/sync` | **HIGH** — writes bank_transactions |
| routes/qbo_bank_sync.py | `POST /api/qbo-bank-sync/transactions/{id}/review-status` | **HIGH** — mutates review state |
| routes/qbo_bank_sync.py | `POST /api/qbo-bank-sync/transactions/{id}/match` | **HIGH** — creates a match |
| routes/qbo_bank_sync.py | `POST /api/qbo-bank-sync/transactions/{id}/unmatch/{match_id}` | **HIGH** |
| routes/cash_balancing.py | `POST /api/cash-balancing/sync` | **HIGH** — writes cash_balancing_days/lines |
| routes/cash_balancing.py | `GET /api/cash-balancing/status` | medium (read) |
| routes/month_end.py | `POST /api/month-end/cash-balancing/build` | **HIGH** — writes journal_batches |
| routes/month_end.py | `POST /api/month-end/manual/build` | **HIGH** — writes journal_batches |
| routes/month_end.py | `GET /api/month-end/cash-balancing/review`, `/manual/review`, `/combined/review` | medium (reads) |
| routes/month_end_hh_ap.py | `POST /api/month-end/hh-ap/build` | **HIGH** — writes HH AP month-end journal |
| routes/month_end_hh_ap.py | `GET /api/month-end/hh-ap/review` | medium (read) |
| routes/month_end_workflow.py | `GET /api/month-end/workflow/batch` | medium (read) |
| routes/hh_ap_overrides.py | `POST /api/hh-ap/invoice-overrides/upsert` | **HIGH** — overrides parsed invoice values |
| routes/hh_ap_overrides.py | `GET /api/hh-ap/invoice-overrides`, `GET /api/hh-ap/review-queue` | medium |
| routes/dashboard.py | `GET /api/dashboard/quickbooks-status` | low (read) |

Many `GET` endpoints across the catalog (e.g. all `.../runs`, `.../status`, `.../suggestions`) also lack `require_role`. The codebase appears to intentionally guard only writes; that's a design choice the frontend team should confirm before relying on it.

### 2. `require_role` cannot read `entity_code` from `Form` or body

`services_auth._extract_entity_code_from_request` reads `entity_code` from `request.query_params` only. On the legacy JWT path, **any route that takes `entity_code` only in a Form or JSON body and uses `Depends(require_role(...))` will reject the request with 400 "entity_code is required to authorize this request" UNLESS the user is `is_superadmin` (who bypasses entity-role checks)**.

Affected endpoints (form/multipart upload + `Depends(require_role)`):

- `POST /api/bank-csv/preview`, `/api/bank-csv/upload`
- `POST /api/bank-pdf/preview`, `/api/bank-pdf/upload`
- `POST /api/gl-import/upload`
- `POST /api/hh-ap/upload-documents`
- `POST /api/hh-ap/invoices/upload-and-parse-batch`
- `POST /api/payroll/runs/upload-hours`
- `POST /api/payroll/runs/upload-register`
- All `POST /api/pos-import/*` upload routes

Body-only entity_code endpoints behave the same way unless a query param is also supplied. Affected:

- `POST /api/accruals/*` (all bookkeeper-gated)
- `POST /api/auto-match/run`
- `POST /api/bank-auto-journal/seed-rules`, `/run`
- `POST /api/cogs/build-journal`
- `POST /api/depreciation/seed-assets`, `/generate-schedule`, `/build-journal`
- `POST /api/gl-import/runs/{run_id}/build-comparison`
- `POST /api/payroll/employees/seed`, `/employees/upsert`, `/runs/manual-hours`, `/runs/{id}/build-journal`, `/runs/{id}/submit|approve|schedule-withdrawals`
- `POST /api/period-close/submit`, `/approve`, `/reopen`
- `POST /api/pos-import/build-store-use-journal`, `/build-donation-journal`, `/build-ar-adjustment-journal`, `/validate-pos-financial`
- `POST /api/vendor-classification/learn-from-gl`, `/memory/upsert`, `/suggestions/{id}/accept|override`
- `POST /api/hh-ap-remittance/build-clearing-journal`
- `POST /api/hh-ap/remittance-bank-match/auto-match`
- `POST /api/month-end/workflow/submit`, `/approve`, `/reject`, `/reopen`

Under **Clerk** auth (`settings.use_clerk_auth=true`), `require_role` reads the user's active org-mapped entity_code from the JWT itself, so it does not need to extract from the request — the form/body issue only affects the legacy JWT path. The defensive `enforce_entity_code(_user, body.entity_code)` calls do still cross-check the body value against the Clerk org.

**Practical impact for the frontend**: until JWT auth is retired, clients hitting these endpoints either need to (a) also append `?entity_code=...` as a query string in addition to the body, OR (b) authenticate as a superadmin user. The latter is what dev/test currently relies on.

### 3. `routes/bank_review.py` is not wired

The `router` defined in `routes/bank_review.py` (prefix `/api/bank-review`) is NOT included in `app/main.py`. Its endpoints (`/summary`, `/transactions`, `/transactions/{id}`, `/transactions/{id}/set-review-status`, `/transactions/{id}/match`, `/transactions/{id}/unmatch`) are dead code in the deployed app. Frontend should consume the QBO bank-sync surface instead (`/api/qbo-bank-sync/*`).

### 4. Schema vs route inconsistencies

- `routes/payroll.py::ManualHoursRow` allows `total_hours` field but the corresponding `services_payroll.build_payroll_run_from_manual_hours` is called with `[h.model_dump() for h in body.hours]` — the service decides how to handle missing week splits. Frontend should pass `week1_hours` + `week2_hours` when known.
- `bank_review.py` and `hh_ap.py::HHAPInvoiceUpsertRequest` both expose `is_statement_only` and `raw_json` — easy to forget to set explicitly.
- `qbo_bank_sync.py` action endpoints (`set-review-status`, `match`, `unmatch`) carry NO `require_role`, while the (unwired) `bank_review.py` equivalents DO. If `bank_review.py` is wired up later, the auth posture will jump from "open" to "bookkeeper required" — consumers should expect that.

### 5. Stub / hard-coded validators

- `GET /api/payroll/validate-feb-2026` is a hard-coded comparison against `_FEB_PERIOD_4_TARGETS` and `_FEB_PERIOD_5_TARGETS` literals. Not a general validator. Don't ship it to production users without renaming or gating.
- `services_payroll_calc.py` docstring states the tax engine is "NOT a bit-for-bit clone of CRA's PDOC" and "Tax-engine variance is expected". Frontend should surface the documented note from `/validate-feb-2026`.

### 6. Two routers, one file

`routes/hh_ap_remittance_bank_match.py` defines BOTH `router` (`/api/hh-ap/remittance-bank-match`) and `clearing_router` (`/api/hh-ap-remittance`). They're imported and included separately in `main.py`. The `clearing_router` has exactly one endpoint (`POST /build-clearing-journal`) so the second router is easy to overlook.

### 7. Shared `/api/hh-ap` prefix

Three route files mount under the `/api/hh-ap` prefix:
- `routes/hh_ap.py` — `/api/hh-ap/*` (documents, invoices, statements, remittances, status, exceptions, reconciliation, match/run)
- `routes/hh_ap_overrides.py` — `/api/hh-ap/invoice-overrides*`, `/api/hh-ap/review-queue`
- `routes/hh_ap_remittance_bank_match.py` — `/api/hh-ap/remittance-bank-match/*`

This is consistent and intentional, but means /docs (OpenAPI) groups endpoints by the **tag**, not the URL prefix.

### 8. Mixed-auth endpoints (entity_code in query for the dep, but request handler takes another entity_code)

`bank_review.py::set_review_status` / `match` / `unmatch` use `Depends(require_role("bookkeeper"))` but accept no `entity_code` query param — the legacy JWT path will reject these with 400 unless caller is superadmin. (See #2.) These endpoints are unwired (#3), so the issue is theoretical until `bank_review.py` is included in main.py.

---

## Section D — Modules where no endpoint exists

### Standard financial reports — coverage

| Report | Status | Where the data lives |
|---|---|---|
| **Income Statement / P&L** | **MISSING — no endpoint** | Could be derived from `journal_lines` joined to `accounts`. No service surface exists. |
| **Balance Sheet** | **MISSING — no endpoint** | Same; would need a new aggregator. |
| **General Ledger** | partial | `GET /api/gl-import/runs/{run_id}/transactions` returns parsed QBO GL output. There is NO app-native GL endpoint (i.e. driven by `journal_lines`). |
| **Trial Balance** | partial | `GET /api/gl-import/runs/{run_id}/trial-balance` returns the QBO-vs-app cross-walk. No standalone "as of date" trial balance endpoint. |
| **AR Aging** | partial | `GET /api/pos-import/aged-ar/latest` (POS snapshot) and `GET /api/direct-vendor-ap/summary` (AP-side). No "live" AR aging report. |
| **AP Aging** | partial | HH AP module exposes `/api/hh-ap/reconciliation` (one statement at a time) and `/api/hh-ap/status`. `direct-vendor-ap/summary` covers the non-HH AP side. No unified AP aging endpoint. |
| **Payroll Summary** | partial | `GET /api/payroll/runs/{id}/summary` for a single run. No multi-run / period-summary endpoint. |
| **Cash Flow Statement** | **MISSING** | — |

### Other missing surfaces

- **Stripe billing endpoints** — None present. No `stripe`-prefixed routes, no `/billing` prefix.
- **Team / user invite endpoints** — Only legacy `POST /api/auth/users/{user_id}/roles` and `GET /api/auth/users` exist. No invite-token flow, no email-invite resend, no "pending invitation" endpoints. Under Clerk, user management is presumably handled in the Clerk dashboard / via the Clerk webhook (`POST /api/webhooks/clerk`), but there's no app endpoint to invite a user to a specific entity.
- **Notification preferences** — None present. No `/notifications`, `/preferences`, or `/users/{id}/preferences` endpoints.
- **Dealer / admin portal** (revenue, MRR, dealer list) — None present. No `/admin`, `/dealer`, `/revenue`, `/mrr`, `/usage` endpoints. The only "admin"-flavoured surface is `GET /api/auth/users` (superadmin only) which lists users + roles.
- **Document storage / retrieval** — Partial: HH AP module accepts uploads at `POST /api/hh-ap/upload-documents` and persists `file_bytes` to `hh_ap_documents`. There is NO endpoint to **fetch** the original bytes back out, NO generic `/documents/{id}` route, NO signed-URL flow. Parsed documents are referenced by ID inside other responses but only their metadata is returned.
- **Tax filings / HST returns** — None present.
- **Bank reconciliation report** — Closest is `GET /api/bank-review/summary` (unwired) and `GET /api/qbo-bank-sync/transactions`.
- **Vendor master / customer master** — None present. Vendor names appear inline in HH AP invoices, direct vendor AP invoices, and `vendor_classification_memory`, but there's no CRUD endpoint for a `vendors` table.
- **Audit log / activity feed** — `auth_events` is written via `services_auth.log_auth_event`, but no endpoint exposes those events. Journal-batch workflow events are exposed indirectly via `/api/month-end/workflow/batch` (shows history for one batch) but there's no global activity feed.

---

### routes/wage_planner.py — prefix `/api/wage-planner`

Professional tier (minimum role: `bookkeeper`; writes require `admin`).

| Method | Path | Auth | entity_code | Request | Response |
|---|---|---|---|---|---|
| GET | `/api/wage-planner/settings` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `{settings: WagePlannerSettings \| null, fiscal_year}` |
| PUT | `/api/wage-planner/settings` | admin + `enforce_entity_code` | body | body `SettingsRequest` {`entity_code`, `fiscal_year?`, `target_wage_pct`, `forecast_sales_change`, `avg_hourly_wage`, `benefits_pct`, `distribution_basis?`, `notes?`, `salaried_staff[]`} | `{settings, fiscal_year}` |
| GET | `/api/wage-planner/pay-periods` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `{fiscal_year, periods[]}` |
| PUT | `/api/wage-planner/pay-periods/{period_number}` | admin + `enforce_entity_code` | body | path `period_number`; body `PayPeriodRequest` {`entity_code`, `fiscal_year?`, `period_start`, `period_end`, `pay_date?`} | period row |
| POST | `/api/wage-planner/pay-periods/backfill` | admin + `enforce_entity_code` | body | body `{entity_code}` | `{inserted, message}` |
| GET | `/api/wage-planner/plan` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `WagePlannerPlan` {`settings`, `periods[]`, `summary`} |
| POST | `/api/wage-planner/refresh` | bookkeeper + `enforce_entity_code` | body | body `RefreshRequest` {`entity_code`, `fiscal_year?`, `period_number`, `payroll_run_id`, `actor_email?`} | refreshed period row |
| POST | `/api/wage-planner/override` | bookkeeper + `enforce_entity_code` | body | body `OverrideRequest` {`entity_code`, `fiscal_year?`, `period_number`, `actual_sales?`, `actual_gross_wages?`, `actual_stat_pay?`, `actual_hours?`} | override result |
| POST | `/api/wage-planner/min-wage-impact` | bookkeeper + `enforce_entity_code` | body | body `{entity_code, new_min_wage}` | `MinWageImpact` {`affected_employees`, `employees[]`, totals} |
| GET | `/api/wage-planner/snapshots` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `{fiscal_year, snapshots[]}` |
| GET | `/api/wage-planner/snapshots/latest` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `{id, pay_period_number, status, generated_at, download_url}` |
| GET | `/api/wage-planner/snapshots/{snapshot_id}/download` | bookkeeper | query | path `snapshot_id`; query `entity_code` | `{download_url}` or `{download_url: null, fallback}` |
| GET | `/api/wage-planner/snapshots/{snapshot_id}/excel` | bookkeeper | query | path `snapshot_id`; query `entity_code` | inline xlsx bytes (fallback when R2 unavailable) |
| GET | `/api/wage-planner/excel` | bookkeeper | query | query `entity_code`, `fiscal_year?` | `{url, r2_key, filename}` or inline xlsx bytes |

**SQL migrations (slots 055–058):**
- `055_payroll_pay_periods.sql` — `payroll_pay_periods` (entity_id, fiscal_year, period_number, period_start, period_end, pay_date, source). UNIQUE(entity_id, fiscal_year, period_number).
- `056_wage_planner_settings.sql` — `wage_planner_settings` + child `wage_planner_salaried_staff`. UNIQUE(entity_id, fiscal_year).
- `057_wage_planner_periods.sql` — `wage_planner_periods` (forecast + actual columns, locked flag). UNIQUE(entity_id, fiscal_year, period_number).
- `058_wage_planner_snapshots.sql` — `wage_planner_snapshots` (immutable Excel archive). UNIQUE(entity_id, fiscal_year, pay_period_number).

---

End of catalog.
