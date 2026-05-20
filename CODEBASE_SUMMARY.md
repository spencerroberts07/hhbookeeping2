# BookWize — Codebase Handoff Summary

**As of commit `77fa779` on `main`.** Two services: FastAPI backend (`backend/`) and Next.js 14 frontend (`frontend/`). One repo. The backend is deployed at `https://hhbookeeping2.onrender.com`. The frontend is deployed at `https://bookwize-frontend.onrender.com` (Render service `bookwize-web`).

This document is meant to be read once, end to end, before touching code. Every backend endpoint is listed. Every frontend page is listed and mapped to the endpoints it calls. Every `// TODO` is enumerated. Every page using mock data is flagged. The full DB schema is here. So are the env vars and the known bugs.

---

## 1. Repository layout

```
hhbookeeping2/
├── backend/                      FastAPI service (Python)
│   ├── app/
│   │   ├── main.py               FastAPI() factory, CORS, route registration
│   │   ├── config.py             pydantic settings (every env var lives here)
│   │   ├── db.py                 SQLAlchemy engine + db_session() context
│   │   ├── services_auth.py      legacy JWT auth (still present, dispatched
│   │   │                          to by require_role() when USE_CLERK_AUTH=false)
│   │   ├── services_auth_clerk.py  Clerk auth — PyJWT + JWKS verification,
│   │   │                            role mapping, webhook sync
│   │   ├── services_billing.py   Stripe customer + subscription helpers
│   │   ├── services_invoice_matching.py  Auto-match invoices to bank/HH/journal
│   │   ├── services_{accruals,auto_match,bank_*,cogs,depreciation,
│   │   │             gl_import,month_end_close,payroll,payroll_calc,
│   │   │             period_close,pos_import,vendor_classification,
│   │   │             claude_classifier}.py    one module per business domain
│   │   ├── services.py           Shared helpers (entity lookup, parsers, etc.)
│   │   ├── routes/               31 route files (see §3)
│   │   ├── schemas.py            response-only Pydantic models
│   │   ├── quickbooks.py         QBO OAuth + REST client
│   │   ├── google_sheets.py      Google Sheets reader (legacy)
│   │   └── journal_batch_workflow.py  approve/reject batch state machine
│   ├── sql/                      27 numbered migrations (see §6)
│   ├── docs/endpoint_catalog.md  full audit catalog from before this doc
│   ├── tests/                    Clerk auth tests (unittest, 18 passing)
│   ├── requirements.txt
│   └── Dockerfile                (legacy, not used by Render)
│
├── frontend/                     Next.js 14 App Router
│   ├── app/
│   │   ├── layout.tsx            ClerkProvider + Toaster + QueryProvider
│   │   ├── globals.css           Tailwind + CSS vars + print stylesheet
│   │   ├── (marketing)/          public site (landing + pricing)
│   │   ├── (auth)/               Clerk sign-in/up + 8-step onboarding
│   │   ├── (app)/                authenticated dealer app
│   │   └── (app)/admin/          BookWize-staff-only portal
│   ├── components/
│   │   ├── layout/               sidebar, topbar, entity-switcher, mobile-sidebar
│   │   ├── providers/            ClerkTokenBridge, QueryProvider
│   │   ├── reports/              report-shell (date controls, print, csv)
│   │   ├── shared/multi-file-upload.tsx   drag-drop + queue + per-file status
│   │   └── ui/                   shadcn primitives (button, card, dialog,
│   │                              input, label, select, popover, badge,
│   │                              skeleton, separator, sheet)
│   ├── lib/
│   │   ├── api/                  one TS module per backend domain (see §4)
│   │   ├── store/                Zustand: entity, user, onboarding
│   │   ├── hooks/use-upload-defaults.ts   auto-inject entity_code + actor_email
│   │   └── utils.ts              cn() + formatMoney/Date/Percent
│   ├── middleware.ts             Clerk route protection
│   ├── public/brand/             11 brand SVGs
│   ├── tailwind.config.ts        BookWize design tokens
│   ├── tsconfig.json             strict + noUncheckedIndexedAccess
│   └── .env.local.example
│
├── CLAUDE.md                     (project conventions — when present)
├── FRONTEND.md                   (frontend build deliverables doc)
├── README.md
└── CODEBASE_SUMMARY.md           this file
```

---

## 2. High-level architecture

- **Multi-tenant model.** Each dealer is one Clerk organization mapped 1:1 to one `entities` row by `entities.clerk_org_id` (migration 025). One dealer with multiple stores → multiple Clerk orgs + multiple `entities` rows + one Stripe customer + N subscriptions (q28).
- **Auth.** `USE_CLERK_AUTH` flag in `services_auth.require_role()` chooses between legacy JWT (`users` + `user_sessions`) and Clerk (JWT verified via Clerk's JWKS endpoint, role read from `org_role` claim). Both shapes resolve to a mapping-shaped object so route handlers don't change.
- **Role hierarchy:** `viewer` (10) < `bookkeeper` (20) < `approver` (30) < `admin` (40). Clerk's `org_role` is normalized — both `org:admin` and bare `admin` resolve to app role `admin`. `superadmin` is a separate column on `users`, unrelated to Clerk.
- **Per-entity scoping.** Every request's `entity_code` is enforced two ways:
  1. The Clerk role-check dependency reads `entity_code` from the user's Clerk org and rejects any `?entity_code=X` query param that doesn't match.
  2. Body/Form `entity_code` is rechecked by `enforce_entity_code(_user, value)` inside ~48 handlers across 14 route files.
- **Journal model.** No `journal_entries` table — every "entry" is one `journal_batches` row (header) with N `journal_lines` rows (Dr/Cr lines). Workflow events live in `journal_batch_workflow_events`.
- **Period model.** `accounting_periods` rows are immutable once the period is closed (`status = 'closed'`). One open period per entity at a time. `period_close_events` is the audit trail.
- **Frontend state.** Zustand (`entity`, `user`, `onboarding`) holds session-level state, localStorage-persisted. React Query handles every server fetch (with cache invalidation on entity switch via `queryClient.clear()`).

---

## 3. Backend endpoints

Auth column meanings: `none` = no auth dep on the route; `none — PUBLIC` = intentionally unauthenticated (webhooks, health); `viewer` / `bookkeeper` / `approver` / `admin` come from `Depends(require_role("..."))` or inline `enforce_role(..., min_role=...)`; `user-context` = `Depends(get_current_user)` — any logged-in user, no entity-role check. `+ enforce_entity_code` means the handler additionally validates the body/form entity_code matches the Clerk-mapped entity (no-op under JWT).

### `routes/auth.py` — prefix `/api/auth`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/auth/register` | bootstrap: none; otherwise inline superadmin | body `{email, password>=8, full_name?, is_superadmin?}` | user dict + roles[]; 201 |
| POST | `/api/auth/login` | none — PUBLIC | body `{email, password}` | `{access_token, token_type, expires_at, user}` |
| POST | `/api/auth/logout` | user-context | — | `{ok: true}` |
| GET | `/api/auth/me` | user-context | — | user dict + roles[] |
| POST | `/api/auth/users/{user_id}/roles` | inline enforce_role(admin) on body.entity_code | path `user_id`; body `{entity_code, role, actor_email?}` | granted role row |
| DELETE | `/api/auth/users/{user_id}/roles/{entity_id}` | user-context + inline admin-on-entity check | path | `{ok: true}` |
| GET | `/api/auth/users` | user-context + inline superadmin | — | `{count, users[]}` |

### `routes/entities.py` — prefix `/api/entities` (+ `/api/me`)

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/entities` | admin | body `{entity_code, entity_name, fiscal_year_end_month, fiscal_year_end_day, province, base_currency?, clerk_org_id?}` | entity dict; 201 |
| PATCH | `/api/entities/{entity_code}` | admin | partial of POST body | entity dict |
| GET | `/api/me/entities` | reads Clerk token directly | query `org_ids?=...,...` (comma) | `{entities: [{entity_code, entity_name, clerk_org_id, role}]}` |

### `routes/billing.py` — prefix `/api/billing` + `/api/webhooks/stripe`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/billing/checkout-session` | admin | `{entity_code, plan_tier: 'starter'\|'professional', success_url, cancel_url}` | `{url, session_id}` |
| POST | `/api/billing/portal-session` | admin | `{entity_code, return_url}` | `{url}` |
| GET | `/api/billing/subscription` | bookkeeper | query `entity_code` | `{status, plan_tier, current_period_end, trial_end, cancel_at_period_end, store_count, customer_id}` |
| POST | `/api/webhooks/stripe` | none — PUBLIC (svix-signed) | Stripe event | `{ok, event_type}` |

### `routes/clerk_webhook.py` — `/api/webhooks/clerk`

| Method | Path | Auth | Body | Response |
|---|---|---|---|---|
| POST | `/api/webhooks/clerk` | none — PUBLIC (svix-signed) | Clerk event (`user.*`, `organizationMembership.*`, `organization.*`) | `{ok, event_type}` |

### `routes/invoice_documents.py` — prefix `/api/invoice-documents`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/invoice-documents/upload` | bookkeeper + enforce_entity_code | form `{entity_code, invoice_type, files[]}` | `{processed[], duplicates[], failed[], record_count}` |
| GET | `/api/invoice-documents` | viewer | `entity_code, status?, invoice_type?, date_from?, date_to?, limit?, offset?` | `{invoices[], total, limit, offset}` |
| GET | `/api/invoice-documents/unmatched-queue` | viewer | `entity_code, period_end?` | `{queue: [{invoice, suggested_matches[]}], total}` |
| GET | `/api/invoice-documents/{id}` | viewer | path; query `entity_code` | `{invoice, links[]}` |
| PATCH | `/api/invoice-documents/{id}` | bookkeeper + enforce_entity_code | partial `{entity_code, invoice_number?, vendor_name?, invoice_date?, due_date?, amount?, notes?}` | invoice dict |
| POST | `/api/invoice-documents/{id}/match` | bookkeeper + enforce_entity_code | `{entity_code, actor_email?, journal_batch_id?, bank_transaction_id?, hh_ap_invoice_id?}` (exactly one of the three target ids) | `{ok, invoice_id, link_type}` |
| POST | `/api/invoice-documents/{id}/post-to-ap` | bookkeeper + enforce_entity_code | `{entity_code, actor_email, ap_account: '2020'\|'2030', expense_account_code?, period_end, memo?}` | `{ok, invoice_id, journal_batch_id, amount, ap_account, expense_account}` |
| POST | `/api/invoice-documents/{id}/delete` | bookkeeper + enforce_entity_code | `{entity_code, reason}` | `{ok, invoice_id, status: 'deleted'}` |
| POST | `/api/invoice-documents/sweep` | bookkeeper + enforce_entity_code | query `entity_code, period_end?` | `{invoices_examined, auto_matched, suggested, unmatched}` |

### `routes/accruals.py` — prefix `/api/accruals`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/accruals/seed-templates` | bookkeeper + enforce_entity_code | `{entity_code, actor_email}` | seed result |
| GET | `/api/accruals/templates` | none | query `entity_code` | `{templates[]}` |
| POST | `/api/accruals/templates/upsert` | bookkeeper + enforce_entity_code | `{entity_code, actor_email, accrual_code, description?, debit_account, credit_account, default_amount?, frequency?, is_active?, notes?}` | template row |
| POST | `/api/accruals/build-journal` | bookkeeper + enforce_entity_code | `{entity_code, period_end, accrual_codes[], amounts_override?, actor_email}` | journal batch + lines |
| GET | `/api/accruals/journals` | none | `entity_code, period_end` | journal rows |

### `routes/auto_match.py` — prefix `/api/auto-match`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/auto-match/run` | bookkeeper + enforce_entity_code | `{entity_code, period_start, period_end, actor_email, triggered_by?, date_window_days?, amount_tolerance?, max_to_apply?}` | run summary |
| GET | `/api/auto-match/runs` | none | `entity_code, limit?` | runs list |
| GET | `/api/auto-match/runs/{run_id}` | none | path; query `entity_code` | run detail |

### `routes/bank_auto_journal.py` — prefix `/api/bank-auto-journal`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/bank-auto-journal/seed-rules` | bookkeeper + enforce_entity_code | `{entity_code, actor_email}` | seeded rules |
| GET | `/api/bank-auto-journal/rules` | none | query `entity_code` | rules list |
| POST | `/api/bank-auto-journal/run` | bookkeeper + enforce_entity_code | `{entity_code, period_start, period_end, actor_email}` | run summary |
| GET | `/api/bank-auto-journal/runs` | none | `entity_code, limit?` | runs list |
| GET | `/api/bank-auto-journal/runs/{run_id}` | none | path; `entity_code` | run detail |
| GET | `/api/bank-auto-journal/unmatched` | none | `entity_code, period_start, period_end` | unmatched txns |

### `routes/bank_csv.py` — prefix `/api/bank-csv`

| Method | Path | Auth | Body / Form / Query | Response |
|---|---|---|---|---|
| GET | `/api/bank-csv/mapping-profiles` | none | — | built-in profiles |
| POST | `/api/bank-csv/preview` | bookkeeper + enforce_entity_code | form `entity_code, file, mapping_profile?, source_account_code?, source_account_name?, column_map_json?, sample_limit?` | preview |
| POST | `/api/bank-csv/upload` | bookkeeper + enforce_entity_code | form `entity_code, actor_email, file, …` | import run; 409 on PeriodLockedError |
| GET | `/api/bank-csv/import-runs` | none | `entity_code, limit?, source_account_code?` | runs |
| GET | `/api/bank-csv/import-runs/{run_id}` | none | path; `entity_code` | run detail |

### `routes/bank_pdf.py` — prefix `/api/bank-pdf`

| Method | Path | Auth | Form | Response |
|---|---|---|---|---|
| POST | `/api/bank-pdf/preview` | bookkeeper + enforce_entity_code | `entity_code, file, source_account_code?, source_account_name?, sample_limit?` | preview |
| POST | `/api/bank-pdf/upload` | bookkeeper + enforce_entity_code | `entity_code, actor_email, file, source_account_code?, source_account_name?, note?` | run; 409 on lock |

### `routes/bank_review.py` — prefix `/api/bank-review` (⚠️ **NOT wired in main.py**)

Endpoint methods exist on disk but `app.include_router(bank_review_router)` is never called — the same matching surface is exposed via `qbo-bank-sync` instead. See §10 "Known issues."

### `routes/card_settlement.py` — prefix `/api/card-settlement`

| Method | Path | Auth | Query | Response |
|---|---|---|---|---|
| GET | `/api/card-settlement/summary` | none | `entity_code, date_from, date_to, reconciliation_status?, bank_match_state?` | summary |
| GET | `/api/card-settlement/batches` | none | same | list |
| POST | `/api/card-settlement/batches/upsert` | bookkeeper (no enforce_entity_code) | query `entity_code`; body batch | result |
| GET | `/api/card-settlement/batches/{batch_id}` | none | path; query | detail |
| POST | `/api/card-settlement/batches/{batch_id}/set-status` | bookkeeper | path; query; body | result |

### `routes/cash_balancing.py` — prefix `/api/cash-balancing`

Read endpoints (`GET /summary`, `/days`, `/days/{id}`) are unauthenticated.
Write endpoints (`POST /days`, `/days/{id}/post-journal`) currently have **no `require_role` dependency** — see §10.

### `routes/cogs.py` — prefix `/api/cogs`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/cogs/build-journal` | bookkeeper + enforce_entity_code | `{entity_code, period_end, actor_email, dating_new_amount, dating_reversal_amount?, other_adjustment_amount?, other_adjustment_memo?, shrinkage_included?}` | journal batch + lines |
| GET | `/api/cogs/status` | none | `entity_code, period_end` | status |
| GET | `/api/cogs/suggested-dating` | none | `entity_code, period_end` | suggestions |

### `routes/dashboard.py` — prefix `/api/dashboard`

| Method | Path | Auth | Query | Response |
|---|---|---|---|---|
| GET | `/api/dashboard/quickbooks_status` | none | `entity_code` | QBO connection summary |

### `routes/depreciation.py` — prefix `/api/depreciation`

POST `/seed-assets`, `/generate-schedule`, `/build-journal` (all bookkeeper + enforce_entity_code).
GET `/assets`, `/schedule`, `/summary` (all unauthenticated).

### `routes/direct_vendor_ap.py` — prefix `/api/direct-vendor-ap`

7 endpoints (most are bookkeeper) for non-HH AP cheque + e-transfer tracking. `entity_code` arrives as query param.

### `routes/gl_import.py` — prefix `/api/gl-import`

| Method | Path | Auth | Body | Response |
|---|---|---|---|---|
| POST | `/api/gl-import/upload` | bookkeeper + enforce_entity_code | form `entity_code, actor_email, file, period_start?, period_end?` | import run |
| POST | `/api/gl-import/runs/{run_id}/build-comparison` | bookkeeper + enforce_entity_code | path; `{entity_code, actor_email}` | comparison summary |
| GET | `/api/gl-import/runs` | none | `entity_code, limit?` | runs |
| GET | `/api/gl-import/runs/{run_id}` | none | path; `entity_code` | run detail |
| GET | `/api/gl-import/runs/{run_id}/trial-balance` | none | path; `entity_code, only_variance?` | rows + totals |
| GET | `/api/gl-import/runs/{run_id}/transactions` | none | path; `entity_code, account_code?, limit?` | transactions |

### `routes/hh_ap.py` — prefix `/api/hh-ap` (largest route file, ~3800 lines)

Bookkeeper-required POSTs: `/upload-documents`, `/invoices/upload-and-parse-batch`, `/invoices/upsert`, `/remittances/upsert`, `/statements/upsert`, `/statements/parse-document`, `/invoices/parse-document`, `/remittances/parse-document`, `/match/run`. Plus a long tail of GET listing endpoints. See `backend/docs/endpoint_catalog.md` for the per-handler breakdown.

### `routes/hh_ap_overrides.py` — prefix `/api/hh-ap/overrides`

CRUD on per-invoice account-code overrides. **No `require_role` on the write endpoints** — see §10.

### `routes/hh_ap_remittance_bank_match.py` — TWO routers

- `/api/hh-ap-remittance-bank-match` — matching surface
- `/api/hh-ap-remittance-clearing` — `POST /build-journal` (clearing journal builder)

### `routes/month_end*.py` — month-end coordination

- `/api/month-end/overview?entity_code=&period_end=` — top-level workflow status (no auth)
- `/api/month-end-close/...` — close-orchestration endpoints
- `/api/month-end-hh-ap/...` — HH AP-specific close subroutines
- `/api/month-end-workflow/{submit,approve,reject,reopen}` — batch state transitions (approver + enforce_entity_code)

### `routes/payroll.py` — prefix `/api/payroll`

Employee + run management:
- `POST /employees/seed`, `/employees/upsert` (bookkeeper + enforce_entity_code)
- `GET /employees` (none)
- `POST /runs/upload-hours`, `/runs/upload-register` (bookkeeper + enforce_entity_code; multipart)
- `POST /runs/manual-hours` (bookkeeper + enforce_entity_code)
- `POST /runs/{id}/build-journal` (bookkeeper + enforce_entity_code)
- `POST /runs/{id}/submit`, `/approve`, `/schedule-withdrawals` (bookkeeper + enforce_entity_code)
- `GET /runs`, `/runs/{id}`, `/runs/{id}/summary` (none)
- `GET /validate-feb-2026?entity_code=` (debug — none)

### `routes/period_close.py` — prefix `/api/period-close`

| Method | Path | Auth | Body / Query | Response |
|---|---|---|---|---|
| POST | `/api/period-close/submit` | approver + enforce_entity_code | `{entity_code, period_end, actor_email, notes?}` | status payload |
| POST | `/api/period-close/approve` | approver + enforce_entity_code | same | status payload |
| POST | `/api/period-close/reopen` | approver + enforce_entity_code | `{…, notes: required}` | status payload |
| GET | `/api/period-close/current` | none | `entity_code` | `{period_end, period_label, status}` (404 if none) |
| GET | `/api/period-close/status` | none | `entity_code, period_end` | full status payload |
| GET | `/api/period-close/history` | none | `entity_code, period_end` | events list |

### `routes/pos_import.py` — prefix `/api/pos-import`

5 multipart uploads (one per report type): `inventory-adjustment`, `pos-financial`, `inventory-value`, `aged-ar`, `ar-adjustment` (all bookkeeper + enforce_entity_code).
4 journal builders: `build-store-use-journal`, `build-donation-journal`, `build-ar-adjustment-journal`, `validate-pos-financial` (all bookkeeper + enforce_entity_code).
5 GETs: `runs`, `runs/{id}`, `inventory-value/latest`, `aged-ar/latest`, `pos-financial/latest` (all unauthenticated).

### `routes/qbo_auth.py` — prefix `/api/auth/quickbooks`

OAuth `/connect` + `/callback` — unauthenticated by design (OAuth flow).

### `routes/qbo_bank_sync.py` — prefix `/api/qbo-bank-sync`

`/summary`, `/transactions`, `/transactions/{id}`, `/transactions/{id}/{set-review-status,match,unmatch}`, `/runs`, `/runs/{id}`, `/run`. The transactions surface is the one the frontend's bank module hits.

### `routes/sync.py` — prefix `/api/sync`

QBO transaction sync. `POST /sync` (no auth — operationally restricted), `GET /jobs`, `GET /jobs/{id}`.

### `routes/vendor_classification.py` — prefix `/api/vendor-classification`

- `POST /learn-from-gl` (bookkeeper + enforce_entity_code)
- `GET /memory`, `/suggestions` (none)
- `POST /memory/upsert`, `/suggestions/{id}/accept`, `/suggestions/{id}/override` (bookkeeper + enforce_entity_code)

### Health
- `GET /health` — `{environment}`. PUBLIC.

---

## 4. Frontend pages → backend endpoints

Every page below sits under `frontend/app/`. Endpoints called are the ones invoked via `lib/api/*` modules; React Query keys are noted where relevant.

### Public — `(marketing)`

| Route | Calls |
|---|---|
| `/` (landing) | — (static) |
| `/pricing` | — (static; plan features from `PLAN_FEATURES` const) |

### Auth — `(auth)`

| Route | Calls |
|---|---|
| `/sign-in` | Clerk (renders `<SignIn />`) |
| `/sign-up` | Clerk (renders `<SignUp />`) |
| `/onboarding` | Step 1: `POST /api/entities` Step 2: `POST /api/bank-pdf/preview` Step 3: `POST /api/hh-ap/upload-documents` Step 6: Clerk `organization.inviteMember()` (frontend-direct) Step 7: `POST /api/billing/checkout-session` |

### Authenticated app — `(app)`

| Route | Calls |
|---|---|
| `/dashboard` | `GET /api/dashboard/quickbooks_status` `GET /api/hh-ap/summary` `GET /api/period-close/current` `GET /api/period-close/status` `GET /api/pos-import/pos-financial/latest` `GET /api/vendor-classification/suggestions` `GET /api/invoice-documents/unmatched-queue` |
| `/month-end` | `GET /api/period-close/status` |
| `/month-end` Step 1 (Documents) | `POST /api/bank-pdf/upload` `POST /api/hh-ap/invoices/upload-and-parse-batch` `POST /api/hh-ap/upload-documents` `POST /api/pos-import/pos-financial` `POST /api/pos-import/inventory-adjustment` `POST /api/payroll/runs/upload-register` (×2 for P1/P2) `POST /api/pos-import/aged-ar` `POST /api/gl-import/upload` `POST /api/invoice-documents/upload` (outside_vendor) |
| `/month-end` Step 2 | `POST /api/bank-auto-journal/run` `POST /api/auto-match/run` `GET /api/bank-auto-journal/runs` |
| `/month-end` Step 3 | `GET /api/vendor-classification/suggestions?status=pending` `POST /api/vendor-classification/suggestions/{id}/accept` |
| `/month-end` Step 4 | `GET /api/cogs/status` `GET /api/cogs/suggested-dating` `POST /api/cogs/build-journal` `POST /api/hh-ap-remittance-clearing/build-journal` `POST /api/depreciation/build-journal` `POST /api/month-end-workflow/approve` |
| `/month-end` Step 5 | `GET /api/pos-import/runs` `POST /api/pos-import/validate-pos-financial` `GET /api/gl-import/runs` `GET /api/gl-import/runs/{id}/trial-balance` `GET /api/invoice-documents/unmatched-queue` `GET /api/invoice-documents` |
| `/month-end` Step 6 | `GET /api/period-close/status` `POST /api/period-close/submit` `POST /api/period-close/approve` |
| `/reports` | (index, links only) |
| `/reports/income-statement` | **MOCK** — `getIncomeStatement` (see §5) |
| `/reports/balance-sheet` | **MOCK** — `getBalanceSheet` (see §5) |
| `/reports/trial-balance` | **MOCK** primary path (`getAsOfTrialBalance`), real fallback `GET /api/gl-import/runs` + `/runs/{id}/trial-balance` |
| `/reports/general-ledger` | `GET /api/gl-import/runs` + `/runs/{id}/transactions` `POST /api/gl-import/upload` (uploader at top) |
| `/reports/ar-aging` | `GET /api/pos-import/aged-ar/latest` |
| `/reports/ap-aging` | `GET /api/hh-ap/summary` `GET /api/hh-ap/invoices` |
| `/reports/payroll` | `GET /api/payroll/runs` |
| `/transactions` | `GET /api/gl-import/runs` `GET /api/gl-import/runs/{id}/transactions` (row-click) `GET /api/invoice-documents` (in detail dialog, filters by amount) |
| `/documents` | Aggregates: `GET /api/gl-import/runs`, `/api/payroll/runs`, `/api/pos-import/runs` (TODO: unified endpoint — §5) |
| `/payroll` | `GET /api/payroll/runs` `GET /api/payroll/employees` |
| `/payroll/new` | `POST /api/payroll/runs/upload-hours` `POST /api/payroll/runs/upload-register` |
| `/ap` (HH tab) | `GET /api/hh-ap/summary` `GET /api/hh-ap/invoices` `POST /api/hh-ap/invoices/upload-and-parse-batch` `POST /api/hh-ap/upload-documents` `POST /api/invoice-documents/upload` (invoice_type=hh_ap) `GET /api/invoice-documents/unmatched-queue` (badge) |
| `/ap` (Outside Vendor tab) | `GET /api/invoice-documents?invoice_type=outside_vendor` `POST /api/invoice-documents/upload` (invoice_type=outside_vendor) |
| `/ap` (Unmatched tab) | Link only → `/ap/unmatched` |
| `/ap/unmatched` | `GET /api/invoice-documents/unmatched-queue` `POST /api/invoice-documents/{id}/match` `POST /api/invoice-documents/{id}/post-to-ap` `POST /api/invoice-documents/{id}/delete` `PATCH /api/invoice-documents/{id}` |
| `/bank` | `GET /api/qbo-bank-sync/summary` `GET /api/qbo-bank-sync/transactions` `POST /api/bank-pdf/upload` `POST /api/bank-pdf/preview` `POST /api/bank-csv/upload` `POST /api/bank-csv/preview` |
| `/settings` | redirect → `/settings/store` |
| `/settings/store` | `PATCH /api/entities/{entity_code}` |
| `/settings/team` | Clerk `useOrganization().memberships` + `inviteMember` / `destroy` (frontend-direct) |
| `/settings/billing` | `GET /api/billing/subscription` `POST /api/billing/portal-session` |
| `/settings/accounts` | **MOCK** placeholder list (§5) |
| `/settings/notifications` | in-component state only (§5) |

### Admin — `(admin)` (BookWize staff)

| Route | Calls |
|---|---|
| `/admin` | redirect → `/admin/dealers` |
| `/admin/dealers` | **MOCK** — `listDealers()` |
| `/admin/revenue` | **MOCK** — `getRevenueSnapshot()` |
| `/admin/support` | **MOCK** — `linkEntityToOrg()` |

---

## 5. TODOs + mock-data inventory

Every place in the frontend where real backend support is pending.

### TODOs (every `// TODO` comment in `frontend/`)

| File | Line | Comment |
|---|---|---|
| `lib/api/reports.ts` | 39 | backend endpoint not built — `POST /api/reports/income-statement` |
| `lib/api/reports.ts` | 107 | backend endpoint not built — `POST /api/reports/balance-sheet` |
| `lib/api/reports.ts` | 159 | backend endpoint not built — `GET /api/reports/trial-balance-as-of` |
| `lib/api/admin.ts` | 19 | backend endpoint not built — `GET /api/admin/dealers` |
| `lib/api/admin.ts` | 46 | backend endpoint not built — `GET /api/admin/revenue` |
| `lib/api/admin.ts` | 59 | backend endpoint not built — `POST /api/admin/entity-org-link` |
| `lib/api/documents.ts` | 37 | backend endpoint not built — `GET /api/documents` (unified list) |
| `app/(app)/dashboard/page.tsx` | 133 | backend endpoint not built — exact bank balance |
| `app/(app)/dashboard/page.tsx` | 194 | backend endpoint not built — gross margin time series |
| `app/(app)/dashboard/_components/alerts-feed.tsx` | 59 | backend endpoint not built — unified alerts feed (period-locks, unmatched bank, missing docs) |
| `app/(app)/dashboard/_components/sales-chart.tsx` | 15 | backend endpoint not built — month-vs-yoy sales time series |
| `app/(app)/dashboard/_components/gross-margin-sparkline.tsx` | 5 | backend endpoint not built — gross margin trend series |
| `app/(app)/reports/general-ledger/page.tsx` | 27 | backend endpoint not built — app-native `/api/reports/general-ledger` |
| `app/(app)/payroll/page.tsx` | 171 | backend endpoint not built — CRA remittance summary |
| `app/(app)/admin/support/page.tsx` | 72 | backend endpoint not built — admin user search |
| `app/(app)/settings/accounts/page.tsx` | 7 | backend endpoint not built — chart of accounts CRUD |
| `app/(app)/settings/notifications/page.tsx` | 7 | backend endpoint not built — notification preferences storage |

### Pages using mock / stub data

| Page | Data source | What's mocked |
|---|---|---|
| `/reports/income-statement` | `lib/api/reports.ts::getIncomeStatement` | Full P&L — revenue, COGS, gross profit, opex, net income. ~250ms simulated latency. |
| `/reports/balance-sheet` | `lib/api/reports.ts::getBalanceSheet` | Full BS — current/fixed assets, current/LT liabilities, equity. |
| `/reports/trial-balance` | `lib/api/reports.ts::getAsOfTrialBalance` (primary) | As-of TB is mock; falls back to real run-based TB if a GL import exists. |
| `/admin/dealers` | `lib/api/admin.ts::listDealers` | Static 1-row mock (Bridlewood). |
| `/admin/revenue` | `lib/api/admin.ts::getRevenueSnapshot` | Static MRR/ARR/dealer-count mock. |
| `/admin/support` | `lib/api/admin.ts::linkEntityToOrg` | No-op stub. |
| `/dashboard` sales chart | `_components/sales-chart.tsx` | 6 months of mock month-vs-YoY data. |
| `/dashboard` gross margin | `_components/gross-margin-sparkline.tsx` | 7-point mock sparkline series. |
| `/dashboard` cash position card | `dashboard/page.tsx:133` | Shows QBO connection status only — exact bank balance not yet endpoint-backed. |
| `/dashboard` alerts feed | `_components/alerts-feed.tsx:59` | Real classification suggestions + unmatched-invoice count; the "Missing HH AP statement" and "Month-end reminder" items are static placeholders. |
| `/payroll` CRA tab | `payroll/page.tsx:171` | Explanatory copy only; no remittance numbers. |
| `/settings/accounts` | `accounts/page.tsx` | Static 10-row sample chart of accounts. |
| `/settings/notifications` | `notifications/page.tsx` | Toggle state in `useState`; no persistence. |
| `/documents` | `lib/api/documents.ts::listDocuments` | Aggregates from per-module run endpoints client-side; no unified `GET /api/documents`. PDF viewing not supported. |

---

## 6. Database schema

PostgreSQL on Render. 27 numbered migrations applied. Tables (grouped by domain):

### Auth + identity
- `users` *(010)* — legacy bcrypt user accounts. PK `id` UUID, unique `email`, `is_superadmin` bool, `is_active` bool.
- `user_sessions` *(010)* — JWT issue/revoke tracking. `token_hash`, `expires_at`, `revoked_at`.
- `user_entity_roles` *(010)* — per-entity role grants under JWT. Unique active row per `(user_id, entity_id)`.
- `auth_events` *(010)* — login/logout/role-change audit trail.
- `clerk_users` *(025)* — Clerk identity mirror, populated by webhook. `clerk_user_id` unique, `entity_code` + cached `role`.

### Entities + billing
- `entities` *(000 baseline; 025 adds `clerk_org_id`; 026 adds `province`, `created_by_clerk_user_id`)* — one row per store/dealer-location. `entity_code` TEXT unique, `clerk_org_id` unique-partial.
- `organizations` *(000)* — parent multi-tenant org; FK from `entities.organization_id`.
- `entity_integrations` *(000)*, `entity_settings` *(000)* — per-entity config.
- `billing_customers` *(026)* — one Stripe customer per dealer (keyed by `clerk_user_id`).
- `billing_subscriptions` *(026)* — one Stripe subscription per entity (1 store = 1 sub). `plan_tier`, `status`, `current_period_end`, `trial_end`, `cancel_at_period_end`.

### Accounting core
- `accounting_periods` *(000)* — per-entity periods. `status` ∈ {`open`, `submitted_for_close`, `closed`}.
- `accounts` *(000)* — *unused 1-2-digit chart that pre-dates the 4-digit QBO chart. Don't use for new code.*
- `journal_batches` *(000)* — one row per logical journal entry. Unique `(entity_id, accounting_period_id, source_module, batch_label)`. `workflow_status` drives approve/reject state machine.
- `journal_lines` *(000)* — Dr/Cr lines within a batch.
- `journal_batch_workflow_events` *(000)* — audit trail of submit/approve/reject/reopen.
- `gl_account_balances` *(016)* — per-period balances from QBO export.
- `gl_transactions` *(016)* — per-line transactions from QBO export.
- `gl_import_runs` *(016)* — each GL export upload.
- `gl_trial_balance_comparisons` *(016)* — TB compare snapshot.

### Period close
- `period_close_events` *(011)* — submit/approve/reopen audit.
- `close_checklist_items` *(000)* — month-end checklist row state.

### Bank
- `bank_transactions` *(000; 004 normalizes)* — every bank txn, source-tagged.
- `bank_transaction_matches` *(004/005)* — which bank txn is matched to which downstream record (remittance, payroll withdrawal, AP, etc.).
- `bank_transaction_review_events` *(005)* — reviewer audit.
- `bank_csv_import_runs` *(000/009)* — CSV upload audit.
- `bank_feed_transactions` *(000)* — legacy feed staging.
- `bank_transaction_rules` *(019)* — auto-journal rules.
- `bank_auto_journal_runs` + `bank_auto_journal_lines` *(019)* — run + line audit.
- `vendor_classification_memory` *(020)* — vendor → account memory.
- `bank_classification_suggestions` *(020)* — model+memory+rules suggestions queue.

### HH AP (Home Hardware AP)
- `hh_ap_documents` *(000)* — uploaded PDFs (statements, invoices, remittances). `source_hash` dedupes.
- `hh_ap_invoices` *(000)* — parsed invoice rows.
- `hh_ap_invoice_overrides` *(000)* — per-invoice account routing overrides.
- `hh_ap_remittances` *(000)* — remittance headers.
- `hh_ap_remittance_lines` *(000)* — remittance line items.
- `hh_ap_statements` *(000)* — monthly statement headers.
- `hh_ap_statement_lines` *(000)* — statement line items.
- `hh_ap_remittance_bank_match_events` *(000/006)* — bank-to-remittance match audit.
- `hh_statement_lines` *(000)* — legacy (separate from `hh_ap_statement_lines`).

### POS + inventory
- `pos_import_runs` *(014)* — every POS report upload.
- `inventory_adjustment_lines` *(014)* — parsed cycle-count + shrinkage entries.
- `inventory_value_snapshots` *(014)* — inventory dollar value at month-end.
- `aged_ar_snapshots` *(014)* — customer AR snapshot.
- `pos_financial_snapshots` *(014)* — monthly POS Financial.
- `ar_adjustment_lines` *(015)* — bad-debt / write-off rows.
- `cogs_journal_inputs` *(022)* — per-period COGS audit (carry-forward source).
- `cash_balancing_days` + `cash_balancing_rows` + `cash_balancing_lines` + `cash_balancing_sources` + `cash_balancing_import_runs` *(000/002)* — daily cash-balancing pipeline.

### Card settlement
- `card_settlement_batches` + `card_settlement_events` *(000/008)* — Moneris/credit-card settlement matching.

### Payroll
- `payroll_employees` *(024)* — employee master (name, rate, vacation %, bank info).
- `payroll_runs` *(013/024)* — pay-period header. Status `draft`/`draft_confirmed`/`submitted`/`approved`/`posted`/`voided`.
- `payroll_run_lines` *(024)* — per-employee gross/deductions/net.
- `payroll_bank_withdrawals` *(024)* — scheduled CRA + payroll bank pulls.
- `payroll_run_events` *(013)* — workflow audit.
- `payroll_batches` *(000)* — legacy.

### Direct vendor AP (non-HH)
- `direct_vendor_ap_invoices` + `direct_vendor_ap_invoice_events` *(007)* — manual vendor AP tracking.

### Depreciation + accruals
- `fixed_assets` + `depreciation_schedules` + `depreciation_journal_lines` *(017)*.
- `accrual_templates` + `accrual_journal_lines` *(018)*.

### Auto-match runner
- `auto_match_runs` *(012)* — post-import auto-match runner audit.

### Invoice audit trail (most recent)
- `invoice_documents` *(027)* — uploaded PDFs (hh_ap + outside_vendor). `status` ∈ {`unmatched`, `matched`, `posted_to_ap`, `deleted`}. Unique `(entity_code, source_hash)` partial.
- `invoice_journal_links` *(027)* — links to `journal_batches`, `bank_transactions`, or `hh_ap_invoices`. CHECK constraint requires at least one.

### Document staging + audit
- `source_files` *(000)*, `normalized_documents` *(000)*, `document_lines` *(000)* — early document-staging pipeline (not all flows use it).
- `audit_log` *(000)* — generic write-trail.
- `posting_rules` *(000)*, `account_mapping_rules` *(000)*, `suggested_entries` + `suggested_entry_lines` *(000)*, `rule_runs` *(000)* — older rule-based posting that pre-dates `bank_auto_journal`.

### QuickBooks
- `quickbooks_connections` *(001)*, `quickbooks_sync_runs` *(001)*, `quickbooks_transactions` *(000)* — OAuth + sync state.

### Misc
- `exception_queue` *(000)* — generic error queue.
- `recurring_month_end_rules` *(000)*, `ecommerce_payout_cycles` *(000)*, `vendors` *(000)* — partially-used scaffolding.

---

## 7. Environment variables

### Backend (`backend/.env`, read by `backend/app/config.py`)

| Var | Required | Used for |
|---|---|---|
| `DATABASE_URL` | yes | SQLAlchemy connection string (`postgresql+psycopg://…`). |
| `APP_ENV` | no | `development` / `production`. Cosmetic. |
| `APP_BASE_URL` | no | Self-URL for QBO redirect, defaults to `http://localhost:8000`. |
| `QBO_CLIENT_ID` | yes | Intuit app ID. |
| `QBO_CLIENT_SECRET` | yes | Intuit app secret. |
| `QBO_REDIRECT_URI` | yes | Intuit OAuth callback URL. |
| `QBO_SCOPE`, `QBO_AUTH_URL`, `QBO_TOKEN_URL`, `QBO_API_BASE_URL`, `QBO_MINOR_VERSION` | no | Have working defaults. |
| `DEFAULT_ENTITY_CODE` | no | Defaults to `1877-8`. |
| `JWT_SECRET` | yes | Legacy JWT signing key. Min 32 chars. Required even under Clerk. |
| `JWT_ALGORITHM`, `JWT_EXPIRY_HOURS` | no | Have defaults. |
| `GOOGLE_SHEETS_SERVICE_ACCOUNT_EMAIL`, `GOOGLE_SHEETS_PRIVATE_KEY`, `GOOGLE_SHEETS_SPREADSHEET_ID` | no (legacy) | Cash-balancing import source. Default `replace_me` no-ops gracefully. |
| `CASH_BALANCING_LOOKBACK_DAYS` | no | Defaults to 56. |
| `ANTHROPIC_API_KEY` | no | Layer-3 LLM bank classifier. Unset → skip Claude fallback. |
| `CLERK_SECRET_KEY` | no (yes when `USE_CLERK_AUTH=true`) | Currently unused on the auth path (PyJWT verifies via JWKS) — set for future backend-initiated Clerk REST calls. |
| `CLERK_PUBLISHABLE_KEY` | yes when `USE_CLERK_AUTH=true` | JWKS URL is derived from this. |
| `CLERK_WEBHOOK_SECRET` | yes when Clerk webhook is registered | svix signature verification. |
| `CLERK_JWKS_URL` | no | Optional override of the derived JWKS URL. |
| `USE_CLERK_AUTH` | yes (set to `true` in prod) | Toggles auth dispatcher. |
| `STRIPE_SECRET_KEY` | yes when billing endpoints are exercised | Stripe API. |
| `STRIPE_WEBHOOK_SECRET` | yes when Stripe webhook is registered | `whsec_…`. |
| `STRIPE_STARTER_PRICE_ID`, `STRIPE_PROFESSIONAL_PRICE_ID`, `STRIPE_ADDITIONAL_STORE_PRICE_ID` | yes | Price IDs for Checkout. |
| `BOOKWIZE_APP_URL` | no | Defaults to `https://bookwize.ca`. Used for checkout return URL defaults. |

### Frontend (`frontend/.env.local`, see `.env.local.example`)

| Var | Required | Used for |
|---|---|---|
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | yes | Clerk client SDK. |
| `CLERK_SECRET_KEY` | yes | Clerk server SDK (server actions, route handlers). |
| `NEXT_PUBLIC_CLERK_SIGN_IN_URL`, `NEXT_PUBLIC_CLERK_SIGN_UP_URL`, `NEXT_PUBLIC_CLERK_AFTER_SIGN_IN_URL`, `NEXT_PUBLIC_CLERK_AFTER_SIGN_UP_URL` | yes | Routing. |
| `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY` | yes | If Stripe Elements is added later. (Not currently used — Checkout redirect only.) |
| `STRIPE_SECRET_KEY` | optional | Only needed if the frontend ever calls Stripe directly. Today the backend owns Stripe. |
| `STRIPE_WEBHOOK_SECRET`, `STRIPE_STARTER_PRICE_ID`, `STRIPE_PROFESSIONAL_PRICE_ID`, `STRIPE_ADDITIONAL_STORE_PRICE_ID` | no on frontend | Live on backend. Mirror only if needed for tooling. |
| `NEXT_PUBLIC_API_URL` | yes | Defaults to `https://hhbookeeping2.onrender.com`. Axios client base URL. |
| `RESEND_API_KEY` | no | Reserved for future transactional email. Not yet wired. |
| `EMAIL_FROM` | no | Defaults to `hello@bookwize.ca`. Reserved. |
| `NEXT_PUBLIC_APP_URL` | yes | Defaults to `https://bookwize.ca`. Used for checkout return URLs, OG metadata. |

---

## 8. Render services

- `hhbookeeping2` — Python web service, runs the FastAPI app. Render auto-detects `requirements.txt` + a Python `start command`. Renders pulls `main` on push.
- `bookwize-web` — Node web service for the Next.js frontend. Root directory `frontend/`. Build `npm install && npm run build`, start `npm start`. Node 20.x.
- Postgres — Basic tier. Connection string is the `DATABASE_URL` env var on both services (SQLAlchemy uses `postgresql+psycopg://` shape; psql wants plain `postgresql://`).

CORS allow-list (in `backend/app/main.py`):
```
http://localhost:3000
https://bookwize.ca
https://bookwize.onrender.com
https://bookwize-frontend.onrender.com
```

Add new origins here before pointing them at the API. The frontend currently lives at `bookwize-frontend.onrender.com` (Render default subdomain). When you attach `bookwize.ca` as a custom domain, both work.

---

## 9. Webhooks to register

- **Clerk → `POST /api/webhooks/clerk`** — events: `user.created`, `user.updated`, `user.deleted`, `organizationMembership.created`, `organizationMembership.updated`, `organizationMembership.deleted`, `organization.created`, `organization.updated`. Signing secret in `CLERK_WEBHOOK_SECRET`.
- **Stripe → `POST /api/webhooks/stripe`** — events: `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`. Signing secret in `STRIPE_WEBHOOK_SECRET`.

---

## 10. Known bugs + incomplete features

### Production-blocking when Clerk path is on
*(All resolved as of `77fa779` — listed here for the runbook.)*
- Migration 027 has been applied. Migration 026 too. Migration 025 too. No outstanding DB changes.
- Stripe webhook endpoint exists but Stripe webhook **must be registered in the Stripe dashboard** before checkout payments will sync back to `billing_subscriptions`. Until then, `GET /api/billing/subscription` returns empty.

### Auth-surface gaps (write endpoints with no `require_role`)
The following write endpoints lack a `require_role` dependency. Anyone with a session can call them (and `enforce_entity_code` only fires when explicitly invoked, which these don't do). Treat as security gaps to plug:
- `routes/cash_balancing.py` — `POST /days`, `POST /days/{id}/post-journal`
- `routes/hh_ap_overrides.py` — all writes
- `routes/sync.py` — `POST /sync`
- `routes/qbo_bank_sync.py` — write endpoints (`/runs/{id}/match`, `/unmatch`, `/set-review-status`) are bookkeeper-required but have **no `enforce_entity_code`** — entity_code is inferred from `transaction_id`, which the caller picks. Add an entity-match check in the handler.

### Wiring gaps
- `routes/bank_review.py` exists on disk but is **not included in `main.py`** (no `app.include_router(...)` call). The frontend uses `qbo_bank_sync` instead. Either wire `bank_review.py` or delete it.
- The legacy `users` / `user_entity_roles` pair is still consulted by `services_auth.require_role` under `USE_CLERK_AUTH=false`. If you ship Clerk-only, those tables can stop being maintained.

### Backend endpoints still TODO (frontend uses mocks)
- `/api/reports/income-statement`, `/api/reports/balance-sheet`, `/api/reports/trial-balance-as-of` — frontend has the UI but pulls from `lib/api/reports.ts` mocks.
- `/api/admin/dealers`, `/api/admin/revenue`, `/api/admin/users/search`, `/api/admin/entity-org-link` — admin portal mocks.
- `/api/documents` (unified) — frontend aggregates from per-module endpoints client-side.
- `/api/documents/{id}/file` — PDF source files are not archived on the server; no viewer.
- `/api/accounts` (chart admin) — `/settings/accounts` is placeholder content.
- `/api/me/notifications` — `/settings/notifications` toggle state lives only in component memory.
- `/api/payroll/cra-remittance` — `/payroll` CRA tab shows explanatory copy only.
- `/api/vendors` (vendor master) — `/ap` other-vendor balance view is explanatory only.
- Live general-ledger / live balance — both depend on dealer GL exports today; no continuously-maintained app ledger.

### Domain features paused
- **Feb 2026 close.** Last working state: `$74,980` absolute variance, 67/105 accounts tied. Held at commit `069b12e` (pre-Clerk-migration). Open items: add account `2320` to QBO chart, build Spencer Visa importer, split vacation_payable + payroll_fees out of `6120` in the register parser, sales-tax filing JE module, decide on HH AP 1120/2020 routing alignment. **Not touched by any of the recent frontend / billing / invoice work.**
- **No automated tests** beyond `backend/tests/test_services_auth_clerk.py` (18 cases, all passing). No frontend tests at all.
- **No file storage.** Every upload endpoint parses + discards. PDF re-view from a `/documents/{id}/file` endpoint is unbuildable until S3 / Render disk is wired.

### Process / repo gotchas
- The repo's `.gitignore` includes `.env.*` which catches `.env.local.example`. The frontend example file was force-added via `git add -f` once (commit `80df767`); future updates need `-f` too, or amend the rule.
- Several migrations declare `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"` redundantly. Harmless but noisy. `pgcrypto` is also installed (from migration 002).
- The bookkeeping memory describes data at points in time. Trust `git log` / current code over memory for anything time-sensitive.
- `services_auth.py` still constructs the legacy JWT bearer scheme at module-import time. Swagger renders both `BearerAuth` (legacy) and `ClerkBearer` (Clerk) in `/docs` security schemes. Cosmetic; not a bug.

### Watch items
- The cryptography pin sits at `44.0.1`. The Clerk SDK (if you ever add it back) wants `>=45`. If you reintroduce the SDK you'll need to bump cryptography + httpx + pydantic — meaningful blast radius (see `frontend/FRONTEND.md` for the avoided path).
- `MultiFileUpload` defaults its record-count extraction to a few common keys (`record_count`, `inserted`, `parsed_rows`, …). Endpoints that return record counts under a non-matching key will show no record count in the summary card. Customizable per call site via `extractRecordCount` prop.
- The invoice-matching hook in `services_cogs.py` runs after each COGS journal_line insert. If a future module adds journal-line writes elsewhere (payroll, accruals, depreciation already have their own writes), wire the same `auto_match_for_journal_line` hook into those if you want symmetric coverage. The hook is failure-isolated by design.

---

## 11. Useful commands

```powershell
# Apply a migration locally (replace 0NN)
$env:DATABASE_URL = (Get-Content backend/.env | Select-String '^DATABASE_URL=' | ForEach-Object { $_ -replace '^DATABASE_URL=', '' -replace 'postgresql\+psycopg', 'postgresql' })
psql $env:DATABASE_URL -f backend/sql/0NN_*.sql

# Backend dev
cd backend
.venv/Scripts/python -m uvicorn app.main:app --reload --port 8000

# Backend tests
cd backend
.venv/Scripts/python -m unittest tests.test_services_auth_clerk -v

# Frontend dev
cd frontend
npm install --legacy-peer-deps    # (corporate TLS issue documented in commits — use --legacy-peer-deps if normal install hangs)
npm run dev

# Frontend typecheck
cd frontend
npm run typecheck

# Commit + push
git add -A
git commit -m "..."
git push origin main
```

---

## 12. Commit history (recent shipping)

```
77fa779  feat: invoice audit trail (migration 027, matching service,
         routes, AP module updates, unmatched queue, transaction
         drill-down, dashboard alert)
3ce9acd  feat: MultiFileUpload + all 14 upload endpoints wired
ee064e8  fix: dashboard fetches most-recent period (no more today-default
         500 on entities without same-day periods)
38521ec  fix: accept short-form Clerk org role 'admin' alongside 'org:admin'
fe6028d  fix: null-safe .toFixed() on dashboard + review-queue
12cb5e4  fix: add bookwize-frontend.onrender.com to CORS allowed origins
80df767  feat: full Phase 4 frontend build + Clerk/Stripe/billing backend
         (131 files, ~11k LOC; SQL migration 026)
2d1062e  feat: migrate auth to Clerk with USE_CLERK_AUTH flag (PyJWT+JWKS,
         not the SDK; 5-tier role hierarchy preserved; SQL migration 025)
069b12e  fix: add 7xxx to TB compare flip prefixes  ← Feb 2026 close
         pause point
```

End of summary.
