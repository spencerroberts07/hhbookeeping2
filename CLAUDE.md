# BookWize — Claude Code Standing Instructions

These rules apply to every Claude Code session in this repo. They override
generic defaults.

## MANDATORY BEFORE EVERY COMMIT

1. From `frontend/`: `npx tsc --noEmit --skipLibCheck`
2. Fix **every** TypeScript error before pushing.
3. Zero tolerance — no "Render will catch it" exceptions.
4. If no node binary is available on this machine: install nvm + Node v20 first.
   - The pre-commit hook in `frontend/.husky/pre-commit` already enforces (1)
     and (2) — activated by `cd frontend && npm install --legacy-peer-deps`.

## TECH STACK

- **Backend**: FastAPI (Python). Routes live in `backend/app/routes/`
  (not `routers/`). Config is `backend/app/config.py` (not
  `core/config.py`). R2 helper is `backend/app/services_storage.py`.
- **Frontend**: Next.js 15.3.2 + Tailwind + shadcn/ui + Clerk v6.
- **DB**: PostgreSQL on Render. Schema migrations live in
  `backend/sql/` numbered sequentially (next: check the highest
  number in that directory).

## CRITICAL SCHEMA RULES

These have bitten previous sessions — confirm the actual column name
in the live DB before writing SQL.

- `bank_transactions.review_status` (not `status`)
- `journal_batches.total_debits` / `total_credits` (plural) +
  `source_module` (not `batch_type`)
- `accounting_periods` uses `entity_id` UUID (not `entity_code`)
- Closed accounting period status is `'closed_locked'` (not `'closed'`)
- `vendor_classification_memory.confidence` is 0-1 scale (not 0-100)
- `assistant_entity_memory.entity_code` TEXT (not `entity_id` UUID)
- `payroll_run_lines.gross_pay` (not `gross_earnings`)
- `payroll_employees.biweekly_salary` (not `salary_biweekly`)
- `payroll_employees.federal_td1_claim_code` is an INTEGER claim
  code, not a dollar BPA amount.

## CODING RULES

- **Multi-tenancy** — every query scoped by `entity_id` UUID or
  `entity_code` TEXT. Never cross entity boundaries.
- **R2 storage** — never store file bytes in PostgreSQL. R2 only,
  store the object key in a `file_path` / `r2_object_key` column.
- **R2 is fail-tolerant** — DB write proceeds even if R2 upload
  fails. `storage_service.upload_file()` returns `None` on failure;
  callers store `None` in the key column and continue.
- **Never post to account 3900** (Opening Balance Equity) outside
  of explicit opening-balance journals.
- **Ask before any DELETE, DROP, or UPDATE on accounting data**
  (journal_lines, journal_batches, payroll_run_lines, posted
  hh_ap_* rows). Read-only investigations are always fine.
- **No mock data** — all values from real DB queries.
- **HH AP statement** is the source of truth for AP totals;
  parsed invoice variance is QA noise, not a blocker.

## MULTI-TENANCY KEYS

- `entity_id` (UUID) — used in most tables.
- `entity_code` (TEXT) — e.g. `'1877-8'`, used in some tables and
  on every URL query string.

Bridlewood Home Hardware:
- `entity_id = 0bab9284-68d9-4769-bfc6-4dac5bd1f5e4`
- `entity_code = 1877-8`

## CRA / PAYROLL

- 2026 CRA rate constants in `services_payroll_calc.py` currently
  carry 2025 values. See the `TODO_CRA_2026_RATES` block at the top
  of that file. **Do not change rates in feature commits** —
  rate-audit lands as its own commit after CRA T4127 verification.
- Bridlewood payroll BN: `753391010RP0001` (hardcoded as
  `PAYROLL_BUSINESS_NUMBER` in `routes/payroll.py` and
  `services_payroll_paystub.py`).
- Bi-weekly periods, Ontario province, fiscal year ends Sep 30
  (FY2026 = Oct 2025 – Sep 2026).
- YTD reset is **manual** via `POST /api/payroll/ytd/reset` with
  `confirm: true` (admin role) on Oct 1 each year.

## LOCAL DEV

- **Node**: v20 required (use `nvm use 20`).
- **npm install**: `--legacy-peer-deps` flag because of corporate TLS
  / peer-resolution quirks on this Windows machine.
- **Backend venv**: `backend/.venv/Scripts/python.exe` (Windows).
- **Render DB connection**: use the URL from `backend/.env`
  (DATABASE_URL). Reads are always fine; writes use the
  `Ask before any UPDATE` rule above.

## DEBUGGING PRINCIPLES

- The user's diagnosis can be wrong. If their stated cause doesn't
  match what the code shows, surface that before "fixing" the
  wrong thing.
- "Ask before DROP" applies on the create side too — don't silently
  introduce parallel tables that duplicate existing functionality.
- When in doubt, run the schema audit, read the live data, and
  report findings before writing code.
