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

## SCHEMA ADDITIONS (migration 044)

The `accounts` table now carries QBO hierarchy fields:

- `parent_code` TEXT — QBO parent account code (resolved from
  ParentRef via the two-pass CoA sync).
- `fully_qualified_name` TEXT — e.g. `"Occupancy Costs:Rent"`.
- `account_type` TEXT — QBO AccountType (e.g. `"Expense"`,
  `"Income"`, `"Other Income"`).
- `account_subtype` TEXT.
- `is_sub_account` BOOLEAN.
- `quickbooks_parent_id` TEXT — raw QBO `ParentRef.value`.

## GL IMPORT PARSER

Parser priority order in `parse_gl_file()`:

1. `_try_qbo_gl_parser` — handles QBO's specific GL CSV format
   (Distribution account column, DD/MM/YYYY dates, single Amount
   column, 4-row preamble). **ALWAYS try first.**
2. `_try_fallback_gl_parser` — generic CSV fallback.
3. Claude parser — last resort only, caps at 200k chars.

QBO GL sign convention (credit-normal by account prefix):

- Debit-normal: `1xxx` (assets), `5xxx` (COGS), `6xxx` (expense).
- Credit-normal: `2xxx` (liabilities), `3xxx` (equity),
  `4xxx` (revenue), `7xxx` (other income).
- A positive QBO `Amount` = an increase for that account type.
- Parser test: `credit_normal = first_digit in ('2','3','4','7')`.

## OPENING BALANCES

Opening-balance TB rules:

- ONLY balance sheet accounts (`1xxx`, `2xxx`, `3xxx`).
- Income statement accounts (`4xxx`–`7xxx`) are skipped with a
  warning — they should be `$0` on a post-close TB.
- If `4xxx`–`7xxx` are non-zero → the TB is pre-close → reject
  with a specific error message.
- Source of truth: the **Sep 30, 2025 post-close TB** for
  Bridlewood (1,511,639 dr/cr, 36 lines). Batch `f07eac9c` is
  already voided; the current active batch from May 2026 is the
  correct one.

## BALANCE SHEET CUTOVER MODEL

The balance sheet uses a **CUTOVER DATE** model:

- Cutover date = `period_end` of the most recent active
  `opening_balance` batch (Sep 30, 2025 for Bridlewood).
- Balance sheet query:
  - **Part A:** the `opening_balance` batch (Sep 2025) only.
  - **Part B:** ALL batch activity where `period_start > cutover`.
- Pre-cutover `historical_import` batches are EXCLUDED from the
  balance sheet but NOT from the income statement.
- Income statement uses the full GL history for all periods
  (no cutover filter).
- Without an `opening_balance` batch: **legacy mode** — all
  batches included (works for fresh entities).

## INCOME STATEMENT STRUCTURE

Section mapping by account code prefix:

- INCOME: `4xxx` (credit − debit).
- COGS: `5xxx` (debit − credit).
- GROSS PROFIT: Income − COGS.
- OPERATING EXPENSES: `6xxx` (debit − credit).
- OTHER INCOME: `7xxx` (credit − debit) — only renders when
  `7xxx` has activity.
- PROFIT / NET INCOME: GP − OpEx + Other Income.

`7xxx` accounts are OTHER INCOME (credit-normal):

- `7000` DGIP Forgiveness: `$3,333.33/mo` Cr 7000 / Dr 2510.
- `7010` Interest Income.
- The net income formula MUST include Other Income.

Grouped format: uses `parent_code` from the `accounts` table to
group sub-accounts under parent headers with subtotals. Requires
the CoA sync to have been run to populate `parent_code`.

## PERIOD STATUS (additions)

Force-close of historical GL periods:

- Historical periods with only `historical_import` batches can be
  force-closed via direct SQL `UPDATE`.
- `close_notes` should state: `'Force-closed based on imported GL
  data (historical_import). GL is source of truth for these
  months.'`
- `closed_by`: `spencer7roberts@gmail.com`.

Bridlewood period status (as of May 2026):

- Sep 2023 → Sep 2025: `draft` (historical reference only,
  excluded from balance sheet via the cutover model).
- Oct 2025, Nov 2025, Dec 2025, Jan 2026: `closed_locked`
  (force-closed from GL data).
- Feb 2026: `closed_locked`
  (period_id: `248ca2ae-530c-4d7d-bd61-9b3c4ecb77c4`).
- Mar, Apr, May 2026: `draft` (pending close).

## NEW API ROUTES

`backend/app/routes/data_import.py`:

- `GET /api/data-import/chart-sync-status`
- `POST /api/onboarding/gl-history/preview` (no-write)

`backend/app/routes/reports.py` additions:

- `GET /api/reports/income-statement`
- `GET /api/reports/income-statement/periods`
- `GET /api/reports/balance-sheet` (existing, updated)

`backend/app/routes/dashboard.py` additions:

- `GET /api/dashboard/sales-history`
- `GET /api/dashboard/gl-cash-balance`

## CoA SYNC (updated)

`import_chart_of_accounts` is now **TWO-PASS**:

- **Pass 1:** Insert/update all accounts with QBO fields including
  `FullyQualifiedName`, `AccountType`, `SubAccount`, and raw
  `ParentRef.value` (`quickbooks_parent_id`).
- **Pass 2:** Resolve `parent_code` by looking up each account's
  `quickbooks_parent_id` against the QBO `Id` → `AcctNum` map.

After adding new dealers or running migrations, the CoA sync must
be re-run to populate hierarchy fields.

## KNOWN DATA DECISIONS

- **3900 Opening Balance Equity:** the ONLY account where variance
  plugs (≤ `$1.00`) are posted during opening-TB import. No other
  corrections to 3900, ever.
- **9999 Suspense:** should always be `$0` after clean imports.
  Any non-zero `9999` balance indicates a parser sign error or an
  unmatched GL line — investigate immediately.
- **Bill 013126** (Lyndhurst Lumber, Jan 2026, 5010): `$593.25`
  known discrepancy between QBO GL and QBO P&L. BookWize correctly
  reflects the GL. Investigate in QBO.
- **opening_balance_correction** (Feb 2026 batch, Dr 2020 / Cr
  2030 `$4,000`): needs a decision — post in QBO or void in
  BookWize.

## WORKFLOW RULES (additions)

1. Always run CoA sync BEFORE GL import — the `accounts` table
   must be populated for correct code matching.
2. The TB must be POST-CLOSE — income statement accounts `$0`.
3. The GL parser detects QBO format automatically — no manual
   format selection needed.
4. The balance sheet is accurate only after the opening TB is
   uploaded. Without it, the BS shows net movements only.
5. The income statement uses full GL history — all 29 months are
   valid for P&L comparisons regardless of cutover.
6. Re-importing GL is safe — idempotency is keyed on
   `(entity_id, period, source_module, batch_label)`. Labels are
   verified byte-for-byte stable against QBO exports for
   Bridlewood.
7. `7xxx` sign: always credit-normal. Historical batches before
   commit `2d4d58c` had `7xxx` on the wrong side — a backfill was
   run May 2026; all 47 batches corrected.

## PRICING MODEL

Two-tier SaaS model:

- **Starter:** `$199/mo` — bookkeeping only, no payroll.
- **Professional:** `$399/mo` — full platform including payroll.
- **Internal:** `$0` — owner accounts (Bridlewood).

Payroll disbursement:

- **CPA 005 EFT:** for dealers with commercial banking + EFT
  origination (like Bridlewood, already built).
- **Telpay** (deferred): for dealers on basic banking
  (TD EasyWeb, etc.) — PAD pull + disburse to employees.
- **Rotessa:** dealer subscription BILLING only (collection).
