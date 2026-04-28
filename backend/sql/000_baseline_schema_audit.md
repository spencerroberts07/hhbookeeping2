# Baseline Schema Audit (live database vs. migration files)

**Generated:** 2026-04-28
**Live database:** Render Postgres (Ohio), PostgreSQL 18.3
**Method:** `pg_dump --schema-only` of the live DB compared against the
ordered application of `backend/schema.sql` + `backend/sql/001..009`.
**Bottom line:** the live schema is internally consistent and *correct* for
the running app, but several migration files are not safe to re-run on a
fresh DB without the fixes called out below. Migration `000_baseline_schema.sql`
in this same folder is a clean snapshot for emergency reproduction; it must
NOT be applied to the existing DB.

---

## Critical findings

### 1. Two conflicting `005_*` migration files

The `backend/sql/` directory contains **two files numbered 005**:

| File | What it creates / alters | Column convention |
| ---- | ------------------------ | ----------------- |
| `005_bank_review_and_matching.sql` | `bank_transaction_review_events` (NEW), adds `target_table_name`, `matched_amount`, `active`, `released_*` columns to `bank_transaction_matches` via `ALTER ... ADD COLUMN IF NOT EXISTS`, plus the `ux_bank_transaction_matches_active_one_per_txn` unique index | **CORRECT — matches live DB and live application code** |
| `005_bank_transaction_review_layer.sql` | Re-creates `bank_transactions` and `bank_transaction_matches` and `bank_transaction_review_events` via `CREATE TABLE IF NOT EXISTS` | **WRONG / OBSOLETE — uses `target_table`, `amount_matched`, `match_status='active'`** |

Because both used `CREATE TABLE IF NOT EXISTS` and the tables already
existed, the second file's `CREATE TABLE` statements were silent no-ops on
the live DB. Only the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements
in the first file actually changed structure.

**Net effect on the live DB:** The good schema landed. But on a *fresh*
install, whichever 005 file the operator runs first will determine the
schema, and `005_bank_transaction_review_layer.sql` produces a structurally
different (and broken) table.

**Action taken in this audit:**
- `005_bank_review_and_matching.sql` is kept as the canonical 005.
- `005_bank_transaction_review_layer.sql` has been replaced with a
  deprecation stub that documents the conflict and contains no DDL. It is
  preserved at the same numeric position so any historical references
  still resolve to a file.

### 2. `006_hh_ap_remittance_bank_matching.sql` referenced wrong column names

The original `006_hh_ap_remittance_bank_matching.sql` defined three indexes
filtered on `target_table = 'hh_ap_remittances' AND match_status = 'active'`.
On the live DB:

- `bank_transaction_matches.target_table` does not exist (the column is
  `target_table_name`).
- `bank_transaction_matches.match_status` *does* exist (legacy column from
  migration 004) but its default value is `'matched'`, not `'active'`.

The two predicate indexes from 006 (`uq_bank_transaction_matches_active_hh_ap_remittance_target`
and `uq_bank_transaction_matches_active_hh_ap_remittance_bank_tx`) are
**absent from the live DB** — confirming that those `CREATE INDEX` statements
either failed or were rolled back when 006 ran. The live DB instead has
correctly-named replacement indexes (`idx_bank_transaction_matches_hh_remittance_active`,
`idx_bank_transaction_matches_hh_remittance_lookup`) that use
`target_table_name` and `active = TRUE`.

**Action taken in this audit:** `006_hh_ap_remittance_bank_matching.sql` has
been rewritten to use the correct column names so the file is once again
safe to re-apply on a fresh DB.

### 3. `bank_transaction_matches` has *three* vintages of columns

The live `bank_transaction_matches` table has a confusing column inventory
because three separate migration files added different columns over time:

| Source migration | Columns it added |
| ---------------- | ---------------- |
| `004_qbo_bank_sync.sql` | `matched_table`, `matched_record_id` (UUID), `match_status` (default `'matched'`, NOT NULL), `matched_amount`, `created_by`, `raw_json` |
| `005_bank_transaction_review_layer.sql` | (no-op on the live DB; only ran ALTERs that the next file also ran) |
| `005_bank_review_and_matching.sql` | `target_table_name`, `target_record_id` (TEXT), `note`, `active` (default TRUE, NOT NULL), `released_by`, `released_at`, `released_note` |
| At some later point, manually | `target_label`, `payload_json` |

The application code uses **only the new schema** (`target_table_name`,
`target_record_id`, `active`). The old schema columns
(`matched_table`, `matched_record_id`, `match_status`) are dead weight
but cannot be dropped without checking that no out-of-tree consumer still
reads them.

**Action recommended (not done in this audit):** A future migration
`014_drop_legacy_bank_match_columns.sql` should:
1. Verify zero queries still reference the legacy columns (grep + production
   query log).
2. `ALTER TABLE bank_transaction_matches DROP COLUMN matched_table, DROP COLUMN matched_record_id, DROP COLUMN match_status;`
3. Drop the legacy index `idx_bank_transaction_matches_entity` which is keyed
   on `match_status`.

### 4. `payroll_batches` is unused dead weight

`backend/schema.sql` (the original bedrock schema) defines a
`payroll_batches` table with columns `pay_period_start`, `pay_period_end`,
`pay_date`, `gross_wages`, `deductions_total`, `employer_burden_total`,
`net_pay`, `source_file_id`, `raw_json`. The live DB has 0 rows, and the
table is referenced from no Python file — it has never been used.

Module 5 (Payroll Control) deliberately creates a new table named
`payroll_runs` with the workflow + bank-clearing columns the running app
actually needs, rather than retrofit the unused `payroll_batches`.

**Action recommended (not done in this audit):** A future migration
`015_drop_unused_payroll_batches.sql` should drop `payroll_batches` once
Module 5 has been live in production for at least one close cycle.

### 5. `accounting_periods.status` default is `'draft'`, not `'open'`

`backend/schema.sql` created `accounting_periods` with
`status TEXT NOT NULL DEFAULT 'draft'`. Module 2's spec asked for default
`'open'`. The migration `011_period_close.sql` uses
`ADD COLUMN IF NOT EXISTS` for the new close-related columns and **does
not change the existing default** (changing the default could surprise
existing rows / future inserts that don't specify status).

The application layer treats `'draft'` as equivalent to `'open'` via the
`effective_period_status()` helper in `services_period_close.py`. New
periods continue to be created with `'draft'`, then transition into the
new state machine when the user submits or closes.

### 6. There is no `.env` example file in the repo

`README.md` and `docs/quickbooks_setup.md` both reference
`backend/.env.example`, but no such file exists. New contributors have no
template to copy. The DB credentials, JWT secret, and QBO credentials all
live in `backend/.env` (which is — now — gitignored via the new
`.gitignore` at the repo root).

**Action recommended:** Add a `backend/.env.example` with placeholders
for `DATABASE_URL`, `JWT_SECRET`, `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET`,
`QBO_REDIRECT_URI`. (This audit deliberately does not add it because the
exact set of expected variables is changing as new modules land.)

---

## Authoritative column names for the critical tables

These are the column names callers must use (and that this audit
confirmed match the live DB):

### `bank_transactions`
- Match by: `entity_id`, `source_system`, `source_transaction_id` (unique
  together), or by `id`.
- Date filter: `transaction_date` (NOT NULL).
- Amount sign: `direction = 'inflow'` -> positive `amount`,
  `direction = 'outflow'` -> negative `amount`.
- CSV-imported rows use `source_system = 'statement_csv'` and have a
  `source_import_run_id` pointing to `bank_csv_import_runs`.

### `bank_transaction_matches`
- **Use:** `target_table_name` (TEXT), `target_record_id` (TEXT, NOT a UUID
  column — the application stores stringified UUIDs there),
  `matched_amount` (NUMERIC), `active` (BOOLEAN, default TRUE),
  `released_by`, `released_at`, `released_note`.
- **Do NOT use:** `matched_table`, `matched_record_id`, `match_status`
  (these are the old schema, retained but inert).
- The active match per bank txn is enforced by the partial unique index
  `ux_bank_transaction_matches_active_one_per_txn` on `(bank_transaction_id)`
  WHERE `active = TRUE`.

### `bank_transaction_review_events`
- `bank_transaction_id`, `entity_id`, `action`, `actor_email`,
  `from_review_status`, `to_review_status`, `note`, `payload_json`,
  `created_at`. No surprises.

### `accounting_periods`
- `status` (TEXT, NOT NULL, default `'draft'`). Treat `'draft'` as
  `'open'` for the close workflow (see `effective_period_status()`).
- After Module 2: `closed_at`, `closed_by`, `reopened_at`, `reopened_by`,
  `close_notes`, `reopen_notes` (all nullable).

### `journal_batches`
- Workflow column: `workflow_status` (NOT `status` — `status` exists for
  legacy purposes).
- Locked when `workflow_status IN ('approved_to_post','posted')` OR
  `locked_at IS NOT NULL`.

### `entities`
- Has `organization_id` referencing `organizations(id)`. Multi-tenancy is
  modelled at the entity level: a user gets a role on an `entity`, not
  on an `organization` directly. The `organization` exists only to group
  entities for billing / tier purposes.

---

## What the original migrations would do on a fresh DB

If you stood up a brand-new Postgres and ran `backend/schema.sql` + `001..009`
in order, **before** the fixes in this audit, the resulting schema would
differ from production in two ways:

1. `bank_transaction_matches` column inventory would depend on which `005_*`
   file ran first (alphabetical: `005_bank_review_and_matching.sql` would
   run first because of the leading 'b' in its name vs. 't' — *if* the
   loader sorts within each numeric prefix; otherwise undefined).
2. The three predicate indexes from the original `006` would either fail
   to create or create against the wrong column names.

After the fixes in this audit, both `005_*` files plus `006_*` are safe to
re-apply on either an empty DB or the live DB (all DDL is `IF NOT EXISTS`-
guarded and references actual live column names).

---

## Files modified by this audit

- `backend/sql/005_bank_transaction_review_layer.sql` — replaced with a
  deprecation note (no DDL).
- `backend/sql/006_hh_ap_remittance_bank_matching.sql` — corrected to use
  `target_table_name` and `active = TRUE` instead of the missing
  `target_table` and `match_status = 'active'`.
- `backend/sql/000_baseline_schema.sql` — NEW, captured by `pg_dump
  --schema-only` against the live DB on 2026-04-28. Marked
  "DO NOT RUN ON EXISTING DB — baseline snapshot only".
- `backend/sql/000_baseline_schema_audit.md` — this file.
- `.gitignore` — NEW (root). Protects `backend/.env` from accidental
  commit.
