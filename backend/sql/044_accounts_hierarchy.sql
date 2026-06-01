-- Migration 044: chart-of-accounts hierarchy fields.
--
-- The income statement needs to render accounts under their parent
-- group headers ("6000 Occupancy Costs" containing 6010-6050) with
-- subtotals, matching QBO's P&L layout. This requires storing the
-- QBO Account entity's hierarchy data, which the existing CoA sync
-- (services.py:import_chart_of_accounts) currently discards.
--
-- Additive only — no renames or drops.
--
-- After deploy, dealers re-run the CoA sync from
-- /settings/integrations to populate the new columns. Reports keep
-- working unchanged until they're updated to use parent_code.

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS parent_code TEXT,
    ADD COLUMN IF NOT EXISTS fully_qualified_name TEXT,
    ADD COLUMN IF NOT EXISTS account_type TEXT,
    ADD COLUMN IF NOT EXISTS account_subtype TEXT,
    ADD COLUMN IF NOT EXISTS is_sub_account BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS quickbooks_parent_id TEXT;

CREATE INDEX IF NOT EXISTS accounts_parent_code_idx
    ON accounts (entity_id, parent_code)
    WHERE parent_code IS NOT NULL;
