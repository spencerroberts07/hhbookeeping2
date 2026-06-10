-- Migration 054: Add created_via audit marker to accounting_periods
--
-- Additive only — ADD COLUMN IF NOT EXISTS, no data changes.
--
-- Rows created by the cash-balancing auto-create path are stamped
-- created_via='auto:cash_balancing_sync'. Rows created manually via the
-- onboarding wizard, the period-close route, or historical imports leave
-- this column NULL so the distinction is visible in the DB without
-- needing to JOIN to audit logs.
--
-- Safe to re-run.

ALTER TABLE accounting_periods
    ADD COLUMN IF NOT EXISTS created_via TEXT;
