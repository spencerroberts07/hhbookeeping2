-- Migration 015: AR adjustment report (5th POS month-end report type)
--
-- Adds 'ar_adjustment' to the pos_import_runs.report_type CHECK
-- constraint and creates the line-level table that holds parsed AR
-- transaction-list rows. The journal builder reads this table for a
-- given pos_import_runs row and writes a balanced journal_batches:
--
--   Dr 6550   Bad Debt / AR Adjustment expense
--   Cr 1085   Accounts Receivable (House Accounts)
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE pos_import_runs
    DROP CONSTRAINT IF EXISTS pos_import_runs_report_type_chk;

ALTER TABLE pos_import_runs
    ADD CONSTRAINT pos_import_runs_report_type_chk
    CHECK (report_type IN (
        'inventory_adjustment',
        'pos_financial',
        'inventory_value',
        'aged_ar',
        'ar_adjustment'
    ));


CREATE TABLE IF NOT EXISTS ar_adjustment_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES pos_import_runs(id) ON DELETE CASCADE,
    accounting_period_id UUID REFERENCES accounting_periods(id),

    transaction_date DATE,
    employee_id TEXT,
    transaction_type TEXT,
    total_amount NUMERIC(15,2),
    reference_number TEXT,
    job_id TEXT,
    adjust_date DATE,
    reason TEXT,
    customer_number TEXT,
    customer_name TEXT,
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ar_adj_lines_run
    ON ar_adjustment_lines (import_run_id);

CREATE INDEX IF NOT EXISTS idx_ar_adj_lines_entity_date
    ON ar_adjustment_lines (entity_id, transaction_date);

CREATE INDEX IF NOT EXISTS idx_ar_adj_lines_journal_batch
    ON ar_adjustment_lines (journal_batch_id)
    WHERE journal_batch_id IS NOT NULL;
