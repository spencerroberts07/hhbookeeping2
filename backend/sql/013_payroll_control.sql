-- Migration 013: Payroll control module
--
-- Tracks each payroll run's journal entry, source-deduction remittance,
-- and bank clearing. NOT a payroll processor — just the control layer
-- that records what happened so the bookkeeper can reconcile.
--
-- Note: there is an unused payroll_batches table from the original
-- backend/schema.sql. It is intentionally left alone here. See
-- 000_baseline_schema_audit.md for the rationale and the planned
-- 015_drop_unused_payroll_batches.sql cleanup.
--
-- Workflow:
--   draft -> reviewed -> approved -> posted
--   bank_cleared and remittance_cleared flip TRUE independently when the
--   bookkeeper marks the corresponding bank withdrawals as matched.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS payroll_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),

    payroll_reference TEXT NOT NULL,
    pay_period_start DATE NOT NULL,
    pay_period_end DATE NOT NULL,
    pay_date DATE NOT NULL,
    processor TEXT,

    gross_wages NUMERIC(15,2),
    employer_cpp NUMERIC(15,2),
    employer_ei NUMERIC(15,2),
    employer_benefits NUMERIC(15,2),
    employee_cpp NUMERIC(15,2),
    employee_ei NUMERIC(15,2),
    employee_tax NUMERIC(15,2),
    employee_benefits NUMERIC(15,2),
    net_pay NUMERIC(15,2),
    remittance_amount NUMERIC(15,2),
    total_employer_cost NUMERIC(15,2),

    status TEXT NOT NULL DEFAULT 'draft',
    workflow_status TEXT NOT NULL DEFAULT 'draft',

    bank_cleared BOOLEAN NOT NULL DEFAULT FALSE,
    bank_transaction_id UUID REFERENCES bank_transactions(id) ON DELETE SET NULL,
    bank_cleared_at TIMESTAMPTZ,
    bank_cleared_by TEXT,

    remittance_cleared BOOLEAN NOT NULL DEFAULT FALSE,
    remittance_bank_transaction_id UUID REFERENCES bank_transactions(id) ON DELETE SET NULL,
    remittance_cleared_at TIMESTAMPTZ,
    remittance_cleared_by TEXT,

    notes TEXT,
    raw_import_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT payroll_runs_entity_reference_unique
        UNIQUE (entity_id, payroll_reference),
    CONSTRAINT payroll_runs_status_chk
        CHECK (status IN ('draft','reviewed','approved','posted')),
    CONSTRAINT payroll_runs_workflow_status_chk
        CHECK (workflow_status IN
               ('draft','submitted','approved','posted','rejected','reopened'))
);

CREATE INDEX IF NOT EXISTS idx_payroll_runs_entity_pay_date
    ON payroll_runs (entity_id, pay_date DESC);

CREATE INDEX IF NOT EXISTS idx_payroll_runs_entity_status
    ON payroll_runs (entity_id, status, pay_date DESC);

CREATE INDEX IF NOT EXISTS idx_payroll_runs_entity_uncleared
    ON payroll_runs (entity_id, bank_cleared, remittance_cleared, pay_date DESC);

CREATE TABLE IF NOT EXISTS payroll_run_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
        -- created, updated, submitted, approved, rejected, reopened,
        -- bank_cleared, bank_uncleared, remittance_cleared,
        -- remittance_uncleared
    from_status TEXT,
    to_status TEXT,
    actor_email TEXT,
    notes TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payroll_run_events_run_created
    ON payroll_run_events (payroll_run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_payroll_run_events_entity_created
    ON payroll_run_events (entity_id, created_at DESC);
