-- Migration 018: Monthly Accruals
--
-- Captures recurring accrual templates and the journal lines posted
-- against them each month. The build_accrual_journal service writes
-- one balanced journal_batch per period containing the selected
-- templates' lines.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS accrual_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accrual_code TEXT NOT NULL,
    description TEXT NOT NULL,
    debit_account TEXT NOT NULL,
    credit_account TEXT NOT NULL,
    default_amount DECIMAL(15,2),
    frequency TEXT DEFAULT 'monthly',
    is_active BOOLEAN DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, accrual_code)
);

CREATE INDEX IF NOT EXISTS idx_accrual_templates_entity_active
    ON accrual_templates (entity_id, is_active);

CREATE TABLE IF NOT EXISTS accrual_journal_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    accrual_template_id UUID REFERENCES accrual_templates(id) ON DELETE CASCADE,
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    period_end DATE NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    reversal_period_end DATE,
    is_reversed BOOLEAN DEFAULT FALSE,
    actor_email TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, accrual_template_id, period_end)
);

CREATE INDEX IF NOT EXISTS idx_accrual_jl_entity_period
    ON accrual_journal_lines (entity_id, period_end);
