-- Migration 022: Monthly COGS journal builder
--
-- Stores the inputs and outputs of build_cogs_journal() so the
-- bookkeeper can review/rebuild + so next period's
-- suggested_dating_reversal_amount can be auto-carried-forward
-- from the prior period's dating_new_amount.

CREATE TABLE IF NOT EXISTS cogs_journal_inputs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    pos_import_run_id UUID REFERENCES pos_import_runs(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    base_cogs NUMERIC(15, 2) NOT NULL,
    dating_new_amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
    dating_reversal_amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
    other_adjustment_amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
    other_adjustment_memo TEXT,
    net_5010_amount NUMERIC(15, 2) NOT NULL,
    net_1120_amount NUMERIC(15, 2) NOT NULL,
    net_2030_dating_amount NUMERIC(15, 2) NOT NULL,
    sanity_check_vs_gl_variance NUMERIC(15, 2),
    sanity_check_vs_inventory_movement NUMERIC(15, 2),
    sanity_check_warning BOOLEAN NOT NULL DEFAULT FALSE,
    sanity_check_notes TEXT,
    journal_batch_id UUID REFERENCES journal_batches(id),
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_cogs_journal_inputs_entity_period_end
    ON cogs_journal_inputs (entity_id, period_end DESC);
