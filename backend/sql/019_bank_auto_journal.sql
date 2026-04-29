-- Migration 019: Bank transaction auto-journal builder
--
-- Reads bank_transactions for a period, runs each row against the
-- bank_transaction_rules table, and builds a single journal_batch
-- (Dr expense / Cr 1020) for every matched transaction. Unmatched
-- and skip-by-design (HOME HARDWARE, card settlements) rows are
-- recorded in bank_auto_journal_lines with their reason so the
-- bookkeeper can review.
--
-- Idempotency: UNIQUE(entity_id, bank_transaction_id) on
-- bank_auto_journal_lines prevents the same transaction being
-- auto-journaled twice.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bank_transaction_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    rule_code TEXT NOT NULL,
    description_pattern TEXT NOT NULL,
    match_type TEXT NOT NULL DEFAULT 'contains',
        -- 'contains' | 'starts_with' | 'exact' | 'regex'
    debit_account TEXT,
    credit_account TEXT,
    transaction_type TEXT,
        -- 'expense' | 'liability_payment' | 'loan_payment' | 'payroll'
        -- | 'card_fee' | 'bank_charge' | 'income' | 'skip'
    requires_split BOOLEAN NOT NULL DEFAULT FALSE,
    split_config_json JSONB DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(entity_id, rule_code),

    CONSTRAINT bank_transaction_rules_match_type_chk
        CHECK (match_type IN ('contains', 'starts_with', 'exact', 'regex'))
);

CREATE INDEX IF NOT EXISTS idx_bank_rules_entity_priority
    ON bank_transaction_rules (entity_id, is_active, priority);

CREATE TABLE IF NOT EXISTS bank_auto_journal_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    transactions_reviewed INTEGER NOT NULL DEFAULT 0,
    transactions_matched INTEGER NOT NULL DEFAULT 0,
    transactions_unmatched INTEGER NOT NULL DEFAULT 0,
    transactions_skipped INTEGER NOT NULL DEFAULT 0,
    transactions_split_required INTEGER NOT NULL DEFAULT 0,
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    actor_email TEXT,
    summary_json JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auto_jrn_runs_entity_period
    ON bank_auto_journal_runs (entity_id, period_end);

CREATE TABLE IF NOT EXISTS bank_auto_journal_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    auto_journal_run_id UUID REFERENCES bank_auto_journal_runs(id) ON DELETE CASCADE,
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    rule_id UUID REFERENCES bank_transaction_rules(id),
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    matched_status TEXT NOT NULL,
        -- 'matched' | 'unmatched' | 'skipped' | 'split_required'
    skip_reason TEXT,
    debit_account TEXT,
    credit_account TEXT,
    amount DECIMAL(15,2),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(entity_id, bank_transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_auto_jrn_lines_run
    ON bank_auto_journal_lines (auto_journal_run_id);

CREATE INDEX IF NOT EXISTS idx_auto_jrn_lines_status
    ON bank_auto_journal_lines (entity_id, matched_status);
