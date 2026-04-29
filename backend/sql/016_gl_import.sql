-- Migration 016: GL Import + Trial Balance Comparison
--
-- Read-only QBO General Ledger ingest. Each upload creates one
-- gl_import_runs row plus per-account balance rows and per-line
-- transaction rows. The trial-balance comparison cross-walks the
-- imported balances against what the app's journal_batch lines total
-- to for the same accounting period, flagging variances.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS gl_import_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    file_name TEXT NOT NULL,
    period_start DATE,
    period_end DATE,
    report_type TEXT DEFAULT 'general_ledger',
    total_accounts INTEGER DEFAULT 0,
    total_debit_activity DECIMAL(15,2) DEFAULT 0,
    total_credit_activity DECIMAL(15,2) DEFAULT 0,
    status TEXT DEFAULT 'imported',
    actor_email TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gl_import_runs_entity_period
    ON gl_import_runs (entity_id, period_end);

CREATE TABLE IF NOT EXISTS gl_account_balances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES gl_import_runs(id) ON DELETE CASCADE,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    account_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    beginning_balance DECIMAL(15,2) DEFAULT 0,
    period_activity DECIMAL(15,2) DEFAULT 0,
    ending_balance DECIMAL(15,2) DEFAULT 0,
    transaction_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, import_run_id, account_code)
);

CREATE INDEX IF NOT EXISTS idx_gl_balances_entity_period_code
    ON gl_account_balances (entity_id, period_end, account_code);

CREATE TABLE IF NOT EXISTS gl_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES gl_import_runs(id) ON DELETE CASCADE,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    account_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    transaction_date DATE,
    transaction_type TEXT,
    reference_number TEXT,
    name TEXT,
    memo TEXT,
    split_account TEXT,
    amount DECIMAL(15,2),
    running_balance DECIMAL(15,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gl_txn_run_account
    ON gl_transactions (import_run_id, account_code);

CREATE INDEX IF NOT EXISTS idx_gl_txn_entity_account_date
    ON gl_transactions (entity_id, account_code, transaction_date);

CREATE TABLE IF NOT EXISTS gl_trial_balance_comparisons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    gl_import_run_id UUID REFERENCES gl_import_runs(id) ON DELETE CASCADE,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    account_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    gl_beginning_balance DECIMAL(15,2),
    gl_period_activity DECIMAL(15,2),
    gl_ending_balance DECIMAL(15,2),
    app_journal_total DECIMAL(15,2),
    variance DECIMAL(15,2),
    variance_pct DECIMAL(8,4),
    has_variance BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, gl_import_run_id, account_code)
);

CREATE INDEX IF NOT EXISTS idx_gl_tbc_entity_period
    ON gl_trial_balance_comparisons (entity_id, period_end);
