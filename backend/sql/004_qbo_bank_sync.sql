CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bank_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    source_system TEXT NOT NULL,
    source_connection_id UUID REFERENCES quickbooks_connections(id),
    source_account_id TEXT,
    source_account_name TEXT,
    source_account_code TEXT,
    source_transaction_id TEXT NOT NULL,
    source_transaction_type TEXT NOT NULL,
    transaction_date DATE NOT NULL,
    posted_date DATE,
    description TEXT NOT NULL,
    reference_number TEXT,
    amount NUMERIC(14,2) NOT NULL,
    currency_code TEXT NOT NULL DEFAULT 'CAD',
    direction TEXT NOT NULL,
    review_status TEXT NOT NULL DEFAULT 'new',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, source_system, source_transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_date
    ON bank_transactions(entity_id, transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_status
    ON bank_transactions(entity_id, review_status, transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_account
    ON bank_transactions(entity_id, source_account_name, transaction_date DESC);

CREATE TABLE IF NOT EXISTS bank_transaction_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id),
    match_type TEXT NOT NULL,
    matched_table TEXT,
    matched_record_id UUID,
    match_status TEXT NOT NULL DEFAULT 'matched',
    matched_amount NUMERIC(14,2),
    note TEXT,
    created_by TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_transaction_matches_bank_txn
    ON bank_transaction_matches(bank_transaction_id, created_at DESC);

ALTER TABLE bank_feed_transactions
    ADD COLUMN IF NOT EXISTS source_system TEXT,
    ADD COLUMN IF NOT EXISTS source_transaction_id TEXT,
    ADD COLUMN IF NOT EXISTS raw_json JSONB NOT NULL DEFAULT '{}'::jsonb;
