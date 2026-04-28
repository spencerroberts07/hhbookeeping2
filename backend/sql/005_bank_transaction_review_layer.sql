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
    transaction_date DATE,
    posted_date DATE,
    description TEXT,
    normalized_description TEXT,
    counterparty_name TEXT,
    reference_number TEXT,
    amount NUMERIC(14,2) NOT NULL,
    currency_code TEXT,
    direction TEXT,
    review_status TEXT NOT NULL DEFAULT 'new',
    review_note TEXT,
    reviewed_by TEXT,
    last_reviewed_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_connection_id UUID REFERENCES quickbooks_connections(id);
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_account_id TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_account_name TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_account_code TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_transaction_id TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS source_transaction_type TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS transaction_date DATE;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS posted_date DATE;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS normalized_description TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS counterparty_name TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS reference_number TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS amount NUMERIC(14,2);
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS currency_code TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS direction TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS review_status TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS review_note TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS reviewed_by TEXT;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS last_reviewed_at TIMESTAMPTZ;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS raw_json JSONB;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ;
ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

UPDATE bank_transactions SET review_status = 'new' WHERE review_status IS NULL;
UPDATE bank_transactions SET raw_json = '{}'::jsonb WHERE raw_json IS NULL;
UPDATE bank_transactions SET imported_at = NOW() WHERE imported_at IS NULL;
UPDATE bank_transactions SET last_seen_at = NOW() WHERE last_seen_at IS NULL;

ALTER TABLE bank_transactions ALTER COLUMN review_status SET DEFAULT 'new';
ALTER TABLE bank_transactions ALTER COLUMN raw_json SET DEFAULT '{}'::jsonb;
ALTER TABLE bank_transactions ALTER COLUMN imported_at SET DEFAULT NOW();
ALTER TABLE bank_transactions ALTER COLUMN last_seen_at SET DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_date
    ON bank_transactions(entity_id, transaction_date);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_review
    ON bank_transactions(entity_id, review_status, transaction_date);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_source_lookup
    ON bank_transactions(entity_id, source_system, source_transaction_id);

CREATE TABLE IF NOT EXISTS bank_transaction_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    match_type TEXT NOT NULL,
    target_table TEXT,
    target_record_id TEXT,
    target_label TEXT NOT NULL,
    amount_matched NUMERIC(14,2) NOT NULL,
    match_status TEXT NOT NULL DEFAULT 'active',
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_by TEXT,
    released_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_bank_transaction_matches_transaction
    ON bank_transaction_matches(bank_transaction_id, match_status, created_at);

CREATE INDEX IF NOT EXISTS idx_bank_transaction_matches_entity
    ON bank_transaction_matches(entity_id, match_status, created_at);

CREATE TABLE IF NOT EXISTS bank_transaction_review_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    from_review_status TEXT,
    to_review_status TEXT,
    actor_email TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_transaction_review_events_transaction
    ON bank_transaction_review_events(bank_transaction_id, created_at);
