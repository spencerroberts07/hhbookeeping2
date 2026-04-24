CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE bank_transactions
    ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS review_note TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS bank_transaction_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    entity_id UUID REFERENCES entities(id),
    match_type TEXT,
    target_table_name TEXT,
    target_record_id TEXT,
    matched_amount NUMERIC(14, 2),
    note TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_by TEXT,
    released_at TIMESTAMPTZ,
    released_note TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE bank_transaction_matches
    ADD COLUMN IF NOT EXISTS entity_id UUID,
    ADD COLUMN IF NOT EXISTS match_type TEXT,
    ADD COLUMN IF NOT EXISTS target_table_name TEXT,
    ADD COLUMN IF NOT EXISTS target_record_id TEXT,
    ADD COLUMN IF NOT EXISTS matched_amount NUMERIC(14, 2),
    ADD COLUMN IF NOT EXISTS note TEXT,
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS created_by TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS released_by TEXT,
    ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS released_note TEXT,
    ADD COLUMN IF NOT EXISTS raw_json JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bank_transaction_matches_active_one_per_txn
    ON bank_transaction_matches(bank_transaction_id)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_bank_transaction_matches_entity_active
    ON bank_transaction_matches(entity_id, active, created_at DESC);

CREATE TABLE IF NOT EXISTS bank_transaction_review_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    entity_id UUID REFERENCES entities(id),
    action TEXT NOT NULL,
    actor_email TEXT,
    from_review_status TEXT,
    to_review_status TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_transaction_review_events_txn_created
    ON bank_transaction_review_events(bank_transaction_id, created_at DESC);

UPDATE bank_transactions
SET review_status = 'new'
WHERE review_status IS NULL OR btrim(review_status) = '';
