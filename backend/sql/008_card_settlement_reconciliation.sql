CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS card_settlement_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    processor_name TEXT NOT NULL,
    merchant_account TEXT,
    settlement_reference TEXT,
    business_date DATE NOT NULL,
    deposit_date DATE,
    currency_code TEXT NOT NULL DEFAULT 'CAD',
    gross_sales_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    refunds_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    chargebacks_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    fees_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    tax_on_fees_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    net_deposit_amount NUMERIC(14,2) NOT NULL,
    expected_cash_balancing_amount NUMERIC(14,2),
    matched_bank_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    reconciliation_status TEXT NOT NULL DEFAULT 'new',
    source_file_name TEXT,
    note TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_card_settlement_batches_entity_business_date
    ON card_settlement_batches(entity_id, business_date, processor_name);

CREATE INDEX IF NOT EXISTS idx_card_settlement_batches_entity_status
    ON card_settlement_batches(entity_id, reconciliation_status, deposit_date, business_date);

CREATE TABLE IF NOT EXISTS card_settlement_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    batch_id UUID NOT NULL REFERENCES card_settlement_batches(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    actor_email TEXT,
    from_status TEXT,
    to_status TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_card_settlement_events_batch_created
    ON card_settlement_events(batch_id, created_at);
