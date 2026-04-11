CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS cash_balancing_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    source_name TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT 'google_sheets',
    spreadsheet_id TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    sync_time_local TIME NOT NULL DEFAULT TIME '21:00:00',
    lookback_days INTEGER NOT NULL DEFAULT 56,
    timezone_name TEXT NOT NULL DEFAULT 'America/Toronto',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, source_name)
);

CREATE TABLE IF NOT EXISTS cash_balancing_import_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    source_id UUID NOT NULL REFERENCES cash_balancing_sources(id),
    run_type TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    tabs_read JSONB NOT NULL DEFAULT '[]'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT
);

CREATE TABLE IF NOT EXISTS cash_balancing_rows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    source_id UUID NOT NULL REFERENCES cash_balancing_sources(id),
    import_run_id UUID NOT NULL REFERENCES cash_balancing_import_runs(id),
    source_tab_name TEXT NOT NULL,
    business_date DATE,
    row_number INTEGER NOT NULL,
    row_key TEXT NOT NULL,
    row_hash TEXT NOT NULL,
    notes TEXT,
    sales_amount NUMERIC(14,2),
    cash_amount NUMERIC(14,2),
    debit_amount NUMERIC(14,2),
    credit_amount NUMERIC(14,2),
    ecommerce_amount NUMERIC(14,2),
    gift_card_amount NUMERIC(14,2),
    hst_amount NUMERIC(14,2),
    over_short_amount NUMERIC(14,2),
    raw_row_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, source_id, source_tab_name, row_key)
);

CREATE INDEX IF NOT EXISTS idx_cash_balancing_rows_entity_date
    ON cash_balancing_rows(entity_id, business_date);
