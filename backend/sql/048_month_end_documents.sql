-- Migration 048: month-end document (Phase 4). Additive.
--
--   month_end_documents  one generated month-end PDF per entity per period.
--                        Re-trigger overwrites the R2 key + row. The PDF bytes
--                        live in R2; only the object key is stored here.

CREATE TABLE IF NOT EXISTS month_end_documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    accounting_period_id UUID NOT NULL REFERENCES accounting_periods(id),
    r2_object_key TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by TEXT,
    email_sent_at TIMESTAMPTZ,
    email_recipients JSONB,
    commentary_json JSONB,
    status TEXT NOT NULL DEFAULT 'generating'
        CHECK (status IN ('generating', 'ready', 'failed')),
    error_msg TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, accounting_period_id)
);

CREATE INDEX IF NOT EXISTS idx_month_end_documents_entity_period
    ON month_end_documents (entity_id, accounting_period_id);
