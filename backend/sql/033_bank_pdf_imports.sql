-- Migration 033: per-file tracking for bank PDF uploads
--
-- Until now, bank PDF uploads parsed straight into bank_transactions
-- with no per-file row. The new /api/documents library needs a unified
-- list of every uploaded file — including bank PDFs — so it can show
-- the dealer their archive and presign R2 view links.
--
-- The schema mirrors bank_csv_import_runs intentionally so the
-- aggregating /api/documents query can UNION across them.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bank_pdf_imports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    source_account_code TEXT,
    source_account_name TEXT,
    file_name TEXT NOT NULL,
    file_path TEXT,
    transactions_parsed INTEGER NOT NULL DEFAULT 0,
    transactions_inserted INTEGER NOT NULL DEFAULT 0,
    transactions_duplicate INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'complete',
    error_message TEXT,
    actor_email TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_pdf_imports_entity
    ON bank_pdf_imports (entity_id, created_at DESC);
