-- Migration 009: Bank CSV upload fallback module
-- Adds:
--   1. bank_csv_import_runs   - one row per upload run (audit + summary)
--   2. bank_transactions.source_import_run_id  - back-link from each
--      CSV-imported bank transaction to the run that created it
--
-- Conventions kept consistent with the rest of the app:
--   - source_system value for CSV-imported rows = 'statement_csv'
--   - amount sign convention is preserved:
--         outflow / withdrawal -> negative amount, direction='outflow'
--         inflow  / deposit    -> positive amount, direction='inflow'
--   - the existing UNIQUE (entity_id, source_system, source_transaction_id)
--     on bank_transactions is reused for duplicate-safe import. The CSV
--     importer computes a deterministic source_transaction_id per row so
--     re-importing the same file (or an overlapping export) is idempotent.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS bank_csv_import_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    file_name TEXT NOT NULL,
    file_checksum_sha256 TEXT,
    file_size_bytes INTEGER,
    mapping_profile TEXT NOT NULL DEFAULT 'generic',
    source_account_code TEXT,
    source_account_name TEXT,
    column_map_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    total_row_count INTEGER NOT NULL DEFAULT 0,
    parsed_row_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    earliest_transaction_date DATE,
    latest_transaction_date DATE,
    status TEXT NOT NULL DEFAULT 'completed',
    is_preview BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_csv_import_runs_entity_created
    ON bank_csv_import_runs(entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_bank_csv_import_runs_entity_account_date
    ON bank_csv_import_runs(entity_id, source_account_code, latest_transaction_date DESC);

ALTER TABLE bank_transactions
    ADD COLUMN IF NOT EXISTS source_import_run_id UUID REFERENCES bank_csv_import_runs(id);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_source_import_run
    ON bank_transactions(source_import_run_id)
    WHERE source_import_run_id IS NOT NULL;
