-- Migration 012: Post-import auto-match runner
--
-- One row per call to run_auto_match(...) — records what was matched
-- across HH remittance, card settlement, and direct vendor AP modules
-- in a single pass, plus what was skipped and why.
--
-- triggered_by:
--   csv_import   - kicked off automatically after a bank CSV upload
--   manual       - kicked off via POST /api/auto-match/run
--   scheduled    - kicked off by a future cron / scheduled task
--
-- trigger_source_id:
--   When triggered_by = 'csv_import', this is the bank_csv_import_runs.id
--   that produced the new bank rows. NULL otherwise.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS auto_match_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    triggered_by TEXT NOT NULL,
    trigger_source_id UUID,
    period_start DATE,
    period_end DATE,

    hh_remittance_matched INTEGER NOT NULL DEFAULT 0,
    hh_remittance_skipped INTEGER NOT NULL DEFAULT 0,
    card_settlement_matched INTEGER NOT NULL DEFAULT 0,
    card_settlement_skipped INTEGER NOT NULL DEFAULT 0,
    direct_vendor_matched INTEGER NOT NULL DEFAULT 0,
    direct_vendor_skipped INTEGER NOT NULL DEFAULT 0,
    total_matched INTEGER NOT NULL DEFAULT 0,
    total_skipped INTEGER NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'running',
    error_text TEXT,
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT auto_match_runs_triggered_by_chk
        CHECK (triggered_by IN ('csv_import','manual','scheduled')),
    CONSTRAINT auto_match_runs_status_chk
        CHECK (status IN ('running','completed','partial','failed'))
);

CREATE INDEX IF NOT EXISTS idx_auto_match_runs_entity_created
    ON auto_match_runs (entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auto_match_runs_entity_triggered_by
    ON auto_match_runs (entity_id, triggered_by, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auto_match_runs_trigger_source
    ON auto_match_runs (trigger_source_id)
    WHERE trigger_source_id IS NOT NULL;
