-- Migration 045: journal-line change audit + correcting-entry linkage.
--
-- Supports the report drill-down "edit / reclassify / correct" feature
-- (Slice 2). Two additive changes — NO drops, NO alters to existing data:
--
--   1. journal_line_change_events — append-only audit log for every
--      reclassify / amount edit / note / correcting-entry action. Mirrors
--      the existing journal_batch_workflow_events domain-event pattern
--      (003_month_end_workflow.sql).
--   2. journal_batches.correction_of_batch_id — links a reversal / re-entry
--      batch back to the original batch it corrects. NULL for all existing
--      rows (additive, nullable, no backfill).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS journal_line_change_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE CASCADE,
    journal_line_id UUID,                 -- nullable: batch-level notes/corrections
    accounting_period_id UUID REFERENCES accounting_periods(id),
    action TEXT NOT NULL,                 -- reclassify | edit_amount | add_note | correcting_entry
    from_account_code TEXT,
    to_account_code TEXT,
    before_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_json  JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason TEXT,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jlce_batch_created
    ON journal_line_change_events (journal_batch_id, created_at);

CREATE INDEX IF NOT EXISTS idx_jlce_entity_period
    ON journal_line_change_events (entity_id, accounting_period_id, created_at);

ALTER TABLE journal_batches
    ADD COLUMN IF NOT EXISTS correction_of_batch_id UUID
        REFERENCES journal_batches(id);

CREATE INDEX IF NOT EXISTS idx_journal_batches_correction_of
    ON journal_batches (correction_of_batch_id)
    WHERE correction_of_batch_id IS NOT NULL;
