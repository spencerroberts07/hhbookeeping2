CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE journal_batches
    ADD COLUMN IF NOT EXISTS workflow_status TEXT,
    ADD COLUMN IF NOT EXISTS submitted_by TEXT,
    ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approved_by TEXT,
    ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approval_note TEXT,
    ADD COLUMN IF NOT EXISTS rejection_note TEXT,
    ADD COLUMN IF NOT EXISTS locked_by TEXT,
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

UPDATE journal_batches
SET workflow_status = CASE
    WHEN COALESCE(status, '') IN ('draft_exception', 'draft_unbalanced') THEN 'draft_exception'
    ELSE 'draft_ready'
END
WHERE workflow_status IS NULL
   OR btrim(workflow_status) = '';

ALTER TABLE journal_batches
    ALTER COLUMN workflow_status SET DEFAULT 'draft_ready';

UPDATE journal_batches
SET workflow_status = 'draft_ready'
WHERE workflow_status IS NULL;

ALTER TABLE journal_batches
    ALTER COLUMN workflow_status SET NOT NULL;

CREATE TABLE IF NOT EXISTS journal_batch_workflow_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journal_batch_id UUID NOT NULL REFERENCES journal_batches(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    source_module TEXT NOT NULL,
    batch_label TEXT NOT NULL,
    action TEXT NOT NULL,
    from_workflow_status TEXT,
    to_workflow_status TEXT,
    actor_email TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_journal_batch_workflow_events_batch_created
    ON journal_batch_workflow_events(journal_batch_id, created_at);

CREATE INDEX IF NOT EXISTS idx_journal_batch_workflow_events_entity_period
    ON journal_batch_workflow_events(entity_id, accounting_period_id, created_at);
