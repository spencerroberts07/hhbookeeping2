-- Migration 042: Off-cycle run typing + EFT-sent audit fields on
-- payroll_runs.
--
-- run_type defaults to 'regular' so every existing row continues
-- to behave as before. parent_run_id is nullable — only correction
-- / retroactive runs reference an originating run.
--
-- eft_sent_at / employees_paid_at are the audit trail for the
-- "Mark EFT sent" + "Mark employees paid" UI step. Both are nullable;
-- the workflow_status field continues to carry the lifecycle state
-- ('draft' → 'submitted_for_review' → 'approved' → 'eft_sent' →
-- 'paid'). status is a freeform text column with no CHECK constraint
-- so 'eft_sent' and 'paid' work without a schema change.

ALTER TABLE payroll_runs
    ADD COLUMN IF NOT EXISTS run_type TEXT NOT NULL DEFAULT 'regular',
    ADD COLUMN IF NOT EXISTS run_description TEXT,
    ADD COLUMN IF NOT EXISTS parent_run_id UUID
        REFERENCES payroll_runs(id),
    ADD COLUMN IF NOT EXISTS eft_sent_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS eft_sent_by TEXT,
    ADD COLUMN IF NOT EXISTS eft_send_notes TEXT,
    ADD COLUMN IF NOT EXISTS employees_paid_at TIMESTAMPTZ;

-- Validate run_type at the application layer (the calc engine and
-- routes only emit known values). CHECK constraint deferred to a
-- later migration once any legacy 'regular'-by-default rows can be
-- verified safe.

CREATE INDEX IF NOT EXISTS idx_payroll_runs_parent
    ON payroll_runs (parent_run_id)
    WHERE parent_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_payroll_runs_eft_sent
    ON payroll_runs (entity_id, eft_sent_at DESC)
    WHERE eft_sent_at IS NOT NULL;
