-- Migration 040: Pay stub archive
--
-- One row per (run, employee). The actual PDF lives in Cloudflare R2
-- under r2_object_key; this table is the index that powers the run-
-- detail and employee-detail "download stub" UIs.
--
-- R2 is fail-tolerant — if upload_file returns None, the row is still
-- written (r2_object_key NULL) so the generation attempt is audited
-- and re-runnable.

CREATE TABLE IF NOT EXISTS payroll_paystubs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    employee_id UUID NOT NULL REFERENCES payroll_employees(id),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    r2_object_key TEXT,
    file_name TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by TEXT,
    UNIQUE (payroll_run_id, employee_id)
);

CREATE INDEX IF NOT EXISTS idx_paystubs_employee
    ON payroll_paystubs (employee_id, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_paystubs_run
    ON payroll_paystubs (payroll_run_id);
