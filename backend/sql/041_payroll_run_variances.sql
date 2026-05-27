-- Migration 041: Pre-approval variance flagging for payroll runs.
--
-- Rows are written when /runs/{id}/analyze-variances is called.
-- Severity 'block' must be acknowledged before /runs/{id}/approve
-- will accept the run — see the modified approve handler.
--
-- analyze_run_variances is idempotent: re-running it on a run
-- DELETEs prior rows (only if not acknowledged) and re-inserts.
-- Acknowledged rows are sticky.

CREATE TABLE IF NOT EXISTS payroll_run_variances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    employee_id UUID NOT NULL REFERENCES payroll_employees(id),
    variance_type TEXT NOT NULL,
    -- Allowed: gross_change, hours_change, new_employee,
    -- missing_employee, cpp_max_reached, ei_max_reached,
    -- zero_pay, large_bonus
    severity TEXT NOT NULL,                -- 'info' | 'warn' | 'block'
    previous_value NUMERIC(10, 2),
    current_value NUMERIC(10, 2),
    change_pct NUMERIC(6, 2),
    message TEXT NOT NULL,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (severity IN ('info', 'warn', 'block'))
);

CREATE INDEX IF NOT EXISTS idx_payroll_run_variances_run
    ON payroll_run_variances (payroll_run_id, severity);

CREATE INDEX IF NOT EXISTS idx_payroll_run_variances_entity
    ON payroll_run_variances (entity_id, created_at DESC);
