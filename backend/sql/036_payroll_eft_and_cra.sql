-- Migration 036: Payroll EFT generation + CRA remittance breakdowns
--
-- Scoped, non-destructive additions on top of the v0.13 payroll module
-- (migration 024). No existing tables are touched. Three new tables:
--
--   payroll_cra_breakdowns   - per-period CRA remittance snapshot,
--                              backfilled when build_payroll_journal runs.
--                              Lets the /payroll/cra dashboard show the
--                              Fed Tax / CPP / EI breakdown without
--                              recomputing from journal_lines every time.
--   payroll_eft_files        - audit-trail row per CPA-005 EFT file
--                              generated. file lives in R2; we store the
--                              object key + a few summary numbers.
--   payroll_hours_uploads    - audit-trail row per ODS hours-upload.
--                              payroll_runs.hours_import_file holds the
--                              latest filename, but that's clobbered on
--                              re-upload. This table keeps the history.
--
-- All three tables enforce entity_id scoping. R2 keys are nullable so a
-- failed R2 upload still allows the DB row to land (fail-tolerant
-- pattern; see services_storage.py).

-- ----------------------------------------------------------------------
-- payroll_cra_breakdowns
-- ----------------------------------------------------------------------
CREATE TABLE payroll_cra_breakdowns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    business_number TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    pay_date DATE NOT NULL,
    gross_taxable NUMERIC(15, 2) NOT NULL DEFAULT 0,
    fed_tax NUMERIC(15, 2) NOT NULL DEFAULT 0,
    cpp_employee NUMERIC(15, 2) NOT NULL DEFAULT 0,
    cpp_employer NUMERIC(15, 2) NOT NULL DEFAULT 0,
    ei_employee NUMERIC(15, 2) NOT NULL DEFAULT 0,
    ei_employer NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_remittance NUMERIC(15, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (payroll_run_id)
);

CREATE INDEX idx_payroll_cra_breakdowns_entity_period
    ON payroll_cra_breakdowns (entity_id, period_end DESC);


-- ----------------------------------------------------------------------
-- payroll_eft_files
-- ----------------------------------------------------------------------
CREATE TABLE payroll_eft_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    -- File metadata
    file_name TEXT NOT NULL,
    file_path TEXT,                  -- R2 object key, null if upload failed
    record_count INTEGER NOT NULL,   -- number of C-records (employees paid)
    total_amount NUMERIC(15, 2) NOT NULL,
    file_creation_number INTEGER NOT NULL,   -- monotonically increasing per entity
    -- Optional context
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payroll_eft_files_entity
    ON payroll_eft_files (entity_id, generated_at DESC);

-- One EFT file per run is the expected case but not enforced — a re-issue
-- (after a failed bank upload, etc.) should be allowed.


-- ----------------------------------------------------------------------
-- payroll_hours_uploads
-- ----------------------------------------------------------------------
CREATE TABLE payroll_hours_uploads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    payroll_run_id UUID REFERENCES payroll_runs(id) ON DELETE SET NULL,
    -- nullable: the upload may happen before a payroll_run is created
    -- (preview/parse flow) or be retained after the run is voided.
    original_filename TEXT NOT NULL,
    file_path TEXT,                  -- R2 object key, null if upload failed
    period_ending DATE,              -- parsed from the ODS header
    employee_rows_parsed INTEGER NOT NULL DEFAULT 0,
    actor_email TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payroll_hours_uploads_entity
    ON payroll_hours_uploads (entity_id, uploaded_at DESC);
