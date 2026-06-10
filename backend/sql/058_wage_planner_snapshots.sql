-- Migration 058: Wage Cost Planner — immutable Excel snapshot archive.
--
-- One row per entity per fiscal year per pay-period number.  The Excel file
-- bytes live in R2; only the object key is stored here (r2_object_key is
-- nullable — None when R2 is not configured or upload failed).
--
-- Each time the payroll run for a period is approved, the on_payroll_run_finalized
-- hook upserts this row and writes a fresh R2 object.  Rows for locked (past)
-- periods are never auto-regenerated; only the current "frontier" period is
-- refreshed on re-approval.
--
-- Modelled on month_end_documents (migration 048).
--
-- Safe to re-run.

CREATE TABLE IF NOT EXISTS wage_planner_snapshots (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    fiscal_year         INTEGER NOT NULL,
    pay_period_number   INTEGER NOT NULL,   -- 1..26
    r2_object_key       TEXT,               -- NULL = R2 unavailable or upload failed
    status              TEXT NOT NULL DEFAULT 'generating'
                            CHECK (status IN ('generating', 'ready', 'failed')),
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by        TEXT,               -- actor email
    error_msg           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year, pay_period_number)
);

CREATE INDEX IF NOT EXISTS idx_wage_planner_snapshots_entity_fy
    ON wage_planner_snapshots (entity_id, fiscal_year, pay_period_number DESC);
