-- Migration 055: Canonical bi-weekly pay-period calendar.
--
-- Provides a stable, entity-scoped calendar of the 26 bi-weekly pay periods
-- per fiscal year.  Rows are backfilled from existing payroll_runs on first
-- use; subsequent periods can be entered manually or auto-inserted when a
-- payroll run is created.
--
-- Used by the Wage Cost Planner (migration 057) and reusable by any module
-- that needs to map a date or a payroll_runs.period_number to a fiscal year.
--
-- Safe to re-run (CREATE TABLE / INDEX IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS payroll_pay_periods (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    fiscal_year     INTEGER NOT NULL,
    period_number   INTEGER NOT NULL,          -- 1..26
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    pay_date        DATE,
    source          TEXT NOT NULL DEFAULT 'manual'
                        CHECK (source IN ('backfill', 'manual', 'auto')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year, period_number)
);

CREATE INDEX IF NOT EXISTS idx_payroll_pay_periods_entity_fy
    ON payroll_pay_periods (entity_id, fiscal_year, period_number);

CREATE INDEX IF NOT EXISTS idx_payroll_pay_periods_dates
    ON payroll_pay_periods (entity_id, period_start, period_end);
