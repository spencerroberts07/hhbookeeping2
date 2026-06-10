-- Migration 057: Wage Cost Planner — per-period forecast + actuals.
--
-- One row per entity per fiscal year per pay-period.  Rows are upserted by
-- compute_plan (forecast columns) and refresh_period_actuals (actual columns +
-- locked flag).  All numeric columns are NULLABLE so we can represent
-- "not yet computed" vs zero clearly.
--
-- manual_override_json stores per-field overrides applied by the bookkeeper:
--   { "actual_sales": 125000.00, "actual_hours": 420.5, ... }
-- When an override is present its value takes precedence over the auto-pulled
-- DB figure.
--
-- Safe to re-run.

CREATE TABLE IF NOT EXISTS wage_planner_periods (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id               UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    fiscal_year             INTEGER NOT NULL,
    period_number           INTEGER NOT NULL,   -- 1..26

    -- Forecast columns (recomputed each time settings change)
    py_sales                NUMERIC(16, 2),     -- prior-year sales for this period window
    forecast_sales          NUMERIC(16, 2),     -- py_sales * (1 + forecast_sales_change)
    target_wage_dollars     NUMERIC(16, 2),     -- forecast_sales * target_wage_pct
    target_hours            NUMERIC(10, 2),     -- after salaried deduction

    -- Actual columns (set when period is locked; may be manually overridden)
    actual_sales            NUMERIC(16, 2),
    actual_gross_wages      NUMERIC(16, 2),     -- SUM(gross_pay) from payroll run
    actual_stat_pay         NUMERIC(16, 2),     -- SUM(stat_pay) — deducted for calc
    actual_hours            NUMERIC(10, 2),     -- SUM(total_hours) for hourly lines

    -- Derived actuals
    hours_over_under        NUMERIC(10, 2),     -- positive = over budget
    adjusted_target_hours   NUMERIC(10, 2),     -- go-forward target (remaining periods only)
    actual_sales_per_hour   NUMERIC(10, 2),
    py_sales_per_hour       NUMERIC(10, 2),     -- NULL if prior-year hours not available

    -- Period-lock state
    locked                  BOOLEAN NOT NULL DEFAULT FALSE,
    locked_at               TIMESTAMPTZ,
    source_payroll_run_id   UUID,               -- payroll_runs.id that triggered the lock

    -- Bookkeeper corrections
    manual_override_json    JSONB,              -- per-field manual overrides

    computed_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year, period_number)
);

CREATE INDEX IF NOT EXISTS idx_wage_planner_periods_entity_fy
    ON wage_planner_periods (entity_id, fiscal_year, period_number);
