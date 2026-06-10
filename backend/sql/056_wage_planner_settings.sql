-- Migration 056: Wage Cost Planner — annual per-entity assumptions.
--
-- wage_planner_settings  one row per entity per fiscal year, capturing the
--                        annual assumptions that drive the planner model.
--
-- wage_planner_salaried_staff  child table, one row per salaried employee
--                              listed in the settings.  Captures salary +
--                              bonus outside of payroll_employees (which has
--                              no bonus field) and assumes 80 hours / period.
--
-- Safe to re-run.

CREATE TABLE IF NOT EXISTS wage_planner_settings (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id               UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    fiscal_year             INTEGER NOT NULL,
    -- Core model inputs
    target_wage_pct         NUMERIC(7, 6) NOT NULL,    -- e.g. 0.110000 = 11%
    forecast_sales_change   NUMERIC(7, 6) NOT NULL DEFAULT 0,  -- e.g. -0.10 = -10%
    avg_hourly_wage         NUMERIC(10, 4) NOT NULL,   -- hourly, excl. salaried
    benefits_pct            NUMERIC(7, 6) NOT NULL DEFAULT 0.04,
    distribution_basis      TEXT NOT NULL DEFAULT 'prior_year'
                                CHECK (distribution_basis IN ('prior_year', 'national_average')),
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year)
);

CREATE TABLE IF NOT EXISTS wage_planner_salaried_staff (
    id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    settings_id                 UUID NOT NULL
                                    REFERENCES wage_planner_settings(id) ON DELETE CASCADE,
    employee_name               TEXT NOT NULL,
    annual_salary               NUMERIC(14, 2) NOT NULL DEFAULT 0,
    bonus                       NUMERIC(14, 2) NOT NULL DEFAULT 0,
    assumed_hours_per_period    INTEGER NOT NULL DEFAULT 80,
    sort_order                  INTEGER NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wage_planner_settings_entity_fy
    ON wage_planner_settings (entity_id, fiscal_year);

CREATE INDEX IF NOT EXISTS idx_wage_planner_salaried_settings
    ON wage_planner_salaried_staff (settings_id);
