-- Migration 050: versioned CRA payroll rates (Phase 6A). Additive.
--
-- A reference table so historical payroll calculations stay reproducible: each
-- rate carries an effective_date. This DOES NOT change the live calc engine
-- (services_payroll_calc.py still reads its constants) — it seeds the versioned
-- store with the current 2025 values and (in 6A) the confirmed 2026 values.
-- Wiring the engine to read from this table is a separate, T4127-verified step.

CREATE TABLE IF NOT EXISTS cra_rate_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_key TEXT NOT NULL,              -- e.g. 'CPP_RATE_EE', 'EI_MAX_INSURABLE_ANNUAL'
    rate_value NUMERIC(14,5) NOT NULL,
    effective_date DATE NOT NULL,        -- first day the value applies (Jan 1 of the tax year)
    source_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (rate_key, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_cra_rate_versions_key_date
    ON cra_rate_versions (rate_key, effective_date);
