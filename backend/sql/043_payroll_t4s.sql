-- Migration 043: T4 (Statement of Remuneration Paid) archive.
--
-- T4 is a CALENDAR YEAR concept (Jan 1 – Dec 31) — Bridlewood's
-- accounting fiscal year (Oct–Sep) is irrelevant for CRA filing.
-- The column name is `calendar_year` to make this explicit.
--
-- Totals are computed dynamically from payroll_run_lines on
-- generate (source of truth: the run lines themselves, filtered by
-- pay_date). The values stored here are a snapshot taken at
-- generation time so the PDF in R2 always matches a known
-- numeric record.

CREATE TABLE IF NOT EXISTS payroll_t4s (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    employee_id UUID NOT NULL REFERENCES payroll_employees(id),
    calendar_year INTEGER NOT NULL,             -- e.g. 2025 for Jan 1–Dec 31 2025
    box_14_employment_income NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_16_cpp_employee NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_17_cpp2_employee NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_18_ei_premiums NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_22_income_tax NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_24_ei_insurable NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_26_cpp_pensionable NUMERIC(12, 2) NOT NULL DEFAULT 0,
    box_40_other_benefits NUMERIC(12, 2) NOT NULL DEFAULT 0,
    r2_object_key TEXT,
    file_name TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_by TEXT,
    filed_with_cra BOOLEAN NOT NULL DEFAULT FALSE,
    filed_at TIMESTAMPTZ,
    UNIQUE (entity_id, employee_id, calendar_year)
);

CREATE INDEX IF NOT EXISTS idx_payroll_t4s_year
    ON payroll_t4s (entity_id, calendar_year DESC);
