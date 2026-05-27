-- Migration 037: Employee-requested additional tax withholding
--
-- Two surfaces:
--   * payroll_employees      — the configuration: how much extra to
--     withhold per period plus the audit fields (effective date and
--     whether the signed TD1 is on file).
--   * payroll_run_lines      — per-period storage so the pay stub
--     can show the additional withholding as its own line and the
--     bookkeeper can audit "was extra tax actually withheld?"
--
-- The calc engine in services_payroll_calc.py adds these AFTER the
-- standard CRA formula, capped at gross_pay (you can't withhold more
-- than the employee earned).

ALTER TABLE payroll_employees
    ADD COLUMN IF NOT EXISTS additional_fed_tax NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS additional_prov_tax NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS additional_tax_effective_date DATE,
    ADD COLUMN IF NOT EXISTS additional_tax_td1_on_file BOOLEAN NOT NULL DEFAULT FALSE;


ALTER TABLE payroll_run_lines
    ADD COLUMN IF NOT EXISTS additional_fed_tax NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS additional_prov_tax NUMERIC(10, 2) NOT NULL DEFAULT 0;


-- Performance: surface employees with additional withholding for the
-- roster's warning badge without a full table scan.
CREATE INDEX IF NOT EXISTS idx_payroll_employees_addl_tax
    ON payroll_employees (entity_id)
    WHERE additional_fed_tax > 0 OR additional_prov_tax > 0;
