-- Migration 039: YTD tracking on payroll_employees
--
-- CPP / EI / Fed-tax annual caps require YTD knowledge to enforce
-- correctly. The calc engine already accepts ytd_cpp_ee /
-- ytd_ei_ee as parameters; this migration provides the columns to
-- persist those totals on the employee master.
--
-- ytd_reset_date is set when the fiscal year rolls over (Bridlewood:
-- Oct 1). The reset is admin-triggered — see POST /api/payroll/ytd/reset.
--
-- CPP2 column: added per spec. The engine doesn't compute CPP2 yet —
-- the column exists for future use once we wire CPP2 alongside CPP1
-- in services_payroll_calc.py.

ALTER TABLE payroll_employees
    ADD COLUMN IF NOT EXISTS ytd_gross NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ytd_cpp_employee NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ytd_cpp2_employee NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ytd_ei_employee NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ytd_fed_tax NUMERIC(10, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ytd_reset_date DATE;
