-- Migration 064: payroll income-tax federal/provincial split (informational)
--
-- Adds federal_tax and provincial_tax as separate columns alongside the existing
-- combined fed_tax column.  Invariant: federal_tax + provincial_tax == fed_tax per row.
-- Box 22 (T4) and GL postings continue to use the combined fed_tax column unchanged.
-- For the estimator path, values come directly from the calc engine.
-- For the register path, provincial_tax is derived from taxable_gross via ON brackets
-- (approximation -- the ENetEmployer PDF only carries a combined FED TAX figure).

ALTER TABLE payroll_run_lines
  ADD COLUMN IF NOT EXISTS federal_tax    NUMERIC(12,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS provincial_tax NUMERIC(12,2) NOT NULL DEFAULT 0;

ALTER TABLE payroll_runs
  ADD COLUMN IF NOT EXISTS total_federal_tax    NUMERIC(12,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS total_provincial_tax NUMERIC(12,2) NOT NULL DEFAULT 0;
