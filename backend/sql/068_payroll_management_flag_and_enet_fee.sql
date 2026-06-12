-- 068: Add is_management flag to payroll_employees and enet_fee to payroll_runs.
-- Additive only — no DROP, no destructive ALTER.

ALTER TABLE payroll_employees
    ADD COLUMN IF NOT EXISTS is_management BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE payroll_runs
    ADD COLUMN IF NOT EXISTS enet_fee NUMERIC(12,2) NOT NULL DEFAULT 0;

-- Seed Spencer Roberts as the management employee for Bridlewood.
UPDATE payroll_employees
SET is_management = TRUE
WHERE entity_id = '0bab9284-68d9-4769-bfc6-4dac5bd1f5e4'
  AND first_name = 'Spencer'
  AND last_name  = 'Roberts';
