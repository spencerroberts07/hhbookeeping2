-- Migration 024: Full payroll calculation module
--
-- Replaces the v0.7 stub payroll_runs (which was a control-layer table
-- recording numbers entered manually) with a proper Canadian payroll
-- calculator. The bank auto-journal's auto_draft_payroll path used
-- the old stub schema — those 2 stub rows are dropped here.
--
-- New tables:
--   payroll_employees       - employee master with TD1, rates, banking
--   payroll_runs (new)      - one row per pay run with totals
--   payroll_run_lines       - per-employee per-run calculated payroll
--   payroll_bank_withdrawals- stub for future EFT/CRA withdrawal automation

-- payroll_run_events references payroll_runs(id) ON DELETE CASCADE.
-- It's empty in production; safe to drop the dependent runs.
DELETE FROM payroll_runs;

-- payroll_batches has no FK into payroll_runs but predates this redesign.
-- Drop it; the new payroll_runs replaces its role.
DROP TABLE IF EXISTS payroll_batches CASCADE;

-- payroll_run_events used the old run_id column. Drop and recreate
-- against the new payroll_runs.
DROP TABLE IF EXISTS payroll_run_events CASCADE;

-- Drop the old payroll_runs.
DROP TABLE IF EXISTS payroll_runs CASCADE;


CREATE TABLE payroll_employees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    employee_number INTEGER,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    employment_type TEXT NOT NULL,
    hourly_rate NUMERIC(10, 4),
    biweekly_salary NUMERIC(12, 2),
    vacation_rate NUMERIC(5, 4) NOT NULL DEFAULT 0.0400,
    province TEXT NOT NULL DEFAULT 'ON',
    federal_td1_claim_code INTEGER NOT NULL DEFAULT 1,
    provincial_td1_claim_code INTEGER NOT NULL DEFAULT 1,
    cpp_exempt BOOLEAN NOT NULL DEFAULT FALSE,
    ei_exempt BOOLEAN NOT NULL DEFAULT FALSE,
    has_life_insurance BOOLEAN NOT NULL DEFAULT FALSE,
    life_insurance_biweekly NUMERIC(10, 2) NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    start_date DATE,
    address TEXT,
    bank_transit TEXT,
    bank_institution TEXT,
    bank_account TEXT,
    ods_name_key TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, employee_number)
);

CREATE INDEX idx_payroll_employees_ods_name_key
    ON payroll_employees (entity_id, lower(ods_name_key));

CREATE INDEX idx_payroll_employees_active
    ON payroll_employees (entity_id) WHERE is_active = TRUE;


CREATE TABLE payroll_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    pay_run_number TEXT NOT NULL,
    period_number INTEGER NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    pay_date DATE NOT NULL,
    pay_type TEXT NOT NULL DEFAULT 'Normal',
    status TEXT NOT NULL DEFAULT 'draft',
    workflow_status TEXT NOT NULL DEFAULT 'draft',
    active_employees INTEGER NOT NULL DEFAULT 0,
    paid_employees INTEGER NOT NULL DEFAULT 0,
    total_gross NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_net_pay NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_fed_tax NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_cpp_ee NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_cpp_er NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_ei_ee NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_ei_er NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_life_taxable NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_vacation_earned NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_vacation_paid NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_stat_pay NUMERIC(15, 2) NOT NULL DEFAULT 0,
    cra_remittance_amount NUMERIC(15, 2) NOT NULL DEFAULT 0,
    journal_batch_id UUID REFERENCES journal_batches(id),
    hours_import_file TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email TEXT,
    submitted_by TEXT,
    submitted_at TIMESTAMPTZ,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, pay_run_number)
);

CREATE INDEX idx_payroll_runs_period
    ON payroll_runs (entity_id, period_end DESC);


CREATE TABLE payroll_run_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    employee_id UUID NOT NULL REFERENCES payroll_employees(id),
    employment_type TEXT NOT NULL,
    week1_hours NUMERIC(6, 2) NOT NULL DEFAULT 0,
    week2_hours NUMERIC(6, 2) NOT NULL DEFAULT 0,
    total_hours NUMERIC(6, 2) NOT NULL DEFAULT 0,
    hourly_rate NUMERIC(10, 4),
    reg_hours_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    overtime_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    salary_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    stat_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    vacation_paid NUMERIC(12, 2) NOT NULL DEFAULT 0,
    gross_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    taxable_gross NUMERIC(12, 2) NOT NULL DEFAULT 0,
    fed_tax NUMERIC(12, 2) NOT NULL DEFAULT 0,
    cpp_ee NUMERIC(12, 2) NOT NULL DEFAULT 0,
    cpp_er NUMERIC(12, 2) NOT NULL DEFAULT 0,
    ei_ee NUMERIC(12, 2) NOT NULL DEFAULT 0,
    ei_er NUMERIC(12, 2) NOT NULL DEFAULT 0,
    life_taxable_benefit NUMERIC(12, 2) NOT NULL DEFAULT 0,
    vacation_earned NUMERIC(12, 2) NOT NULL DEFAULT 0,
    net_pay NUMERIC(12, 2) NOT NULL DEFAULT 0,
    is_on_vacation BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (payroll_run_id, employee_id)
);

CREATE INDEX idx_payroll_run_lines_employee
    ON payroll_run_lines (employee_id);


CREATE TABLE payroll_bank_withdrawals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payroll_run_id UUID NOT NULL REFERENCES payroll_runs(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id),
    employee_id UUID REFERENCES payroll_employees(id),
    withdrawal_type TEXT NOT NULL,
    amount NUMERIC(15, 2) NOT NULL,
    scheduled_date DATE,
    status TEXT NOT NULL DEFAULT 'pending',
    bank_reference TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payroll_bank_withdrawals_run
    ON payroll_bank_withdrawals (payroll_run_id);
