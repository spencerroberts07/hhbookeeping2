-- Migration 038: Vacation accrual ledger
--
-- Every accrual / payout / adjustment writes one row to
-- payroll_vacation_ledger. The current balance is denormalized onto
-- payroll_employees for fast access from the roster + assistant
-- preview cards. The ledger remains the source of truth — a balance
-- can always be reconstructed by replaying entries in created_at
-- order.

CREATE TABLE IF NOT EXISTS payroll_vacation_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    employee_id UUID NOT NULL REFERENCES payroll_employees(id),
    payroll_run_id UUID REFERENCES payroll_runs(id),
    entry_type TEXT NOT NULL,
    -- Allowed: 'accrual', 'payout', 'adjustment', 'opening_balance'
    hours_delta NUMERIC(8, 2) NOT NULL DEFAULT 0,
    dollars_delta NUMERIC(10, 2) NOT NULL DEFAULT 0,
    balance_hours_after NUMERIC(8, 2),
    balance_dollars_after NUMERIC(10, 2),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CHECK (entry_type IN ('accrual', 'payout', 'adjustment', 'opening_balance'))
);

CREATE INDEX IF NOT EXISTS idx_pvl_employee_chrono
    ON payroll_vacation_ledger (employee_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pvl_entity_chrono
    ON payroll_vacation_ledger (entity_id, created_at DESC);


ALTER TABLE payroll_employees
    ADD COLUMN IF NOT EXISTS vacation_hours_balance NUMERIC(8, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS vacation_dollars_balance NUMERIC(10, 2) NOT NULL DEFAULT 0;
