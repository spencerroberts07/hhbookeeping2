-- Migration 017: Fixed Asset / Depreciation
--
-- Three asset classes for Bridlewood (Class 8 equipment, Class 8
-- computers, Class 10 vehicles), Canadian CCA declining-balance with
-- the half-year rule applied in the year of acquisition.
--
-- The journal builder posts a monthly depreciation entry per asset:
--    Dr  expense GL account  [monthly_dep]
--    Cr  accumulated depn    [monthly_dep]
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fixed_assets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    asset_code TEXT NOT NULL,
    description TEXT NOT NULL,
    cca_class TEXT NOT NULL,
        -- 'class_8_equipment' | 'class_8_computer' | 'class_10_vehicle'
    cca_rate DECIMAL(8,4) NOT NULL,  -- 0.15 or 0.30
    asset_gl_account TEXT NOT NULL,         -- '1510' | '1520' | '1540'
    accum_depn_gl_account TEXT NOT NULL,    -- '1610' | '1620' | '1640'
    depn_expense_gl_account TEXT NOT NULL,  -- '6810' | '6820' | '6830'
    acquisition_date DATE,
    cost DECIMAL(15,2) NOT NULL DEFAULT 0,
    opening_nbv DECIMAL(15,2) NOT NULL,
    opening_nbv_date DATE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    disposal_date DATE,
    disposal_proceeds DECIMAL(15,2),
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, asset_code)
);

CREATE INDEX IF NOT EXISTS idx_fixed_assets_entity_active
    ON fixed_assets (entity_id, is_active);

CREATE TABLE IF NOT EXISTS depreciation_schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    fixed_asset_id UUID NOT NULL REFERENCES fixed_assets(id) ON DELETE CASCADE,
    fiscal_year INTEGER NOT NULL,
    opening_nbv DECIMAL(15,2) NOT NULL,
    annual_cca_rate DECIMAL(8,4) NOT NULL,
    half_year_rule_applies BOOLEAN DEFAULT FALSE,
    annual_depreciation DECIMAL(15,2) NOT NULL,
    monthly_depreciation DECIMAL(15,2) NOT NULL,
    closing_nbv DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, fixed_asset_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_depn_sched_entity_year
    ON depreciation_schedules (entity_id, fiscal_year);

CREATE TABLE IF NOT EXISTS depreciation_journal_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    fixed_asset_id UUID NOT NULL REFERENCES fixed_assets(id) ON DELETE CASCADE,
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    monthly_depreciation DECIMAL(15,2) NOT NULL,
    debit_account TEXT NOT NULL,
    credit_account TEXT NOT NULL,
    posted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, fixed_asset_id, period_start)
);

CREATE INDEX IF NOT EXISTS idx_depn_jrn_entity_period
    ON depreciation_journal_lines (entity_id, period_end);
