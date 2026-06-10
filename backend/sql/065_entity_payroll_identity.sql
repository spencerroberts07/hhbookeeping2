-- Migration 065: per-entity payroll identity (BN + address)
--
-- Adds payroll_business_number, street_address, city, postal_code to entities.
-- province already exists (migration 026).
-- Bridlewood seed uses COALESCE so re-running is safe.
--
-- After this migration, payroll_business_number is the single source of truth
-- for the CRA Business Number on T4s, paystubs, and EFT files.
-- The hardcoded PAYROLL_BUSINESS_NUMBER Python constants in routes/payroll.py,
-- services_payroll_t4.py, services_payroll_paystub.py, and
-- services_payroll_employment.py are superseded by this column.

ALTER TABLE entities
  ADD COLUMN IF NOT EXISTS payroll_business_number TEXT,
  ADD COLUMN IF NOT EXISTS street_address          TEXT,
  ADD COLUMN IF NOT EXISTS city                    TEXT,
  ADD COLUMN IF NOT EXISTS postal_code             TEXT;

-- Seed Bridlewood (1877-8). COALESCE keeps any existing value.
UPDATE entities SET
  payroll_business_number = COALESCE(payroll_business_number, '753391010RP0001'),
  street_address          = COALESCE(street_address, '90 Michael Cowpland Dr'),
  city                    = COALESCE(city, 'Kanata'),
  province                = COALESCE(province, 'ON'),
  postal_code             = COALESCE(postal_code, 'K2M 1P6')
WHERE entity_code = '1877-8';
