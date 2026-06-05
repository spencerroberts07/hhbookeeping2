-- Migration 051: per-entity payroll EFT settings (Phase 6B). Additive.
--
-- Lifts the hard-coded TD originator/return-routing constants out of the code
-- so each dealer can carry its own CPA-005 origination values. The code reads
-- these with a hard-coded fallback, so behaviour is unchanged until populated.

ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_originator_id TEXT;
ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_short_name TEXT;
ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_long_name TEXT;
ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_return_institution TEXT;
ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_return_transit TEXT;
ALTER TABLE entity_settings ADD COLUMN IF NOT EXISTS td_return_account TEXT;

-- Seed Bridlewood (entity_code 1877-8) with its obtained TD origination values.
-- UPDATE-only (its entity_settings row already exists); if it didn't, the code
-- falls back to the hard-coded constants, so this is non-fatal either way.
UPDATE entity_settings es
   SET td_originator_id      = COALESCE(es.td_originator_id, 'TPBHC10203'),
       td_short_name         = COALESCE(es.td_short_name, 'BRIDLEWOOD HH'),
       td_long_name          = COALESCE(es.td_long_name, 'BRIDLEWOOD HOME HARDWARE'),
       td_return_institution = COALESCE(es.td_return_institution, '0004'),
       td_return_transit     = COALESCE(es.td_return_transit, '10202'),
       td_return_account     = COALESCE(es.td_return_account, '06905660371')
  FROM entities e
 WHERE es.entity_id = e.id AND e.entity_code = '1877-8';
