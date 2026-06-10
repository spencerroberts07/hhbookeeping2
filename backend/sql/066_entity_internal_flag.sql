-- Migration 066: per-entity internal flag
-- Supersedes config.internal_entity_codes (now deleted). An entity with
-- is_internal=TRUE is treated as plan_tier='internal': Professional features
-- unlocked, Stripe bypassed. DEMO- prefix check in services_billing.py is
-- retained as a secondary guard for ad-hoc demo entities.

ALTER TABLE entities
  ADD COLUMN IF NOT EXISTS is_internal BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE entities
   SET is_internal = TRUE
 WHERE entity_code = '1877-8';
