-- Migration 006: HH AP remittance -> bank match indexes + audit table
--
-- Adds:
--   1. hh_ap_remittance_bank_match_events  - per-remittance audit log
--   2. supporting indexes on hh_ap_remittances and bank_transactions
--   3. partial unique indexes on bank_transaction_matches that prevent
--      double-matching the same remittance or the same bank withdrawal.
--
-- IMPORTANT — column naming:
--   This file originally referenced bank_transaction_matches.target_table
--   and match_status = 'active'. Those columns/values do NOT exist on
--   the live DB; the canonical columns are target_table_name and
--   active = TRUE. The file has been corrected.
--   See backend/sql/000_baseline_schema_audit.md for history.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS hh_ap_remittance_bank_match_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    remittance_id UUID NOT NULL REFERENCES hh_ap_remittances(id) ON DELETE CASCADE,
    bank_transaction_id UUID REFERENCES bank_transactions(id) ON DELETE SET NULL,
    bank_transaction_match_id UUID REFERENCES bank_transaction_matches(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    actor_email TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hh_ap_remittance_bank_match_events_remittance_created
    ON hh_ap_remittance_bank_match_events(remittance_id, created_at);

CREATE INDEX IF NOT EXISTS idx_hh_ap_remittance_bank_match_events_entity_created
    ON hh_ap_remittance_bank_match_events(entity_id, created_at);

CREATE INDEX IF NOT EXISTS idx_hh_ap_remittances_entity_withdrawal
    ON hh_ap_remittances(entity_id, withdrawal_date, remittance_date, total_amount);

CREATE INDEX IF NOT EXISTS idx_bank_transactions_entity_direction_date_amount
    ON bank_transactions(entity_id, direction, transaction_date, amount);

-- Partial unique index: at most one active match per HH remittance.
CREATE UNIQUE INDEX IF NOT EXISTS uq_bank_transaction_matches_active_hh_ap_remittance_target
    ON bank_transaction_matches(target_record_id)
    WHERE active = TRUE AND target_table_name = 'hh_ap_remittances';

-- Lookup support index for "find the active HH remittance match for an entity".
CREATE INDEX IF NOT EXISTS idx_bank_transaction_matches_hh_remittance_lookup
    ON bank_transaction_matches(entity_id, target_record_id)
    WHERE active = TRUE AND target_table_name = 'hh_ap_remittances';
