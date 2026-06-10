-- Migration 059: Revive the vendors table as a self-learning per-entity vendor master.
-- The table exists but was previously unused (no code referenced it).
-- We extend it with banking, contact, payment-terms, and confidence columns.
-- Additive only: ADD COLUMN IF NOT EXISTS.
-- Unique index vendors_entity_id_vendor_normalized_key already exists on
-- (entity_id, vendor_normalized) — that remains the dedup key.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Core learning / profile fields
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS remittance_email       TEXT;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS payment_terms_days     INTEGER;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS payment_terms_confidence NUMERIC(4,3) NOT NULL DEFAULT 0;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS profile_confidence     NUMERIC(4,3) NOT NULL DEFAULT 0;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS invoice_count          INTEGER      NOT NULL DEFAULT 0;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS first_seen_at          TIMESTAMPTZ;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS last_seen_at           TIMESTAMPTZ;

-- Banking details (entered once → auto-populated forever in payment files)
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS bank_transit           TEXT;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS bank_institution       TEXT;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS bank_account           TEXT;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS eft_transaction_type   TEXT;  -- CPA-005 3-digit code; NULL = default
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS banking_confirmed_at   TIMESTAMPTZ;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS banking_confirmed_by   TEXT;

-- Timestamps (were missing from the original table)
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Additional index for fast lookup by vendor_normalized within entity
CREATE INDEX IF NOT EXISTS idx_vendors_entity_normalized
    ON vendors (entity_id, vendor_normalized);

-- Payment terms observations: each invoice's observed invoice_date→due_date gap,
-- so we can compute modal terms and confidence from the distribution.
CREATE TABLE IF NOT EXISTS vendor_payment_terms_observations (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id         UUID NOT NULL REFERENCES vendors(id) ON DELETE CASCADE,
    entity_id         UUID NOT NULL REFERENCES entities(id),
    invoice_id        UUID,  -- FK intent; avoid hard FK to allow orphan cleanup
    invoice_date      DATE NOT NULL,
    due_date          DATE NOT NULL,
    terms_days        INTEGER GENERATED ALWAYS AS ((due_date - invoice_date)) STORED,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vendor_terms_observations_vendor
    ON vendor_payment_terms_observations (vendor_id, observed_at);
