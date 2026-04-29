-- Migration 020: Self-improving vendor classification (Layers 2 + 3)
--
-- Adds two tables that sit on top of bank_transaction_rules and let
-- the bank-auto-journal builder learn from QBO GL history and from
-- Claude-API suggestions that the bookkeeper accepts.
--
--   vendor_classification_memory   deterministic cache of
--                                  (normalized_vendor_key → account_code).
--                                  Source can be 'gl_history',
--                                  'user_confirmed', or 'ai_seeded'.
--   bank_classification_suggestions  one row per non-Layer-1 match,
--                                    so we can show the bookkeeper a
--                                    review queue and capture their
--                                    accept / override feedback.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS vendor_classification_memory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    normalized_vendor_key TEXT NOT NULL,
    raw_examples JSONB NOT NULL DEFAULT '[]'::jsonb,
    account_code TEXT NOT NULL,
    debit_or_credit TEXT NOT NULL DEFAULT 'debit',
    source TEXT NOT NULL,
        -- 'gl_history' | 'user_confirmed' | 'ai_seeded'
    occurrences_count INTEGER NOT NULL DEFAULT 1,
    confidence_score DECIMAL(4,3) NOT NULL DEFAULT 0.500,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT,

    CONSTRAINT vcm_source_chk
        CHECK (source IN ('gl_history', 'user_confirmed', 'ai_seeded')),
    CONSTRAINT vcm_dr_or_cr_chk
        CHECK (debit_or_credit IN ('debit', 'credit')),
    CONSTRAINT vcm_unique
        UNIQUE (entity_id, normalized_vendor_key, account_code)
);

CREATE INDEX IF NOT EXISTS idx_vcm_entity_key
    ON vendor_classification_memory (entity_id, normalized_vendor_key);

CREATE INDEX IF NOT EXISTS idx_vcm_entity_account
    ON vendor_classification_memory (entity_id, account_code);

CREATE TABLE IF NOT EXISTS bank_classification_suggestions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    bank_transaction_id UUID NOT NULL REFERENCES bank_transactions(id) ON DELETE CASCADE,
    auto_journal_run_id UUID REFERENCES bank_auto_journal_runs(id) ON DELETE CASCADE,
    layer TEXT NOT NULL,
        -- 'rules' | 'vendor_memory' | 'claude'
    suggested_account_code TEXT,
    suggested_debit_or_credit TEXT,
    confidence_score DECIMAL(4,3),
    reasoning TEXT,
    raw_response_json JSONB DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
        -- 'pending' | 'accepted' | 'overridden' | 'rejected'
    final_account_code TEXT,
    feedback_actor_email TEXT,
    feedback_recorded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT bcs_layer_chk
        CHECK (layer IN ('rules', 'vendor_memory', 'claude')),
    CONSTRAINT bcs_status_chk
        CHECK (status IN ('pending', 'accepted', 'overridden', 'rejected')),
    CONSTRAINT bcs_unique
        UNIQUE (entity_id, bank_transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_bcs_entity_status
    ON bank_classification_suggestions (entity_id, status);

CREATE INDEX IF NOT EXISTS idx_bcs_run
    ON bank_classification_suggestions (auto_journal_run_id);
