-- Migration 047: bank reconciliation (Phase 3C). Additive.
--
--   bank_reconciliations  one saved/lockable rec per cash account per period,
--                         attached to accounting_periods for the close.
--   bank_transaction_matches.match_group_id  groups the rows of a one-to-many
--                         match (one book line <-> many bank lines, or split).

CREATE TABLE IF NOT EXISTS bank_reconciliations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    accounting_period_id UUID NOT NULL REFERENCES accounting_periods(id),
    source_account_code TEXT NOT NULL,                  -- '1020'
    statement_date DATE NOT NULL,
    statement_opening_balance NUMERIC(14,2),
    statement_closing_balance NUMERIC(14,2) NOT NULL,   -- signed (negative = overdraft)
    book_balance NUMERIC(14,2) NOT NULL,                -- _account_sums on the account, as-of statement_date
    cleared_book_total NUMERIC(14,2) NOT NULL DEFAULT 0,
    cleared_bank_total NUMERIC(14,2) NOT NULL DEFAULT 0,
    outstanding_deposits_total NUMERIC(14,2) NOT NULL DEFAULT 0,  -- in book, not on statement
    outstanding_cheques_total NUMERIC(14,2) NOT NULL DEFAULT 0,   -- in book, not on statement
    bank_only_items_total NUMERIC(14,2) NOT NULL DEFAULT 0,       -- on statement, no book line (-> 3D)
    variance NUMERIC(14,2) NOT NULL DEFAULT 0,
    ties BOOLEAN NOT NULL DEFAULT FALSE,                -- abs(variance) <= 0.01
    tie_out_ok BOOLEAN,                                 -- running-balance ingest flag
    status TEXT NOT NULL DEFAULT 'draft',               -- 'draft' | 'locked'
    r2_statement_key TEXT,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, source_account_code, accounting_period_id)
);

CREATE INDEX IF NOT EXISTS idx_bank_rec_entity_period
    ON bank_reconciliations (entity_id, accounting_period_id);

ALTER TABLE bank_transaction_matches
    ADD COLUMN IF NOT EXISTS match_group_id UUID;

CREATE INDEX IF NOT EXISTS idx_btm_match_group
    ON bank_transaction_matches (entity_id, match_group_id)
    WHERE match_group_id IS NOT NULL;
