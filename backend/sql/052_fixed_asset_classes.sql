-- Migration 052: Fixed-asset class config + disposal table
--
-- Adds a per-entity asset-class configuration table so each entity can map
-- CCA class → rate, expense account, and accumulated-depreciation account.
-- Also adds the disposal-event table and a FK from fixed_assets back to classes.
--
-- Depreciation EXPENSE stays on a single account (6900 for Bridlewood) per the
-- confirmed account strategy. The per-class SPLIT applies only to the
-- accumulated-depreciation (balance sheet) accounts.
--
-- Safe to re-run (all CREATE … IF NOT EXISTS, ADD COLUMN IF NOT EXISTS).

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -------------------------------------------------------------------
-- fixed_asset_classes
-- Per-entity CCA class configuration.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fixed_asset_classes (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    class_code      TEXT NOT NULL,          -- e.g. 'class_8_equipment'
    class_name      TEXT NOT NULL,          -- e.g. 'Store Equipment'
    cca_rate        NUMERIC(6,4) NOT NULL,  -- e.g. 0.15
    expense_account TEXT NOT NULL,          -- depreciation expense GL code (e.g. '6900')
    accum_account   TEXT NOT NULL,          -- accumulated-depreciation GL code (e.g. '1610')
    -- Optional safe_eval formula override. When set, replaces the standard
    -- declining-balance calc for this class. Uses acct_XXXX token notation.
    formula_expr    TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    display_order   INTEGER  NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, class_code)
);

CREATE INDEX IF NOT EXISTS idx_fixed_asset_classes_entity
    ON fixed_asset_classes (entity_id);

-- -------------------------------------------------------------------
-- Add class FK to fixed_assets
-- -------------------------------------------------------------------
ALTER TABLE fixed_assets
    ADD COLUMN IF NOT EXISTS fixed_asset_class_id UUID
        REFERENCES fixed_asset_classes(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_fixed_assets_class_id
    ON fixed_assets (fixed_asset_class_id);

-- -------------------------------------------------------------------
-- fixed_asset_disposals
-- Append-only disposal events (one per disposal transaction).
-- NOTE: gain_loss_account and proceeds_account are configurable TEXT
-- columns rather than hard-coded, because those GL codes vary by entity
-- and must be confirmed against the live chart of accounts before use.
-- For Bridlewood, 4020 (gain) and 6950 (loss) do NOT exist in the CoA
-- as of migration date — ask the bookkeeper before enabling disposals.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fixed_asset_disposals (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    fixed_asset_id      UUID NOT NULL REFERENCES fixed_assets(id) ON DELETE RESTRICT,
    disposal_date       DATE NOT NULL,
    proceeds            NUMERIC(14,2) NOT NULL DEFAULT 0,
    nbv_at_disposal     NUMERIC(14,2) NOT NULL,
    gain_loss           NUMERIC(14,2) NOT NULL,     -- proceeds - nbv_at_disposal
    -- GL accounts used in the disposal journal (must exist in accounts table)
    proceeds_account    TEXT,                        -- e.g. cash account receiving proceeds
    gain_loss_account   TEXT,                        -- e.g. 4020 gain / 6950 loss (entity-specific)
    journal_batch_id    UUID REFERENCES journal_batches(id),
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fixed_asset_disposals_entity
    ON fixed_asset_disposals (entity_id);
CREATE INDEX IF NOT EXISTS idx_fixed_asset_disposals_asset
    ON fixed_asset_disposals (fixed_asset_id);
