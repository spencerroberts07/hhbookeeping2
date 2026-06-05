-- Migration 046: ratio engine (Phase 2C/2D).
--
-- Additive only — four new config/analytics tables. No accounting data is
-- touched. Idempotent (CREATE TABLE IF NOT EXISTS).
--
--   ratio_account_roles      per-entity map of accounts to ratio roles that
--                            the prefix convention can't infer (inventory,
--                            interest-bearing debt, interest expense, D&A,
--                            income tax, AR/AP/cash, current portion of LTD).
--                            Auto-seeded from QBO type/subtype; admin-editable.
--   entity_ratio_config      per-entity enable/disable + optional thresholds
--                            for built-in AND custom ratios.
--   entity_ratio_inputs      per-entity manual numeric inputs the GL doesn't
--                            hold (e.g. annual_debt_service for DSCR).
--   custom_ratio_definitions per-entity custom formulas (numerator/denominator
--                            expressions, output type) — used by 2D.

CREATE TABLE IF NOT EXISTS ratio_account_roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    account_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, role, account_code)
);
CREATE INDEX IF NOT EXISTS idx_ratio_roles_entity ON ratio_account_roles (entity_id, role);

CREATE TABLE IF NOT EXISTS entity_ratio_config (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    ratio_key TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    threshold_min NUMERIC(18,4),
    threshold_max NUMERIC(18,4),
    threshold_direction TEXT,            -- 'min' | 'max' | NULL
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, ratio_key)
);

CREATE TABLE IF NOT EXISTS entity_ratio_inputs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value NUMERIC(18,4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, key)
);

CREATE TABLE IF NOT EXISTS custom_ratio_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    label TEXT NOT NULL,
    numerator_expr TEXT NOT NULL,
    denominator_expr TEXT,               -- NULL => numerator is the whole value
    output_type TEXT NOT NULL DEFAULT 'ratio',   -- 'ratio' | 'percent' | 'dollar'
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    threshold_min NUMERIC(18,4),
    threshold_max NUMERIC(18,4),
    threshold_direction TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, key)
);
