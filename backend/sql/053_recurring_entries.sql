-- Migration 053: Recurring journal-entry engine
--
-- Three tables:
--   recurring_entry_templates  — one config row per recurring entry per entity
--   recurring_entry_lines      — debit/credit lines for fixed/formula templates
--                                (schedule-type lines are dynamic at post time)
--   recurring_entry_postings   — append-only log of every posting action
--
-- Design notes:
--   • is_active defaults FALSE — all templates ship OFF; explicit opt-in.
--   • auto_post TRUE → posts straight to status='posted' (no approval needed).
--     auto_post FALSE → posts as status='draft' for one-click approval.
--     Fixed-amount entries default auto_post=TRUE; formula/schedule default FALSE
--     because their amount changes each period and has not been reviewed yet.
--   • The UNIQUE on recurring_entry_postings (entity_id, template_id, posted_period_end)
--     prevents double-posting into the same period.
--
-- Safe to re-run (all CREATE … IF NOT EXISTS).

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -------------------------------------------------------------------
-- recurring_entry_templates
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recurring_entry_templates (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    description     TEXT,

    -- When set, identifies this as a system-defined standard template
    -- (e.g. 'dgip_forgiveness', 'percentage_rent', 'interest_accrual',
    -- 'depreciation'). NULL for user-defined custom entries.
    standard_key    TEXT,

    -- Amount source ------------------------------------------------
    -- 'fixed'    — constant amount (fixed_amount column)
    -- 'formula'  — computed from GL via safe_eval (formula_expr column)
    -- 'schedule' — amount supplied by a feeder module at post time
    --              (schedule_source names the feeder)
    calc_type       TEXT NOT NULL
        CHECK (calc_type IN ('fixed', 'formula', 'schedule')),
    fixed_amount    NUMERIC(14,2),  -- for calc_type='fixed'
    formula_expr    TEXT,           -- for calc_type='formula'; safe_eval expression
    schedule_source TEXT,           -- for calc_type='schedule'; e.g. 'fixed_asset_depreciation'

    -- Posting cadence -----------------------------------------------
    cadence         TEXT NOT NULL DEFAULT 'monthly'
        CHECK (cadence IN ('monthly', 'on_close', 'annual')),
    posting_day     INTEGER DEFAULT 1,  -- day of month (monthly cadence only)

    -- Autonomy ------------------------------------------------------
    is_active       BOOLEAN NOT NULL DEFAULT FALSE,
    auto_post       BOOLEAN NOT NULL DEFAULT FALSE,
    -- TRUE  → POST directly (status='posted'); suitable for fixed amounts.
    -- FALSE → create DRAFT (status='draft'); bookkeeper approves each period.
    --         Forced FALSE for formula/schedule calc_types unless overridden.

    -- Tracking ------------------------------------------------------
    last_posted_at          TIMESTAMPTZ,
    last_posted_period_end  DATE,
    source_module           TEXT NOT NULL DEFAULT 'recurring_entry',
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (entity_id, name)
);

CREATE INDEX IF NOT EXISTS idx_recurring_templates_entity
    ON recurring_entry_templates (entity_id, is_active);

-- -------------------------------------------------------------------
-- recurring_entry_lines
-- Static Dr/Cr line definitions for fixed and formula templates.
-- Schedule-type templates have no static lines; their journal lines are
-- generated dynamically from the feeder module at post time.
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recurring_entry_lines (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_id UUID NOT NULL
        REFERENCES recurring_entry_templates(id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL,
    account_code TEXT NOT NULL,
    direction   TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    memo        TEXT,
    UNIQUE (template_id, line_number)
);

CREATE INDEX IF NOT EXISTS idx_recurring_lines_template
    ON recurring_entry_lines (template_id);

-- -------------------------------------------------------------------
-- recurring_entry_postings
-- Append-only audit log of every posting (auto-post or manual trigger).
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recurring_entry_postings (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    template_id         UUID NOT NULL
        REFERENCES recurring_entry_templates(id) ON DELETE RESTRICT,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    journal_batch_id    UUID REFERENCES journal_batches(id),
    posted_period_start DATE,
    posted_period_end   DATE,
    amount              NUMERIC(14,2),  -- grand total amount for the posting
    auto_posted         BOOLEAN NOT NULL DEFAULT FALSE,
    actor_email         TEXT,
    posted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Prevent double-posting into the same period
    UNIQUE (entity_id, template_id, posted_period_end)
);

CREATE INDEX IF NOT EXISTS idx_recurring_postings_entity
    ON recurring_entry_postings (entity_id, posted_period_end DESC);
CREATE INDEX IF NOT EXISTS idx_recurring_postings_template
    ON recurring_entry_postings (template_id);
