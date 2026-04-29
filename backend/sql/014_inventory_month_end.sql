-- Migration 014: Inventory adjustment + month-end POS imports
--
-- Captures the four reports the store POS exports at month end:
--
--   inventory_adjustment  -- store_use, donation, etc. line-level
--   pos_financial         -- payment-type / sales / COGS / HST totals
--   inventory_value       -- snapshot of stocked inventory cost & retail
--   aged_ar               -- house-account aging buckets
--
-- pos_import_runs is the parent envelope (one per uploaded report).
-- The four type-specific tables hold the parsed data:
--
--   inventory_adjustment_lines      one row per adjusted SKU
--   inventory_value_snapshots       one row per snapshot date
--   aged_ar_snapshots               one row per snapshot date
--   pos_financial_snapshots         one row per (period_start, period_end)
--
-- The store_use and donation journal builders read
-- inventory_adjustment_lines for a given pos_import_runs row and write a
-- balanced journal_batches row (Dr expense / Cr inventory).
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS pos_import_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    report_type TEXT NOT NULL,
        -- 'inventory_adjustment' | 'pos_financial'
        -- | 'inventory_value' | 'aged_ar'
    period_start DATE,
    period_end DATE,
    file_name TEXT NOT NULL,
    adjustment_reason TEXT,  -- only for inventory_adjustment imports
    total_amount NUMERIC(15,2),
    row_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'imported',
    parsed_data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_text TEXT,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pos_import_runs_report_type_chk
        CHECK (report_type IN (
            'inventory_adjustment',
            'pos_financial',
            'inventory_value',
            'aged_ar'
        )),
    CONSTRAINT pos_import_runs_status_chk
        CHECK (status IN ('imported','failed','superseded'))
);

CREATE INDEX IF NOT EXISTS idx_pos_import_runs_entity_period
    ON pos_import_runs (entity_id, period_end DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pos_import_runs_entity_type_created
    ON pos_import_runs (entity_id, report_type, created_at DESC);


CREATE TABLE IF NOT EXISTS inventory_adjustment_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES pos_import_runs(id) ON DELETE CASCADE,
    accounting_period_id UUID REFERENCES accounting_periods(id),

    sku_number TEXT NOT NULL,
    description TEXT,
    mfg_number TEXT,
    date_adjusted DATE,
    quantity_adjusted NUMERIC(12,4),
    quantity_after NUMERIC(12,4),
    adjustment_cost NUMERIC(15,4),
    adjustment_reason TEXT,
    reason_description TEXT,
    employee_id TEXT,
    journal_batch_id UUID REFERENCES journal_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inv_adj_lines_run
    ON inventory_adjustment_lines (import_run_id);

CREATE INDEX IF NOT EXISTS idx_inv_adj_lines_entity_reason_date
    ON inventory_adjustment_lines (entity_id, adjustment_reason, date_adjusted);

CREATE INDEX IF NOT EXISTS idx_inv_adj_lines_journal_batch
    ON inventory_adjustment_lines (journal_batch_id)
    WHERE journal_batch_id IS NOT NULL;


CREATE TABLE IF NOT EXISTS inventory_value_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES pos_import_runs(id) ON DELETE SET NULL,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    snapshot_date DATE NOT NULL,
    total_sku_count INTEGER,
    total_cost_value NUMERIC(15,2),
    total_retail_value NUMERIC(15,2),
    total_gm_dollars NUMERIC(15,2),
    total_gm_pct NUMERIC(8,4),
    department_breakdown_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT inventory_value_snapshots_unique
        UNIQUE (entity_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_inv_value_snapshots_entity_date
    ON inventory_value_snapshots (entity_id, snapshot_date DESC);


CREATE TABLE IF NOT EXISTS aged_ar_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES pos_import_runs(id) ON DELETE SET NULL,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    snapshot_date DATE NOT NULL,
    total_ar NUMERIC(15,2),
    current_amount NUMERIC(15,2),
    over_30 NUMERIC(15,2),
    over_60 NUMERIC(15,2),
    over_90 NUMERIC(15,2),
    over_120 NUMERIC(15,2),
    customer_detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT aged_ar_snapshots_unique
        UNIQUE (entity_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_aged_ar_snapshots_entity_date
    ON aged_ar_snapshots (entity_id, snapshot_date DESC);


CREATE TABLE IF NOT EXISTS pos_financial_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    import_run_id UUID REFERENCES pos_import_runs(id) ON DELETE SET NULL,
    accounting_period_id UUID REFERENCES accounting_periods(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- Tender side (debits, in POS terminology)
    cash_amount NUMERIC(15,2) NOT NULL DEFAULT 0,
    cheque_amount NUMERIC(15,2) NOT NULL DEFAULT 0,
    visa_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    mastercard_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    debit_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    amex_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    house_account_debit NUMERIC(15,2) NOT NULL DEFAULT 0,
    house_account_credit NUMERIC(15,2) NOT NULL DEFAULT 0,
    gift_card_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    ecommerce_net NUMERIC(15,2) NOT NULL DEFAULT 0,
    other_tender_json JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Sales / COGS / tax (credits, in POS terminology)
    merchandise_sales NUMERIC(15,2) NOT NULL DEFAULT 0,
    non_merchandise_sales NUMERIC(15,2) NOT NULL DEFAULT 0,
    cogs_merchandise NUMERIC(15,2) NOT NULL DEFAULT 0,
    cogs_non_merchandise NUMERIC(15,2) NOT NULL DEFAULT 0,
    hst_collected NUMERIC(15,2) NOT NULL DEFAULT 0,
    hst_5pct NUMERIC(15,2) NOT NULL DEFAULT 0,

    total_debit_side NUMERIC(15,2) NOT NULL DEFAULT 0,
    total_credit_side NUMERIC(15,2) NOT NULL DEFAULT 0,
    is_balanced BOOLEAN NOT NULL DEFAULT FALSE,
    raw_parsed_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pos_financial_snapshots_unique
        UNIQUE (entity_id, period_start, period_end)
);

CREATE INDEX IF NOT EXISTS idx_pos_financial_snapshots_entity_period
    ON pos_financial_snapshots (entity_id, period_end DESC);
