CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE entities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    entity_code TEXT NOT NULL UNIQUE,
    entity_name TEXT NOT NULL,
    fiscal_year_end_month SMALLINT NOT NULL,
    fiscal_year_end_day SMALLINT NOT NULL,
    base_currency TEXT NOT NULL DEFAULT 'CAD',
    quickbooks_company_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE accounting_periods (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    period_label TEXT NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    UNIQUE (entity_id, period_start, period_end)
);

CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    account_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    account_class TEXT NOT NULL,
    statement_type TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    quickbooks_account_id TEXT,
    UNIQUE (entity_id, account_code)
);

CREATE TABLE vendors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    vendor_name TEXT NOT NULL,
    vendor_normalized TEXT NOT NULL,
    default_account_code TEXT,
    quickbooks_vendor_id TEXT,
    UNIQUE (entity_id, vendor_normalized)
);

CREATE TABLE source_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    file_name TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    mime_type TEXT,
    source_type TEXT NOT NULL,
    checksum_sha256 TEXT,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_status TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE normalized_documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_file_id UUID NOT NULL REFERENCES source_files(id) ON DELETE CASCADE,
    document_type TEXT NOT NULL,
    store_code TEXT,
    vendor_name TEXT,
    external_document_number TEXT,
    invoice_number TEXT,
    statement_number TEXT,
    document_date DATE,
    due_date DATE,
    subtotal NUMERIC(14,2),
    tax_amount NUMERIC(14,2),
    total_amount NUMERIC(14,2),
    extracted_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence_score NUMERIC(5,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE document_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    normalized_document_id UUID NOT NULL REFERENCES normalized_documents(id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL,
    description TEXT NOT NULL,
    quantity NUMERIC(14,4),
    unit_price NUMERIC(14,4),
    line_amount NUMERIC(14,2) NOT NULL,
    tax_amount NUMERIC(14,2),
    suggested_account_code TEXT,
    extracted_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE quickbooks_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    quickbooks_txn_id TEXT,
    txn_type TEXT NOT NULL,
    txn_date DATE,
    memo TEXT,
    counterparty_name TEXT,
    bank_memo TEXT,
    amount NUMERIC(14,2) NOT NULL,
    source_account_code TEXT,
    source_account_name TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE bank_feed_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    bank_account_code TEXT NOT NULL,
    transaction_date DATE NOT NULL,
    posted_date DATE,
    memo TEXT NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    direction TEXT NOT NULL,
    source_file_id UUID REFERENCES source_files(id),
    quickbooks_txn_id TEXT
);

CREATE TABLE cash_balancing_days (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    business_date DATE NOT NULL,
    tab_name TEXT,
    total_sales NUMERIC(14,2),
    total_hst NUMERIC(14,2),
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (entity_id, business_date)
);

CREATE TABLE cash_balancing_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cash_balancing_day_id UUID NOT NULL REFERENCES cash_balancing_days(id) ON DELETE CASCADE,
    line_code TEXT,
    line_label TEXT NOT NULL,
    amount NUMERIC(14,2) NOT NULL,
    mapped_account_code TEXT,
    translation_status TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE hh_statement_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    normalized_document_id UUID NOT NULL REFERENCES normalized_documents(id) ON DELETE CASCADE,
    statement_line_type TEXT,
    invoice_number TEXT,
    amount NUMERIC(14,2) NOT NULL,
    tax_amount NUMERIC(14,2),
    due_date DATE,
    matched_document_id UUID REFERENCES normalized_documents(id),
    status TEXT NOT NULL DEFAULT 'unmatched'
);

CREATE TABLE ecommerce_payout_cycles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    cycle_start DATE,
    cycle_end DATE,
    dealer_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    order_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
    fulfill_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
    fee_tax NUMERIC(14,2) NOT NULL DEFAULT 0,
    retail_total NUMERIC(14,2) NOT NULL DEFAULT 0,
    retail_tax NUMERIC(14,2) NOT NULL DEFAULT 0,
    payout_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    payout_status TEXT NOT NULL DEFAULT 'open',
    source_file_id UUID REFERENCES source_files(id)
);

CREATE TABLE payroll_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    pay_period_start DATE,
    pay_period_end DATE,
    pay_date DATE,
    gross_wages NUMERIC(14,2),
    deductions_total NUMERIC(14,2),
    employer_burden_total NUMERIC(14,2),
    net_pay NUMERIC(14,2),
    source_file_id UUID REFERENCES source_files(id),
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE posting_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    rule_code TEXT NOT NULL,
    area TEXT NOT NULL,
    source TEXT NOT NULL,
    trigger_match TEXT NOT NULL,
    posting_logic TEXT NOT NULL,
    key_accounts TEXT[] NOT NULL DEFAULT '{}',
    auto_level TEXT NOT NULL,
    review_tier TEXT NOT NULL,
    exception_logic TEXT,
    evidence TEXT,
    conditions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    outputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    UNIQUE (entity_id, rule_code)
);

CREATE TABLE recurring_month_end_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    rule_code TEXT NOT NULL,
    entry_name TEXT NOT NULL,
    frequency TEXT NOT NULL,
    debit_account_code TEXT NOT NULL,
    credit_account_code TEXT NOT NULL,
    default_amount NUMERIC(14,2),
    logic TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (entity_id, rule_code)
);

CREATE TABLE rule_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    run_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE suggested_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    source_rule_code TEXT,
    entry_date DATE NOT NULL,
    memo TEXT NOT NULL,
    review_status TEXT NOT NULL DEFAULT 'draft',
    confidence_score NUMERIC(5,2),
    source_refs JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE suggested_entry_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    suggested_entry_id UUID NOT NULL REFERENCES suggested_entries(id) ON DELETE CASCADE,
    account_code TEXT NOT NULL,
    description TEXT,
    debit NUMERIC(14,2) NOT NULL DEFAULT 0,
    credit NUMERIC(14,2) NOT NULL DEFAULT 0
);

CREATE TABLE exception_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    exception_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    status TEXT NOT NULL DEFAULT 'open',
    source_ref TEXT,
    summary TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE close_checklist_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    item_code TEXT NOT NULL,
    item_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    notes TEXT
);

CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID REFERENCES entities(id),
    actor_email TEXT,
    action TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
