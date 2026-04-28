CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS direct_vendor_ap_invoices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID REFERENCES accounting_periods(id),
    vendor_name TEXT NOT NULL,
    vendor_code TEXT,
    invoice_number TEXT NOT NULL,
    invoice_date DATE NOT NULL,
    due_date DATE,
    received_date DATE,
    currency_code TEXT NOT NULL DEFAULT 'CAD',
    subtotal_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    total_amount NUMERIC(14,2) NOT NULL,
    paid_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    open_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    payment_status TEXT NOT NULL DEFAULT 'unpaid',
    priority TEXT NOT NULL DEFAULT 'normal',
    source_document_name TEXT,
    note TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    last_payment_date DATE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, vendor_name, invoice_number)
);

CREATE INDEX IF NOT EXISTS idx_direct_vendor_ap_invoices_entity_invoice_date
    ON direct_vendor_ap_invoices(entity_id, invoice_date);

CREATE INDEX IF NOT EXISTS idx_direct_vendor_ap_invoices_entity_due_status
    ON direct_vendor_ap_invoices(entity_id, due_date, status, payment_status);

CREATE TABLE IF NOT EXISTS direct_vendor_ap_invoice_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    invoice_id UUID NOT NULL REFERENCES direct_vendor_ap_invoices(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    actor_email TEXT,
    from_status TEXT,
    to_status TEXT,
    note TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_direct_vendor_ap_invoice_events_invoice_created
    ON direct_vendor_ap_invoice_events(invoice_id, created_at);
