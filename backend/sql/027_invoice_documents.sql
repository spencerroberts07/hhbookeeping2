-- Migration 027: Invoice audit trail (upload → match → post → drill-down)
--
-- Two invoice streams (per spec):
--   1. 'hh_ap'           — individual HH AP invoice PDFs, matched to rows
--                          on the monthly statement (hh_ap_invoices) by
--                          invoice/PO number. Posts to 2030 AP HHSL.
--   2. 'outside_vendor'  — non-HH vendor invoices, matched to
--                          bank_transactions by amount + date (±30d).
--                          Posts to 2020 Accounts Payable.
--
-- Schema notes (deviations from spec — see commit message):
--   - The spec referenced `journal_entries`. The actual schema uses
--     `journal_batches` (parent) + `journal_lines` (children). A logical
--     journal entry == one batch row, so invoice_journal_links.journal_entry_id
--     FKs to journal_batches(id).
--   - VARCHAR(N) replaced by TEXT, TIMESTAMP replaced by TIMESTAMPTZ, and
--     gen_random_uuid() replaced by uuid_generate_v4() — all to match the
--     conventions every prior migration uses. pgcrypto and uuid-ossp are
--     both already installed (see #002 and #004) so either generator works.
--   - Decimal precisions widened from (12,2) to (14,2) to match every other
--     money column in the database.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS invoice_documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code TEXT NOT NULL REFERENCES entities(entity_code) ON DELETE RESTRICT,
    invoice_type TEXT NOT NULL,
    invoice_number TEXT,
    vendor_name TEXT,
    invoice_date DATE,
    due_date DATE,
    amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'CAD',
    status TEXT NOT NULL DEFAULT 'unmatched',
    ap_account TEXT,
    file_path TEXT,
    file_name TEXT,
    file_size_bytes INTEGER,
    source_hash TEXT,
    uploaded_by_clerk_user_id TEXT,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    matched_at TIMESTAMPTZ,
    matched_by_clerk_user_id TEXT,
    match_confidence NUMERIC(5,2),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invoice_documents_type_chk
        CHECK (invoice_type IN ('hh_ap','outside_vendor')),
    CONSTRAINT invoice_documents_status_chk
        CHECK (status IN ('unmatched','matched','posted_to_ap','deleted')),
    CONSTRAINT invoice_documents_ap_account_chk
        CHECK (ap_account IS NULL OR ap_account IN ('2020','2030')),
    CONSTRAINT invoice_documents_confidence_chk
        CHECK (match_confidence IS NULL OR (match_confidence >= 0 AND match_confidence <= 100))
);

-- Source-hash uniqueness within an entity prevents accidentally re-uploading
-- the same PDF twice. NULL hashes are allowed (e.g. when the upload bypassed
-- the hashing path for some reason).
CREATE UNIQUE INDEX IF NOT EXISTS ux_invoice_documents_entity_source_hash
    ON invoice_documents (entity_code, source_hash)
    WHERE source_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_invoice_documents_entity
    ON invoice_documents (entity_code);

CREATE INDEX IF NOT EXISTS idx_invoice_documents_entity_status
    ON invoice_documents (entity_code, status);

-- Used by the outside-vendor matcher (amount + date window).
CREATE INDEX IF NOT EXISTS idx_invoice_documents_amount_date
    ON invoice_documents (entity_code, amount, invoice_date);

CREATE INDEX IF NOT EXISTS idx_invoice_documents_invoice_number
    ON invoice_documents (entity_code, invoice_number)
    WHERE invoice_number IS NOT NULL;


CREATE TABLE IF NOT EXISTS invoice_journal_links (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_document_id UUID NOT NULL
        REFERENCES invoice_documents(id) ON DELETE CASCADE,
    entity_code TEXT NOT NULL,
    journal_batch_id UUID
        REFERENCES journal_batches(id) ON DELETE SET NULL,
    journal_line_id UUID
        REFERENCES journal_lines(id) ON DELETE SET NULL,
    bank_transaction_id UUID
        REFERENCES bank_transactions(id) ON DELETE SET NULL,
    hh_ap_invoice_id UUID
        REFERENCES hh_ap_invoices(id) ON DELETE SET NULL,
    link_type TEXT NOT NULL,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    linked_by TEXT NOT NULL DEFAULT 'auto',
    confidence NUMERIC(5,2),
    CONSTRAINT invoice_journal_links_link_type_chk
        CHECK (link_type IN ('journal','bank','hh_ap')),
    CONSTRAINT at_least_one_link CHECK (
        journal_batch_id IS NOT NULL OR
        bank_transaction_id IS NOT NULL OR
        hh_ap_invoice_id IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_invoice_journal_links_invoice
    ON invoice_journal_links (invoice_document_id);

CREATE INDEX IF NOT EXISTS idx_invoice_journal_links_batch
    ON invoice_journal_links (journal_batch_id)
    WHERE journal_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_invoice_journal_links_bank
    ON invoice_journal_links (bank_transaction_id)
    WHERE bank_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_invoice_journal_links_hh_ap
    ON invoice_journal_links (hh_ap_invoice_id)
    WHERE hh_ap_invoice_id IS NOT NULL;
