-- Migration 060: Extend direct_vendor_ap_invoices with payment lifecycle fields.
-- Adds: payment_pending status support, link to EFT file, link back to
-- originating invoice_document, and link to the vendor master.
-- Additive only: ADD COLUMN IF NOT EXISTS.

-- Link to the vendor master (set by the invoice-upload bridge)
ALTER TABLE direct_vendor_ap_invoices
    ADD COLUMN IF NOT EXISTS vendor_id UUID REFERENCES vendors(id);

-- Bridge to the originating invoice_documents row (set at upload time)
ALTER TABLE direct_vendor_ap_invoices
    ADD COLUMN IF NOT EXISTS source_invoice_document_id UUID;

-- EFT payment file lifecycle
ALTER TABLE direct_vendor_ap_invoices
    ADD COLUMN IF NOT EXISTS payment_file_id UUID;      -- FK to vendor_eft_files.id (set after table 061 is created)

ALTER TABLE direct_vendor_ap_invoices
    ADD COLUMN IF NOT EXISTS payment_pending_at TIMESTAMPTZ;

-- Index to accelerate due-date alert queries
CREATE INDEX IF NOT EXISTS idx_direct_vendor_ap_invoices_entity_status_due
    ON direct_vendor_ap_invoices (entity_id, status, due_date)
    WHERE status NOT IN ('paid', 'void');
