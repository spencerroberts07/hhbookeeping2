-- Migration 061: Vendor EFT payment file tracking.
-- Clones the shape of payroll_eft_files (migration 036) but entity-scoped
-- (not FK-bound to payroll_runs). One row per generated CPA-005 file.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS vendor_eft_files (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID NOT NULL REFERENCES entities(id),
    file_name           TEXT NOT NULL,
    file_path           TEXT,              -- R2 object key; NULL if R2 upload failed (fail-tolerant)
    record_count        INTEGER NOT NULL DEFAULT 0,
    total_amount        NUMERIC(14,2) NOT NULL DEFAULT 0,
    file_creation_number INTEGER NOT NULL DEFAULT 1,
    payment_date        DATE NOT NULL,
    invoice_ids         JSONB NOT NULL DEFAULT '[]'::jsonb,  -- array of direct_vendor_ap_invoice UUIDs
    vendor_count        INTEGER NOT NULL DEFAULT 0,
    summary_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    actor_email         TEXT,
    status              TEXT NOT NULL DEFAULT 'generated',
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vendor_eft_files_entity_generated
    ON vendor_eft_files (entity_id, generated_at DESC);

-- After migration 060 is applied, add the FK from direct_vendor_ap_invoices
-- to vendor_eft_files. Guard with a DO block so re-running is idempotent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_direct_vendor_ap_invoices_payment_file'
          AND table_name = 'direct_vendor_ap_invoices'
    ) THEN
        ALTER TABLE direct_vendor_ap_invoices
            ADD CONSTRAINT fk_direct_vendor_ap_invoices_payment_file
            FOREIGN KEY (payment_file_id) REFERENCES vendor_eft_files(id)
            ON DELETE SET NULL;
    END IF;
END $$;
