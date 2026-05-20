-- Migration 028: file_path columns for R2-stored uploads
--
-- The new services_storage.py uploads every received PDF / xlsx / ods /
-- csv to Cloudflare R2 and returns an object_key. This migration adds a
-- `file_path TEXT` column to every table where an upload-result row is
-- created so the object_key can be persisted alongside the parsed
-- metadata.
--
-- Tables that already have file_path:
--   invoice_documents (added in 027)
--   hh_ap_documents  (already present in 000 baseline)
--
-- Tables this migration adds file_path to:
--   hh_ap_statements
--   hh_ap_remittances
--   payroll_runs
--   pos_import_runs
--   gl_import_runs
--   bank_csv_import_runs
--
-- bank_pdf has no per-upload table — the parsed transactions land
-- directly in bank_transactions. We use the source_import_run_id pattern
-- there (via bank_csv_import_runs) for the same purpose.
--
-- Safe to re-run. No data is lost. NULL on the new columns is the
-- "uploaded before R2 was configured" sentinel.

ALTER TABLE hh_ap_statements
    ADD COLUMN IF NOT EXISTS file_path TEXT;

ALTER TABLE hh_ap_remittances
    ADD COLUMN IF NOT EXISTS file_path TEXT;

ALTER TABLE payroll_runs
    ADD COLUMN IF NOT EXISTS file_path TEXT;

ALTER TABLE pos_import_runs
    ADD COLUMN IF NOT EXISTS file_path TEXT;

ALTER TABLE gl_import_runs
    ADD COLUMN IF NOT EXISTS file_path TEXT;

ALTER TABLE bank_csv_import_runs
    ADD COLUMN IF NOT EXISTS file_path TEXT;
