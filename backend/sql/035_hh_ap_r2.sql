-- Migration 035: r2_object_key on hh_ap_documents
--
-- The hh_ap_documents table is the only place in BookWize that stores
-- file bytes inline (16 MB / 278 rows for Bridlewood today — 84 % of
-- DB data). This migration adds the R2 object-key column so a parallel
-- migration script can move bytes to R2 and clear file_bytes per row.
--
-- TODO: Once every row has r2_object_key NOT NULL and file_bytes = NULL,
-- run `ALTER TABLE hh_ap_documents DROP COLUMN file_bytes;` in a
-- separate maintenance window to reclaim the table's row footprint.
-- The migrate_hh_ap_r2.py script reports when that's safe.
--
-- Safe to re-run.

ALTER TABLE hh_ap_documents
    ADD COLUMN IF NOT EXISTS r2_object_key TEXT;

CREATE INDEX IF NOT EXISTS idx_hh_ap_documents_r2_key
    ON hh_ap_documents (r2_object_key)
    WHERE r2_object_key IS NOT NULL;
