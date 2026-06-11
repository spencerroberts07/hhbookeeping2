-- Migration 067: AR aging — write-down audit link
--
-- Adds an optional back-link from ar_adjustment_lines to the aged_ar_snapshots
-- row that the write-down was raised against.  NULL for rows created via the
-- AR Transaction List import path (which has no snapshot) or legacy rows.
-- Additive only — no DROP, no NOT NULL constraint.

ALTER TABLE ar_adjustment_lines
    ADD COLUMN IF NOT EXISTS aged_ar_snapshot_id UUID
        REFERENCES aged_ar_snapshots(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_ar_adj_lines_snapshot
    ON ar_adjustment_lines (aged_ar_snapshot_id)
    WHERE aged_ar_snapshot_id IS NOT NULL;
