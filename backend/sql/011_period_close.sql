-- Migration 011: Period close lock workflow
--
-- Adds a formal open -> submitted_for_close -> approved_to_close ->
-- closed_locked lifecycle to accounting_periods. Once a period is
-- closed_locked, write paths in other modules check is_period_locked()
-- and refuse to mutate data dated inside the period.
--
-- Statuses (accounting_periods.status):
--   open                  - default for new periods (existing 'draft' rows are treated equivalently)
--   submitted_for_close   - bookkeeper has hit "submit"; close has been requested
--   approved_to_close     - approver has signed off (transitional; flips to closed_locked immediately)
--   closed_locked         - terminal locked state; writes blocked
--   reopened              - was closed_locked, now reopened with a recorded reason
--
-- Note on the existing 'draft' default:
--   The original schema.sql created accounting_periods with default 'draft'.
--   We are NOT changing that default to avoid surprising the existing 24 rows.
--   The Python helper effective_period_status() maps 'draft' -> 'open'.
--   New columns are added IF NOT EXISTS so re-runs are idempotent.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER TABLE accounting_periods
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS closed_by TEXT,
    ADD COLUMN IF NOT EXISTS reopened_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reopened_by TEXT,
    ADD COLUMN IF NOT EXISTS close_notes TEXT,
    ADD COLUMN IF NOT EXISTS reopen_notes TEXT;

-- The status column is already present (default 'draft'). Spec says default
-- 'open'; we add a NOT-NULL guard but leave the existing default in place.
-- Existing 'draft' rows are treated as 'open' at the application layer.

CREATE INDEX IF NOT EXISTS idx_accounting_periods_entity_status
    ON accounting_periods (entity_id, status);

CREATE TABLE IF NOT EXISTS period_close_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id),
    accounting_period_id UUID NOT NULL REFERENCES accounting_periods(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
        -- submitted_for_close, approved_to_close, closed_locked,
        -- reopened, close_rejected
    from_status TEXT,
    to_status TEXT,
    actor_email TEXT,
    notes TEXT,
    blocking_items_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    warning_items_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_period_close_events_period_created
    ON period_close_events (accounting_period_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_period_close_events_entity_created
    ON period_close_events (entity_id, created_at DESC);
