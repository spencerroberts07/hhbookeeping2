-- Migration 032: per-entity notification preferences
--
-- One JSONB column on entities holds the toggle bag. Defaults below
-- match the four switches that exist on /settings/notifications today.
-- Adding a new toggle later means adding a key to this default + a
-- key to the Pydantic model in routes/entities.py — no migration
-- needed for additions.
--
-- Safe to re-run.

ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS notification_preferences JSONB
    NOT NULL DEFAULT '{
        "month_end_reminders": true,
        "variance_alerts": true,
        "approval_requests": true,
        "payment_receipts": true
    }'::jsonb;
