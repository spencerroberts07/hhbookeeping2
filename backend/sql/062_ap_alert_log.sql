-- Migration 062: AP due-date alert deduplication log.
-- One row per (entity, invoice, threshold_days) pair; the UNIQUE constraint
-- is the dedup guard. INSERT ... ON CONFLICT DO NOTHING fires each alert
-- at most once per threshold per invoice lifetime.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS ap_alert_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID NOT NULL REFERENCES entities(id),
    invoice_id      UUID NOT NULL,  -- references direct_vendor_ap_invoices.id
    threshold_days  INTEGER NOT NULL,  -- 7 or 3
    due_date        DATE NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    in_app_fired    BOOLEAN NOT NULL DEFAULT TRUE,
    email_fired     BOOLEAN NOT NULL DEFAULT FALSE,
    email_status    TEXT,           -- 'sent', 'skipped', 'error', NULL
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, invoice_id, threshold_days)
);

CREATE INDEX IF NOT EXISTS idx_ap_alert_log_entity_fired
    ON ap_alert_log (entity_id, fired_at DESC);
