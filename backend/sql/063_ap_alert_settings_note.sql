-- Migration 063: AP alert settings (per-entity preferences).
-- Settings are stored in entities.notification_preferences JSONB (already
-- established by migration 032) under the key "ap_alerts". No schema
-- change is needed — JSONB is already nullable with no constraint.
--
-- Expected structure of notification_preferences->>'ap_alerts':
--   {
--     "email_enabled": true,
--     "remittance_advice_enabled": true,
--     "thresholds": [7, 3]
--   }
--
-- Default behaviour (when key absent): email_enabled=true,
-- remittance_advice_enabled=true, thresholds=[7,3].
--
-- This migration is documentation-only: it seeds a comment so DBAs
-- know the JSONB schema is stable.

COMMENT ON COLUMN entities.notification_preferences IS
    'Per-entity notification toggles. Keys: month_end_reminders, variance_alerts, '
    'approval_requests, payment_receipts (legacy), ap_alerts '
    '({"email_enabled":bool,"remittance_advice_enabled":bool,"thresholds":[7,3]}).';
