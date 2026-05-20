-- Migration 025: Clerk authentication integration
--
-- Adds:
--   entities.clerk_org_id  - one Clerk org maps to exactly one entity
--   clerk_users            - mapping table populated by Clerk webhooks
--
-- Role hierarchy stays the one from migration 010:
--   viewer < bookkeeper < approver < admin
-- (superadmin remains a column on users; it is independent of Clerk and
--  cannot be granted by a Clerk org role.)
--
-- The auth path resolves entity_code and role like this:
--   1. Verify the Clerk session token (signed JWT)
--   2. Read user_id, active org_id, and org_role from the token claims
--   3. JOIN entities ON entities.clerk_org_id = $org_id -> entity_code
--   4. Translate org_role ('org:bookkeeper' etc.) to the app role
--
-- clerk_users.role is a *cached* copy of the last role we saw from a
-- webhook event, kept for offline admin tooling and audit. Live auth
-- decisions read from the token claim, not this table.
--
-- Safe to re-run.

ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS clerk_org_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_entities_clerk_org_id
    ON entities (clerk_org_id)
    WHERE clerk_org_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS clerk_users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clerk_user_id TEXT NOT NULL,
    entity_code TEXT REFERENCES entities(entity_code) ON DELETE SET NULL,
    role TEXT,
    email TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT clerk_users_role_chk
        CHECK (role IS NULL OR role IN ('viewer','bookkeeper','approver','admin'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_clerk_users_clerk_user_id
    ON clerk_users (clerk_user_id);

CREATE INDEX IF NOT EXISTS idx_clerk_users_entity_active
    ON clerk_users (entity_code, is_active);
