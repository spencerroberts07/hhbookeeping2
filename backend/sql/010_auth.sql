-- Migration 010: Authentication + multi-tenant user management
--
-- Tables:
--   users                - human accounts; bcrypt password hashes
--   user_entity_roles    - per-entity RBAC; users can have a role on each entity
--   user_sessions        - issued JWTs (token_hash for revocation lookup)
--   auth_events          - login/logout/role-grant audit trail
--
-- Role hierarchy (low -> high):
--   viewer < bookkeeper < approver < admin < superadmin
--
-- Conventions:
--   - All UUIDs use uuid_generate_v4()
--   - actor_email is captured on every write that goes through the API layer;
--     auth_events.user_id is the *subject* of the event (whose account this is
--     about), so logins for unknown emails resolve to NULL user_id.
--   - active role for (user_id, entity_id) is the most recent row with
--     is_active = TRUE; revocation flips is_active to FALSE and stamps
--     revoked_at. Role changes are also logged as auth_events.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL,
    hashed_password TEXT NOT NULL,
    full_name TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_superadmin BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

-- Email is stored case-preserved; uniqueness is case-insensitive.
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_email_lower
    ON users (LOWER(email));

CREATE INDEX IF NOT EXISTS idx_users_active
    ON users (is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS user_entity_roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT user_entity_roles_role_chk
        CHECK (role IN ('viewer','bookkeeper','approver','admin'))
);

-- Only one active role per (user, entity); a new grant supersedes the old.
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_entity_roles_active
    ON user_entity_roles (user_id, entity_id)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_user_entity_roles_entity_active
    ON user_entity_roles (entity_id, is_active, role);

CREATE INDEX IF NOT EXISTS idx_user_entity_roles_user_active
    ON user_entity_roles (user_id, is_active);

CREATE TABLE IF NOT EXISTS user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    ip_address TEXT,
    user_agent TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_sessions_token_hash
    ON user_sessions (token_hash);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_active
    ON user_sessions (user_id, revoked_at, expires_at);

CREATE TABLE IF NOT EXISTS auth_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    entity_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    actor_email TEXT,
    ip_address TEXT,
    user_agent TEXT,
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auth_events_user_created
    ON auth_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_events_event_type_created
    ON auth_events (event_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_auth_events_entity_created
    ON auth_events (entity_id, created_at DESC)
    WHERE entity_id IS NOT NULL;
