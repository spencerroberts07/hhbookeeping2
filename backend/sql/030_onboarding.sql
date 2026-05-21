-- Migration 030: Dealer onboarding infrastructure
--
-- Two new tables:
--   background_jobs    — async job tracking for long-running imports
--                        (GL history pulls can take minutes; the route
--                        returns immediately with a job_id and the frontend
--                        polls progress).
--   oauth_state_cache  — short-lived CSRF tokens for the QBO OAuth flow.
--                        Previously /connect generated a state value that
--                        /callback never verified. Now /connect persists
--                        it and /callback consumes-and-deletes.
--
-- Plus two flag columns on entities so the dashboard / banner / status
-- endpoint can answer "is this entity fully onboarded yet?" without
-- recomputing across journal_batches every render.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE IF NOT EXISTS background_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID REFERENCES entities(id) ON DELETE CASCADE,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    pct_complete INTEGER NOT NULL DEFAULT 0,
    current_step TEXT,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    actor_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT background_jobs_status_chk
        CHECK (status IN ('pending', 'running', 'complete', 'error')),
    CONSTRAINT background_jobs_pct_chk
        CHECK (pct_complete BETWEEN 0 AND 100)
);

CREATE INDEX IF NOT EXISTS idx_background_jobs_entity_status
    ON background_jobs (entity_id, status);

CREATE INDEX IF NOT EXISTS idx_background_jobs_created
    ON background_jobs (created_at DESC);


CREATE TABLE IF NOT EXISTS oauth_state_cache (
    state TEXT PRIMARY KEY,
    entity_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes'
);

CREATE INDEX IF NOT EXISTS idx_oauth_state_expires
    ON oauth_state_cache (expires_at);


ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS onboarding_complete BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS onboarding_completed_at TIMESTAMPTZ;
