-- Migration 029: BookWize AI Assistant
--
-- Five tables back the conversational assistant. The assistant calls
-- Claude (haiku-4.5) per request, but every piece of structured state —
-- conversations, messages, learned memory, pending intents,
-- period-close observations — lives here.
--
-- Conventions:
--   - entity_code TEXT FKs to entities(entity_code) ON DELETE CASCADE
--   - All timestamps are TIMESTAMPTZ
--   - All ids are uuid_generate_v4()
--   - Status / role / type columns are TEXT with CHECK constraints so
--     a typo is a hard error, not silent corruption.
--
-- Safe to re-run.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Conversation = one chat session for one entity-user pair. New
-- conversation when the user opens the widget fresh; we don't append
-- forever to a single row.
CREATE TABLE IF NOT EXISTS assistant_conversations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code TEXT NOT NULL REFERENCES entities(entity_code) ON DELETE CASCADE,
    clerk_user_id TEXT,
    channel TEXT NOT NULL DEFAULT 'dashboard',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_message_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT assistant_conversations_channel_chk
        CHECK (channel IN ('dashboard', 'sms', 'api'))
);

CREATE INDEX IF NOT EXISTS idx_asst_conv_entity
    ON assistant_conversations (entity_code, started_at DESC);


-- Individual user/assistant message turns. resolved=true means the
-- user confirmed a proposed action (or the assistant answered a pure
-- query); the transaction_id / journal_batch_id link to whatever was
-- created or updated as a result.
CREATE TABLE IF NOT EXISTS assistant_messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id UUID NOT NULL
        REFERENCES assistant_conversations(id) ON DELETE CASCADE,
    entity_code TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    intent TEXT,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    transaction_id UUID,
    journal_batch_id UUID,
    -- JSON metadata for the proposed action (account codes, amounts,
    -- transaction match ids, etc.) so the /confirm endpoint can replay
    -- without re-parsing.
    proposal_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT assistant_messages_role_chk
        CHECK (role IN ('user', 'assistant', 'system')),
    CONSTRAINT assistant_messages_intent_chk
        CHECK (intent IS NULL OR intent IN (
            'classify_transaction',
            'query_balance',
            'add_note',
            'query_period',
            'general_question',
            'correction',
            'other'
        ))
);

CREATE INDEX IF NOT EXISTS idx_asst_msg_conv
    ON assistant_messages (conversation_id, created_at);

CREATE INDEX IF NOT EXISTS idx_asst_msg_entity_intent
    ON assistant_messages (entity_code, intent);

CREATE INDEX IF NOT EXISTS idx_asst_msg_unresolved
    ON assistant_messages (entity_code, resolved, created_at DESC)
    WHERE resolved = FALSE;


-- What the assistant has learned about this entity. The unique
-- constraint on (entity_code, memory_type, memory_key) makes upserts
-- straightforward and prevents drift across many message turns.
CREATE TABLE IF NOT EXISTS assistant_entity_memory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code TEXT NOT NULL REFERENCES entities(entity_code) ON DELETE CASCADE,
    memory_type TEXT NOT NULL,
    memory_key TEXT NOT NULL,
    memory_value TEXT NOT NULL,
    confidence NUMERIC(5,2) NOT NULL DEFAULT 100,
    times_confirmed INTEGER NOT NULL DEFAULT 1,
    times_corrected INTEGER NOT NULL DEFAULT 0,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_code, memory_type, memory_key),
    CONSTRAINT assistant_memory_type_chk
        CHECK (memory_type IN (
            'terminology',
            'vendor_account',
            'transaction_pattern',
            'month_end_pattern',
            'correction',
            'owner_preference',
            'app_improvement'
        )),
    CONSTRAINT assistant_memory_confidence_chk
        CHECK (confidence >= 0 AND confidence <= 100)
);

CREATE INDEX IF NOT EXISTS idx_asst_memory_entity_type
    ON assistant_entity_memory (entity_code, memory_type, confidence DESC);


-- "I paid $X for Y on date Z" with no matching bank txn yet. We hold
-- these for 7 days and try to match each time new bank transactions
-- arrive. expires_at + status='pending' makes the cleanup query cheap.
CREATE TABLE IF NOT EXISTS assistant_pending_intents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code TEXT NOT NULL REFERENCES entities(entity_code) ON DELETE CASCADE,
    conversation_id UUID
        REFERENCES assistant_conversations(id) ON DELETE SET NULL,
    original_message TEXT NOT NULL,
    parsed_amount NUMERIC(14,2),
    parsed_date DATE,
    parsed_description TEXT,
    suggested_account_code TEXT,
    suggested_account_name TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    matched_transaction_id UUID,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT assistant_pending_status_chk
        CHECK (status IN ('pending', 'matched', 'expired', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_asst_pending_entity_status
    ON assistant_pending_intents (entity_code, status, expires_at);

-- Used by the auto-match-on-upload pass.
CREATE INDEX IF NOT EXISTS idx_asst_pending_amount_date
    ON assistant_pending_intents (entity_code, parsed_amount, parsed_date)
    WHERE status = 'pending';


-- Period-close learning log. learn_from_period_close() writes rows
-- here when a period closes — counts, anomalies, timing observations.
-- The app-improvement insights generator reads them back.
CREATE TABLE IF NOT EXISTS assistant_period_observations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code TEXT NOT NULL REFERENCES entities(entity_code) ON DELETE CASCADE,
    period_end DATE NOT NULL,
    observation_type TEXT NOT NULL,
    observation TEXT NOT NULL,
    amount NUMERIC(14,2),
    account_code TEXT,
    severity TEXT NOT NULL DEFAULT 'info',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT assistant_period_obs_type_chk
        CHECK (observation_type IN (
            'journal_created',
            'variance_found',
            'document_late',
            'correction_made',
            'unusual_amount',
            'new_vendor',
            'close_duration_days',
            'app_improvement'
        )),
    CONSTRAINT assistant_period_obs_severity_chk
        CHECK (severity IN ('info', 'warning', 'anomaly'))
);

CREATE INDEX IF NOT EXISTS idx_asst_period_obs_entity
    ON assistant_period_observations (entity_code, period_end DESC);
