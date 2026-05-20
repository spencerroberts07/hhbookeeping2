-- Migration 026: entity creation surface + billing scaffolding
--
-- 1. entities.province (TEXT, nullable) - captured at onboarding step 1.
-- 2. entities.created_by_clerk_user_id (TEXT, nullable) - audit who created
--    the entity row, since under Clerk the legacy users.id no longer applies.
-- 3. billing_customers (one Stripe customer per dealer; a dealer can own
--    multiple entities under one customer).
-- 4. billing_subscriptions (one Stripe subscription per entity).
--
-- All idempotent / safe to re-run.

ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS province TEXT;

ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS created_by_clerk_user_id TEXT;

-- A dealer's billing identity. The dealer's primary Clerk user is the
-- "owner" of the customer record; entities owned by that dealer are linked
-- by clerk_user_id on the customer side, and by entity_code on the
-- subscription side.
CREATE TABLE IF NOT EXISTS billing_customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    clerk_user_id TEXT NOT NULL,
    stripe_customer_id TEXT NOT NULL,
    email TEXT,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_customers_clerk_user_id
    ON billing_customers (clerk_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_customers_stripe_id
    ON billing_customers (stripe_customer_id);

-- One subscription per entity (per-store billing model, per q28).
CREATE TABLE IF NOT EXISTS billing_subscriptions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    billing_customer_id UUID NOT NULL REFERENCES billing_customers(id) ON DELETE CASCADE,
    stripe_subscription_id TEXT NOT NULL,
    plan_tier TEXT NOT NULL,
    status TEXT NOT NULL,
    current_period_end TIMESTAMPTZ,
    trial_end TIMESTAMPTZ,
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT billing_subscriptions_plan_chk
        CHECK (plan_tier IN ('starter','professional')),
    CONSTRAINT billing_subscriptions_status_chk
        CHECK (status IN ('trialing','active','past_due','canceled','incomplete','unpaid','paused'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_subscriptions_stripe_id
    ON billing_subscriptions (stripe_subscription_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_subscriptions_entity
    ON billing_subscriptions (entity_id);

CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_customer
    ON billing_subscriptions (billing_customer_id);
