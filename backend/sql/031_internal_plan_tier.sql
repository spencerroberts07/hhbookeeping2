-- Migration 031: 'internal' plan tier for owner + demo accounts
--
-- 'internal' is a non-Stripe tier with all Professional features. It
-- exists so owner accounts (BookWize's own books) and demo stores can
-- run on the platform without a billing relationship.
--
-- This migration:
--   1. Widens the billing_subscriptions.plan_tier CHECK to include 'internal'.
--   2. Creates a single internal billing_customers row that owns all
--      internal subscriptions ('internal_owner').
--   3. Seeds the Bridlewood (1877-8) subscription as internal.
--
-- TODO: Replace with real Stripe subscription when owner is ready to
-- be billed. Simply delete the billing_subscriptions row with
-- plan_tier='internal' and run through /settings/billing checkout flow.
--
-- Safe to re-run.

ALTER TABLE billing_subscriptions
    DROP CONSTRAINT IF EXISTS billing_subscriptions_plan_chk;

ALTER TABLE billing_subscriptions
    ADD CONSTRAINT billing_subscriptions_plan_chk
    CHECK (plan_tier IN ('starter', 'professional', 'internal'));


-- One billing_customers row backs every internal subscription. We
-- reuse 'internal_owner' as both clerk_user_id and stripe_customer_id
-- so it's easy to spot in admin queries. Neither value actually
-- exists in Clerk or Stripe — the service layer short-circuits any
-- API call before it would resolve.
INSERT INTO billing_customers (clerk_user_id, stripe_customer_id, name)
VALUES (
    'internal_owner',
    'internal_owner',
    'BookWize Internal — Owner & Demo Accounts'
)
ON CONFLICT (clerk_user_id) DO NOTHING;


-- Seed Bridlewood (the owner's own store) as internal.
-- stripe_subscription_id is namespaced by entity_code so multiple
-- internal subscriptions don't collide on the unique constraint.
INSERT INTO billing_subscriptions (
    entity_id,
    billing_customer_id,
    stripe_subscription_id,
    plan_tier,
    status,
    current_period_end,
    cancel_at_period_end
)
SELECT
    e.id,
    c.id,
    'internal_owner:' || e.entity_code,
    'internal',
    'active',
    NOW() + INTERVAL '100 years',
    FALSE
FROM entities e
CROSS JOIN billing_customers c
WHERE e.entity_code = '1877-8'
  AND c.clerk_user_id = 'internal_owner'
ON CONFLICT (entity_id) DO NOTHING;
