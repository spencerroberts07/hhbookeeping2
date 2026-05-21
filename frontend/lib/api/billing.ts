import { api } from './client';

// 'internal' is a non-Stripe tier for owner + demo accounts. The
// backend short-circuits every Stripe call when plan_tier is internal
// and the billing UI hides itself accordingly.
//
// TODO: Replace with real Stripe subscription when an internal account
// is ready to be billed. Delete the billing_subscriptions row with
// plan_tier='internal' and run through /settings/billing checkout flow.
export type PlanTier = 'starter' | 'professional' | 'internal';

export interface SubscriptionInfo {
  status: 'trialing' | 'active' | 'past_due' | 'canceled' | 'incomplete' | null;
  plan_tier: PlanTier | null;
  current_period_end: string | null;
  trial_end: string | null;
  cancel_at_period_end: boolean;
  store_count: number;
  /** Stripe customer id — useful for support handoff, never display to users. */
  customer_id: string | null;
  /** Present only when plan_tier='internal' — explains why the billing UI is suppressed. */
  message?: string;
}

/** POST /api/billing/checkout-session — kicks off the Stripe Checkout flow. */
export async function createCheckoutSession(input: {
  entity_code: string;
  plan_tier: PlanTier;
  success_url: string;
  cancel_url: string;
}): Promise<{ url: string; session_id: string }> {
  const res = await api.post('/api/billing/checkout-session', input);
  return res.data;
}

/** POST /api/billing/portal-session — opens the Stripe Customer Portal for self-serve billing. */
export async function createPortalSession(input: {
  entity_code: string;
  return_url: string;
}): Promise<{ url: string }> {
  const res = await api.post('/api/billing/portal-session', input);
  return res.data;
}

/** GET /api/billing/subscription — current subscription state for the entity. */
export async function getSubscription(
  entityCode: string,
): Promise<SubscriptionInfo> {
  const res = await api.get('/api/billing/subscription', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
