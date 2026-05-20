import { api } from './client';

export type PlanTier = 'starter' | 'professional';

export interface SubscriptionInfo {
  status: 'trialing' | 'active' | 'past_due' | 'canceled' | 'incomplete' | null;
  plan_tier: PlanTier | null;
  current_period_end: string | null;
  trial_end: string | null;
  cancel_at_period_end: boolean;
  store_count: number;
  /** Stripe customer id — useful for support handoff, never display to users. */
  customer_id: string | null;
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
