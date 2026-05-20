/**
 * Admin portal API surface — stubs only.
 *
 * The dealer-list, MRR, impersonation, and entity-org mapping endpoints
 * are not built on the backend yet. Per q21, this module returns realistic
 * mock data so the (admin) tree renders end-to-end.
 */

export interface DealerListRow {
  entity_code: string;
  store_name: string;
  province: string;
  plan_tier: 'starter' | 'professional';
  mrr: number;
  last_active: string;
  month_end_status: 'open' | 'in_progress' | 'closed';
}

// TODO: backend endpoint not built — GET /api/admin/dealers
export async function listDealers(): Promise<{ dealers: DealerListRow[] }> {
  await new Promise((r) => setTimeout(r, 250));
  return {
    dealers: [
      {
        entity_code: '1877-8',
        store_name: 'Bridlewood Home Hardware',
        province: 'ON',
        plan_tier: 'professional',
        mrr: 149,
        last_active: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
        month_end_status: 'in_progress',
      },
    ],
  };
}

export interface RevenueSnapshot {
  mrr: number;
  arr: number;
  active_dealers: number;
  trialing_dealers: number;
  churn_last_30d: number;
  new_signups_last_30d: number;
}

// TODO: backend endpoint not built — GET /api/admin/revenue
export async function getRevenueSnapshot(): Promise<RevenueSnapshot> {
  await new Promise((r) => setTimeout(r, 250));
  return {
    mrr: 149,
    arr: 1788,
    active_dealers: 1,
    trialing_dealers: 0,
    churn_last_30d: 0,
    new_signups_last_30d: 1,
  };
}

// TODO: backend endpoint not built — POST /api/admin/entity-org-link
export async function linkEntityToOrg(_input: {
  entity_code: string;
  clerk_org_id: string;
}): Promise<{ ok: true }> {
  await new Promise((r) => setTimeout(r, 200));
  return { ok: true };
}
