import { api } from './client';

export interface CashBalancingLatest {
  business_date: string;
  opening_balance: number | null;
  closing_balance: number | null;
  total_deposits: number;
  total_withdrawals: number | null;
  variance: number | null;
  status: 'balanced' | 'review';
  tab_name: string | null;
}

/** Most-recent cash_balancing_days row. 404 if no rows exist for the entity. */
export async function getLatestCashBalancing(
  entityCode: string,
): Promise<CashBalancingLatest> {
  const res = await api.get<CashBalancingLatest>('/api/cash-balancing/latest', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
