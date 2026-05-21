import { api } from './client';

export interface QboBankAccount {
  account_name: string;
  account_code: string;
  current_balance: number;
  currency: string;
}

export interface QboBankBalancesResponse {
  entity_code: string;
  connected: boolean;
  accounts: QboBankAccount[];
  total_balance: number;
  currency: string;
  fetched_at: string | null;
  error?: string;
}

export async function getQboBankBalances(
  entityCode: string,
): Promise<QboBankBalancesResponse> {
  const res = await api.get<QboBankBalancesResponse>(
    '/api/qbo/bank-balances',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}
