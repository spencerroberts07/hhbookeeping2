import { api } from './client';

export interface ChartAccount {
  code: string;
  name: string;
  type: string;
  normal_balance: 'debit' | 'credit' | string;
  is_active: boolean;
  source: 'accounts_table' | 'journal_lines_seed';
}

export interface ListAccountsResponse {
  entity_code: string;
  accounts: ChartAccount[];
  count: number;
  seeded_from: 'accounts_table' | 'journal_lines' | 'empty';
}

export async function listAccounts(
  entityCode: string,
): Promise<ListAccountsResponse> {
  const res = await api.get<ListAccountsResponse>('/api/accounts', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function upsertAccount(input: {
  entity_code: string;
  account_code: string;
  account_name: string;
  account_type: string;
  normal_balance?: 'debit' | 'credit';
  parent_code?: string | null;
}): Promise<ChartAccount> {
  const res = await api.post<ChartAccount>('/api/accounts', input);
  return res.data;
}
