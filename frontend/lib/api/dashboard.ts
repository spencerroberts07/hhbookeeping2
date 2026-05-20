import { api } from './client';

export interface QuickbooksStatus {
  entity_code: string;
  is_connected: boolean;
  realm_id: string | null;
  company_name: string | null;
  last_synced_at: string | null;
}

export async function getQuickbooksStatus(
  entityCode: string,
): Promise<QuickbooksStatus> {
  const res = await api.get('/api/dashboard/quickbooks_status', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
