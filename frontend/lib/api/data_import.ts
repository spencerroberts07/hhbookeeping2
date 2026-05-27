import { api } from './client';

export interface ChartSyncStatus {
  entity_code: string;
  last_synced_at: string | null;
  accounts_count: number;
  qbo_mapped_count: number;
  qbo_connected: boolean;
  qbo_realm_id: string | null;
}

export async function getChartSyncStatus(
  entityCode: string,
): Promise<ChartSyncStatus> {
  const res = await api.get<ChartSyncStatus>(
    '/api/data-import/chart-sync-status',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}
