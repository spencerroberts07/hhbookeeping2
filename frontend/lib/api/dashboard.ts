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
  const res = await api.get('/api/dashboard/quickbooks-status', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export interface GrossMarginResponse {
  entity_code: string;
  period_end: string | null;
  period_label?: string;
  sales: number;
  cogs: number;
  margin_pct: number;
}

export async function getGrossMargin(
  entityCode: string,
): Promise<GrossMarginResponse> {
  const res = await api.get<GrossMarginResponse>('/api/dashboard/gross-margin', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export type AlertSeverity = 'info' | 'warning' | 'error';

export interface DashboardAlert {
  type: string;
  severity: AlertSeverity;
  label: string;
  detail: string;
  href?: string;
}

export interface DashboardAlertsResponse {
  entity_code: string;
  alerts: DashboardAlert[];
  count: number;
}

export async function getDashboardAlerts(
  entityCode: string,
): Promise<DashboardAlertsResponse> {
  const res = await api.get<DashboardAlertsResponse>('/api/dashboard/alerts', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
