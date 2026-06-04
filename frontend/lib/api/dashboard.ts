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

export interface QuickbooksConnectResponse {
  entity_code: string;
  authorization_url: string;
  state: string;
}

export async function startQuickbooksConnect(
  entityCode: string,
): Promise<QuickbooksConnectResponse> {
  const res = await api.get<QuickbooksConnectResponse>(
    '/api/auth/quickbooks/connect',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

export interface QuickbooksDisconnectResponse {
  entity_code: string;
  disconnected_connections: Array<{ id: string; realm_id: string }>;
  account_mappings_cleared: number;
}

export async function disconnectQuickbooks(
  entityCode: string,
): Promise<QuickbooksDisconnectResponse> {
  const res = await api.post<QuickbooksDisconnectResponse>(
    '/api/auth/quickbooks/disconnect',
    { entity_code: entityCode },
  );
  return res.data;
}

export interface GrossMarginResponse {
  entity_code: string;
  period_end: string | null;
  period_label?: string;
  sales: number;
  cogs: number;
  margin_pct: number;
  ttm_sales?: number;
  ttm_cogs?: number;
  ttm_margin_pct?: number;
}

export async function getGrossMargin(
  entityCode: string,
): Promise<GrossMarginResponse> {
  const res = await api.get<GrossMarginResponse>('/api/dashboard/gross-margin', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export interface SalesHistoryPoint {
  period_end: string;
  period_label: string;
  sales: number;
  cogs: number;
  margin_pct: number;
}

export interface SalesHistoryResponse {
  entity_code: string;
  months: number;
  series: SalesHistoryPoint[];
}

export async function getSalesHistory(
  entityCode: string,
  months = 24,
): Promise<SalesHistoryResponse> {
  const res = await api.get<SalesHistoryResponse>(
    '/api/dashboard/sales-history',
    { params: { entity_code: entityCode, months } },
  );
  return res.data;
}

export interface GlCashBalanceResponse {
  entity_code: string;
  account_code: string;
  balance: number;
}

export async function getGlCashBalance(
  entityCode: string,
): Promise<GlCashBalanceResponse> {
  const res = await api.get<GlCashBalanceResponse>(
    '/api/dashboard/gl-cash-balance',
    { params: { entity_code: entityCode } },
  );
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

// --------------------------------------------------------------------------
// Sales drill-down (Phase 2A). source: 'gl_net' (monthly/rolling, reconciles
// to the income statement) vs 'pos_gross' (daily/MTD, from cash balancing).
// --------------------------------------------------------------------------

export type SalesSource = 'gl_net' | 'pos_gross';

export interface SalesMonthlyPoint {
  period_end: string;
  period_label: string;
  sales: number;
  cogs: number;
  margin_pct: number;
  py_sales: number | null;
  py_cogs: number | null;
  py_margin_pct: number | null;
  yoy_growth_pct: number | null;
  mom_growth_pct: number | null;
}

export interface SalesMonthlyResponse {
  entity_code: string;
  source: SalesSource;
  months: number;
  series: SalesMonthlyPoint[];
}

export async function getSalesMonthly(
  entityCode: string,
  months = 24,
): Promise<SalesMonthlyResponse> {
  const res = await api.get<SalesMonthlyResponse>('/api/dashboard/sales/monthly', {
    params: { entity_code: entityCode, months },
  });
  return res.data;
}

export interface SalesRolling12Point {
  period_end: string;
  period_label: string;
  rolling12_sales: number | null;
  py_rolling12_sales: number | null;
  yoy_growth_pct: number | null;
}

export interface SalesRolling12Response {
  entity_code: string;
  source: SalesSource;
  months: number;
  series: SalesRolling12Point[];
}

export async function getSalesRolling12(
  entityCode: string,
  months = 24,
): Promise<SalesRolling12Response> {
  const res = await api.get<SalesRolling12Response>('/api/dashboard/sales/rolling12', {
    params: { entity_code: entityCode, months },
  });
  return res.data;
}

export interface SalesDailyPoint {
  date: string;
  sales: number;
  py_date: string;
  py_sales: number | null;
}

export interface SalesDailyResponse {
  entity_code: string;
  source: SalesSource;
  days: number;
  series: SalesDailyPoint[];
}

export async function getSalesDaily(
  entityCode: string,
  days = 90,
): Promise<SalesDailyResponse> {
  const res = await api.get<SalesDailyResponse>('/api/dashboard/sales/daily', {
    params: { entity_code: entityCode, days },
  });
  return res.data;
}

export interface SalesMtdResponse {
  entity_code: string;
  source: SalesSource;
  as_of: string;
  py_as_of: string;
  month_label: string;
  days_elapsed: number;
  mtd_sales: number;
  py_mtd_sales: number;
  yoy_growth_pct: number | null;
}

export async function getSalesMtd(entityCode: string): Promise<SalesMtdResponse> {
  const res = await api.get<SalesMtdResponse>('/api/dashboard/sales/mtd', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
