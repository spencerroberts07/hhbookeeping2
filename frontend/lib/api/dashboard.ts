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
  /** False when the accounting period isn't closed yet — GL totals are
   *  incomplete; the UI renders these as gaps, not $0. */
  closed: boolean;
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

// --- 2B: metric trends (cash / inventory / AR balance, margin) ---

export interface AccountTrendPoint {
  period_end: string;
  period_label: string;
  balance: number;
  py_balance: number;
  yoy_growth_pct: number | null;
}

export interface AccountTrendResponse {
  entity_code: string;
  account_code: string;
  source: SalesSource;
  series: AccountTrendPoint[];
}

export async function getAccountTrend(
  entityCode: string,
  accountCode: string,
  months = 24,
): Promise<AccountTrendResponse> {
  const res = await api.get<AccountTrendResponse>('/api/dashboard/metric/account-trend', {
    params: { entity_code: entityCode, account_code: accountCode, months },
  });
  return res.data;
}

export interface MarginTrendPoint {
  period_end: string;
  period_label: string;
  closed: boolean;
  gross_margin_pct: number | null;
  operating_margin_pct: number | null;
  net_margin_pct: number | null;
  py_gross_margin_pct: number | null;
  py_operating_margin_pct: number | null;
  py_net_margin_pct: number | null;
}

export interface MarginTrendResponse {
  entity_code: string;
  source: SalesSource;
  months: number;
  series: MarginTrendPoint[];
}

export async function getMarginTrend(
  entityCode: string,
  months = 24,
): Promise<MarginTrendResponse> {
  const res = await api.get<MarginTrendResponse>('/api/dashboard/metric/margin-trend', {
    params: { entity_code: entityCode, months },
  });
  return res.data;
}

// --- As-of reference date ---
// Single source of truth for the "last closed_locked period" label used
// by AP, Margin, Ratios, and any other period-bound dashboard card.

export interface AsOfResponse {
  entity_code: string;
  /** ISO date string, null when no closed_locked period exists. */
  last_closed_period_end: string | null;
  /** Human label like "Feb 2026", null when no closed_locked period exists. */
  last_closed_period_label: string | null;
}

export async function getAsOf(entityCode: string): Promise<AsOfResponse> {
  const res = await api.get<AsOfResponse>('/api/dashboard/as-of', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
