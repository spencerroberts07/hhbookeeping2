/**
 * Ratio engine client (Phase 2C/2D). Read endpoints + admin config CRUD.
 */
import { api } from './client';

export type RatioFormat = 'ratio' | 'percent' | 'dollar' | 'days';

export interface RatioRow {
  key: string;
  label: string;
  category: string;
  format: RatioFormat;
  value: number | null;
  py_value: number | null;
  enabled: boolean;
  threshold_min: number | null;
  threshold_max: number | null;
  threshold_direction: string | null;
  breached: boolean;
  custom?: boolean;
  error?: string | null;
}

export interface RatiosResponse {
  entity_code: string;
  period_label: string;
  period_start: string;
  period_end: string;
  ttm_start: string;
  ttm_end: string;
  context: {
    ttm_ebitda: number;
    ttm_ebitda_excl_dgip: number;
    total_debt: number;
    overdraft_reclassified: number;
    equity_reclassified: number;
    balances_balanced: boolean;
    annual_debt_service: number;
    annual_debt_service_source: string;
    debt_service_breakdown: Record<string, number | string>;
    fixed_charges: number;
    fixed_charges_source: string;
    fixed_charges_breakdown: Record<string, number | string>;
  };
  ratios: RatioRow[];
}

export async function getRatios(entityCode: string, periodEnd?: string): Promise<RatiosResponse> {
  const params: Record<string, string> = { entity_code: entityCode };
  if (periodEnd) params.period_end = periodEnd;
  const res = await api.get<RatiosResponse>('/api/dashboard/ratios', { params });
  return res.data;
}

export interface RoleRow {
  role: string;
  account_code: string;
  account_name: string;
  account_type: string | null;
  account_subtype: string | null;
}

export async function getRatioRoles(entityCode: string): Promise<{ roles: RoleRow[] }> {
  const res = await api.get('/api/dashboard/ratios/roles', { params: { entity_code: entityCode } });
  return res.data;
}

export async function getRatioTokens(entityCode: string): Promise<{
  tokens: { totals: string[]; accounts: string[] };
  builtin_formulas: Record<string, { numerator_expr: string; denominator_expr: string; output_type: string }>;
}> {
  const res = await api.get('/api/dashboard/ratios/tokens', { params: { entity_code: entityCode } });
  return res.data;
}

export interface CustomRatio {
  key: string;
  label: string;
  numerator_expr: string;
  denominator_expr: string | null;
  output_type: string;
  enabled: boolean;
  threshold_min: number | null;
  threshold_max: number | null;
  threshold_direction: string | null;
}

export async function listCustomRatios(entityCode: string): Promise<{ custom: CustomRatio[] }> {
  const res = await api.get('/api/dashboard/ratios/custom', { params: { entity_code: entityCode } });
  return res.data;
}

export async function upsertRatioConfig(entityCode: string, body: {
  ratio_key: string; enabled: boolean;
  threshold_min: number | null; threshold_max: number | null; threshold_direction: string | null;
}) {
  return (await api.put('/api/dashboard/ratios/config', body, { params: { entity_code: entityCode } })).data;
}

export async function setRatioInput(entityCode: string, key: string, value: number) {
  return (await api.put('/api/dashboard/ratios/inputs', { key, value }, { params: { entity_code: entityCode } })).data;
}

export async function clearRatioInput(entityCode: string, key: string) {
  return (await api.delete(`/api/dashboard/ratios/inputs/${key}`, { params: { entity_code: entityCode } })).data;
}

export async function addRatioRole(entityCode: string, role: string, account_code: string) {
  return (await api.post('/api/dashboard/ratios/roles', { role, account_code }, { params: { entity_code: entityCode } })).data;
}

export async function removeRatioRole(entityCode: string, role: string, account_code: string) {
  return (await api.delete('/api/dashboard/ratios/roles', { params: { entity_code: entityCode }, data: { role, account_code } })).data;
}

export async function upsertCustomRatio(entityCode: string, body: CustomRatio) {
  return (await api.post('/api/dashboard/ratios/custom', body, { params: { entity_code: entityCode } })).data;
}

export async function deleteCustomRatio(entityCode: string, key: string) {
  return (await api.delete(`/api/dashboard/ratios/custom/${key}`, { params: { entity_code: entityCode } })).data;
}

export async function previewCustomRatio(entityCode: string, body: {
  key: string; label: string; numerator_expr: string; denominator_expr: string | null; output_type: string;
  enabled: boolean; threshold_min: number | null; threshold_max: number | null; threshold_direction: string | null;
}): Promise<{ ok: boolean; value?: number | null; error?: string }> {
  return (await api.post('/api/dashboard/ratios/custom/preview', body, { params: { entity_code: entityCode } })).data;
}
