import { api } from './client';

// -------------------------------------------------------------------------
// Types
// -------------------------------------------------------------------------

export interface SalariedStaffItem {
  id?: string;
  employee_name: string;
  annual_salary: number;
  bonus: number;
  assumed_hours_per_period: number;
  sort_order?: number;
}

export interface WagePlannerSettings {
  id: string;
  entity_id: string;
  fiscal_year: number;
  target_wage_pct: string;
  forecast_sales_change: string;
  avg_hourly_wage: string;
  benefits_pct: string;
  distribution_basis: 'prior_year' | 'national_average';
  notes: string | null;
  salaried_staff: SalariedStaffItem[];
  created_at: string;
  updated_at: string;
}

export interface WagePlannerPeriod {
  period_number: number;
  period_start: string;
  period_end: string;
  pay_date: string | null;
  py_sales: string | null;
  forecast_sales: string | null;
  target_wage_dollars: string | null;
  target_hours: string | null;
  salaried_hours_pp: number;
  actual_sales: string | null;
  actual_gross_wages: string | null;
  actual_stat_pay: string | null;
  actual_hours: string | null;
  hours_over_under: string | null;
  adjusted_target_hours: string | null;
  actual_sales_per_hour: string | null;
  py_sales_per_hour: string | null;
  locked: boolean;
}

export interface WagePlannerSummary {
  forecast_annual_sales: string | null;
  target_annual_wage_dollars: string | null;
  cum_over_under: string | null;
  periods_locked: number;
  periods_remaining: number;
}

export interface WagePlannerPlan {
  settings: WagePlannerSettings | null;
  periods: WagePlannerPeriod[];
  summary: WagePlannerSummary;
}

export interface PayPeriodRow {
  id: string;
  fiscal_year: number;
  period_number: number;
  period_start: string;
  period_end: string;
  pay_date: string | null;
  source: string;
}

export interface WagePlannerSnapshot {
  id: string;
  fiscal_year: number;
  pay_period_number: number;
  status: 'generating' | 'ready' | 'failed';
  generated_at: string | null;
  generated_by: string | null;
  error_msg: string | null;
  has_file: boolean;
}

export interface MinWageEmployee {
  employee_id: string;
  full_name: string;
  current_rate: string;
  new_rate: string;
  delta_rate: string;
  current_biweekly_est: string;
  projected_biweekly_est: string;
  delta_biweekly_est: string;
}

export interface MinWageImpact {
  new_min_wage: string;
  affected_employees: number;
  employees: MinWageEmployee[];
  total_current_biweekly_est: string;
  total_projected_biweekly_est: string;
  total_delta_biweekly_est: string;
  total_delta_annual_est: string;
}

// -------------------------------------------------------------------------
// API calls
// -------------------------------------------------------------------------

export async function getWagePlannerSettings(
  entityCode: string,
  fiscalYear?: number,
): Promise<{ settings: WagePlannerSettings | null; fiscal_year: number }> {
  const res = await api.get('/api/wage-planner/settings', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}

export async function saveWagePlannerSettings(input: {
  entity_code: string;
  fiscal_year?: number;
  target_wage_pct: number;
  forecast_sales_change: number;
  avg_hourly_wage: number;
  benefits_pct: number;
  distribution_basis?: string;
  notes?: string | null;
  salaried_staff?: SalariedStaffItem[];
}): Promise<{ settings: WagePlannerSettings; fiscal_year: number }> {
  const res = await api.put('/api/wage-planner/settings', input);
  return res.data;
}

export async function getWagePlannerPlan(
  entityCode: string,
  fiscalYear?: number,
): Promise<WagePlannerPlan> {
  const res = await api.get('/api/wage-planner/plan', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}

export async function getPayPeriods(
  entityCode: string,
  fiscalYear?: number,
): Promise<{ fiscal_year: number; periods: PayPeriodRow[] }> {
  const res = await api.get('/api/wage-planner/pay-periods', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}

export async function backfillPayPeriods(
  entityCode: string,
): Promise<{ inserted: number; message: string }> {
  const res = await api.post('/api/wage-planner/pay-periods/backfill', {
    entity_code: entityCode,
  });
  return res.data;
}

export async function refreshPeriodActuals(input: {
  entity_code: string;
  fiscal_year?: number;
  period_number: number;
  payroll_run_id: string;
  actor_email?: string;
}): Promise<unknown> {
  const res = await api.post('/api/wage-planner/refresh', input);
  return res.data;
}

export async function applyPeriodOverride(input: {
  entity_code: string;
  fiscal_year?: number;
  period_number: number;
  actual_sales?: number | null;
  actual_gross_wages?: number | null;
  actual_stat_pay?: number | null;
  actual_hours?: number | null;
}): Promise<unknown> {
  const res = await api.post('/api/wage-planner/override', input);
  return res.data;
}

export async function getMinWageImpact(input: {
  entity_code: string;
  new_min_wage: number;
}): Promise<MinWageImpact> {
  const res = await api.post('/api/wage-planner/min-wage-impact', input);
  return res.data;
}

export async function getWagePlannerSnapshots(
  entityCode: string,
  fiscalYear?: number,
): Promise<{ fiscal_year: number; snapshots: WagePlannerSnapshot[] }> {
  const res = await api.get('/api/wage-planner/snapshots', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}

export async function getLatestSnapshot(
  entityCode: string,
  fiscalYear?: number,
): Promise<{ id: string; pay_period_number: number; status: string; generated_at: string | null; download_url: string | null }> {
  const res = await api.get('/api/wage-planner/snapshots/latest', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}

export async function getSnapshotDownloadUrl(
  snapshotId: string,
  entityCode: string,
): Promise<{ download_url: string | null; fallback?: string }> {
  const res = await api.get(`/api/wage-planner/snapshots/${snapshotId}/download`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function downloadFreshExcel(
  entityCode: string,
  fiscalYear?: number,
): Promise<{ url?: string; r2_key?: string; filename?: string }> {
  const res = await api.get('/api/wage-planner/excel', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
  });
  return res.data;
}
