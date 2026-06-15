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

// -------------------------------------------------------------------------
// Dashboard summary types + API call (Steps 5–7)
// -------------------------------------------------------------------------

export interface DashboardSummaryNearMinEmployee {
  employee_id: string;
  full_name: string;
  current_rate: string;
  gap_to_min: string;
  est_annual_raise_cost: string;
}

export interface DashboardSummarySalariedStaff {
  employee_name: string;
  annual_salary: string;
  bonus: string;
  annual_cost: string;
  per_period: string;
}

export interface DashboardSummaryTrendPoint {
  fy: number;
  period_number: number;
  label: string;
  actual_wage_pct: string;
  target_pct: string | null;
  prior_year_pct: string;
  actual_wages: string;
  actual_sales: string;
  basis: 'gl_6120' | 'runline_gross' | 'none';
}

export interface WagePlannerDashboardSummary {
  fiscal_year: number;
  settings_present: boolean;
  ytd: {
    start: string;
    end: string;
    periods_completed: number;
    periods_remaining: number;
  };
  card1_headline: {
    ytd_managed_wage_pct: string;
    target_wage_pct: string;
    prior_year_same_period_pct: string;
    wage_basis: 'gl_6120' | 'runline_gross' | 'none';
    prior_year_basis: 'gl_6120' | 'runline_gross' | 'none';
    health: 'green' | 'yellow' | 'red';
  };
  card2_forward_target: {
    next_unlocked_period_number: number | null;
    adjusted_target_hours: string | null;
    cum_over_under: string | null;
    color: 'emerald' | 'red' | 'muted';
  };
  card3_ytd_actuals: {
    actual_wages_ytd: string;
    target_wages_ytd: string;
    wages_variance: string;
    actual_sales_ytd: string;
    forecast_sales_ytd: string;
    sales_variance: string;
    wage_basis: 'gl_6120' | 'runline_gross' | 'none';
  };
  card4_salaried: {
    per_period: string;
    annual: string;
    pct_of_annual_target: string | null;
    staff: DashboardSummarySalariedStaff[];
  };
  card5_min_wage: {
    ontario_min_wage: string;
    alert_band: string;
    near_min_employees: DashboardSummaryNearMinEmployee[];
    affected_count: number;
    total_delta_annual_est: string;
  };
  trend: DashboardSummaryTrendPoint[];
}

export async function getWagePlannerDashboardSummary(
  entityCode: string,
  fiscalYear?: number,
): Promise<WagePlannerDashboardSummary> {
  const res = await api.get('/api/wage-planner/dashboard-summary', {
    params: { entity_code: entityCode, ...(fiscalYear ? { fiscal_year: fiscalYear } : {}) },
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
