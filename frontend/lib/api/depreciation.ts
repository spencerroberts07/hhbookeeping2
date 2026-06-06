import { api } from './client';

export interface FixedAsset {
  id: string;
  asset_code: string;
  description: string;
  acquisition_date: string | null;
  cost: string;
  opening_nbv: string;
  opening_nbv_date: string | null;
  is_active: boolean;
  disposal_date: string | null;
  disposal_proceeds: string | null;
  cca_class: string;
  cca_rate: string;
  asset_gl_account: string;
  accum_depn_gl_account: string;
  depn_expense_gl_account: string;
  notes: string | null;
}

export interface AssetClass {
  id: string;
  class_code: string;
  class_name: string;
  cca_rate: string;
  expense_account: string;
  accum_account: string;
  formula_expr: string | null;
  is_active: boolean;
  display_order: number;
}

export interface MonthlyClassAmount {
  class_id: string | null;
  class_code: string;
  class_name: string;
  expense_account: string;
  accum_account: string;
  amount: string;
}

export async function listFixedAssets(
  entityCode: string,
): Promise<{ assets: FixedAsset[] }> {
  const res = await api.get('/api/depreciation/assets', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function seedFixedAssets(input: {
  entity_code: string;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post('/api/depreciation/seed-assets', input);
  return res.data;
}

export async function generateDepreciationSchedule(input: {
  entity_code: string;
  fiscal_year: number;
  actor_email: string;
  half_year_asset_codes?: string[];
}): Promise<unknown> {
  const res = await api.post('/api/depreciation/generate-schedule', input);
  return res.data;
}

export async function buildDepreciationJournal(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post('/api/depreciation/build-journal', input);
  return res.data;
}

export async function getDepreciationSummary(
  entityCode: string,
  periodEnd: string,
): Promise<unknown> {
  const res = await api.get('/api/depreciation/summary', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

// ---- Module B additions ----

export async function listAssetClasses(
  entityCode: string,
): Promise<{ classes: AssetClass[]; count: number }> {
  const res = await api.get('/api/depreciation/classes', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function upsertAssetClass(input: {
  entity_code: string;
  class_code: string;
  class_name: string;
  cca_rate: number;
  expense_account: string;
  accum_account: string;
  formula_expr?: string | null;
  is_active?: boolean;
  display_order?: number;
}): Promise<{ id: string; class_code: string }> {
  const res = await api.post('/api/depreciation/classes', input);
  return res.data;
}

export async function seedAssetClasses(entityCode: string): Promise<unknown> {
  const res = await api.post('/api/depreciation/seed-classes', { entity_code: entityCode });
  return res.data;
}

export async function linkAssetClasses(entityCode: string): Promise<unknown> {
  const res = await api.post('/api/depreciation/link-classes', { entity_code: entityCode });
  return res.data;
}

export async function addFixedAsset(input: {
  entity_code: string;
  asset_code: string;
  description: string;
  fixed_asset_class_id: string;
  acquisition_date: string;
  cost: number;
  opening_nbv?: number | null;
  opening_nbv_date?: string | null;
  notes?: string | null;
  actor_email: string;
}): Promise<{ id: string; asset_code: string }> {
  const res = await api.post('/api/depreciation/add-asset', input);
  return res.data;
}

export async function getMonthlyAmounts(
  entityCode: string,
  periodEnd: string,
): Promise<{ classes: MonthlyClassAmount[]; grand_total_monthly: string }> {
  const res = await api.get('/api/depreciation/monthly-amounts', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function disposeAsset(input: {
  entity_code: string;
  fixed_asset_id: string;
  disposal_date: string;
  proceeds: number;
  proceeds_account: string;
  gain_account: string;
  loss_account: string;
  actor_email: string;
  dry_run?: boolean;
}): Promise<unknown> {
  const res = await api.post('/api/depreciation/dispose', input);
  return res.data;
}

export async function downloadScheduleExcel(
  entityCode: string,
  fiscalYear: number,
): Promise<{ url: string; filename: string }> {
  const res = await api.get('/api/depreciation/schedule/excel', {
    params: { entity_code: entityCode, fiscal_year: fiscalYear },
  });
  return res.data;
}
