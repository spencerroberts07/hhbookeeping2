import { api } from './client';

export interface FixedAsset {
  id: string;
  asset_code: string;
  description: string;
  acquired_date: string;
  cost: number;
  useful_life_months: number;
  is_half_year: boolean;
  is_active: boolean;
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
