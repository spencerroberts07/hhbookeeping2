import { api } from './client';

export interface PosImportRun {
  id: string;
  report_type: string;
  period_start: string;
  period_end: string;
  file_name: string;
  imported_at: string;
  actor_email: string;
}

export async function listPosRuns(params: {
  entity_code: string;
  period_start?: string;
  period_end?: string;
}): Promise<{ runs: PosImportRun[] }> {
  const res = await api.get('/api/pos-import/runs', { params });
  return res.data;
}

export async function getLatestAgedAr(
  entityCode: string,
): Promise<{
  snapshot_date: string | null;
  customers: Array<{
    customer_name: string;
    current: number;
    over_30: number;
    over_60: number;
    over_90: number;
    total: number;
  }>;
}> {
  const res = await api.get('/api/pos-import/aged-ar/latest', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getLatestInventoryValue(entityCode: string): Promise<{
  snapshot_date: string | null;
  inventory_value: number;
}> {
  const res = await api.get('/api/pos-import/inventory-value/latest', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getLatestPosFinancial(entityCode: string): Promise<{
  snapshot_period_end: string | null;
  total_sales: number;
}> {
  const res = await api.get('/api/pos-import/pos-financial/latest', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function importPos(
  reportType:
    | 'inventory-adjustment'
    | 'pos-financial'
    | 'inventory-value'
    | 'aged-ar'
    | 'ar-adjustment',
  input: {
    entity_code: string;
    actor_email: string;
    file: File;
    snapshot_date?: string;
  },
): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  if (input.snapshot_date) fd.append('snapshot_date', input.snapshot_date);
  const res = await api.post(`/api/pos-import/${reportType}`, fd);
  return res.data;
}

export async function validatePosFinancial(input: {
  entity_code: string;
  import_run_id: string;
}): Promise<unknown> {
  const res = await api.post('/api/pos-import/validate-pos-financial', input);
  return res.data;
}
