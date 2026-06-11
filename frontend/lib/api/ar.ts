import { api } from './client';

export interface ArBuckets {
  current: number;
  over_30: number;
  over_60: number;
  over_90: number;
  over_120: number;
}

export interface ArCustomer {
  customer_number: string | null;
  customer_name: string | null;
  total: number;
  current: number;
  over_30: number;
  over_60: number;
  over_90: number;
  over_120: number;
}

export interface ArSnapshot {
  id: string;
  snapshot_date: string | null;
  total_ar: number;
  buckets: ArBuckets;
  customers: ArCustomer[];
}

export interface ArAgingResponse {
  entity_code: string;
  bucket_labels: Record<string, string>;
  current: ArSnapshot | null;
  prior: ArSnapshot | null;
}

export interface WriteDownRequest {
  entity_code: string;
  amount: number;
  customer_name?: string;
  customer_number?: string;
  memo?: string;
  aged_ar_snapshot_id?: string;
  bad_debt_account_code?: string;
  ar_account_code?: string;
}

export interface WriteDownResponse {
  entity_code: string;
  journal_batch_id: string;
  adjustment_line_id: string;
  period_label: string;
  amount: string;
  dr_account: string;
  cr_account: string;
  memo: string;
}

export async function getArAging(entityCode: string): Promise<ArAgingResponse> {
  const res = await api.get<ArAgingResponse>('/api/ar/aging', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function postArWriteDown(
  payload: WriteDownRequest,
): Promise<WriteDownResponse> {
  const res = await api.post<WriteDownResponse>('/api/ar/write-down', payload);
  return res.data;
}

export async function getArAgingExcelUrl(
  entityCode: string,
): Promise<{ url: string; filename: string }> {
  const res = await api.get<{ url: string; filename: string }>(
    '/api/ar/aging/excel',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}
