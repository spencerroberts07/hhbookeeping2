import { api } from './client';

export interface GlImportRun {
  id: string;
  file_name: string;
  period_start: string | null;
  period_end: string | null;
  imported_at: string;
  account_count: number;
  transaction_count: number;
  trial_balance_built_at: string | null;
}

export interface TrialBalanceRow {
  account_code: string;
  account_name: string;
  qbo_debit: number;
  qbo_credit: number;
  qbo_net: number;
  app_net: number;
  variance: number;
  flipped: boolean;
}

export interface GlTransaction {
  date: string;
  account_code: string;
  description: string;
  debit: number;
  credit: number;
  ref: string | null;
}

export async function listGlRuns(
  entityCode: string,
  limit = 50,
): Promise<{ runs: GlImportRun[] }> {
  const res = await api.get('/api/gl-import/runs', {
    params: { entity_code: entityCode, limit },
  });
  return res.data;
}

export async function getGlRun(
  entityCode: string,
  runId: string,
): Promise<GlImportRun> {
  const res = await api.get(`/api/gl-import/runs/${runId}`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getTrialBalance(
  entityCode: string,
  runId: string,
  onlyVariance = false,
): Promise<{ rows: TrialBalanceRow[]; total_debit: number; total_credit: number }> {
  const res = await api.get(`/api/gl-import/runs/${runId}/trial-balance`, {
    params: { entity_code: entityCode, only_variance: onlyVariance },
  });
  return res.data;
}

export async function getGlTransactions(
  entityCode: string,
  runId: string,
  accountCode?: string,
  limit = 1000,
): Promise<{ transactions: GlTransaction[] }> {
  const res = await api.get(`/api/gl-import/runs/${runId}/transactions`, {
    params: {
      entity_code: entityCode,
      account_code: accountCode,
      limit,
    },
  });
  return res.data;
}

export async function uploadGl(input: {
  entity_code: string;
  actor_email: string;
  file: File;
  period_start?: string;
  period_end?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  if (input.period_start) fd.append('period_start', input.period_start);
  if (input.period_end) fd.append('period_end', input.period_end);
  const res = await api.post('/api/gl-import/upload', fd);
  return res.data;
}

export async function buildTrialBalanceComparison(
  runId: string,
  body: { entity_code: string; actor_email: string },
): Promise<unknown> {
  const res = await api.post(
    `/api/gl-import/runs/${runId}/build-comparison`,
    body,
  );
  return res.data;
}
