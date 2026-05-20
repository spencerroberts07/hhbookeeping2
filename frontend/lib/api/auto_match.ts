import { api } from './client';

export async function runAutoMatch(input: {
  entity_code: string;
  period_start: string;
  period_end: string;
  actor_email: string;
  triggered_by?: string;
  date_window_days?: number;
  amount_tolerance?: number;
  max_to_apply?: number;
}): Promise<unknown> {
  const res = await api.post('/api/auto-match/run', input);
  return res.data;
}

export async function listAutoMatchRuns(
  entityCode: string,
  limit = 50,
): Promise<unknown> {
  const res = await api.get('/api/auto-match/runs', {
    params: { entity_code: entityCode, limit },
  });
  return res.data;
}

export async function getAutoMatchRun(
  entityCode: string,
  runId: string,
): Promise<unknown> {
  const res = await api.get(`/api/auto-match/runs/${runId}`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}
