import { api } from './client';

export type PeriodStatus = 'open' | 'submitted_for_close' | 'closed';

export interface PeriodCloseStatus {
  entity_code: string;
  period_end: string;
  status: PeriodStatus;
  submitted_by: string | null;
  approved_by: string | null;
  approved_at: string | null;
  blocking_items: Array<{ description: string; module: string }>;
  warning_items: Array<{ description: string; module: string }>;
}

export interface BatchSummary {
  id: string;
  source_module: string;
  batch_label: string;
  status: string;
  total_debit: number;
  total_credit: number;
  line_count: number;
}

export interface CurrentPeriod {
  period_end: string;
  period_label: string;
  status: PeriodStatus;
}

// --- period_close ---

/**
 * Returns the period the dashboard should land on. 404 means no
 * accounting_periods rows exist for the entity yet — the caller should
 * render an "Start your first month-end" empty state.
 */
export async function getCurrentPeriod(
  entityCode: string,
): Promise<CurrentPeriod> {
  const res = await api.get<CurrentPeriod>('/api/period-close/current', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getPeriodStatus(
  entityCode: string,
  periodEnd: string,
): Promise<PeriodCloseStatus> {
  const res = await api.get('/api/period-close/status', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function getPeriodHistory(
  entityCode: string,
  periodEnd: string,
): Promise<unknown> {
  const res = await api.get('/api/period-close/history', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function submitPeriodForClose(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
  notes?: string;
}): Promise<unknown> {
  const res = await api.post('/api/period-close/submit', input);
  return res.data;
}

export async function approvePeriodClose(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
  notes?: string;
}): Promise<unknown> {
  const res = await api.post('/api/period-close/approve', input);
  return res.data;
}

export async function reopenPeriod(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
  notes: string;
}): Promise<unknown> {
  const res = await api.post('/api/period-close/reopen', input);
  return res.data;
}

// --- month_end_workflow (batch transitions) ---

export async function submitBatch(input: {
  entity_code: string;
  period_end: string;
  source_module: string;
  batch_label: string;
  actor_email: string;
  note?: string;
}): Promise<unknown> {
  const res = await api.post('/api/month-end-workflow/submit', input);
  return res.data;
}

export async function approveBatch(input: {
  entity_code: string;
  period_end: string;
  source_module: string;
  batch_label: string;
  actor_email: string;
  note?: string;
}): Promise<unknown> {
  const res = await api.post('/api/month-end-workflow/approve', input);
  return res.data;
}

export async function rejectBatch(input: {
  entity_code: string;
  period_end: string;
  source_module: string;
  batch_label: string;
  actor_email: string;
  note?: string;
}): Promise<unknown> {
  const res = await api.post('/api/month-end-workflow/reject', input);
  return res.data;
}

export async function reopenBatch(input: {
  entity_code: string;
  period_end: string;
  source_module: string;
  batch_label: string;
  actor_email: string;
  note?: string;
}): Promise<unknown> {
  const res = await api.post('/api/month-end-workflow/reopen', input);
  return res.data;
}

// --- month_end (status overview) ---

export async function getMonthEndOverview(
  entityCode: string,
  periodEnd: string,
): Promise<unknown> {
  const res = await api.get('/api/month-end/overview', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}
