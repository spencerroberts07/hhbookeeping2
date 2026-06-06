import { api } from './client';

export interface RecurringLine {
  line_number: number;
  account_code: string;
  direction: 'debit' | 'credit';
  memo?: string | null;
}

export interface RecurringTemplate {
  id: string;
  name: string;
  description: string | null;
  standard_key: string | null;
  calc_type: 'fixed' | 'formula' | 'schedule';
  fixed_amount: string | null;
  formula_expr: string | null;
  schedule_source: string | null;
  cadence: 'monthly' | 'on_close' | 'annual';
  posting_day: number;
  is_active: boolean;
  auto_post: boolean;
  last_posted_at: string | null;
  last_posted_period_end: string | null;
  total_postings: number;
  notes: string | null;
  lines: RecurringLine[];
}

export interface MonthEndStatusItem {
  template_id: string;
  name: string;
  status: 'posted' | 'pending';
  amount: string | null;
  journal_batch_id: string | null;
  auto_posted?: boolean;
}

export async function listRecurringTemplates(
  entityCode: string,
): Promise<{ templates: RecurringTemplate[]; count: number }> {
  const res = await api.get('/api/recurring-entries', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function createRecurringTemplate(input: {
  entity_code: string;
  name: string;
  description?: string | null;
  calc_type: string;
  fixed_amount?: number | null;
  formula_expr?: string | null;
  schedule_source?: string | null;
  cadence?: string;
  posting_day?: number;
  is_active?: boolean;
  auto_post?: boolean | null;
  notes?: string | null;
  lines?: RecurringLine[];
  actor_email: string;
}): Promise<{ id: string }> {
  const res = await api.post('/api/recurring-entries', input);
  return res.data;
}

export async function updateRecurringTemplate(
  templateId: string,
  input: {
    entity_code: string;
    name: string;
    description?: string | null;
    calc_type: string;
    fixed_amount?: number | null;
    formula_expr?: string | null;
    schedule_source?: string | null;
    cadence?: string;
    posting_day?: number;
    is_active?: boolean;
    auto_post?: boolean | null;
    notes?: string | null;
    lines?: RecurringLine[] | null;
    actor_email: string;
  },
): Promise<{ id: string }> {
  const res = await api.put(`/api/recurring-entries/${templateId}`, input);
  return res.data;
}

export async function toggleRecurringTemplate(
  templateId: string,
  entityCode: string,
  isActive: boolean,
): Promise<{ id: string; is_active: boolean }> {
  const res = await api.patch(`/api/recurring-entries/${templateId}/toggle`, {
    entity_code: entityCode,
    is_active: isActive,
  });
  return res.data;
}

export async function deleteRecurringTemplate(
  templateId: string,
  entityCode: string,
): Promise<{ id: string; is_active: boolean }> {
  const res = await api.delete(`/api/recurring-entries/${templateId}`, {
    data: { entity_code: entityCode },
  });
  return res.data;
}

export async function postRecurringTemplate(input: {
  entity_code: string;
  template_id: string;
  period_end: string;
  actor_email: string;
  dry_run?: boolean;
}): Promise<{
  dry_run: boolean;
  journal_batch_id?: string;
  batch_status?: string;
  grand_total: string;
  journal_lines: Array<{
    line_number: number;
    account_code: string;
    debit_amount: string;
    credit_amount: string;
    memo: string;
  }>;
}> {
  const { template_id, ...body } = input;
  const res = await api.post(`/api/recurring-entries/${template_id}/post`, body);
  return res.data;
}

export async function postAllDueTemplates(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
}): Promise<{
  posted_count: number;
  skipped_count: number;
  failed_count: number;
  posted: Array<{ template_id: string; journal_batch_id: string; grand_total: string }>;
  skipped: Array<{ template_id: string; reason: string }>;
  failed: Array<{ template_id: string; error: string }>;
}> {
  const res = await api.post('/api/recurring-entries/post-all', input);
  return res.data;
}

export async function seedRecurringTemplates(entityCode: string): Promise<{
  inserted: number;
  skipped: number;
}> {
  const res = await api.post('/api/recurring-entries/seed', { entity_code: entityCode });
  return res.data;
}

export async function getRecurringMonthEndStatus(
  entityCode: string,
  periodEnd: string,
): Promise<{
  status: string;
  summary: string;
  items: MonthEndStatusItem[];
  all_posted: boolean;
}> {
  const res = await api.get('/api/recurring-entries/month-end-status', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}
