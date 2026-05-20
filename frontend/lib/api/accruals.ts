import { api } from './client';

export interface AccrualTemplate {
  id: string;
  accrual_code: string;
  description: string | null;
  debit_account: string;
  credit_account: string;
  default_amount: number | null;
  frequency: string;
  is_active: boolean;
  notes: string | null;
}

export async function seedAccrualTemplates(input: {
  entity_code: string;
  actor_email: string;
}): Promise<{ inserted: number }> {
  const res = await api.post('/api/accruals/seed-templates', input);
  return res.data;
}

export async function listAccrualTemplates(
  entityCode: string,
): Promise<{ templates: AccrualTemplate[] }> {
  const res = await api.get('/api/accruals/templates', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function upsertAccrualTemplate(input: {
  entity_code: string;
  actor_email: string;
  accrual_code: string;
  description?: string;
  debit_account: string;
  credit_account: string;
  default_amount?: number;
  frequency?: string;
  is_active?: boolean;
  notes?: string;
}): Promise<AccrualTemplate> {
  const res = await api.post('/api/accruals/templates/upsert', input);
  return res.data;
}

export async function buildAccrualJournal(input: {
  entity_code: string;
  period_end: string;
  accrual_codes: string[];
  amounts_override?: Record<string, number>;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post('/api/accruals/build-journal', input);
  return res.data;
}

export async function listAccrualJournals(
  entityCode: string,
  periodEnd: string,
): Promise<unknown> {
  const res = await api.get('/api/accruals/journals', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}
