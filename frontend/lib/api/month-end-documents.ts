import { api } from './client';

export interface MonthEndDocument {
  period_id: string;
  entity_code?: string;
  period_label?: string;
  status: 'not_generated' | 'generating' | 'ready' | 'failed';
  presigned_url: string | null;
  r2_object_key?: string | null;
  generated_at?: string | null;
  generated_by?: string | null;
  email_sent_at?: string | null;
  email_recipients?: unknown;
  error_msg?: string | null;
}

export interface GenerateResult {
  status: string;
  r2_object_key: string | null;
  presigned_url: string | null;
  sections: { section: string; state: string }[];
  commentary_available: boolean;
  email?: { sent: boolean; skipped: boolean; error: string | null };
}

export async function getMonthEndDocument(
  entityCode: string,
  periodEnd: string,
): Promise<MonthEndDocument> {
  const res = await api.get('/api/reports/month-end/document', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function generateMonthEndDocument(input: {
  entity_code: string;
  period_end: string;
  actor_email?: string;
  send_email?: boolean;
}): Promise<GenerateResult> {
  const res = await api.post('/api/reports/month-end/generate', input);
  return res.data;
}

export async function resendMonthEndDocument(input: {
  entity_code: string;
  period_end: string;
  actor_email?: string;
}): Promise<GenerateResult> {
  const res = await api.post('/api/reports/month-end/resend', input);
  return res.data;
}
