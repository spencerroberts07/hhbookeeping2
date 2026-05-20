import { api } from './client';

export interface HHAPSummary {
  current_balance: number;
  aging: {
    current: number;
    over_30: number;
    over_60: number;
    over_90: number;
  };
  dating_total: number;
  last_statement_date: string | null;
}

export interface HHAPInvoice {
  id: string;
  document_date: string;
  document_type: string;
  document_number: string | null;
  amount: number;
  source_hash: string;
}

export async function getHHAPSummary(entityCode: string): Promise<HHAPSummary> {
  const res = await api.get('/api/hh-ap/summary', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function listHHAPInvoices(params: {
  entity_code: string;
  date_from?: string;
  date_to?: string;
  document_type?: string;
  limit?: number;
  offset?: number;
}): Promise<{ invoices: HHAPInvoice[]; total: number }> {
  const res = await api.get('/api/hh-ap/invoices', { params });
  return res.data;
}

export async function uploadHHAPDocuments(input: {
  entity_code: string;
  document_type: string;
  files: File[];
  document_date?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('document_type', input.document_type);
  if (input.document_date) fd.append('document_date', input.document_date);
  for (const file of input.files) fd.append('files', file);
  const res = await api.post('/api/hh-ap/upload-documents', fd);
  return res.data;
}

export async function uploadAndParseInvoices(input: {
  entity_code: string;
  files: File[];
  document_date?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  if (input.document_date) fd.append('document_date', input.document_date);
  for (const file of input.files) fd.append('files', file);
  const res = await api.post('/api/hh-ap/invoices/upload-and-parse-batch', fd);
  return res.data;
}

// Remittance clearing journal
export async function buildRemittanceClearing(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post(
    '/api/hh-ap-remittance-clearing/build-journal',
    input,
  );
  return res.data;
}
