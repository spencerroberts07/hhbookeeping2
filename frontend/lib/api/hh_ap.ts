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

// --------------------------------------------------------------------------
// Document parsing status (Item A)
// --------------------------------------------------------------------------

export interface HHAPDocumentRow {
  id: string;
  source_filename: string;
  document_type: string;
  processing_status: string;
  file_size_bytes: number | null;
  created_at: string | null;
  period: string | null;
  records_parsed: number | null;
  error_message: string | null;
  file_url: string | null;
}

export interface HHAPDocumentsResponse {
  documents: HHAPDocumentRow[];
  summary: {
    total: number;
    parsed: number;
    pending: number;
    errors: number;
  };
}

export async function listHHAPDocuments(input: {
  entity_code: string;
  status?: 'pending' | 'parsed' | 'errors' | string;
  limit?: number;
  offset?: number;
}): Promise<HHAPDocumentsResponse> {
  const params: Record<string, string | number> = { entity_code: input.entity_code };
  if (input.status) params.status = input.status;
  if (input.limit != null) params.limit = input.limit;
  if (input.offset != null) params.offset = input.offset;
  const res = await api.get<HHAPDocumentsResponse>('/api/hh-ap/documents', { params });
  return res.data;
}

export async function reprocessHHAPDocument(input: {
  document_id: string;
  entity_code: string;
}): Promise<{ ok: boolean; document_id: string; parsing_queued: boolean; message: string }> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  const res = await api.post(
    `/api/hh-ap/documents/${input.document_id}/reprocess`,
    fd,
  );
  return res.data;
}
