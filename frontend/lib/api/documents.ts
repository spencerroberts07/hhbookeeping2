/**
 * Documents library — unified backend endpoint at GET /api/documents.
 *
 * Backend aggregates from invoice_documents, bank_pdf_imports,
 * bank_csv_import_runs, payroll_runs, pos_import_runs, gl_import_runs,
 * and hh_ap_statements; presigns R2 file_path values into 1-hour URLs.
 */
import { api } from './client';

export interface UnifiedDocument {
  id: string;
  document_type: string;
  filename: string;
  upload_date: string | null;
  parsed_record_count: number;
  file_url: string | null;
  status: string;
}

export interface DocumentsListResponse {
  entity_code: string;
  documents: UnifiedDocument[];
  total: number;
  limit: number;
  offset: number;
}

export async function listDocuments(input: {
  entity_code: string;
  type?: string;
  year?: number;
  month?: number;
  limit?: number;
  offset?: number;
}): Promise<DocumentsListResponse> {
  const params: Record<string, string | number> = {
    entity_code: input.entity_code,
  };
  if (input.type) params.type = input.type;
  if (input.year) params.year = input.year;
  if (input.month) params.month = input.month;
  if (input.limit) params.limit = input.limit;
  if (input.offset) params.offset = input.offset;
  const res = await api.get<DocumentsListResponse>('/api/documents', { params });
  return res.data;
}
