import { api } from './client';

export type InvoiceType = 'hh_ap' | 'outside_vendor';
export type InvoiceStatus =
  | 'unmatched'
  | 'matched'
  | 'posted_to_ap'
  | 'deleted';
export type ApAccount = '2020' | '2030';
export type MatchType = 'journal' | 'bank' | 'hh_ap';

export interface InvoiceDocument {
  id: string;
  entity_code: string;
  invoice_type: InvoiceType;
  invoice_number: string | null;
  vendor_name: string | null;
  invoice_date: string | null;
  due_date: string | null;
  amount: string | null;
  currency: string;
  status: InvoiceStatus;
  ap_account: ApAccount | null;
  file_name: string | null;
  file_size_bytes: number | null;
  /** R2 object key — opaque, use file_url to open. */
  file_path: string | null;
  /** Presigned R2 URL valid ~1 hour. Null when no file was archived. */
  file_url: string | null;
  uploaded_at: string | null;
  matched_at: string | null;
  match_confidence: number | null;
  notes: string | null;
}

export interface InvoiceLink {
  id: string;
  link_type: MatchType;
  linked_at: string | null;
  linked_by: string;
  confidence: number | null;
  journal_batch_id: string | null;
  journal_source_module: string | null;
  journal_batch_label: string | null;
  journal_status: string | null;
  bank_transaction_id: string | null;
  bank_date: string | null;
  bank_amount: string | null;
  bank_description: string | null;
  hh_ap_invoice_id: string | null;
  hh_invoice_number: string | null;
  hh_vendor_name: string | null;
  hh_invoice_date: string | null;
  hh_invoice_amount: string | null;
}

export interface InvoiceDetail {
  invoice: InvoiceDocument;
  links: InvoiceLink[];
}

export interface SuggestedMatch {
  type: MatchType;
  id: string;
  amount: string;
  date: string | null;
  description: string;
  confidence: number;
}

export interface UnmatchedQueueRow {
  invoice: InvoiceDocument;
  suggested_matches: SuggestedMatch[];
}

export interface UnmatchedQueueResponse {
  queue: UnmatchedQueueRow[];
  total: number;
}

export interface InvoiceListResponse {
  invoices: InvoiceDocument[];
  total: number;
  limit: number;
  offset: number;
}

// --------------------------------------------------------------------------
// Endpoints
// --------------------------------------------------------------------------

export async function listInvoiceDocuments(params: {
  entity_code: string;
  status?: InvoiceStatus;
  invoice_type?: InvoiceType;
  date_from?: string;
  date_to?: string;
  limit?: number;
  offset?: number;
}): Promise<InvoiceListResponse> {
  const res = await api.get<InvoiceListResponse>('/api/invoice-documents', {
    params,
  });
  return res.data;
}

export async function getInvoiceDocument(
  invoiceId: string,
  entityCode: string,
): Promise<InvoiceDetail> {
  const res = await api.get<InvoiceDetail>(
    `/api/invoice-documents/${invoiceId}`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

export async function getUnmatchedQueue(params: {
  entity_code: string;
  period_end?: string;
}): Promise<UnmatchedQueueResponse> {
  const res = await api.get<UnmatchedQueueResponse>(
    '/api/invoice-documents/unmatched-queue',
    { params },
  );
  return res.data;
}

export async function manualMatchInvoice(
  invoiceId: string,
  body: {
    entity_code: string;
    actor_email?: string;
    journal_batch_id?: string;
    bank_transaction_id?: string;
    hh_ap_invoice_id?: string;
  },
): Promise<{ ok: true; invoice_id: string; link_type: MatchType }> {
  const res = await api.post(
    `/api/invoice-documents/${invoiceId}/match`,
    body,
  );
  return res.data;
}

export async function postInvoiceToAp(
  invoiceId: string,
  body: {
    entity_code: string;
    actor_email: string;
    ap_account: ApAccount;
    expense_account_code?: string;
    period_end: string;
    memo?: string;
  },
): Promise<{
  ok: true;
  invoice_id: string;
  journal_batch_id: string;
  amount: string;
  ap_account: ApAccount;
  expense_account: string;
}> {
  const res = await api.post(
    `/api/invoice-documents/${invoiceId}/post-to-ap`,
    body,
  );
  return res.data;
}

export async function softDeleteInvoice(
  invoiceId: string,
  body: { entity_code: string; reason: string },
): Promise<{ ok: true; invoice_id: string; status: 'deleted' }> {
  const res = await api.post(
    `/api/invoice-documents/${invoiceId}/delete`,
    body,
  );
  return res.data;
}

export async function updateInvoiceDocument(
  invoiceId: string,
  body: {
    entity_code: string;
    invoice_number?: string;
    vendor_name?: string;
    invoice_date?: string;
    due_date?: string;
    amount?: number | string;
    notes?: string;
  },
): Promise<InvoiceDocument> {
  const res = await api.patch<InvoiceDocument>(
    `/api/invoice-documents/${invoiceId}`,
    body,
  );
  return res.data;
}

export async function runInvoiceSweep(params: {
  entity_code: string;
  period_end?: string;
}): Promise<{
  invoices_examined: number;
  auto_matched: number;
  suggested: number;
  unmatched: number;
}> {
  const res = await api.post('/api/invoice-documents/sweep', null, {
    params,
  });
  return res.data;
}
