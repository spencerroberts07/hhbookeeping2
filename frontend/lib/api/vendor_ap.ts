/**
 * Outside-Vendor AP module API client.
 *
 * Covers three surfaces:
 *  1. Vendor master CRUD (list, get, set banking, set email)
 *  2. EFT payment file (preview, generate, list files, download)
 *  3. AP alert log (used by dashboard alerts feed)
 *
 * HH AP (account 2030) is excluded at the backend; these endpoints
 * only handle outside-vendor invoices.
 */
import { api } from './client';

// ---------------------------------------------------------------------------
// Types — Vendor master
// ---------------------------------------------------------------------------

export interface Vendor {
  id: string;
  entity_id: string;
  vendor_name: string;
  vendor_normalized: string;
  default_account_code: string | null;
  remittance_email: string | null;
  bank_transit: string | null;
  bank_institution: string | null;
  bank_account: string | null;
  eft_transaction_type: string | null;
  payment_terms_days: number | null;
  payment_terms_confidence: number | null;
  profile_confidence: number | null;
  profile_confidence_computed: number;
  invoice_count: number;
  first_seen_at: string | null;
  last_seen_at: string | null;
  banking_complete: boolean;
  banking_confirmed_at: string | null;
  banking_confirmed_by: string | null;
  created_at: string;
  updated_at: string;
}

export interface SetBankingRequest {
  entity_code: string;
  transit: string;
  institution: string;
  account: string;
  eft_transaction_type?: string | null;
}

export interface SetEmailRequest {
  entity_code: string;
  email: string | null;
}

// ---------------------------------------------------------------------------
// Types — EFT payment files
// ---------------------------------------------------------------------------

export interface VendorBatchItem {
  vendor_id: string | null;
  vendor_name: string;
  amount: string;
  invoice_ids: string[];
  banking_complete: boolean;
}

export interface PreviewResult {
  vendors_in_batch: VendorBatchItem[];
  missing_banking: Array<{ vendor_id: string | null; vendor_name: string }>;
  banking_complete: boolean;
  total_amount: string;
  vendor_count: number;
  invoice_count: number;
}

export interface GenerateResult {
  file_id: string;
  file_name: string;
  file_path: string | null;
  download_url: string | null;
  record_count: number;
  total_amount: number;
  vendor_count: number;
  invoice_count: number;
  file_creation_number: number;
  dry_run: true;
  submission_note: string;
}

export interface VendorEftFile {
  id: string;
  file_name: string;
  file_path: string | null;
  record_count: number;
  total_amount: string;
  file_creation_number: number;
  payment_date: string;
  vendor_count: number;
  invoice_ids: string[];
  status: string;
  actor_email: string | null;
  generated_at: string;
}

// ---------------------------------------------------------------------------
// Vendor master
// ---------------------------------------------------------------------------

export async function listVendors(entityCode: string): Promise<Vendor[]> {
  const res = await api.get<Vendor[]>('/api/vendor-eft/vendors', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getVendor(
  vendorId: string,
  entityCode: string,
): Promise<Vendor> {
  const res = await api.get<Vendor>(`/api/vendor-eft/vendors/${vendorId}`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function setVendorBanking(
  vendorId: string,
  body: SetBankingRequest,
): Promise<Vendor> {
  const res = await api.put<Vendor>(
    `/api/vendor-eft/vendors/${vendorId}/banking`,
    body,
  );
  return res.data;
}

export async function setVendorEmail(
  vendorId: string,
  body: SetEmailRequest,
): Promise<Vendor> {
  const res = await api.put<Vendor>(
    `/api/vendor-eft/vendors/${vendorId}/email`,
    body,
  );
  return res.data;
}

// ---------------------------------------------------------------------------
// EFT payment file
// ---------------------------------------------------------------------------

export async function previewVendorPaymentFile(body: {
  entity_code: string;
  invoice_ids: string[];
}): Promise<PreviewResult> {
  const res = await api.post<PreviewResult>('/api/vendor-eft/preview', body);
  return res.data;
}

export async function generateVendorPaymentFile(body: {
  entity_code: string;
  invoice_ids: string[];
  payment_date: string;
}): Promise<GenerateResult> {
  const res = await api.post<GenerateResult>('/api/vendor-eft/generate', body);
  return res.data;
}

export async function listVendorEftFiles(
  entityCode: string,
): Promise<VendorEftFile[]> {
  const res = await api.get<VendorEftFile[]>('/api/vendor-eft/files', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function getVendorEftFile(
  fileId: string,
  entityCode: string,
): Promise<VendorEftFile> {
  const res = await api.get<VendorEftFile>(`/api/vendor-eft/files/${fileId}`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function downloadVendorEftFile(
  fileId: string,
  entityCode: string,
): Promise<{ file_id: string; file_name: string; download_url: string; expires_in_seconds: number }> {
  const res = await api.get(`/api/vendor-eft/files/${fileId}/download`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

// ---------------------------------------------------------------------------
// Direct vendor AP invoices (payment lifecycle)
// ---------------------------------------------------------------------------

export type VendorInvoiceStatus =
  | 'open'
  | 'needs_review'
  | 'approved'
  | 'payment_pending'
  | 'paid'
  | 'void';

export type VendorPaymentStatus = 'unpaid' | 'partial' | 'paid';

export interface VendorInvoice {
  id: string;
  entity_id: string;
  vendor_id: string | null;
  vendor_name: string;
  invoice_number: string;
  invoice_date: string | null;
  due_date: string | null;
  total_amount: string;
  paid_amount: string;
  open_amount: string;
  status: VendorInvoiceStatus;
  payment_status: VendorPaymentStatus;
  payment_file_id: string | null;
  payment_pending_at: string | null;
  source_invoice_document_id: string | null;
  priority: string;
  currency_code: string;
  created_at: string;
  updated_at: string;
}

export interface VendorInvoiceListResponse {
  entity_code: string;
  date_from: string;
  date_to: string;
  invoices: VendorInvoice[];
  total: number;
  summary: {
    total_open: string;
    total_payment_pending: string;
    total_paid: string;
    count_open: number;
    count_payment_pending: number;
    count_overdue: number;
  };
}

export async function listVendorInvoices(params: {
  entity_code: string;
  date_from: string;
  date_to: string;
  status?: VendorInvoiceStatus;
  payment_status?: VendorPaymentStatus;
  due_state?: 'overdue' | 'due_soon' | 'current';
}): Promise<VendorInvoiceListResponse> {
  const res = await api.get<VendorInvoiceListResponse>(
    '/api/direct-vendor-ap/invoices',
    { params },
  );
  return res.data;
}
