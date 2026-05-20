import { api } from './client';

export type BankMatchState = 'matched' | 'needs_review' | 'unmatched' | 'ignored';
export type BankReviewStatus = 'pending' | 'reviewed' | 'flagged';

export interface BankTransactionRow {
  id: string;
  txn_date: string;
  description: string;
  amount: number;
  direction: 'inflow' | 'outflow' | 'unknown';
  source_account_code: string | null;
  match_state: BankMatchState;
  review_status: BankReviewStatus;
  matched_to: string | null;
}

export interface BankReviewSummary {
  total: number;
  matched: number;
  needs_review: number;
  unmatched: number;
  ignored: number;
}

// --- bank_review (qbo-bank-sync proxy, since bank_review router isn't wired) ---

export async function getBankSummary(params: {
  entity_code: string;
  date_from: string;
  date_to: string;
}): Promise<BankReviewSummary> {
  const res = await api.get('/api/qbo-bank-sync/summary', { params });
  return res.data;
}

export async function listBankTransactions(params: {
  entity_code: string;
  date_from: string;
  date_to: string;
  review_status?: BankReviewStatus;
  match_state?: BankMatchState;
  limit?: number;
  offset?: number;
}): Promise<{ transactions: BankTransactionRow[]; total: number }> {
  const res = await api.get('/api/qbo-bank-sync/transactions', { params });
  return res.data;
}

// --- bank_csv ---

export async function previewBankCsv(input: {
  entity_code: string;
  file: File;
  mapping_profile?: string;
  source_account_code?: string;
  source_account_name?: string;
  column_map_json?: string;
  sample_limit?: number;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('file', input.file);
  if (input.mapping_profile) fd.append('mapping_profile', input.mapping_profile);
  if (input.source_account_code)
    fd.append('source_account_code', input.source_account_code);
  if (input.source_account_name)
    fd.append('source_account_name', input.source_account_name);
  if (input.column_map_json) fd.append('column_map_json', input.column_map_json);
  if (input.sample_limit != null)
    fd.append('sample_limit', String(input.sample_limit));
  const res = await api.post('/api/bank-csv/preview', fd);
  return res.data;
}

export async function uploadBankCsv(input: {
  entity_code: string;
  actor_email: string;
  file: File;
  mapping_profile?: string;
  source_account_code?: string;
  note?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  if (input.mapping_profile) fd.append('mapping_profile', input.mapping_profile);
  if (input.source_account_code)
    fd.append('source_account_code', input.source_account_code);
  if (input.note) fd.append('note', input.note);
  const res = await api.post('/api/bank-csv/upload', fd);
  return res.data;
}

// --- bank_pdf ---

export async function previewBankPdf(input: {
  entity_code: string;
  file: File;
  source_account_code?: string;
  sample_limit?: number;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('file', input.file);
  if (input.source_account_code)
    fd.append('source_account_code', input.source_account_code);
  if (input.sample_limit != null)
    fd.append('sample_limit', String(input.sample_limit));
  const res = await api.post('/api/bank-pdf/preview', fd);
  return res.data;
}

export async function uploadBankPdf(input: {
  entity_code: string;
  actor_email: string;
  file: File;
  source_account_code?: string;
  note?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  if (input.source_account_code)
    fd.append('source_account_code', input.source_account_code);
  if (input.note) fd.append('note', input.note);
  const res = await api.post('/api/bank-pdf/upload', fd);
  return res.data;
}

// --- bank_auto_journal ---

export async function listBankRules(entityCode: string): Promise<unknown> {
  const res = await api.get('/api/bank-auto-journal/rules', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function runBankAutoJournal(input: {
  entity_code: string;
  period_start: string;
  period_end: string;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post('/api/bank-auto-journal/run', input);
  return res.data;
}

export async function listBankAutoJournalRuns(
  entityCode: string,
  limit = 50,
): Promise<unknown> {
  const res = await api.get('/api/bank-auto-journal/runs', {
    params: { entity_code: entityCode, limit },
  });
  return res.data;
}

export async function listUnmatchedBankTxns(params: {
  entity_code: string;
  period_start: string;
  period_end: string;
}): Promise<unknown> {
  const res = await api.get('/api/bank-auto-journal/unmatched', { params });
  return res.data;
}
