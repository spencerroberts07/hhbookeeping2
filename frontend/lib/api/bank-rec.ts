import { api } from './client';

export interface RecNamedItems {
  outstanding_cheques: number;
  deposits_in_transit: number;
  payroll_deductions_bank_only: number;
  other_bank_only: number;
}

export interface RecSummary {
  named_items: RecNamedItems;
  payroll_deduction_residual: number;
  implied_dit_to_close: number;
  bank_only_items: { date: string; amount: number; description: string }[];
  outstanding_book: { source_module: string; amount: number; memo: string | null }[];
  match: {
    bank_count?: number;
    book_count?: number;
    pre_cleared?: number;
    auto_cleared?: number;
    suggested?: number;
  };
}

export interface BankRec {
  id: string;
  statement_closing_balance: number;
  book_balance: number;
  deposits_in_transit: number;
  outstanding_cheques: number;
  bank_only_items_total: number;
  expected_closing: number;
  payroll_deductions: number;
  implied_dit_to_close: number;
  variance: number;
  ties: boolean;
  status?: string;
  summary: RecSummary;
}

export interface JournalCandidateLine {
  account_code: string;
  account_name: string;
  debit: number;
  credit: number;
}

export interface JournalCandidate {
  kind: string;
  description: string;
  lines: JournalCandidateLine[];
  post_to: string;
  status: string;
}

export async function computeBankRec(input: {
  entity_code: string;
  source_account_code?: string;
  period_end: string;
  statement_date: string;
  statement_closing_balance: number;
  statement_opening_balance?: number | null;
  confirmed_deposits_in_transit?: number | null;
}): Promise<BankRec> {
  const res = await api.post('/api/bank-rec/compute', input);
  return res.data;
}

export async function getBankRec(params: {
  entity_code: string;
  source_account_code?: string;
  period_end: string;
}): Promise<BankRec> {
  const res = await api.get('/api/bank-rec', { params });
  return res.data;
}

export async function lockBankRec(
  recId: string,
  overrideNote?: string,
): Promise<{ ok: boolean; status: string }> {
  const res = await api.post(`/api/bank-rec/${recId}/lock`, null, {
    params: overrideNote ? { override_note: overrideNote } : undefined,
  });
  return res.data;
}

export async function getJournalCandidates(
  recId: string,
): Promise<{ rec_id: string; candidates: JournalCandidate[]; note: string }> {
  const res = await api.get(`/api/bank-rec/${recId}/journal-candidates`);
  return res.data;
}
