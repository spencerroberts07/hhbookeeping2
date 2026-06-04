/**
 * Real backend-backed financial reports.
 *
 * The three top reports (Income Statement, Balance Sheet, Trial Balance)
 * now live at /api/reports/* — see backend/app/routes/reports.py. The
 * earlier mock implementations have been retired.
 *
 * The historical re-export helpers (getArAging / getApAging from POS + HH
 * AP modules) stay because those reports are sourced from snapshot tables,
 * not from journal_lines.
 */
import { api } from './client';
import type { PlanTier } from './billing';

// --------------------------------------------------------------------------
// Income Statement (4-column: current period, prior year, %, prior %)
// --------------------------------------------------------------------------

export type IncomeStatementPreset =
  | 'month'
  | 'ytd'
  | 'rolling12'
  | 'qtd'
  | 'trailing3'
  | 'last6'
  | 'custom';

export interface IncomeStatementAccount {
  account_code: string;
  account_name: string;
  /** Indentation level — 0 = top-level under section. */
  depth?: number;
  /** True for group header rows (no amount on the line). */
  is_group_header?: boolean;
  /** True for the "Total <code> <name>" subtotal row of a group. */
  is_group_subtotal?: boolean;
  current_amount: number | null;
  prior_amount: number | null;
  current_pct: number | null;
  prior_pct: number | null;
}

export type IncomeStatementSectionLabel =
  | 'INCOME'
  | 'COST OF GOODS SOLD'
  | 'GROSS PROFIT'
  | 'EXPENSES'
  | 'OTHER INCOME'
  | 'PROFIT';

export interface IncomeStatementSection {
  section: IncomeStatementSectionLabel;
  accounts: IncomeStatementAccount[];
  section_total: number;
  prior_total: number;
  section_pct: number | null;
  prior_pct: number | null;
}

export interface IncomeStatementReport {
  entity_code: string;
  preset: IncomeStatementPreset;
  period_label: string;
  prior_label: string;
  period_start: string;
  period_end: string;
  prior_start: string;
  prior_end: string;
  total_revenue: number;
  prior_revenue: number;
  sections: IncomeStatementSection[];
}

export async function getIncomeStatement(params: {
  entity_code: string;
  preset: IncomeStatementPreset;
  period_end?: string;
  date_from?: string;
  date_to?: string;
}): Promise<IncomeStatementReport> {
  const res = await api.get<IncomeStatementReport>(
    '/api/reports/income-statement',
    { params },
  );
  return res.data;
}

export interface IncomeStatementPeriod {
  period_label: string;
  period_start: string;
  period_end: string;
  status: string;
  fiscal_year: number | null;
  fiscal_period_number: number | null;
}

export interface IncomeStatementPeriodsResponse {
  entity_code: string;
  periods: IncomeStatementPeriod[];
}

export async function getIncomeStatementPeriods(
  entityCode: string,
): Promise<IncomeStatementPeriodsResponse> {
  const res = await api.get<IncomeStatementPeriodsResponse>(
    '/api/reports/income-statement/periods',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

// --------------------------------------------------------------------------
// Balance Sheet
// --------------------------------------------------------------------------

export interface BalanceSheetRow {
  account_code: string;
  account_name: string;
  balance: number;
}

export interface BalanceSheetReport {
  entity_code: string;
  as_of_date: string;
  assets: {
    current: BalanceSheetRow[];
    current_total: number;
    fixed: BalanceSheetRow[];
    fixed_total: number;
    total: number;
  };
  liabilities: {
    current: BalanceSheetRow[];
    current_total: number;
    long_term: BalanceSheetRow[];
    long_term_total: number;
    total: number;
  };
  equity: {
    accounts: BalanceSheetRow[];
    total: number;
  };
  liabilities_and_equity_total: number;
  balanced: boolean;
  variance: number;
}

export async function getBalanceSheet(params: {
  entity_code: string;
  as_of_date: string;
}): Promise<BalanceSheetReport> {
  const res = await api.get<BalanceSheetReport>(
    '/api/reports/balance-sheet',
    { params },
  );
  return res.data;
}

// --------------------------------------------------------------------------
// Trial Balance
// --------------------------------------------------------------------------

export interface TrialBalanceRow {
  account_code: string;
  account_name: string;
  account_type: string;
  normal_balance: 'debit' | 'credit';
  total_debits: number;
  total_credits: number;
  net_balance: number;
  unexpected_balance: boolean;
}

export interface TrialBalanceReport {
  entity_code: string;
  as_of_date: string;
  accounts: TrialBalanceRow[];
  totals: {
    total_debits: number;
    total_credits: number;
    difference: number;
  };
  balanced: boolean;
}

export async function getTrialBalance(params: {
  entity_code: string;
  as_of_date: string;
}): Promise<TrialBalanceReport> {
  const res = await api.get<TrialBalanceReport>(
    '/api/reports/trial-balance',
    { params },
  );
  return res.data;
}

// Alias for the older import name used by the trial-balance page.
export const getAsOfTrialBalance = getTrialBalance;

// --------------------------------------------------------------------------
// AR / AP aging — re-exported from per-module clients
// --------------------------------------------------------------------------

export { getLatestAgedAr as getArAging } from './pos';
export { getHHAPSummary as getApAging } from './hh_ap';

// --------------------------------------------------------------------------
// Plan-tier feature copy (used by the marketing pricing page + onboarding)
// --------------------------------------------------------------------------

export const PLAN_FEATURES: Record<PlanTier, string[]> = {
  starter: [
    'Dashboard',
    'Transactions',
    'Trial Balance',
    'Income Statement',
    'Balance Sheet',
    'Month-end checklist',
    '1 user',
  ],
  professional: [
    'Everything in Starter',
    'AI classifier suggestions',
    'Multi-entity (per-store add-on)',
    'Audit-trail drilldown',
    'Payroll module',
    'CSV exports',
    'Up to 5 team members',
  ],
  // Owner / demo accounts — same feature surface as Professional with
  // no billing relationship. The pricing page hides this tier; the
  // entry exists so Record<PlanTier, ...> stays exhaustive.
  // TODO: Replace with real Stripe subscription when owner is ready to
  // be billed. Delete the billing_subscriptions row with
  // plan_tier='internal' and run through /settings/billing checkout flow.
  internal: [
    'Dashboard',
    'Transactions',
    'Reports',
    'Month-end workflow',
    'AI Assistant',
    'Payroll',
    'AP module',
    'Bank module',
    'Multi-entity',
    'Audit trail',
    'Document storage',
    'Priority support',
  ],
};

// --------------------------------------------------------------------------
// Live General Ledger
// --------------------------------------------------------------------------

export interface GLTransaction {
  id: string;
  journal_batch_id: string;
  posting_date: string;
  description: string;
  reference: string;
  debit: number;
  credit: number;
  balance: number;
  source_module: string;
}

export interface GeneralLedgerResponse {
  entity_code: string;
  account_code: string;
  account_name: string;
  date_from: string | null;
  date_to: string | null;
  opening_balance: number;
  closing_balance: number;
  transactions: GLTransaction[];
  transaction_count: number;
}

export async function getGeneralLedgerReport(input: {
  entity_code: string;
  account_code: string;
  date_from?: string;
  date_to?: string;
}): Promise<GeneralLedgerResponse> {
  const params: Record<string, string> = {
    entity_code: input.entity_code,
    account_code: input.account_code,
  };
  if (input.date_from) params.date_from = input.date_from;
  if (input.date_to) params.date_to = input.date_to;
  const res = await api.get<GeneralLedgerResponse>(
    '/api/reports/general-ledger',
    { params },
  );
  return res.data;
}

// --------------------------------------------------------------------------
// Report drill-down (Slice 1, read-only)
//
// Report line -> account GL activity -> full journal entry -> source document.
// `mode` matches the originating report so the panel reconciles to the line:
//   'period'      Income Statement — activity inside [period_start, period_end]
//   'cumulative'  Balance Sheet / Trial Balance — cumulative-to-date w/ cutover
// --------------------------------------------------------------------------

export type AccountActivityMode = 'period' | 'cumulative';

export interface AccountActivityTxn {
  id: string;
  journal_batch_id: string;
  posting_date: string;
  description: string;
  reference: string;
  debit: number;
  credit: number;
  balance: number;
  source_module: string;
  batch_status: string;
  has_document: boolean;
}

export interface AccountActivityResponse {
  entity_code: string;
  account_code: string;
  account_name: string;
  mode: AccountActivityMode;
  period_start: string | null;
  period_end: string;
  opening_balance: number;
  closing_balance: number;
  transactions: AccountActivityTxn[];
  transaction_count: number;
}

export async function getAccountActivity(input: {
  entity_code: string;
  account_code: string;
  mode: AccountActivityMode;
  period_end: string;
  period_start?: string | null;
}): Promise<AccountActivityResponse> {
  const params: Record<string, string> = {
    entity_code: input.entity_code,
    account_code: input.account_code,
    mode: input.mode,
    period_end: input.period_end,
  };
  if (input.period_start) params.period_start = input.period_start;
  const res = await api.get<AccountActivityResponse>(
    '/api/reports/account-activity',
    { params },
  );
  return res.data;
}

export interface JournalEntryLine {
  id: string;
  line_number: number;
  account_code: string;
  account_name: string;
  debit: number;
  credit: number;
  memo: string | null;
  reference: string | null;
}

export interface JournalEntryResponse {
  batch: {
    id: string;
    source_module: string;
    batch_label: string;
    status: string;
    workflow_status: string;
    total_debits: number;
    total_credits: number;
    balanced: boolean;
    created_at: string | null;
    period: {
      id: string;
      period_label: string;
      period_start: string;
      period_end: string;
      status: string;
    };
  };
  lines: JournalEntryLine[];
  has_documents: boolean;
}

export async function getJournalEntry(input: {
  entity_code: string;
  journal_batch_id: string;
}): Promise<JournalEntryResponse> {
  const res = await api.get<JournalEntryResponse>(
    `/api/reports/journal-entry/${input.journal_batch_id}`,
    { params: { entity_code: input.entity_code } },
  );
  return res.data;
}

export interface JournalEntryDocument {
  link_id: string;
  link_type: string;
  invoice_document_id: string;
  file_name: string | null;
  presigned_url: string | null;
  vendor_name: string | null;
  invoice_number: string | null;
  invoice_type: string | null;
  amount: number | null;
  source: 'invoice_documents' | 'hh_ap_documents';
}

export interface JournalEntryDocumentsResponse {
  journal_batch_id: string;
  documents: JournalEntryDocument[];
}

export async function getJournalEntryDocuments(input: {
  entity_code: string;
  journal_batch_id: string;
  journal_line_id?: string;
}): Promise<JournalEntryDocumentsResponse> {
  const params: Record<string, string> = { entity_code: input.entity_code };
  if (input.journal_line_id) params.journal_line_id = input.journal_line_id;
  const res = await api.get<JournalEntryDocumentsResponse>(
    `/api/reports/journal-entry/${input.journal_batch_id}/documents`,
    { params },
  );
  return res.data;
}
