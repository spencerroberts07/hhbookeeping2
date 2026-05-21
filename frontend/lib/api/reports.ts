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
// Income Statement
// --------------------------------------------------------------------------

export interface IncomeStatementRow {
  account_code: string;
  account_name: string;
  amount: number;
}

export interface IncomeStatementBody {
  revenue: IncomeStatementRow[];
  revenue_total: number;
  cogs: IncomeStatementRow[];
  cogs_total: number;
  gross_profit: number;
  gross_margin_pct: number | null;
  operating_expenses: IncomeStatementRow[];
  operating_expenses_total: number;
  net_income: number;
}

export interface IncomeStatementReport extends IncomeStatementBody {
  entity_code: string;
  period: { from: string; to: string };
  comparison: IncomeStatementBody | null;
}

export async function getIncomeStatement(params: {
  entity_code: string;
  date_from: string;
  date_to: string;
  compare_to?: 'prior_period' | 'prior_year';
}): Promise<IncomeStatementReport> {
  const res = await api.get<IncomeStatementReport>(
    '/api/reports/income-statement',
    { params },
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
