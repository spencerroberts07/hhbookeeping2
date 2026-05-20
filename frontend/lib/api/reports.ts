/**
 * Reports — frontend mock surface.
 *
 * The backend does NOT have Income Statement, Balance Sheet, app-native
 * General Ledger, "as-of" Trial Balance, or live AR/AP aging endpoints yet
 * (see backend/docs/endpoint_catalog.md, section D). These functions return
 * mock data so the UI renders end-to-end while the backend catches up.
 *
 * Every function here is flagged with a TODO marker that the QA pass and
 * the deliverables doc both pick up.
 */
import type { PlanTier } from './billing';

export interface IncomeStatementSection {
  label: string;
  rows: Array<{ account_code: string; account_name: string; amount: number }>;
  subtotal: number;
}

export interface IncomeStatementReport {
  entity_code: string;
  period_start: string;
  period_end: string;
  revenue: IncomeStatementSection;
  cogs: IncomeStatementSection;
  gross_profit: number;
  operating_expenses: IncomeStatementSection;
  net_income: number;
  comparison: {
    period_start: string;
    period_end: string;
    revenue: number;
    cogs: number;
    operating_expenses: number;
    net_income: number;
  } | null;
}

// TODO: backend endpoint not built — POST /api/reports/income-statement
export async function getIncomeStatement(_params: {
  entity_code: string;
  period_start: string;
  period_end: string;
  compare_to?: 'prior_period' | 'prior_year' | null;
}): Promise<IncomeStatementReport> {
  await new Promise((r) => setTimeout(r, 250)); // simulate latency
  return {
    entity_code: _params.entity_code,
    period_start: _params.period_start,
    period_end: _params.period_end,
    revenue: {
      label: 'Revenue',
      rows: [
        { account_code: '4010', account_name: 'Lumber Sales', amount: 248_120.55 },
        { account_code: '4020', account_name: 'Hardware Sales', amount: 312_805.12 },
        { account_code: '4030', account_name: 'Paint & Sundries', amount: 41_280.33 },
        { account_code: '4040', account_name: 'Garden Centre', amount: 18_440.87 },
      ],
      subtotal: 620_646.87,
    },
    cogs: {
      label: 'Cost of Goods Sold',
      rows: [
        { account_code: '5010', account_name: 'COGS — Lumber', amount: 184_220.0 },
        { account_code: '5020', account_name: 'COGS — Hardware', amount: 226_780.0 },
        { account_code: '5030', account_name: 'COGS — Paint', amount: 28_900.0 },
      ],
      subtotal: 439_900.0,
    },
    gross_profit: 180_746.87,
    operating_expenses: {
      label: 'Operating Expenses',
      rows: [
        { account_code: '6120', account_name: 'Wages & Benefits', amount: 64_220.5 },
        { account_code: '6510', account_name: 'Rent', amount: 11_452.0 },
        { account_code: '6550', account_name: 'Bad Debt', amount: 870.0 },
        { account_code: '6610', account_name: 'Utilities', amount: 4_120.22 },
        { account_code: '6710', account_name: 'Insurance', amount: 2_840.0 },
      ],
      subtotal: 83_502.72,
    },
    net_income: 97_244.15,
    comparison: null,
  };
}

export interface BalanceSheetReport {
  entity_code: string;
  as_of_date: string;
  assets: {
    current: Array<{ account_code: string; name: string; amount: number }>;
    fixed: Array<{ account_code: string; name: string; amount: number }>;
    total: number;
  };
  liabilities: {
    current: Array<{ account_code: string; name: string; amount: number }>;
    long_term: Array<{ account_code: string; name: string; amount: number }>;
    total: number;
  };
  equity: {
    rows: Array<{ account_code: string; name: string; amount: number }>;
    total: number;
  };
  balances: boolean;
}

// TODO: backend endpoint not built — POST /api/reports/balance-sheet
export async function getBalanceSheet(_params: {
  entity_code: string;
  as_of_date: string;
}): Promise<BalanceSheetReport> {
  await new Promise((r) => setTimeout(r, 250));
  return {
    entity_code: _params.entity_code,
    as_of_date: _params.as_of_date,
    assets: {
      current: [
        { account_code: '1020', name: 'TD Operating', amount: 142_388.12 },
        { account_code: '1085', name: 'Accounts Receivable', amount: 14_220.45 },
        { account_code: '1120', name: 'Inventory', amount: 612_400.0 },
      ],
      fixed: [
        { account_code: '1500', name: 'Buildings — Cost', amount: 480_000.0 },
        { account_code: '1650', name: 'A/D Provision', amount: -120_000.0 },
      ],
      total: 1_129_008.57,
    },
    liabilities: {
      current: [
        { account_code: '2020', name: 'Accounts Payable', amount: 198_440.0 },
        { account_code: '2030', name: 'HH AP Clearing', amount: 0.0 },
        { account_code: '2300', name: 'HST Payable', amount: 8_220.4 },
      ],
      long_term: [
        { account_code: '2700', name: 'Term Loan', amount: 220_000.0 },
      ],
      total: 426_660.4,
    },
    equity: {
      rows: [
        { account_code: '3010', name: 'Owner Capital', amount: 500_000.0 },
        { account_code: '3020', name: 'Retained Earnings', amount: 202_348.17 },
      ],
      total: 702_348.17,
    },
    balances: true,
  };
}

export interface AsOfTrialBalanceRow {
  account_code: string;
  account_name: string;
  debit: number;
  credit: number;
  expected_normal: 'debit' | 'credit';
  unexpected_balance: boolean;
}

// TODO: backend endpoint not built — GET /api/reports/trial-balance-as-of
// Until built, the Trial Balance page falls back to the run-based endpoint
// in lib/api/gl.ts (getTrialBalance) for the most recent GL import run.
export async function getAsOfTrialBalance(_params: {
  entity_code: string;
  as_of_date: string;
}): Promise<{
  rows: AsOfTrialBalanceRow[];
  total_debit: number;
  total_credit: number;
}> {
  await new Promise((r) => setTimeout(r, 200));
  const rows: AsOfTrialBalanceRow[] = [
    {
      account_code: '1020',
      account_name: 'TD Operating',
      debit: 142_388.12,
      credit: 0,
      expected_normal: 'debit',
      unexpected_balance: false,
    },
    {
      account_code: '1120',
      account_name: 'Inventory',
      debit: 612_400.0,
      credit: 0,
      expected_normal: 'debit',
      unexpected_balance: false,
    },
    {
      account_code: '2020',
      account_name: 'Accounts Payable',
      debit: 0,
      credit: 198_440.0,
      expected_normal: 'credit',
      unexpected_balance: false,
    },
  ];
  const total_debit = rows.reduce((s, r) => s + r.debit, 0);
  const total_credit = rows.reduce((s, r) => s + r.credit, 0);
  return { rows, total_debit, total_credit };
}

export interface ArAgingRow {
  customer_name: string;
  current: number;
  over_30: number;
  over_60: number;
  over_90: number;
  total: number;
}

// Uses real /api/pos-import/aged-ar/latest endpoint via lib/api/pos.ts.
// Re-exported here for the reports page to import from one place.
export { getLatestAgedAr as getArAging } from './pos';

export interface ApAgingRow {
  due_bucket: 'current' | 'over_30' | 'over_60' | 'over_90';
  invoice_count: number;
  amount: number;
}

// Uses real /api/hh-ap/summary aging buckets via lib/api/hh_ap.ts.
export { getHHAPSummary as getApAging } from './hh_ap';

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
};
