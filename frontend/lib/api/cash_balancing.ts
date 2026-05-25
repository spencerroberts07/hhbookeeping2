import { api } from './client';

export interface CashBalancingLatest {
  business_date: string;
  opening_balance: number | null;
  closing_balance: number | null;
  total_deposits: number;
  total_withdrawals: number | null;
  variance: number | null;
  status: 'balanced' | 'review';
  tab_name: string | null;
}

/** Most-recent cash_balancing_days row. 404 if no rows exist for the entity. */
export async function getLatestCashBalancing(
  entityCode: string,
): Promise<CashBalancingLatest> {
  const res = await api.get<CashBalancingLatest>('/api/cash-balancing/latest', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

// ---------- /days ----------------------------------------------------------

export type CashBalancingStatus = 'over' | 'short' | 'balanced';

export interface CashBalancingTenderLine {
  line_label: string;
  line_code: string | null;
  amount: number;
  mapped_account_code: string | null;
}

export interface CashBalancingDay {
  id: string;
  business_date: string;
  day_of_week: string;
  opening_cash: number;
  closing_cash: number;
  total_sales: number;
  total_hst: number;
  paid_outs: number;
  over_short: number;
  status: CashBalancingStatus;
  lines: CashBalancingTenderLine[];
}

export interface CashBalancingDaysResponse {
  entity_code: string;
  date_from: string;
  date_to: string;
  days: CashBalancingDay[];
  summary: {
    total_sales: number;
    total_hst: number;
    total_over: number;
    total_short: number;
    net_variance: number;
    day_count: number;
    balanced_days: number;
    over_days: number;
    short_days: number;
  };
}

export async function getCashBalancingDays(params: {
  entity_code: string;
  date_from?: string;
  date_to?: string;
}): Promise<CashBalancingDaysResponse> {
  const res = await api.get<CashBalancingDaysResponse>(
    '/api/cash-balancing/days',
    { params },
  );
  return res.data;
}

// ---------- /month-end-batch ----------------------------------------------

export interface CashBalancingJournalLine {
  line_number: number;
  account_code: string;
  memo: string | null;
  debit_amount: number;
  credit_amount: number;
}

export interface CashBalancingMonthEndBatch {
  batch_id: string | null;
  status: string | null;
  period_id: string | null;
  batch_label?: string | null;
  total_debits: number;
  total_credits: number;
  imbalance: number;
  is_balanced: boolean;
  summary_json?: Record<string, unknown>;
  lines: CashBalancingJournalLine[];
}

export async function getCashBalancingMonthEndBatch(params: {
  entity_code: string;
  period_id?: string;
}): Promise<CashBalancingMonthEndBatch> {
  const res = await api.get<CashBalancingMonthEndBatch>(
    '/api/cash-balancing/month-end-batch',
    { params },
  );
  return res.data;
}

// ---------- /fix-imbalance -------------------------------------------------

export interface FixImbalancePayload {
  entity_code: string;
  period_id: string;
  offset_account_code?: string;
  offset_description?: string;
  actor_email?: string;
}

export interface FixImbalanceResponse {
  batch_id: string;
  status: string;
  total_debits: number;
  total_credits: number;
  imbalance_resolved: number;
  balancing_line: {
    line_number: number;
    account_code: string;
    debit_amount: number;
    credit_amount: number;
    memo: string;
  };
}

export async function fixCashBalancingImbalance(
  body: FixImbalancePayload,
): Promise<FixImbalanceResponse> {
  const res = await api.post<FixImbalanceResponse>(
    '/api/cash-balancing/fix-imbalance',
    body,
  );
  return res.data;
}

// ---------- /sync-history --------------------------------------------------

export interface CashBalancingSyncRun {
  id: string;
  run_type: string;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  duration_seconds: number | null;
  tabs_read: number | null;
  days_upserted: number | null;
  lines_inserted: number | null;
  error_text: string | null;
}

export async function getCashBalancingHistory(params: {
  entity_code: string;
  limit?: number;
}): Promise<{ runs: CashBalancingSyncRun[]; count: number }> {
  const res = await api.get<{ runs: CashBalancingSyncRun[]; count: number }>(
    '/api/cash-balancing/sync-history',
    { params },
  );
  return res.data;
}

// ---------- /sync (POST) ---------------------------------------------------

export interface TriggerSyncResponse {
  status: string;
  run_id?: string;
  days_upserted?: number;
  lines_inserted?: number;
  message?: string;
}

export async function triggerCashBalancingSync(
  entityCode: string,
  lookbackDays?: number,
): Promise<TriggerSyncResponse> {
  const res = await api.post<TriggerSyncResponse>('/api/cash-balancing/sync', {
    entity_code: entityCode,
    lookback_days: lookbackDays,
  });
  return res.data;
}
