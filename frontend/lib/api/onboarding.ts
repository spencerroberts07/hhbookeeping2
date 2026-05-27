import { api } from './client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface OnboardingStatus {
  entity_code: string;
  entity_name: string;
  has_chart_of_accounts: boolean;
  account_count: number;
  has_opening_balances: boolean;
  opening_balance_date: string | null;
  has_gl_history: boolean;
  gl_history_from: string | null;
  gl_history_to: string | null;
  journal_line_count: number;
  has_hh_ap_history: boolean;
  hh_ap_months_loaded: number;
  has_bank_transactions: boolean;
  bank_transaction_count: number;
  has_qbo_connection: boolean;
  qbo_realm_id: string | null;
  qbo_connected_at: string | null;
  onboarding_complete: boolean;
  onboarding_completed_at: string | null;
}

export interface ChartPreviewAccount {
  code: string;
  name: string;
  type: string;
  subtype: string;
  normal_balance: 'debit' | 'credit' | string;
  parent_code: string | null;
}

export interface ChartPreviewResponse {
  entity_code: string;
  filename: string;
  preview: {
    accounts: ChartPreviewAccount[];
    count: number;
  };
}

export interface ChartParseProgress {
  job_id: string;
  job_type: 'parse_chart_of_accounts';
  status: 'pending' | 'running' | 'complete' | 'error';
  pct_complete: number;
  current_step: string | null;
  preview: { accounts: ChartPreviewAccount[]; count: number } | null;
  filename: string | null;
  entity_code: string | null;
  error: string | null;
}

export interface TbParseProgress {
  job_id: string;
  job_type: 'parse_trial_balance';
  status: 'pending' | 'running' | 'complete' | 'error';
  pct_complete: number;
  current_step: string | null;
  preview: {
    tb_lines: TbPreviewLine[];
    total_debits: number;
    total_credits: number;
    variance: number;
    balanced: boolean;
  } | null;
  filename: string | null;
  entity_code: string | null;
  as_of_date: string | null;
  error: string | null;
}

export interface ChartConfirmResponse {
  entity_code: string;
  saved_count: number;
  conflicts: Array<{ code: string; name: string }>;
  source: string;
}

export interface QboChartResponse {
  entity_code: string;
  account_count: number;
  bank_account_count: number;
  source: 'qbo';
}

export interface TbPreviewLine {
  account_code: string;
  account_name: string;
  debit_balance: number;
  credit_balance: number;
}

export interface TbPreviewResponse {
  entity_code: string;
  as_of_date: string;
  filename: string;
  preview: {
    tb_lines: TbPreviewLine[];
    total_debits: number;
    total_credits: number;
    variance: number;
    balanced: boolean;
  };
}

export interface OpeningConfirmResponse {
  entity_code: string;
  batch_id: string;
  line_count: number;
  total_debits: number;
  total_credits: number;
  balanced: boolean;
}

export interface GLJobResponse {
  job_id: string;
  status: 'pending' | 'running' | 'complete' | 'error';
}

export interface SuspenseEntry {
  account_code: string;
  transaction_count: number;
  total_amount: number;
  sample_name: string;
}

export interface GLProgressResponse {
  job_id: string;
  job_type: string;
  status: 'pending' | 'running' | 'complete' | 'error';
  pct_complete: number;
  current_step: string | null;
  months_imported: number;
  lines_created: number;
  batches_created: number;
  suspense_entries: SuspenseEntry[];
  error: string | null;
}

export interface GLPreviewPeriod {
  month: string;
  year: number;
  month_num: number;
  transaction_count: number;
}

export interface GLPreviewResponse {
  entity_code: string;
  periods_detected: GLPreviewPeriod[];
  total_transactions: number;
  date_range: { start: string | null; end: string | null } | null;
  accounts_found: number;
  unmatched_accounts: number;
  unmatched_codes: string[];
}

export interface CompleteOnboardingResponse {
  entity_code: string;
  first_period: { period_start: string; period_end: string };
  accounts_loaded: number;
  journal_lines_loaded: number;
  vendors_learned: number;
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

export async function getOnboardingStatus(
  entityCode: string,
): Promise<OnboardingStatus> {
  const res = await api.get<OnboardingStatus>('/api/onboarding/status', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function uploadChartFile(input: {
  entity_code: string;
  actor_email: string;
  file: File;
}): Promise<GLJobResponse> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  // Returns immediately with a job_id — the wizard polls
  // getChartParseProgress for the actual preview.
  const res = await api.post<GLJobResponse>(
    '/api/onboarding/chart-of-accounts/upload',
    fd,
  );
  return res.data;
}

export async function getChartParseProgress(
  jobId: string,
): Promise<ChartParseProgress> {
  const res = await api.get<ChartParseProgress>(
    `/api/onboarding/chart-of-accounts/progress/${jobId}`,
  );
  return res.data;
}

export async function confirmChart(input: {
  entity_code: string;
  actor_email: string;
  accounts: ChartPreviewAccount[];
}): Promise<ChartConfirmResponse> {
  const res = await api.post<ChartConfirmResponse>(
    '/api/onboarding/chart-of-accounts/confirm',
    input,
  );
  return res.data;
}

export async function pullChartFromQbo(input: {
  entity_code: string;
  actor_email: string;
}): Promise<QboChartResponse> {
  const res = await api.post<QboChartResponse>(
    '/api/onboarding/chart-of-accounts/qbo',
    input,
  );
  return res.data;
}

export async function uploadOpeningBalancesFile(input: {
  entity_code: string;
  actor_email: string;
  as_of_date: string;
  file: File;
}): Promise<GLJobResponse> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('as_of_date', input.as_of_date);
  fd.append('file', input.file);
  // Returns immediately with a job_id — the wizard polls
  // getOpeningBalancesParseProgress for the actual preview.
  const res = await api.post<GLJobResponse>(
    '/api/onboarding/opening-balances/upload',
    fd,
  );
  return res.data;
}

export async function getOpeningBalancesParseProgress(
  jobId: string,
): Promise<TbParseProgress> {
  const res = await api.get<TbParseProgress>(
    `/api/onboarding/opening-balances/progress/${jobId}`,
  );
  return res.data;
}

export async function confirmOpeningBalances(input: {
  entity_code: string;
  actor_email: string;
  as_of_date: string;
  tb_lines: TbPreviewLine[];
}): Promise<OpeningConfirmResponse> {
  const res = await api.post<OpeningConfirmResponse>(
    '/api/onboarding/opening-balances/confirm',
    input,
  );
  return res.data;
}

export async function pullOpeningBalancesFromQbo(input: {
  entity_code: string;
  actor_email: string;
  as_of_date: string;
}): Promise<OpeningConfirmResponse> {
  const res = await api.post<OpeningConfirmResponse>(
    '/api/onboarding/opening-balances/qbo',
    input,
  );
  return res.data;
}

export async function previewGLHistoryFile(input: {
  entity_code: string;
  actor_email: string;
  file: File;
}): Promise<GLPreviewResponse> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  // Synchronous parse — no job polling. The endpoint reads, parses,
  // returns the period + unmatched summary, and writes nothing.
  const res = await api.post<GLPreviewResponse>(
    '/api/onboarding/gl-history/preview',
    fd,
  );
  return res.data;
}

export async function startGLHistoryFromFile(input: {
  entity_code: string;
  actor_email: string;
  date_from: string;
  date_to: string;
  file: File;
}): Promise<GLJobResponse> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('date_from', input.date_from);
  fd.append('date_to', input.date_to);
  fd.append('file', input.file);
  const res = await api.post<GLJobResponse>(
    '/api/onboarding/gl-history/upload',
    fd,
  );
  return res.data;
}

export async function startGLHistoryFromQbo(input: {
  entity_code: string;
  actor_email: string;
  date_from: string;
  date_to: string;
}): Promise<GLJobResponse> {
  const res = await api.post<GLJobResponse>(
    '/api/onboarding/gl-history/qbo',
    input,
  );
  return res.data;
}

export async function getGLHistoryProgress(
  jobId: string,
): Promise<GLProgressResponse> {
  const res = await api.get<GLProgressResponse>(
    `/api/onboarding/gl-history/progress/${jobId}`,
  );
  return res.data;
}

export async function completeOnboarding(input: {
  entity_code: string;
  actor_email: string;
}): Promise<CompleteOnboardingResponse> {
  const res = await api.post<CompleteOnboardingResponse>(
    '/api/onboarding/complete',
    input,
  );
  return res.data;
}

export async function startQboConnect(
  entityCode: string,
): Promise<{ entity_code: string; authorization_url: string; state: string }> {
  const res = await api.get('/api/auth/quickbooks/connect', {
    params: { entity_code: entityCode },
  });
  return res.data;
}
