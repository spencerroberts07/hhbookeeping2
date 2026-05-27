import { api } from './client';

export type PayrollRunStatus =
  | 'draft'
  | 'draft_confirmed'
  | 'submitted'
  | 'approved'
  | 'posted'
  | 'voided';

export interface Employee {
  id: string;
  employee_number: number;
  first_name: string | null;
  last_name: string | null;
  full_name?: string | null;
  employment_type: string | null;
  hourly_rate: number | null;
  biweekly_salary: number | null;
  vacation_rate: number | null;
  has_life_insurance: boolean;
  life_insurance_biweekly: number | null;
  is_active: boolean;
  ods_name_key: string | null;
  notes: string | null;
  // Fields exposed by the editor drawer (returned from PUT
  // /employees/{id} and listed by /employees).
  province?: string | null;
  federal_td1_claim_code?: number | null;
  provincial_td1_claim_code?: number | null;
  cpp_exempt?: boolean;
  ei_exempt?: boolean;
  start_date?: string | null;
  address?: string | null;
  bank_transit?: string | null;
  bank_institution?: string | null;
  bank_account?: string | null;
  // Feature 1 — additional withholding
  additional_fed_tax?: number | null;
  additional_prov_tax?: number | null;
  additional_tax_effective_date?: string | null;
  additional_tax_td1_on_file?: boolean | null;
  // Feature 2 — vacation balances (denormalized)
  vacation_hours_balance?: number | null;
  vacation_dollars_balance?: number | null;
  // Feature 3 — YTD totals
  ytd_gross?: number | null;
  ytd_cpp_employee?: number | null;
  ytd_cpp2_employee?: number | null;
  ytd_ei_employee?: number | null;
  ytd_fed_tax?: number | null;
  ytd_reset_date?: string | null;
}

export interface UpdateEmployeeInput {
  entity_code: string;
  actor_email: string;
  first_name?: string;
  last_name?: string;
  employment_type?: string;
  hourly_rate?: number;
  biweekly_salary?: number;
  vacation_rate?: number;
  province?: string;
  federal_td1_claim_code?: number;
  provincial_td1_claim_code?: number;
  cpp_exempt?: boolean;
  ei_exempt?: boolean;
  has_life_insurance?: boolean;
  life_insurance_biweekly?: number;
  is_active?: boolean;
  start_date?: string;
  address?: string;
  bank_transit?: string;
  bank_institution?: string;
  bank_account?: string;
  notes?: string;
  // Feature 1 — additional withholding
  additional_fed_tax?: number;
  additional_prov_tax?: number;
  additional_tax_effective_date?: string;
  additional_tax_td1_on_file?: boolean;
}

export async function updateEmployee(
  employeeId: string,
  input: UpdateEmployeeInput,
): Promise<Employee> {
  const res = await api.put(`/api/payroll/employees/${employeeId}`, input);
  return res.data;
}

export async function getEmployeeDetail(
  entityCode: string,
  employeeId: string,
): Promise<Employee> {
  const res = await api.get<Employee>(
    `/api/payroll/employees/${employeeId}`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

// ---------- Vacation ledger (Feature 2) ----------

export interface VacationLedgerEntry {
  id: string;
  payroll_run_id: string | null;
  pay_run_number: string | null;
  period_end: string | null;
  entry_type: 'accrual' | 'payout' | 'adjustment' | 'opening_balance';
  hours_delta: number;
  dollars_delta: number;
  balance_hours_after: number;
  balance_dollars_after: number;
  notes: string | null;
  created_at: string | null;
  created_by: string | null;
}

export interface VacationLedgerResponse {
  employee_id: string;
  employee_name: string;
  vacation_rate: number;
  balance_hours: number;
  balance_dollars: number;
  entries: VacationLedgerEntry[];
}

export async function getVacationLedger(
  entityCode: string,
  employeeId: string,
): Promise<VacationLedgerResponse> {
  const res = await api.get<VacationLedgerResponse>(
    `/api/payroll/employees/${employeeId}/vacation-ledger`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

// ---------- Stat-day calendar (Feature 4) ----------

export interface StatDay {
  holiday_name: string;
  holiday_date: string;
  observed_date: string;
}

export async function getStatDays(
  year: number,
  province = 'ON',
): Promise<{ year: number; province: string; stat_days: StatDay[]; count: number }> {
  const res = await api.get('/api/payroll/stat-days', {
    params: { year, province },
  });
  return res.data;
}

// ---------- YTD reset (Feature 3) ----------

export async function resetYtd(input: {
  entity_code: string;
  actor_email: string;
  confirm: boolean;
}): Promise<{ ok: boolean; entity_code: string; employees_reset: number; reset_date: string }> {
  const res = await api.post('/api/payroll/ytd/reset', input);
  return res.data;
}

// ---------- Pay stubs (Feature 5) ----------

export interface PaystubSummary {
  id: string;
  employee_id?: string;
  employee_name?: string;
  employee_number?: number;
  payroll_run_id?: string;
  pay_run_number?: string;
  period_start?: string | null;
  period_end?: string | null;
  pay_date?: string | null;
  file_name: string;
  r2_uploaded: boolean;
  generated_at: string | null;
  generated_by?: string | null;
}

export async function generatePaystubs(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<{
  ok: boolean;
  generated: number;
  r2_upload_failures: number;
  results: Array<{
    employee_name: string;
    file_name?: string;
    ok: boolean;
    error?: string;
    paystub_id?: string;
    r2_uploaded?: boolean;
  }>;
}> {
  const res = await api.post(
    `/api/payroll/runs/${payrollRunId}/generate-paystubs`,
    body,
  );
  return res.data;
}

export async function listRunPaystubs(
  entityCode: string,
  payrollRunId: string,
): Promise<{ payroll_run_id: string; paystubs: PaystubSummary[]; count: number }> {
  const res = await api.get(
    `/api/payroll/runs/${payrollRunId}/paystubs`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

export async function listEmployeePaystubs(
  entityCode: string,
  employeeId: string,
  limit = 12,
): Promise<{ employee_id: string; paystubs: PaystubSummary[]; count: number }> {
  const res = await api.get(
    `/api/payroll/employees/${employeeId}/paystubs`,
    { params: { entity_code: entityCode, limit } },
  );
  return res.data;
}

export async function getPaystubDownload(
  entityCode: string,
  paystubId: string,
): Promise<{
  file_name: string;
  download_url: string;
  expires_in_seconds: number;
  generated_at: string | null;
}> {
  const res = await api.get(`/api/payroll/paystubs/${paystubId}/download`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

// ===== Tier-2 additions =====

// ---------- F1 variance alerts ----------

export type VarianceSeverity = 'block' | 'warn' | 'info';

export interface PayrollRunVariance {
  id: string;
  employee_id: string;
  employee_name: string;
  variance_type: string;
  severity: VarianceSeverity;
  previous_value: number | null;
  current_value: number | null;
  change_pct: number | null;
  message: string;
  acknowledged: boolean;
  acknowledged_by: string | null;
  acknowledged_at: string | null;
  created_at: string | null;
}

export async function analyzeRunVariances(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<{
  payroll_run_id: string;
  variances: Array<Omit<PayrollRunVariance, 'id' | 'acknowledged' | 'acknowledged_by' | 'acknowledged_at' | 'created_at'>>;
  counts: { block: number; warn: number; info: number };
}> {
  const res = await api.post(`/api/payroll/runs/${payrollRunId}/analyze-variances`, body);
  return res.data;
}

export async function listRunVariances(
  entityCode: string,
  payrollRunId: string,
): Promise<{ payroll_run_id: string; variances: PayrollRunVariance[]; count: number }> {
  const res = await api.get(`/api/payroll/runs/${payrollRunId}/variances`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function acknowledgeVariance(
  payrollRunId: string,
  varianceId: string,
  body: { entity_code: string; actor_email: string },
): Promise<{ ok: boolean; variance_id: string }> {
  const res = await api.post(
    `/api/payroll/runs/${payrollRunId}/variances/${varianceId}/acknowledge`,
    body,
  );
  return res.data;
}

// ---------- F2 EFT-sent confirmation ----------

export async function markEftSent(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string; notes?: string | null },
): Promise<{ ok: boolean }> {
  const res = await api.post(`/api/payroll/runs/${payrollRunId}/mark-eft-sent`, body);
  return res.data;
}

export async function markEmployeesPaid(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<{ ok: boolean }> {
  const res = await api.post(`/api/payroll/runs/${payrollRunId}/mark-employees-paid`, body);
  return res.data;
}

// ---------- F3 retro + correction runs ----------

export interface RetroPeriod {
  payroll_run_id: string;
  period_start: string;
  period_end: string;
  pay_date: string;
  hours: number;
  old_gross: number;
  new_gross: number;
  delta: number;
}

export interface RetroCalcResponse {
  employee_id: string;
  employee_name: string;
  old_rate: number;
  new_rate: number;
  effective_date: string;
  retro_amount_gross: number;
  estimated_cpp: number;
  estimated_ei: number;
  estimated_fed_tax: number;
  estimated_net: number;
  note: string;
  periods: RetroPeriod[];
}

export async function calculateRetro(input: {
  entity_code: string;
  employee_id: string;
  old_rate: number;
  new_rate: number;
  effective_date: string;
}): Promise<RetroCalcResponse> {
  const res = await api.post('/api/payroll/calculate-retro', input);
  return res.data;
}

export interface CorrectionEmployeeSpec {
  employee_id: string;
  override_gross?: number;
  retro_old_rate?: number;
  retro_new_rate?: number;
  retro_periods?: number;
  hours_per_period?: number;
}

export async function createCorrectionRun(input: {
  entity_code: string;
  actor_email: string;
  run_type: 'correction' | 'bonus' | 'retroactive' | 'offcycle';
  description: string;
  period_start: string;
  period_end: string;
  pay_date: string;
  parent_run_id?: string | null;
  employees: CorrectionEmployeeSpec[];
}): Promise<{
  ok: boolean;
  payroll_run_id: string;
  pay_run_number: string;
  run_type: string;
  total_gross: number;
  total_net: number;
}> {
  const res = await api.post('/api/payroll/runs/create-correction', input);
  return res.data;
}

// ---------- F4 employee history + employment record ----------

export interface PayHistoryLine {
  payroll_run_id: string;
  pay_run_number: string;
  run_type: string;
  period_start: string | null;
  period_end: string | null;
  pay_date: string | null;
  total_hours: number;
  gross_pay: number;
  net_pay: number;
  cpp_ee: number;
  ei_ee: number;
  fed_tax: number;
  vacation_earned: number;
  vacation_paid: number;
  stat_pay: number;
}

export async function getEmployeeHistory(
  entityCode: string,
  employeeId: string,
): Promise<{ employee_id: string; employee_name: string; history: PayHistoryLine[]; count: number }> {
  const res = await api.get(`/api/payroll/employees/${employeeId}/history`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function generateEmploymentRecord(
  employeeId: string,
  body: { entity_code: string; actor_email: string },
): Promise<{
  ok: boolean;
  file_name: string;
  r2_uploaded: boolean;
  download_url: string | null;
  pdf_base64: string | null;
}> {
  const res = await api.post(
    `/api/payroll/employees/${employeeId}/employment-record`,
    body,
  );
  return res.data;
}

// ---------- F5 T4 generation ----------

export interface T4Row {
  id: string;
  employee_id: string;
  employee_name: string;
  employee_number: number | null;
  calendar_year: number;
  box_14: number;
  box_16: number;
  box_18: number;
  box_22: number;
  box_24: number;
  box_26: number;
  box_40: number;
  file_name: string | null;
  r2_uploaded: boolean;
  generated_at: string | null;
  filed_with_cra: boolean;
  filed_at: string | null;
}

export async function generateT4s(body: {
  entity_code: string;
  actor_email: string;
  calendar_year: number;
}): Promise<{
  ok: boolean;
  calendar_year: number;
  employees_count: number;
  r2_upload_failures: number;
  totals: { employment_income: number; cpp: number; ei: number; tax: number };
  results: Array<{
    t4_id: string | null;
    employee_id: string;
    employee_name: string;
    file_name: string;
    r2_uploaded: boolean;
    box_14: number;
  }>;
}> {
  const res = await api.post('/api/payroll/t4s/generate', body);
  return res.data;
}

export async function listT4s(
  entityCode: string,
  calendarYear: number,
): Promise<{ calendar_year: number; t4s: T4Row[]; count: number }> {
  const res = await api.get('/api/payroll/t4s', {
    params: { entity_code: entityCode, calendar_year: calendarYear },
  });
  return res.data;
}

export async function getT4Download(
  entityCode: string,
  t4Id: string,
): Promise<{ file_name: string; download_url: string; expires_in_seconds: number }> {
  const res = await api.get(`/api/payroll/t4s/${t4Id}/download`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function markT4Filed(
  t4Id: string,
  body: { entity_code: string; actor_email: string },
): Promise<{ ok: boolean }> {
  const res = await api.post(`/api/payroll/t4s/${t4Id}/mark-filed`, body);
  return res.data;
}

export interface PayrollRun {
  id: string;
  pay_run_number: string;
  period_number: number;
  period_start: string;
  period_end: string;
  pay_date: string;
  status: PayrollRunStatus;
  gross_total: number;
  net_total: number;
  cra_total: number;
  vacation_payable_total: number;
  created_at: string;
}

export async function listEmployees(
  entityCode: string,
): Promise<{ employees: Employee[] }> {
  const res = await api.get('/api/payroll/employees', {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function upsertEmployee(input: {
  entity_code: string;
  employee_number: number;
  actor_email: string;
  // ... remaining fields are all optional ...
  first_name?: string;
  last_name?: string;
  employment_type?: string;
  hourly_rate?: number;
  biweekly_salary?: number;
  vacation_rate?: number;
  has_life_insurance?: boolean;
  life_insurance_biweekly?: number;
  is_active?: boolean;
  ods_name_key?: string;
  notes?: string;
}): Promise<Employee> {
  const res = await api.post('/api/payroll/employees/upsert', input);
  return res.data;
}

export async function seedEmployees(input: {
  entity_code: string;
  actor_email: string;
}): Promise<{ inserted: number }> {
  const res = await api.post('/api/payroll/employees/seed', input);
  return res.data;
}

export async function listPayrollRuns(params: {
  entity_code: string;
  period_end?: string;
  limit?: number;
}): Promise<{ runs: PayrollRun[] }> {
  const res = await api.get('/api/payroll/runs', { params });
  return res.data;
}

export interface PayrollRunLine {
  id: string;
  employee_id: string;
  employee_number: number;
  full_name: string;
  employment_type: string;
  is_on_vacation: boolean;
  week1_hours: string;
  week2_hours: string;
  total_hours: string;
  hourly_rate: string | null;
  reg_hours_pay: string;
  salary_pay: string;
  stat_pay: string;
  vacation_paid: string;
  gross_pay: string;
  taxable_gross: string;
  fed_tax: string;
  cpp_ee: string;
  cpp_er: string;
  ei_ee: string;
  ei_er: string;
  life_taxable_benefit: string;
  vacation_earned: string;
  net_pay: string;
  notes: string | null;
}

export interface PayrollRunDetail {
  entity_code: string;
  run: {
    id: string;
    pay_run_number: string;
    period_number: number;
    period_start: string;
    period_end: string;
    pay_date: string;
    pay_type: string;
    status: string;
    workflow_status: string;
    active_employees: number;
    paid_employees: number;
    total_gross: string;
    total_net_pay: string;
    total_fed_tax: string;
    total_cpp_ee: string;
    total_cpp_er: string;
    total_ei_ee: string;
    total_ei_er: string;
    total_life_taxable: string;
    total_vacation_earned: string;
    total_vacation_paid: string;
    total_stat_pay: string;
    cra_remittance_amount: string;
    journal_batch_id: string | null;
    summary: Record<string, unknown>;
  };
  lines: PayrollRunLine[];
}

export async function getPayrollRun(
  entityCode: string,
  payrollRunId: string,
): Promise<PayrollRunDetail> {
  const res = await api.get<PayrollRunDetail>(
    `/api/payroll/runs/${payrollRunId}`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}

export async function getPayrollRunSummary(
  entityCode: string,
  payrollRunId: string,
): Promise<unknown> {
  const res = await api.get(`/api/payroll/runs/${payrollRunId}/summary`, {
    params: { entity_code: entityCode },
  });
  return res.data;
}

export async function uploadPayrollHours(input: {
  entity_code: string;
  pay_run_number: string;
  period_number: number;
  period_start: string;
  period_end: string;
  pay_date: string;
  actor_email: string;
  file: File;
  stat_pay_overrides?: string;
  vacation_paid_overrides?: string;
}): Promise<unknown> {
  const fd = new FormData();
  for (const [k, v] of Object.entries(input)) {
    if (v === undefined || v === null) continue;
    if (k === 'file') fd.append(k, v as File);
    else fd.append(k, String(v));
  }
  const res = await api.post('/api/payroll/runs/upload-hours', fd);
  return res.data;
}

export async function uploadPayrollRegister(input: {
  entity_code: string;
  actor_email: string;
  file: File;
  pay_run_number?: string;
  period_number?: number;
  pay_date?: string;
}): Promise<unknown> {
  const fd = new FormData();
  fd.append('entity_code', input.entity_code);
  fd.append('actor_email', input.actor_email);
  fd.append('file', input.file);
  if (input.pay_run_number) fd.append('pay_run_number', input.pay_run_number);
  if (input.period_number != null)
    fd.append('period_number', String(input.period_number));
  if (input.pay_date) fd.append('pay_date', input.pay_date);
  const res = await api.post('/api/payroll/runs/upload-register', fd);
  return res.data;
}

export async function buildPayrollJournal(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<unknown> {
  const res = await api.post(
    `/api/payroll/runs/${payrollRunId}/build-journal`,
    body,
  );
  return res.data;
}

export async function submitPayrollRun(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<unknown> {
  const res = await api.post(`/api/payroll/runs/${payrollRunId}/submit`, body);
  return res.data;
}

export async function approvePayrollRun(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<unknown> {
  const res = await api.post(`/api/payroll/runs/${payrollRunId}/approve`, body);
  return res.data;
}

export async function schedulePayrollWithdrawals(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<unknown> {
  const res = await api.post(
    `/api/payroll/runs/${payrollRunId}/schedule-withdrawals`,
    body,
  );
  return res.data;
}

// ---- EFT generation ------------------------------------------------------

export interface GenerateEftResponse {
  id: string;
  payroll_run_id: string;
  file_name: string;
  file_path: string | null;
  r2_uploaded: boolean;
  record_count: number;
  credit_count: number;
  total_amount: number;
  file_creation_number: number;
  generated_at: string;
}

export async function generatePayrollEft(
  payrollRunId: string,
  body: { entity_code: string; actor_email: string },
): Promise<GenerateEftResponse> {
  const res = await api.post<GenerateEftResponse>(
    `/api/payroll/runs/${payrollRunId}/generate-eft`,
    body,
  );
  return res.data;
}

export interface PayrollEftDownload {
  file_name: string;
  download_url: string;
  expires_in_seconds: number;
  record_count: number;
  total_amount: number;
  file_creation_number: number;
  generated_at: string;
}

export async function getPayrollEftDownload(
  entityCode: string,
  payrollRunId: string,
): Promise<PayrollEftDownload> {
  const res = await api.get<PayrollEftDownload>(
    `/api/payroll/runs/${payrollRunId}/eft/download`,
    { params: { entity_code: entityCode } },
  );
  return res.data;
}
