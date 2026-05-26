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
