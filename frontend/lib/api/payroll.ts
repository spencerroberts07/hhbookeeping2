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

export async function getPayrollRun(
  entityCode: string,
  payrollRunId: string,
): Promise<unknown> {
  const res = await api.get(`/api/payroll/runs/${payrollRunId}`, {
    params: { entity_code: entityCode },
  });
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
