import { api } from './client';

export interface CraRemittanceLine {
  period_end: string;
  period_label: string;
  gross_payroll: number | null;
  cpp_employer: number | null;
  cpp_employee: number | null;
  ei_employer: number | null;
  ei_remittable: number | null;
  income_tax: number | null;
  total_owing: number;
  status: 'owing' | 'remitted';
  remitted_date: string | null;
}

export interface CraRemittanceResponse {
  entity_code: string;
  year: number;
  remittances: CraRemittanceLine[];
  total_outstanding: number;
  cra_account_code: string;
}

export async function getCraRemittance(
  entityCode: string,
  year: number,
): Promise<CraRemittanceResponse> {
  const res = await api.get<CraRemittanceResponse>(
    '/api/payroll/cra-remittance',
    { params: { entity_code: entityCode, year } },
  );
  return res.data;
}
