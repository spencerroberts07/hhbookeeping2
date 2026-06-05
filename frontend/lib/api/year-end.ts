import { api } from './client';

export type YearEndStatus = 'draft' | 'in_review' | 'final_locked' | null;

export interface YearEndState {
  entity_code: string;
  fy: number;
  fy_start: string;
  fy_end: string;
  year_end_status: YearEndStatus;
  september_period_closed: boolean;
  periods_total: number;
  periods_closed: number;
  all_periods_closed: boolean;
  adjusting_entry_count: number;
}

export interface AdjustingLine {
  account_code: string;
  debit?: number;
  credit?: number;
  memo?: string;
}

export async function getYearEndStatus(fy: number, entityCode: string): Promise<YearEndState> {
  const res = await api.get(`/api/year-end/${fy}`, { params: { entity_code: entityCode } });
  return res.data;
}

export async function setYearEndStatus(input: {
  fy: number;
  entity_code: string;
  status: 'draft' | 'in_review' | 'final_locked';
  actor_email?: string;
}): Promise<YearEndState> {
  const { fy, ...body } = input;
  const res = await api.post(`/api/year-end/${fy}/status`, body);
  return res.data;
}

export async function postAdjustingEntry(input: {
  fy: number;
  entity_code: string;
  label: string;
  lines: AdjustingLine[];
  actor_email?: string;
}): Promise<{ batch_id: string; total_debits: number; total_credits: number; line_count: number }> {
  const { fy, ...body } = input;
  const res = await api.post(`/api/year-end/${fy}/adjusting-entry`, body);
  return res.data;
}
