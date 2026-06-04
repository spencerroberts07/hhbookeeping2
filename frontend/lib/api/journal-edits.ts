/**
 * Slice 2 write path — report drill-down edits. Admin-only on the backend.
 * Open periods edit in place; locked periods post a reversal + re-entry.
 */
import { api } from './client';

export interface EditResult {
  ok: boolean;
  action: string;
  journal_batch_id?: string;
}

export interface CorrectResult {
  ok: boolean;
  action: 'correcting_entry';
  original_batch_id: string;
  reversal_batch_id: string;
  reentry_batch_id: string;
  posted_into_period: string;
}

export async function reclassifyLine(input: {
  entity_code: string;
  journal_line_id: string;
  to_account_code: string;
  reason: string;
}): Promise<EditResult> {
  const res = await api.post<EditResult>('/api/journal-edits/reclassify', input);
  return res.data;
}

export async function editLineAmount(input: {
  entity_code: string;
  journal_line_id: string;
  new_debit: number;
  new_credit: number;
  reason: string;
}): Promise<EditResult> {
  const res = await api.post<EditResult>('/api/journal-edits/edit-amount', input);
  return res.data;
}

export async function correctEntry(input: {
  entity_code: string;
  journal_batch_id: string;
  action: 'reclassify' | 'edit_amount';
  journal_line_id: string;
  to_account_code?: string;
  new_debit?: number;
  new_credit?: number;
  reason: string;
}): Promise<CorrectResult> {
  const res = await api.post<CorrectResult>('/api/journal-edits/correct', input);
  return res.data;
}

export async function addEntryNote(input: {
  entity_code: string;
  journal_batch_id: string;
  journal_line_id?: string;
  note: string;
}): Promise<EditResult> {
  const res = await api.post<EditResult>('/api/journal-edits/note', input);
  return res.data;
}
