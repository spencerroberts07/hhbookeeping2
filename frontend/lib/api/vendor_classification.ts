import { api } from './client';

export interface VendorMemoryEntry {
  id: string;
  normalized_vendor_key: string;
  account_code: string;
  debit_or_credit: 'debit' | 'credit';
  source: 'gl_history' | 'user_confirmed' | 'ai_seeded';
  occurrence_count: number;
  last_seen_at: string;
  notes: string | null;
}

export interface ClassificationSuggestion {
  id: string;
  bank_transaction_id: string;
  vendor_key: string;
  suggested_account_code: string;
  suggested_debit_or_credit: 'debit' | 'credit';
  confidence: number;
  source: 'rules' | 'memory' | 'claude';
  status: 'pending' | 'accepted' | 'overridden' | 'rejected';
  created_at: string;
}

export async function listVendorMemory(params: {
  entity_code: string;
  source?: VendorMemoryEntry['source'];
  limit?: number;
}): Promise<{ entries: VendorMemoryEntry[] }> {
  const res = await api.get('/api/vendor-classification/memory', { params });
  return res.data;
}

export async function upsertVendorMemory(input: {
  entity_code: string;
  normalized_vendor_key: string;
  account_code: string;
  debit_or_credit: 'debit' | 'credit';
  actor_email: string;
  notes?: string;
}): Promise<VendorMemoryEntry> {
  const res = await api.post('/api/vendor-classification/memory/upsert', input);
  return res.data;
}

export async function listSuggestions(params: {
  entity_code: string;
  status?: ClassificationSuggestion['status'];
  limit?: number;
}): Promise<{ suggestions: ClassificationSuggestion[] }> {
  const res = await api.get('/api/vendor-classification/suggestions', {
    params,
  });
  return res.data;
}

export async function acceptSuggestion(
  suggestionId: string,
  body: {
    entity_code: string;
    actor_email: string;
    final_account_code?: string;
    final_debit_or_credit?: 'debit' | 'credit';
  },
): Promise<unknown> {
  const res = await api.post(
    `/api/vendor-classification/suggestions/${suggestionId}/accept`,
    body,
  );
  return res.data;
}

export async function overrideSuggestion(
  suggestionId: string,
  body: {
    entity_code: string;
    actor_email: string;
    final_account_code: string;
    final_debit_or_credit?: 'debit' | 'credit';
  },
): Promise<unknown> {
  const res = await api.post(
    `/api/vendor-classification/suggestions/${suggestionId}/override`,
    body,
  );
  return res.data;
}

export async function learnFromGl(input: {
  entity_code: string;
  gl_import_run_id: string;
  actor_email: string;
}): Promise<unknown> {
  const res = await api.post('/api/vendor-classification/learn-from-gl', input);
  return res.data;
}
