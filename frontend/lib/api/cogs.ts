import { api } from './client';

export interface CogsStatus {
  entity_code: string;
  period_end: string;
  components: {
    pos_cogs: { built: boolean; amount: number | null };
    dating_reversal: { built: boolean; amount: number | null };
    dating_new: { built: boolean; amount: number | null };
  };
  batch_id: string | null;
  status: 'not_built' | 'draft' | 'pending_approval' | 'approved' | 'posted';
}

export async function getCogsStatus(
  entityCode: string,
  periodEnd: string,
): Promise<CogsStatus> {
  const res = await api.get('/api/cogs/status', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function getSuggestedDating(
  entityCode: string,
  periodEnd: string,
): Promise<{
  suggested_reversal_amount: number;
  suggested_new_amount: number;
  prior_period_carried_dating: number;
}> {
  const res = await api.get('/api/cogs/suggested-dating', {
    params: { entity_code: entityCode, period_end: periodEnd },
  });
  return res.data;
}

export async function buildCogsJournal(input: {
  entity_code: string;
  period_end: string;
  actor_email: string;
  dating_new_amount: string | number;
  dating_reversal_amount?: string | number;
  other_adjustment_amount?: string | number;
  other_adjustment_memo?: string;
  shrinkage_included?: boolean;
}): Promise<unknown> {
  const res = await api.post('/api/cogs/build-journal', input);
  return res.data;
}
