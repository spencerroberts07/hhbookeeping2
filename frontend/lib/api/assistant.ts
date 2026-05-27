import { api } from './client';

export type AssistantIntent =
  | 'classify_transaction'
  | 'query_balance'
  | 'add_note'
  | 'query_period'
  | 'correction'
  | 'general_question'
  | 'other';

export type AssistantActionType =
  | 'classify_transaction'
  | 'add_note'
  | 'post_to_pending'
  | 'update_employee_rate'
  | 'update_employee_salary'
  | 'one_time_pay_override'
  | 'add_bonus_line'
  | 'none';

export type AssistantIntentExtended =
  | AssistantIntent
  | 'update_employee_rate'
  | 'update_employee_salary'
  | 'one_time_pay_override'
  | 'add_bonus_line';

export interface JournalPreview {
  debit_account_code: string | null;
  debit_account_name: string | null;
  credit_account_code: string | null;
  credit_account_name: string | null;
  amount: number;
  note: string;
}

export interface TransactionPreview {
  date: string;
  amount: number;
  description: string;
  direction: string;
}

/**
 * Before/after preview for payroll-changing actions
 * (update_employee_rate, update_employee_salary,
 * one_time_pay_override, add_bonus_line).
 *
 * All money values are dollar amounts; tax_delta_* are the per-period
 * deltas in CPP / EI / Fed Tax between BEFORE and AFTER. annual_impact
 * is the difference × 26 periods for rate/salary changes.
 */
export interface PayrollChangePreview {
  employee_id: string;
  employee_name: string;
  change_type:
    | 'hourly_rate'
    | 'biweekly_salary'
    | 'one_time_override'
    | 'bonus_line';
  payroll_run_id?: string | null;
  run_period_label?: string | null;
  before: {
    hourly_rate?: number | null;
    biweekly_salary?: number | null;
    sample_biweekly_gross?: number | null;
    net_est?: number | null;
  };
  after: {
    hourly_rate?: number | null;
    biweekly_salary?: number | null;
    sample_biweekly_gross?: number | null;
    net_est?: number | null;
  };
  cpp_delta_per_period?: number | null;
  ei_delta_per_period?: number | null;
  fed_tax_delta_per_period?: number | null;
  annual_impact?: number | null;
  note?: string | null;
}

export interface ProposedAction {
  action_type: AssistantActionType;
  transaction_id: string | null;
  transaction_preview: TransactionPreview | null;
  journal_preview: JournalPreview | null;
  payroll_preview: PayrollChangePreview | null;
  pending_intent_id: string | null;
}

export interface MatchedTransaction {
  transaction_id: string;
  date: string;
  amount: number;
  description: string;
  direction: string;
  score: number;
}

export interface MessageResponse {
  conversation_id: string;
  user_message_id: string | null;
  message_id: string | null;
  reply: string;
  intent: AssistantIntent;
  needs_confirmation: boolean;
  proposed_action: ProposedAction | null;
  matched_transactions: MatchedTransaction[];
}

export async function sendAssistantMessage(input: {
  entity_code: string;
  message: string;
  conversation_id?: string | null;
  /** Current route the user is viewing — gives Claude context (e.g.
   *  '/payroll/runs/ea2a8b4e-...' or '/bank'). Also written to
   *  assistant_conversations.channel so we can analyse which pages
   *  drive which questions. */
  page_context?: string | null;
}): Promise<MessageResponse> {
  const res = await api.post<MessageResponse>('/api/assistant/message', input);
  return res.data;
}

export async function confirmAssistantAction(input: {
  entity_code: string;
  message_id: string;
  confirmed: boolean;
  correction?: string;
}): Promise<{ ok: boolean; action?: string; transaction_id?: string }> {
  const res = await api.post('/api/assistant/confirm', input);
  return res.data;
}

export interface ConversationHistoryMessage {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  intent: AssistantIntent | null;
  resolved: boolean;
  proposed_action: ProposedAction | null;
  transaction_id: string | null;
  created_at: string | null;
}

export interface ConversationHistory {
  id: string;
  started_at: string | null;
  last_message_at: string | null;
  messages: ConversationHistoryMessage[];
}

export async function getAssistantHistory(
  entityCode: string,
  limit = 20,
): Promise<{ conversations: ConversationHistory[] }> {
  const res = await api.get('/api/assistant/history', {
    params: { entity_code: entityCode, limit },
  });
  return res.data;
}

export interface AssistantMemoryEntry {
  id: string;
  memory_type: string;
  memory_key: string;
  memory_value: string;
  confidence: number;
  times_confirmed: number;
  times_corrected: number;
  last_seen_at: string | null;
  created_at: string | null;
}

export async function getAssistantMemory(
  entityCode: string,
  memoryType?: string,
): Promise<{ memory: AssistantMemoryEntry[] }> {
  const res = await api.get('/api/assistant/memory', {
    params: { entity_code: entityCode, memory_type: memoryType },
  });
  return res.data;
}

export interface AssistantInsight {
  type: string;
  description: string;
  severity: 'info' | 'warning' | 'action';
  action?: string;
  action_url?: string;
}

export interface AssistantInsightsResponse {
  entity_code: string;
  insights: AssistantInsight[];
  count: number;
}

export async function getAssistantInsights(
  entityCode: string,
): Promise<AssistantInsightsResponse> {
  const res = await api.get<AssistantInsightsResponse>(
    '/api/assistant/insights',
    { params: { entity_code: entityCode } },
  );
  return res.data;
}
