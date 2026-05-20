'use client';

import { useEffect, useRef, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Sparkles, X, Send, Minus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { useUserStore } from '@/lib/store/user';
import {
  sendAssistantMessage,
  confirmAssistantAction,
  type MessageResponse,
} from '@/lib/api/assistant';
import { AssistantMessage, type AssistantMessageItem } from './assistant-message';
import { cn } from '@/lib/utils';
import { toast } from 'sonner';

/**
 * Floating chat panel anchored bottom-right of the dashboard. Two
 * states: collapsed circle + expanded 380×500 panel.
 *
 * Conversation state lives in component memory for the session. We
 * pass conversation_id back to the backend on each turn so it stitches
 * the messages together server-side.
 */
export function AssistantWidget() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const entityName = useEntityStore((s) => s.activeEntityName);
  const userFullName = useUserStore((s) => s.fullName);

  const [open, setOpen] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<AssistantMessageItem[]>([]);
  const [input, setInput] = useState('');
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const qc = useQueryClient();

  // Greeting on first open of a session.
  useEffect(() => {
    if (open && messages.length === 0 && entityCode) {
      const greetingName = (userFullName ?? '').split(' ')[0] || 'there';
      const hour = new Date().getHours();
      const timeOfDay = hour < 12 ? 'morning' : hour < 17 ? 'afternoon' : 'evening';
      setMessages([
        {
          id: 'greeting',
          role: 'assistant',
          content:
            `Good ${timeOfDay}, ${greetingName}. I'm your BookWize assistant — ` +
            `I can classify transactions, add notes, or answer questions about ` +
            `${entityName ?? entityCode}. What would you like to do?`,
        },
      ]);
    }
  }, [open, entityCode, entityName, userFullName, messages.length]);

  useEffect(() => {
    if (open && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, open]);

  const sendMutation = useMutation({
    mutationFn: (text: string) =>
      sendAssistantMessage({
        entity_code: entityCode!,
        message: text,
        conversation_id: conversationId,
      }),
    onSuccess: (res: MessageResponse, sentText) => {
      setConversationId(res.conversation_id);
      // Push the user's message (with the real id from server) + assistant reply.
      setMessages((m) => [
        ...m.filter((mm) => mm.id !== '__pending_user__'),
        {
          id: res.user_message_id ?? `user-${Date.now()}`,
          role: 'user',
          content: sentText,
        },
        {
          id: res.message_id ?? `assistant-${Date.now()}`,
          role: 'assistant',
          content: res.reply,
          journal_preview: res.proposed_action?.journal_preview ?? null,
          transaction_preview: res.proposed_action?.transaction_preview ?? null,
          needs_confirmation: res.needs_confirmation,
          isLatestActionable: res.needs_confirmation,
          resolved: false,
        },
      ]);
    },
    onError: () => {
      setMessages((m) => [
        ...m.filter((mm) => mm.id !== '__pending_user__'),
        {
          id: `err-${Date.now()}`,
          role: 'assistant',
          content: 'Something went wrong reaching the assistant. Try again in a moment.',
        },
      ]);
    },
  });

  const confirmMutation = useMutation({
    mutationFn: (input: { message_id: string; confirmed: boolean; correction?: string }) =>
      confirmAssistantAction({
        entity_code: entityCode!,
        ...input,
      }),
    onSuccess: (res, vars) => {
      if (res.ok) {
        toast.success(
          res.action === 'classify_transaction' ? 'Transaction classified' : 'Done',
        );
      }
      setMessages((m) =>
        m.map((mm) =>
          mm.id === vars.message_id
            ? { ...mm, resolved: vars.confirmed, isLatestActionable: false }
            : { ...mm, isLatestActionable: false },
        ),
      );
      // Refresh anything downstream — bank list, dashboard counters, queue.
      qc.invalidateQueries({ queryKey: ['bank-txns'] });
      qc.invalidateQueries({ queryKey: ['unmatched-queue'] });
    },
  });

  if (!entityCode) return null;

  const onSend = () => {
    const text = input.trim();
    if (!text || sendMutation.isPending) return;
    setInput('');
    // Optimistic user bubble until the server returns.
    setMessages((m) => [
      ...m.map((mm) => ({ ...mm, isLatestActionable: false })),
      { id: '__pending_user__', role: 'user', content: text },
    ]);
    sendMutation.mutate(text);
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSend();
    }
  };

  // Count actionable items pending confirmation (for the badge).
  const pendingCount = messages.filter(
    (m) => m.isLatestActionable && !m.resolved,
  ).length;

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        aria-label="Open BookWize assistant"
        className={cn(
          'fixed bottom-6 right-6 z-40 grid h-14 w-14 place-items-center rounded-full',
          'bg-deep-navy text-white shadow-lg hover:bg-ledger-blue transition',
          'focus:outline-none focus:ring-4 focus:ring-bw-teal/40',
        )}
      >
        <Sparkles className="h-6 w-6" strokeWidth={1.5} />
        {pendingCount > 0 && (
          <span className="absolute -top-1 -right-1 grid h-5 min-w-5 place-items-center rounded-full bg-amber-500 text-xs font-bold text-white px-1">
            {pendingCount}
          </span>
        )}
      </button>
    );
  }

  return (
    <div
      role="dialog"
      aria-label="BookWize assistant"
      className={cn(
        'fixed bottom-6 right-6 z-40 flex flex-col rounded-2xl shadow-2xl border border-border bg-cloud',
        'w-[380px] h-[500px] overflow-hidden',
      )}
    >
      {/* Header */}
      <header className="flex items-center justify-between px-3 py-2 bg-deep-navy text-white">
        <div className="flex items-center gap-2 min-w-0">
          <Sparkles className="h-5 w-5 text-bw-teal shrink-0" strokeWidth={1.5} />
          <div className="min-w-0">
            <div className="text-sm font-semibold truncate">Assistant</div>
            <div className="text-[10px] text-white/60">Powered by Claude</div>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setOpen(false)}
            aria-label="Minimize"
            className="rounded-md p-1 hover:bg-white/10"
          >
            <Minus className="h-4 w-4" strokeWidth={1.5} />
          </button>
          <button
            onClick={() => {
              setOpen(false);
              setConversationId(null);
              setMessages([]);
            }}
            aria-label="Close conversation"
            className="rounded-md p-1 hover:bg-white/10"
          >
            <X className="h-4 w-4" strokeWidth={1.5} />
          </button>
        </div>
      </header>

      {/* Messages */}
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto px-3 py-3 space-y-2 bg-cloud"
      >
        {messages.map((m) => (
          <AssistantMessage
            key={m.id}
            message={m}
            busy={confirmMutation.isPending}
            onConfirm={() =>
              confirmMutation.mutate({ message_id: m.id, confirmed: true })
            }
            onChange={() => {
              // Open a follow-up prompt for the user to type their correction.
              setInput(`Change to: `);
              // Mark this one no longer actionable so the user sees a fresh proposal.
              setMessages((all) =>
                all.map((mm) =>
                  mm.id === m.id ? { ...mm, isLatestActionable: false } : mm,
                ),
              );
              confirmMutation.mutate({
                message_id: m.id,
                confirmed: false,
                correction: 'user wants different account',
              });
            }}
          />
        ))}
        {sendMutation.isPending && (
          <div className="flex items-center gap-2 text-xs text-slate px-2">
            <span className="h-2 w-2 rounded-full bg-slate animate-pulse" />
            Thinking…
          </div>
        )}
      </div>

      {/* Input */}
      <div className="border-t border-border bg-white p-2">
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onKeyDown}
            rows={1}
            placeholder="Ask anything or describe a transaction…"
            className={cn(
              'flex-1 resize-none rounded-lg border border-input px-3 py-2 text-sm text-ink',
              'focus:outline-none focus:ring-2 focus:ring-ledger-blue max-h-32',
            )}
          />
          <Button
            size="sm"
            variant="primary"
            onClick={onSend}
            disabled={!input.trim() || sendMutation.isPending}
            aria-label="Send"
          >
            <Send className="h-4 w-4" strokeWidth={1.5} />
          </Button>
        </div>
      </div>
    </div>
  );
}
