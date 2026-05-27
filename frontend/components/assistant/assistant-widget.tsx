'use client';

import { useEffect, useRef } from 'react';
import { usePathname } from 'next/navigation';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Sparkles, X, Send, Minus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { useUserStore } from '@/lib/store/user';
import { useAssistantStore } from '@/lib/store/assistant';
import {
  sendAssistantMessage,
  confirmAssistantAction,
  getAssistantInsights,
  type MessageResponse,
} from '@/lib/api/assistant';
import { AssistantMessage } from './assistant-message';
import { cn } from '@/lib/utils';
import { toast } from 'sonner';

/**
 * Global floating chat panel. Mounted once in (app)/layout.tsx, so it
 * survives every route navigation. Conversation state lives in
 * useAssistantStore (Zustand) — closing the panel (X) clears the
 * thread; minimizing (-) just hides the panel and preserves it.
 *
 * Per-message page_context: the current pathname is included with
 * every outbound message so the backend can tailor the system prompt
 * and label the conversation channel (e.g. '/payroll/runs/abc').
 */
export function AssistantWidget() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const entityName = useEntityStore((s) => s.activeEntityName);
  const userFullName = useUserStore((s) => s.fullName);
  const pathname = usePathname() || '/';

  const isOpen = useAssistantStore((s) => s.isOpen);
  const conversationId = useAssistantStore((s) => s.conversationId);
  const messages = useAssistantStore((s) => s.messages);
  const open = useAssistantStore((s) => s.open);
  const minimize = useAssistantStore((s) => s.minimize);
  const closeAndReset = useAssistantStore((s) => s.closeAndReset);
  const setConversationId = useAssistantStore((s) => s.setConversationId);
  const setMessages = useAssistantStore((s) => s.setMessages);
  const pushMessage = useAssistantStore((s) => s.pushMessage);
  const patchMessage = useAssistantStore((s) => s.patchMessage);

  // Use a ref-controlled textarea for input so typing doesn't churn
  // the store on every keystroke.
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  // Greeting on first open of a session.
  useEffect(() => {
    if (isOpen && messages.length === 0 && entityCode) {
      const greetingName = (userFullName ?? '').split(' ')[0] || 'there';
      const hour = new Date().getHours();
      const timeOfDay = hour < 12 ? 'morning' : hour < 17 ? 'afternoon' : 'evening';
      setMessages([
        {
          id: 'greeting',
          role: 'assistant',
          content:
            `Good ${timeOfDay}, ${greetingName}. I'm your BookWize assistant — ` +
            `I can classify transactions, change payroll, add notes, or answer ` +
            `questions about ${entityName ?? entityCode}. What would you like to do?`,
        },
      ]);
    }
  }, [isOpen, entityCode, entityName, userFullName, messages.length, setMessages]);

  useEffect(() => {
    if (isOpen && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isOpen]);

  const sendMutation = useMutation({
    mutationFn: (text: string) =>
      sendAssistantMessage({
        entity_code: entityCode!,
        message: text,
        conversation_id: conversationId,
        page_context: pathname,
      }),
    onSuccess: (res: MessageResponse, sentText) => {
      setConversationId(res.conversation_id);
      const next = messages
        .filter((mm) => mm.id !== '__pending_user__')
        .map((mm) => ({ ...mm, isLatestActionable: false }));
      next.push({
        id: res.user_message_id ?? `user-${Date.now()}`,
        role: 'user',
        content: sentText,
        isLatestActionable: false,
      });
      next.push({
        id: res.message_id ?? `assistant-${Date.now()}`,
        role: 'assistant',
        content: res.reply,
        journal_preview: res.proposed_action?.journal_preview ?? null,
        transaction_preview: res.proposed_action?.transaction_preview ?? null,
        payroll_preview: res.proposed_action?.payroll_preview ?? null,
        needs_confirmation: res.needs_confirmation,
        isLatestActionable: res.needs_confirmation,
        resolved: false,
      });
      setMessages(next);
    },
    onError: () => {
      const next = messages
        .filter((mm) => mm.id !== '__pending_user__')
        .concat({
          id: `err-${Date.now()}`,
          role: 'assistant',
          content:
            'Something went wrong reaching the assistant. Try again in a moment.',
          isLatestActionable: false,
        });
      setMessages(next);
    },
  });

  const confirmMutation = useMutation({
    mutationFn: (input: {
      message_id: string;
      confirmed: boolean;
      correction?: string;
    }) =>
      confirmAssistantAction({
        entity_code: entityCode!,
        ...input,
      }),
    onSuccess: (res, vars) => {
      if (res.ok) {
        const verb =
          res.action === 'classify_transaction'
            ? 'Transaction classified'
            : res.action?.startsWith('update_employee') ||
                res.action?.includes('payroll')
              ? 'Payroll updated'
              : 'Done';
        toast.success(verb);
      }
      patchMessage(vars.message_id, {
        resolved: vars.confirmed,
        isLatestActionable: false,
      });
    },
  });

  if (!entityCode) return null;

  const onSend = () => {
    const text = inputRef.current?.value.trim() ?? '';
    if (!text || sendMutation.isPending) return;
    if (inputRef.current) inputRef.current.value = '';
    const next = messages.map((mm) => ({ ...mm, isLatestActionable: false }));
    next.push({
      id: '__pending_user__',
      role: 'user',
      content: text,
      isLatestActionable: false,
    });
    setMessages(next);
    sendMutation.mutate(text);
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSend();
    }
  };

  const pendingCount = messages.filter(
    (m) => m.isLatestActionable && !m.resolved,
  ).length;

  if (!isOpen) {
    return (
      <button
        type="button"
        onClick={open}
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
            onClick={minimize}
            aria-label="Minimize"
            className="rounded-md p-1 hover:bg-white/10"
          >
            <Minus className="h-4 w-4" strokeWidth={1.5} />
          </button>
          <button
            onClick={closeAndReset}
            aria-label="Close conversation"
            className="rounded-md p-1 hover:bg-white/10"
          >
            <X className="h-4 w-4" strokeWidth={1.5} />
          </button>
        </div>
      </header>

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
              patchMessage(m.id, { isLatestActionable: false });
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

      <QuickActionChips
        pathname={pathname}
        onPick={(prompt) => {
          if (inputRef.current) {
            inputRef.current.value = prompt;
            inputRef.current.focus();
          }
        }}
      />

      <div className="border-t border-border bg-white p-2">
        <div className="flex items-end gap-2">
          <textarea
            ref={inputRef}
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
            disabled={sendMutation.isPending}
            aria-label="Send"
          >
            <Send className="h-4 w-4" strokeWidth={1.5} />
          </Button>
        </div>
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// QuickActionChips — page-aware suggestions
//
// Switches the chip set based on the current pathname. Insights-driven
// chips on the dashboard (the original behaviour) still light up when
// the backend has surfaced them.
// --------------------------------------------------------------------------

function QuickActionChips({
  pathname,
  onPick,
}: {
  pathname: string;
  onPick: (prompt: string) => void;
}) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const insights = useQuery({
    queryKey: ['assistant-insights', entityCode],
    enabled: !!entityCode && pathname.startsWith('/dashboard'),
    queryFn: () => getAssistantInsights(entityCode!),
    staleTime: 60_000,
  });

  const chips = chipsForPath(pathname);

  // On the dashboard, append insight-driven chips when they're available.
  if (pathname.startsWith('/dashboard')) {
    const types = new Set((insights.data?.insights ?? []).map((i) => i.type));
    if (types.has('unclassified_backlog'))
      chips.push('Review my unclassified transactions');
    if (types.has('period_overdue'))
      chips.push("What's left for this month-end close?");
    if (types.has('pending_intents')) chips.push('Check my pending notes');
    if (types.has('anomaly')) chips.push('Explain my largest variance');
  }

  return (
    <div className="border-t border-border bg-cloud/40 px-2 py-1.5 flex gap-1.5 flex-wrap">
      {chips.map((c) => (
        <button
          key={c}
          type="button"
          onClick={() => onPick(c)}
          className="text-[11px] rounded-full border border-border bg-white px-2.5 py-1 text-deep-navy hover:bg-cloud transition-colors"
        >
          {c}
        </button>
      ))}
    </div>
  );
}

function chipsForPath(pathname: string): string[] {
  if (pathname.startsWith('/payroll')) {
    return [
      "What's my next payroll date?",
      'Show me CRA remittance summary',
      'Change an employee rate',
      'Review this pay run',
    ];
  }
  if (pathname.startsWith('/bank')) {
    return [
      'Review my unclassified transactions',
      'Show me uncategorized inflows',
      "What's my cash balance?",
    ];
  }
  if (pathname.startsWith('/dashboard')) {
    return ["What's my cash balance?", "How's my gross margin?"];
  }
  return ['What needs my attention?', 'Help me with this page'];
}
