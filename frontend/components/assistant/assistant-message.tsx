'use client';

import { JournalPreview } from './journal-preview';
import { TransactionMatch } from './transaction-match';
import { PayrollChangePreview } from './payroll-change-preview';
import { cn } from '@/lib/utils';
import type {
  JournalPreview as JournalPreviewType,
  TransactionPreview,
  PayrollChangePreview as PayrollChangePreviewType,
} from '@/lib/api/assistant';

export interface AssistantMessageItem {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  transaction_preview?: TransactionPreview | null;
  journal_preview?: JournalPreviewType | null;
  payroll_preview?: PayrollChangePreviewType | null;
  needs_confirmation?: boolean;
  resolved?: boolean;
  /** Only the most recent assistant message that still needs confirmation
   *  shows the buttons. Older ones render frozen. */
  isLatestActionable?: boolean;
}

interface Props {
  message: AssistantMessageItem;
  onConfirm?: () => void;
  onChange?: () => void;
  busy?: boolean;
}

export function AssistantMessage({ message, onConfirm, onChange, busy }: Props) {
  const isUser = message.role === 'user';
  const frozen = message.resolved || !message.isLatestActionable;
  return (
    <div className={cn('flex', isUser ? 'justify-end' : 'justify-start')}>
      <div
        className={cn(
          'max-w-[88%] rounded-2xl px-3 py-2 text-sm space-y-2',
          isUser
            ? 'bg-ledger-blue text-white rounded-br-md'
            : 'bg-white border border-border text-ink rounded-bl-md',
        )}
      >
        {message.content && (
          <div className={cn('whitespace-pre-wrap', isUser && 'text-white')}>
            {message.content}
          </div>
        )}
        {!isUser && message.transaction_preview && (
          <TransactionMatch preview={message.transaction_preview} />
        )}
        {!isUser && message.journal_preview && (
          <JournalPreview
            preview={message.journal_preview}
            onConfirm={() => onConfirm?.()}
            onChange={() => onChange?.()}
            busy={busy}
            resolved={frozen}
          />
        )}
        {!isUser && message.payroll_preview && (
          <PayrollChangePreview
            preview={message.payroll_preview}
            onConfirm={() => onConfirm?.()}
            onCancel={() => onChange?.()}
            busy={busy}
            resolved={frozen}
          />
        )}
      </div>
    </div>
  );
}
