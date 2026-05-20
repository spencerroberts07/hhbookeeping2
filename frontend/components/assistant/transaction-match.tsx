'use client';

import { Banknote } from 'lucide-react';
import { formatDate, formatMoney } from '@/lib/utils';
import type { TransactionPreview } from '@/lib/api/assistant';

export function TransactionMatch({
  preview,
}: {
  preview: TransactionPreview;
}) {
  return (
    <div className="rounded-lg border border-border bg-cloud p-3 text-sm">
      <div className="flex items-start gap-2">
        <Banknote className="h-4 w-4 text-ledger-blue mt-0.5 shrink-0" strokeWidth={1.5} />
        <div className="min-w-0">
          <div className="text-xs uppercase tracking-wider text-slate">
            Found transaction
          </div>
          <div className="font-semibold text-deep-navy truncate">
            {preview.description || '—'}
          </div>
          <div className="text-xs text-slate">
            {formatMoney(preview.amount)} · {formatDate(preview.date)} ·{' '}
            <span className="capitalize">{preview.direction}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
