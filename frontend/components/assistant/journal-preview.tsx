'use client';

import { Check, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { formatMoney } from '@/lib/utils';
import type { JournalPreview as JournalPreviewType } from '@/lib/api/assistant';

interface Props {
  preview: JournalPreviewType;
  onConfirm: () => void;
  onChange: () => void;
  busy?: boolean;
  resolved?: boolean;
}

export function JournalPreview({
  preview,
  onConfirm,
  onChange,
  busy,
  resolved,
}: Props) {
  const dr = preview.debit_account_name ?? preview.debit_account_code ?? '—';
  const cr = preview.credit_account_name ?? preview.credit_account_code ?? '—';
  return (
    <div className="rounded-lg border border-border bg-white p-3 text-sm space-y-3">
      <div>
        <div className="text-xs uppercase tracking-wider text-slate mb-1">
          Proposed classification
        </div>
        <div className="font-mono text-xs text-ink space-y-0.5">
          <div className="flex justify-between gap-3">
            <span>
              Dr {dr}
              {preview.debit_account_code && (
                <span className="text-slate"> ({preview.debit_account_code})</span>
              )}
            </span>
            <span className="tabular-nums">{formatMoney(preview.amount)}</span>
          </div>
          <div className="flex justify-between gap-3">
            <span>
              Cr {cr}
              {preview.credit_account_code && (
                <span className="text-slate"> ({preview.credit_account_code})</span>
              )}
            </span>
            <span className="tabular-nums">{formatMoney(preview.amount)}</span>
          </div>
        </div>
      </div>

      {preview.note && (
        <div className="text-xs text-slate">
          Note: <span className="text-ink">{preview.note}</span>
        </div>
      )}

      {!resolved && (
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="accent"
            onClick={onConfirm}
            disabled={busy}
          >
            <Check className="h-4 w-4" strokeWidth={2} />
            Confirm
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={onChange}
            disabled={busy}
          >
            <X className="h-4 w-4" strokeWidth={2} />
            Change
          </Button>
        </div>
      )}
      {resolved && (
        <div className="text-xs text-bw-teal font-semibold">Confirmed ✓</div>
      )}
    </div>
  );
}
