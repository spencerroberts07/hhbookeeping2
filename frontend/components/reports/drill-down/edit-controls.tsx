'use client';

import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Lock, Pencil } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import { listAccounts } from '@/lib/api/accounts';
import {
  reclassifyLine,
  editLineAmount,
  correctEntry,
  addEntryNote,
} from '@/lib/api/journal-edits';
import type { JournalEntryLine } from '@/lib/api/reports';

/**
 * Edit / reclassify / correct / note controls for the journal-entry view.
 * Open periods edit in place; locked periods post a reversal + re-entry.
 * edit-amount is offered only for genuine 2-line entries.
 */
export function EditControls({
  journalBatchId,
  lines,
  periodLocked,
  onChanged,
}: {
  journalBatchId: string;
  lines: JournalEntryLine[];
  periodLocked: boolean;
  onChanged: () => void;
}) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<'reclassify' | 'edit_amount'>('reclassify');
  const [lineId, setLineId] = useState(lines[0]?.id ?? '');
  const [toAccount, setToAccount] = useState('');
  const [newDebit, setNewDebit] = useState('0');
  const [newCredit, setNewCredit] = useState('0');
  const [reason, setReason] = useState('');
  const [note, setNote] = useState('');

  const accountsQ = useQuery({
    queryKey: ['accounts', entityCode],
    enabled: !!entityCode && open,
    queryFn: () => listAccounts(entityCode!),
  });

  const twoLine = lines.length === 2;

  const reset = () => {
    setToAccount('');
    setNewDebit('0');
    setNewCredit('0');
    setReason('');
  };

  const change = useMutation({
    mutationFn: async () => {
      if (!entityCode) throw new Error('No entity selected');
      if (mode === 'reclassify') {
        if (!toAccount) throw new Error('Pick the account to move the line to.');
        if (periodLocked) {
          return correctEntry({
            entity_code: entityCode,
            journal_batch_id: journalBatchId,
            action: 'reclassify',
            journal_line_id: lineId,
            to_account_code: toAccount,
            reason,
          });
        }
        return reclassifyLine({
          entity_code: entityCode,
          journal_line_id: lineId,
          to_account_code: toAccount,
          reason,
        });
      }
      // edit_amount
      const d = Number(newDebit) || 0;
      const c = Number(newCredit) || 0;
      if (periodLocked) {
        return correctEntry({
          entity_code: entityCode,
          journal_batch_id: journalBatchId,
          action: 'edit_amount',
          journal_line_id: lineId,
          new_debit: d,
          new_credit: c,
          reason,
        });
      }
      return editLineAmount({
        entity_code: entityCode,
        journal_line_id: lineId,
        new_debit: d,
        new_credit: c,
        reason,
      });
    },
    onSuccess: () => {
      reset();
      onChanged();
    },
  });

  const noteMut = useMutation({
    mutationFn: async () => {
      if (!entityCode) throw new Error('No entity selected');
      return addEntryNote({
        entity_code: entityCode,
        journal_batch_id: journalBatchId,
        journal_line_id: lineId || undefined,
        note,
      });
    },
    onSuccess: () => {
      setNote('');
      onChanged();
    },
  });

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="mt-4 inline-flex items-center gap-2 rounded-md border border-border px-3 py-1.5 text-sm font-medium text-deep-navy hover:bg-cloud"
      >
        <Pencil className="h-4 w-4" strokeWidth={1.5} />
        Adjust this entry
      </button>
    );
  }

  const errMsg = (e: unknown) =>
    (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
    (e as Error)?.message ??
    'Something went wrong.';

  return (
    <div className="mt-4 rounded-lg border border-border p-3">
      {periodLocked ? (
        <div className="mb-3 flex items-start gap-2 rounded-md bg-amber-50 px-2.5 py-2 text-xs text-amber-800">
          <Lock className="mt-0.5 h-3.5 w-3.5 shrink-0" strokeWidth={1.5} />
          <span>
            This period is locked. Changes will not touch the original entry —
            they post a <strong>full reversal + corrected re-entry</strong> into
            the current open period.
          </span>
        </div>
      ) : (
        <div className="mb-3 text-xs text-slate">
          Open period — changes apply to this entry in place.
        </div>
      )}

      <div className="grid grid-cols-1 gap-2 text-sm">
        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-slate">Line</span>
          <select
            value={lineId}
            onChange={(e) => setLineId(e.target.value)}
            className="rounded-md border border-input bg-white px-2 py-1.5"
          >
            {lines.map((l) => (
              <option key={l.id} value={l.id}>
                {l.account_code} · {l.account_name} ({l.debit ? `Dr ${l.debit}` : `Cr ${l.credit}`})
              </option>
            ))}
          </select>
        </label>

        <div className="flex gap-3 text-xs">
          <label className="flex items-center gap-1.5">
            <input
              type="radio"
              checked={mode === 'reclassify'}
              onChange={() => setMode('reclassify')}
            />
            Reclassify
          </label>
          <label className="flex items-center gap-1.5" title={twoLine ? '' : 'Only available for 2-line entries'}>
            <input
              type="radio"
              checked={mode === 'edit_amount'}
              disabled={!twoLine}
              onChange={() => setMode('edit_amount')}
            />
            Edit amount {twoLine ? '' : '(2-line only)'}
          </label>
        </div>

        {mode === 'reclassify' ? (
          <label className="flex flex-col gap-1">
            <span className="text-xs font-medium text-slate">Move to account</span>
            <select
              value={toAccount}
              onChange={(e) => setToAccount(e.target.value)}
              className="rounded-md border border-input bg-white px-2 py-1.5"
            >
              <option value="">Pick an account…</option>
              {(accountsQ.data?.accounts ?? []).map((a) => (
                <option key={a.code} value={a.code}>
                  {a.code} · {a.name}
                </option>
              ))}
            </select>
          </label>
        ) : (
          <div className="flex gap-2">
            <label className="flex flex-1 flex-col gap-1">
              <span className="text-xs font-medium text-slate">New debit</span>
              <input
                type="number"
                step="0.01"
                value={newDebit}
                onChange={(e) => setNewDebit(e.target.value)}
                className="rounded-md border border-input bg-white px-2 py-1.5 tabular-nums"
              />
            </label>
            <label className="flex flex-1 flex-col gap-1">
              <span className="text-xs font-medium text-slate">New credit</span>
              <input
                type="number"
                step="0.01"
                value={newCredit}
                onChange={(e) => setNewCredit(e.target.value)}
                className="rounded-md border border-input bg-white px-2 py-1.5 tabular-nums"
              />
            </label>
          </div>
        )}

        <label className="flex flex-col gap-1">
          <span className="text-xs font-medium text-slate">Reason (required)</span>
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="Why is this change being made?"
            className="rounded-md border border-input bg-white px-2 py-1.5"
          />
        </label>

        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={!reason || change.isPending}
            onClick={() => change.mutate()}
            className="rounded-md bg-deep-navy px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
          >
            {change.isPending
              ? 'Working…'
              : periodLocked
                ? 'Post correction (reversal + re-entry)'
                : 'Apply in place'}
          </button>
          <button
            type="button"
            onClick={() => {
              setOpen(false);
              reset();
            }}
            className="rounded-md border border-border px-3 py-1.5 text-sm text-slate hover:bg-cloud"
          >
            Cancel
          </button>
        </div>
        {change.isError && (
          <p className="text-xs text-red-700">{errMsg(change.error)}</p>
        )}
        {change.isSuccess && (
          <p className="text-xs text-green-700">Change posted.</p>
        )}
      </div>

      <div className="mt-3 border-t border-border pt-3">
        <label className="flex flex-col gap-1 text-sm">
          <span className="text-xs font-medium text-slate">Add a note (audit trail)</span>
          <textarea
            value={note}
            onChange={(e) => setNote(e.target.value)}
            rows={2}
            className="rounded-md border border-input bg-white px-2 py-1.5 text-sm"
          />
        </label>
        <button
          type="button"
          disabled={!note || noteMut.isPending}
          onClick={() => noteMut.mutate()}
          className="mt-2 rounded-md border border-border px-3 py-1.5 text-sm font-medium text-deep-navy hover:bg-cloud disabled:opacity-50"
        >
          {noteMut.isPending ? 'Saving…' : 'Add note'}
        </button>
        {noteMut.isError && (
          <p className="mt-1 text-xs text-red-700">{errMsg(noteMut.error)}</p>
        )}
        {noteMut.isSuccess && (
          <p className="mt-1 text-xs text-green-700">Note saved.</p>
        )}
      </div>
    </div>
  );
}
