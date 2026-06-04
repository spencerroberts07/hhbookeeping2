'use client';

import { useQuery } from '@tanstack/react-query';
import { FileText } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import { getJournalEntry } from '@/lib/api/reports';
import { formatMoney, formatDate } from '@/lib/utils';
import { useDrillDown } from './use-drill-down';

export function EntryView({ journalBatchId }: { journalBatchId: string }) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { push } = useDrillDown();

  const q = useQuery({
    queryKey: ['journal-entry', journalBatchId, entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      getJournalEntry({ entity_code: entityCode!, journal_batch_id: journalBatchId }),
  });

  if (q.isLoading) return <Skeleton className="h-80" />;
  if (q.isError) return <p className="text-red-700 text-sm">Could not load the journal entry.</p>;
  if (!q.data) return <p className="text-slate text-sm">No data.</p>;

  const { batch, lines, has_documents } = q.data;
  const periodLocked =
    batch.period.status === 'closed_locked' || batch.period.status === 'approved_to_close';

  return (
    <div>
      <div className="mb-4 rounded-lg border border-border bg-cloud/40 p-3">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="font-semibold text-deep-navy">{batch.batch_label}</div>
            <div className="mt-0.5 text-xs text-slate">
              {batch.period.period_label} · {formatDate(batch.period.period_start)} –{' '}
              {formatDate(batch.period.period_end)}
            </div>
            <div className="mt-0.5 font-mono text-[10px] text-slate">
              {batch.source_module} · {batch.status}
            </div>
          </div>
          <div className="flex flex-col items-end gap-1.5">
            <Badge variant={batch.balanced ? 'complete' : 'error'}>
              {batch.balanced ? 'Balanced' : 'Out of balance'}
            </Badge>
            {periodLocked && (
              <Badge variant="warning" className="text-[10px]">
                Period locked
              </Badge>
            )}
          </div>
        </div>
      </div>

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud">
            <tr>
              <th className="px-3 py-2 text-left font-semibold text-deep-navy">Account</th>
              <th className="px-3 py-2 text-right font-semibold text-deep-navy">Debit</th>
              <th className="px-3 py-2 text-right font-semibold text-deep-navy">Credit</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {lines.map((ln) => (
              <tr key={ln.id} className="hover:bg-cloud/60">
                <td className="px-3 py-2 text-ink">
                  <span className="font-mono text-xs text-slate mr-2">{ln.account_code}</span>
                  {ln.account_name}
                  {ln.memo && (
                    <span className="block text-[10px] text-slate">{ln.memo}</span>
                  )}
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-ink">
                  {ln.debit ? formatMoney(ln.debit) : '—'}
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-ink">
                  {ln.credit ? formatMoney(ln.credit) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t-2 border-deep-navy bg-cloud font-semibold text-deep-navy">
              <td className="px-3 py-2">Totals</td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(batch.total_debits)}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(batch.total_credits)}
              </td>
            </tr>
          </tfoot>
        </table>
      </div>

      <div className="mt-4">
        {has_documents ? (
          <button
            type="button"
            onClick={() => push({ kind: 'document', journal_batch_id: journalBatchId })}
            className="inline-flex items-center gap-2 rounded-md border border-ledger-blue px-3 py-1.5 text-sm font-medium text-ledger-blue hover:bg-ledger-blue/5"
          >
            <FileText className="h-4 w-4" strokeWidth={1.5} />
            View source document
          </button>
        ) : (
          <p className="text-xs text-slate">No source document attached to this entry.</p>
        )}
      </div>
    </div>
  );
}
