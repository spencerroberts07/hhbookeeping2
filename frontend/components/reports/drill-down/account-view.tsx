'use client';

import { useQuery } from '@tanstack/react-query';
import { Paperclip, CheckCircle2, AlertTriangle } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getAccountActivity } from '@/lib/api/reports';
import { formatMoney, formatDate } from '@/lib/utils';
import { useDrillDown, type DrillLevel } from './use-drill-down';

type AccountLevel = Extract<DrillLevel, { kind: 'account' }>;

export function AccountView({ level }: { level: AccountLevel }) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { push } = useDrillDown();

  const q = useQuery({
    queryKey: [
      'account-activity',
      entityCode,
      level.account_code,
      level.mode,
      level.period_start,
      level.period_end,
    ],
    enabled: !!entityCode,
    queryFn: () =>
      getAccountActivity({
        entity_code: entityCode!,
        account_code: level.account_code,
        mode: level.mode,
        period_end: level.period_end,
        period_start: level.period_start,
      }),
  });

  if (q.isLoading) return <Skeleton className="h-80" />;
  if (q.isError) return <p className="text-red-700 text-sm">Could not load account activity.</p>;
  if (!q.data) return <p className="text-slate text-sm">No data.</p>;

  // Reconcile by magnitude — report sign conventions differ (BS type-signed,
  // IS signed-by-type, TB raw dr-cr) but the magnitude is the invariant.
  const reconciles =
    Math.abs(Math.abs(q.data.closing_balance) - Math.abs(level.line_amount)) < 0.01;

  return (
    <div>
      <div className="mb-3 flex items-center justify-between text-sm">
        <div className="text-slate">
          {q.data.transaction_count} transaction
          {q.data.transaction_count === 1 ? '' : 's'}
          {level.mode === 'period' && level.period_start
            ? ` · ${formatDate(level.period_start)} – ${formatDate(level.period_end)}`
            : ` · through ${formatDate(level.period_end)}`}
        </div>
        <div
          className={
            'flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium ' +
            (reconciles ? 'bg-green-50 text-green-800' : 'bg-amber-50 text-amber-800')
          }
        >
          {reconciles ? (
            <CheckCircle2 className="h-3.5 w-3.5" strokeWidth={1.5} />
          ) : (
            <AlertTriangle className="h-3.5 w-3.5" strokeWidth={1.5} />
          )}
          {reconciles ? 'Reconciles' : 'Variance'} ·{' '}
          <span className="tabular-nums">{formatMoney(q.data.closing_balance, { signed: true })}</span>
        </div>
      </div>

      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud">
            <tr>
              <th className="px-3 py-2 text-left font-semibold text-deep-navy">Date</th>
              <th className="px-3 py-2 text-left font-semibold text-deep-navy">Description</th>
              <th className="px-3 py-2 text-right font-semibold text-deep-navy">Debit</th>
              <th className="px-3 py-2 text-right font-semibold text-deep-navy">Credit</th>
              <th className="px-3 py-2 text-right font-semibold text-deep-navy">Balance</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {q.data.transactions.map((t) => (
              <tr
                key={t.id}
                className="cursor-pointer hover:bg-cloud"
                onClick={() => push({ kind: 'entry', journal_batch_id: t.journal_batch_id })}
                title="View full journal entry"
              >
                <td className="px-3 py-2 text-ink whitespace-nowrap">{formatDate(t.posting_date)}</td>
                <td className="px-3 py-2 text-ink">
                  <span className="inline-flex items-center gap-1.5">
                    {t.has_document && (
                      <Paperclip className="h-3 w-3 text-ledger-blue shrink-0" strokeWidth={1.5} />
                    )}
                    <span>{t.description}</span>
                  </span>
                  <span className="ml-2 font-mono text-[10px] text-slate">{t.reference}</span>
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-ink">
                  {t.debit ? formatMoney(t.debit) : '—'}
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-ink">
                  {t.credit ? formatMoney(t.credit) : '—'}
                </td>
                <td className="px-3 py-2 text-right tabular-nums font-medium text-deep-navy">
                  {formatMoney(t.balance)}
                </td>
              </tr>
            ))}
            {q.data.transactions.length === 0 && (
              <tr>
                <td colSpan={5} className="px-3 py-4 text-center text-slate">
                  No activity for this account in the selected period.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
