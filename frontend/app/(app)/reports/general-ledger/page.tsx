'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { listGlRuns, getGlTransactions } from '@/lib/api/gl';
import { formatMoney, formatDate } from '@/lib/utils';

export default function GeneralLedgerPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [accountCode, setAccountCode] = useState('');

  // Use the latest GL import run as the data source (no live ledger yet).
  // TODO: backend endpoint not built — app-native /api/reports/general-ledger.
  const runs = useQuery({
    queryKey: ['gl-runs', entityCode],
    enabled: !!entityCode,
    queryFn: () => listGlRuns(entityCode!),
  });
  const latest = runs.data?.runs[0];

  const txns = useQuery({
    queryKey: ['gl-txns', entityCode, latest?.id, accountCode],
    enabled: !!latest && !!entityCode && !!accountCode,
    queryFn: () =>
      getGlTransactions(entityCode!, latest!.id, accountCode || undefined),
  });

  return (
    <ReportShell
      title="General Ledger"
      subtitle={
        latest
          ? `Data from import ${formatDate(latest.imported_at)} (${latest.file_name})`
          : 'No GL import on file'
      }
    >
      <div className="grid grid-cols-2 gap-3 mb-4 max-w-md no-print">
        <div>
          <Label htmlFor="acct">Account code</Label>
          <Input
            id="acct"
            value={accountCode}
            onChange={(e) => setAccountCode(e.target.value)}
            placeholder="e.g. 1020"
          />
        </div>
      </div>
      {runs.isLoading ? (
        <Skeleton className="h-96" />
      ) : !latest ? (
        <p className="text-slate">
          No GL export available. Upload one from Month-end → Documents.
        </p>
      ) : !accountCode ? (
        <p className="text-slate">Enter an account code to view its transactions.</p>
      ) : txns.isLoading ? (
        <Skeleton className="h-96" />
      ) : !txns.data?.transactions.length ? (
        <p className="text-slate">No transactions for this account.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Description</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Ref</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Debit</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Credit</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {txns.data.transactions.map((t, idx) => (
                <tr key={idx} className="hover:bg-cloud">
                  <td className="px-4 py-2 text-ink">{formatDate(t.date)}</td>
                  <td className="px-4 py-2 text-ink">{t.description}</td>
                  <td className="px-4 py-2 text-slate font-mono text-xs">{t.ref ?? '—'}</td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {t.debit ? formatMoney(t.debit) : '—'}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {t.credit ? formatMoney(t.credit) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}
