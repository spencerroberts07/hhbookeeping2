'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { getAsOfTrialBalance } from '@/lib/api/reports';
import { listGlRuns, getTrialBalance } from '@/lib/api/gl';
import { formatMoney, formatDate } from '@/lib/utils';

export default function TrialBalancePage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
  const [asOf, setAsOf] = useState(lastMonthEnd.toISOString().slice(0, 10));

  // 1. Primary path — as-of trial balance (currently stub, see lib/api/reports.ts).
  const asOfTb = useQuery({
    queryKey: ['tb-as-of', entityCode, asOf],
    enabled: !!entityCode,
    queryFn: () =>
      getAsOfTrialBalance({ entity_code: entityCode!, as_of_date: asOf }),
  });

  // 2. Fallback — most-recent GL import run, which DOES exist.
  const glRuns = useQuery({
    queryKey: ['gl-runs', entityCode],
    enabled: !!entityCode,
    queryFn: () => listGlRuns(entityCode!),
  });
  const latestGlRun = glRuns.data?.runs[0];
  const runBasedTb = useQuery({
    queryKey: ['tb-run', entityCode, latestGlRun?.id],
    enabled: !!latestGlRun,
    queryFn: () => getTrialBalance(entityCode!, latestGlRun!.id),
  });

  const data = asOfTb.data ?? runBasedTb.data;
  const loading = asOfTb.isLoading || (!data && glRuns.isLoading);

  return (
    <ReportShell
      title="Trial Balance"
      subtitle={`As of ${formatDate(asOf)}`}
      onExportCsv={() => {
        if (!data) return;
        const header = 'Account,Name,Debit,Credit';
        const rows = ('rows' in data ? data.rows : []).map((r: any) =>
          `${r.account_code},"${r.account_name}",${r.debit ?? r.qbo_debit ?? 0},${r.credit ?? r.qbo_credit ?? 0}`,
        );
        const blob = new Blob([[header, ...rows].join('\n')], {
          type: 'text/csv',
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `trial-balance-${asOf}.csv`;
        a.click();
        URL.revokeObjectURL(url);
      }}
    >
      <div className="max-w-xs mb-4 no-print">
        <Label htmlFor="asof">As-of date</Label>
        <Input
          id="asof"
          type="date"
          value={asOf}
          onChange={(e) => setAsOf(e.target.value)}
        />
      </div>
      {loading ? (
        <Skeleton className="h-96" />
      ) : !data ? (
        <p className="text-slate">No data.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Account</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Name</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Debit</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Credit</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {('rows' in data ? data.rows : []).map((r: any) => (
                <tr key={r.account_code} className="hover:bg-cloud">
                  <td className="px-4 py-2 font-mono text-xs text-slate">{r.account_code}</td>
                  <td className="px-4 py-2 text-ink">{r.account_name}</td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(r.debit ?? r.qbo_debit ?? 0)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(r.credit ?? r.qbo_credit ?? 0)}
                  </td>
                </tr>
              ))}
              <tr className="bg-cloud font-semibold text-deep-navy border-t-2 border-deep-navy">
                <td colSpan={2} className="px-4 py-2">Totals</td>
                <td className="px-4 py-2 tabular-nums text-right">
                  {formatMoney(data.total_debit)}
                </td>
                <td className="px-4 py-2 tabular-nums text-right">
                  {formatMoney(data.total_credit)}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}
