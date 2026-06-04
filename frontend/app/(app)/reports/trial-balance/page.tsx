'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { AlertTriangle } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import { getTrialBalance } from '@/lib/api/reports';
import { formatMoney, formatDate } from '@/lib/utils';
import { cn } from '@/lib/utils';
import { useDrillDown } from '@/components/reports/drill-down/use-drill-down';

export default function TrialBalancePage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
  const [asOf, setAsOf] = useState(lastMonthEnd.toISOString().slice(0, 10));
  const [onlyUnexpected, setOnlyUnexpected] = useState(false);
  const { openAt } = useDrillDown();

  const tb = useQuery({
    queryKey: ['trial-balance', entityCode, asOf],
    enabled: !!entityCode,
    queryFn: () => getTrialBalance({ entity_code: entityCode!, as_of_date: asOf }),
  });

  const rows = tb.data?.accounts ?? [];
  const visible = onlyUnexpected ? rows.filter((r) => r.unexpected_balance) : rows;

  return (
    <ReportShell
      title="Trial Balance"
      subtitle={`As of ${formatDate(asOf)}`}
      onExportCsv={() => {
        if (!tb.data) return;
        const header = 'Account,Name,Type,Normal,Debit,Credit,Net,Unexpected';
        const lines = rows.map(
          (r) =>
            `${r.account_code},"${r.account_name}",${r.account_type},${r.normal_balance},${r.total_debits},${r.total_credits},${r.net_balance},${r.unexpected_balance}`,
        );
        const blob = new Blob([[header, ...lines].join('\n')], {
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
      <div className="flex flex-wrap items-end gap-3 mb-4 no-print">
        <div>
          <Label htmlFor="asof">As-of date</Label>
          <Input
            id="asof"
            type="date"
            value={asOf}
            onChange={(e) => setAsOf(e.target.value)}
          />
        </div>
        <label className="flex items-center gap-2 text-sm text-ink cursor-pointer">
          <input
            type="checkbox"
            checked={onlyUnexpected}
            onChange={(e) => setOnlyUnexpected(e.target.checked)}
            className="h-4 w-4 rounded border-input text-ledger-blue"
          />
          Only show accounts with unexpected balance type
        </label>
      </div>

      {tb.data && !tb.data.balanced && (
        <div className="mb-4 rounded-lg border border-red-300 bg-red-50 p-3 flex items-start gap-2">
          <AlertTriangle className="h-5 w-5 text-red-700 mt-0.5" strokeWidth={1.5} />
          <div className="text-sm">
            <div className="font-semibold text-red-800">Out of balance</div>
            <div className="text-red-700">
              Total debits do not equal total credits. Difference:{' '}
              <span className="font-mono">
                {formatMoney(tb.data.totals.difference, { signed: true })}
              </span>
              .
            </div>
          </div>
        </div>
      )}

      {tb.isLoading ? (
        <Skeleton className="h-96" />
      ) : tb.isError ? (
        <p className="text-red-700">Could not load the trial balance.</p>
      ) : !rows.length ? (
        <p className="text-slate">No accounts have activity through this date.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Account</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Name</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Debit</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Credit</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Net</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {visible.map((r) => (
                <tr
                  key={r.account_code}
                  onClick={() =>
                    openAt({
                      kind: 'account',
                      account_code: r.account_code,
                      account_name: r.account_name,
                      mode: 'cumulative',
                      period_start: null,
                      period_end: asOf,
                      line_amount: r.net_balance,
                    })
                  }
                  title="View account activity"
                  className={cn(
                    'cursor-pointer hover:bg-cloud',
                    r.unexpected_balance && 'bg-amber-50',
                  )}
                >
                  <td className="px-4 py-2 font-mono text-xs text-slate">{r.account_code}</td>
                  <td className="px-4 py-2 text-ink">{r.account_name}</td>
                  <td className="px-4 py-2 text-slate text-xs">
                    {r.account_type}{' '}
                    <span className="text-slate uppercase">({r.normal_balance})</span>
                    {r.unexpected_balance && (
                      <Badge variant="warning" className="ml-2 text-[10px]">
                        unexpected
                      </Badge>
                    )}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(r.total_debits)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(r.total_credits)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(r.net_balance, { signed: true })}
                  </td>
                </tr>
              ))}
              {tb.data && (
                <tr className="bg-cloud font-semibold text-deep-navy border-t-2 border-deep-navy">
                  <td colSpan={3} className="px-4 py-2">
                    Totals
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right">
                    {formatMoney(tb.data.totals.total_debits)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right">
                    {formatMoney(tb.data.totals.total_credits)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right">
                    {formatMoney(tb.data.totals.difference, { signed: true })}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}
