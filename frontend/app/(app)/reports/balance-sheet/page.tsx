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
import { getBalanceSheet, type BalanceSheetRow } from '@/lib/api/reports';
import { formatMoney, formatDate } from '@/lib/utils';

export default function BalanceSheetPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
  const [asOf, setAsOf] = useState(lastMonthEnd.toISOString().slice(0, 10));

  const report = useQuery({
    queryKey: ['balance-sheet', entityCode, asOf],
    enabled: !!entityCode,
    queryFn: () =>
      getBalanceSheet({ entity_code: entityCode!, as_of_date: asOf }),
  });

  return (
    <ReportShell title="Balance Sheet" subtitle={`As of ${formatDate(asOf)}`}>
      <div className="max-w-xs mb-4 no-print">
        <Label htmlFor="asof">As-of date</Label>
        <Input
          id="asof"
          type="date"
          value={asOf}
          onChange={(e) => setAsOf(e.target.value)}
        />
      </div>

      {report.isLoading ? (
        <Skeleton className="h-96" />
      ) : report.isError ? (
        <p className="text-red-700">Could not load the balance sheet.</p>
      ) : !report.data ? (
        <p className="text-slate">No data.</p>
      ) : (
        <>
          {!report.data.balanced && (
            <div className="mb-4 rounded-lg border border-red-300 bg-red-50 p-3 flex items-start gap-2">
              <AlertTriangle
                className="h-5 w-5 text-red-700 mt-0.5 shrink-0"
                strokeWidth={1.5}
              />
              <div className="text-sm">
                <div className="font-semibold text-red-800">Out of balance</div>
                <div className="text-red-700">
                  Assets do not equal liabilities + equity. Variance:{' '}
                  <span className="font-mono">
                    {formatMoney(report.data.variance, { signed: true })}
                  </span>
                  . Likely causes: opening balances not yet posted, or a P&L
                  account misclassified by prefix.
                </div>
              </div>
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            <div>
              <h3 className="text-h2 text-deep-navy mb-3">Assets</h3>
              <Group label="Current assets" rows={report.data.assets.current} total={report.data.assets.current_total} />
              <Group label="Fixed assets" rows={report.data.assets.fixed} total={report.data.assets.fixed_total} />
              <div className="border-t-2 border-deep-navy mt-3 pt-2 flex justify-between font-bold text-deep-navy">
                <span>Total assets</span>
                <span className="tabular-nums">{formatMoney(report.data.assets.total)}</span>
              </div>
            </div>

            <div>
              <h3 className="text-h2 text-deep-navy mb-3">Liabilities & Equity</h3>
              <Group label="Current liabilities" rows={report.data.liabilities.current} total={report.data.liabilities.current_total} />
              <Group label="Long-term liabilities" rows={report.data.liabilities.long_term} total={report.data.liabilities.long_term_total} />
              <div className="border-t border-border pt-2 mt-2 flex justify-between font-semibold text-deep-navy">
                <span>Total liabilities</span>
                <span className="tabular-nums">{formatMoney(report.data.liabilities.total)}</span>
              </div>
              <Group
                label="Equity"
                rows={report.data.equity.accounts}
                total={report.data.equity.total}
                className="mt-4"
              />
              <div className="border-t-2 border-deep-navy mt-3 pt-2 flex justify-between font-bold text-deep-navy">
                <span>Total liabilities + equity</span>
                <span className="tabular-nums">{formatMoney(report.data.liabilities_and_equity_total)}</span>
              </div>
              <div className="mt-3">
                {report.data.balanced ? (
                  <Badge variant="complete">Balanced</Badge>
                ) : (
                  <Badge variant="error">Out of balance</Badge>
                )}
              </div>
            </div>
          </div>
        </>
      )}
    </ReportShell>
  );
}

function Group({
  label,
  rows,
  total,
  className,
}: {
  label: string;
  rows: BalanceSheetRow[];
  total: number;
  className?: string;
}) {
  return (
    <div className={className}>
      <div className="text-sm font-semibold text-deep-navy uppercase tracking-wider mt-3 mb-1">
        {label}
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-slate py-1">No balance.</div>
      ) : (
        <div className="divide-y divide-border">
          {rows.map((r) => (
            <div key={r.account_code} className="flex justify-between py-1.5 text-sm">
              <span className="text-ink">
                <span className="text-slate font-mono mr-2">{r.account_code}</span>
                {r.account_name}
              </span>
              <span className="tabular-nums text-ink">{formatMoney(r.balance)}</span>
            </div>
          ))}
        </div>
      )}
      <div className="flex justify-between py-1.5 text-sm font-semibold text-deep-navy">
        <span>Total {label}</span>
        <span className="tabular-nums">{formatMoney(total)}</span>
      </div>
    </div>
  );
}
