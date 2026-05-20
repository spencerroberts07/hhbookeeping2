'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import { getBalanceSheet } from '@/lib/api/reports';
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
    <ReportShell
      title="Balance Sheet"
      subtitle={`As of ${formatDate(asOf)}`}
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
      {report.isLoading ? (
        <Skeleton className="h-96" />
      ) : !report.data ? (
        <p className="text-slate">No data.</p>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {/* Assets */}
          <div>
            <h3 className="text-h2 text-deep-navy mb-3">Assets</h3>
            <Group label="Current assets" rows={report.data.assets.current} />
            <Group label="Fixed assets" rows={report.data.assets.fixed} />
            <div className="border-t-2 border-deep-navy mt-3 pt-2 flex justify-between font-bold text-deep-navy">
              <span>Total assets</span>
              <span className="tabular-nums">{formatMoney(report.data.assets.total)}</span>
            </div>
          </div>
          {/* Liabilities + Equity */}
          <div>
            <h3 className="text-h2 text-deep-navy mb-3">Liabilities & Equity</h3>
            <Group
              label="Current liabilities"
              rows={report.data.liabilities.current}
            />
            <Group
              label="Long-term liabilities"
              rows={report.data.liabilities.long_term}
            />
            <div className="border-t border-border pt-2 mt-2 flex justify-between font-semibold text-deep-navy">
              <span>Total liabilities</span>
              <span className="tabular-nums">{formatMoney(report.data.liabilities.total)}</span>
            </div>
            <Group label="Equity" rows={report.data.equity.rows} className="mt-4" />
            <div className="flex justify-between font-semibold text-deep-navy">
              <span>Total equity</span>
              <span className="tabular-nums">{formatMoney(report.data.equity.total)}</span>
            </div>
            <div className="border-t-2 border-deep-navy mt-3 pt-2 flex justify-between font-bold text-deep-navy">
              <span>Total liabilities + equity</span>
              <span className="tabular-nums">
                {formatMoney(report.data.liabilities.total + report.data.equity.total)}
              </span>
            </div>
            <div className="mt-3">
              {report.data.balances ? (
                <Badge variant="complete">Balanced</Badge>
              ) : (
                <Badge variant="error">Out of balance</Badge>
              )}
            </div>
          </div>
        </div>
      )}
    </ReportShell>
  );
}

function Group({
  label,
  rows,
  className,
}: {
  label: string;
  rows: Array<{ account_code: string; name: string; amount: number }>;
  className?: string;
}) {
  return (
    <div className={className}>
      <div className="text-sm font-semibold text-deep-navy uppercase tracking-wider mt-3 mb-1">
        {label}
      </div>
      <div className="divide-y divide-border">
        {rows.map((r) => (
          <div key={r.account_code} className="flex justify-between py-1.5 text-sm">
            <span className="text-ink">
              <span className="text-slate font-mono mr-2">{r.account_code}</span>
              {r.name}
            </span>
            <span className="tabular-nums text-ink">{formatMoney(r.amount)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
