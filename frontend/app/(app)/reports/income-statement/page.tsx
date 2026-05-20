'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import {
  getIncomeStatement,
  type IncomeStatementRow,
  type IncomeStatementBody,
} from '@/lib/api/reports';
import { formatMoney, formatMonthLabel, formatPercent } from '@/lib/utils';

type CompareTo = '' | 'prior_period' | 'prior_year';

export default function IncomeStatementPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const lastMonthEnd = new Date(today.getFullYear(), today.getMonth(), 0);
  const lastMonthStart = new Date(today.getFullYear(), today.getMonth() - 1, 1);
  const [periodStart, setPeriodStart] = useState(
    lastMonthStart.toISOString().slice(0, 10),
  );
  const [periodEnd, setPeriodEnd] = useState(
    lastMonthEnd.toISOString().slice(0, 10),
  );
  const [compareTo, setCompareTo] = useState<CompareTo>('');

  const report = useQuery({
    queryKey: ['income-statement', entityCode, periodStart, periodEnd, compareTo],
    enabled: !!entityCode,
    queryFn: () =>
      getIncomeStatement({
        entity_code: entityCode!,
        date_from: periodStart,
        date_to: periodEnd,
        compare_to: compareTo || undefined,
      }),
  });

  const subtitle = useMemo(
    () => `${formatMonthLabel(periodStart)} – ${formatMonthLabel(periodEnd)}`,
    [periodStart, periodEnd],
  );

  return (
    <ReportShell
      title="Income Statement"
      subtitle={subtitle}
      onExportCsv={() => {
        if (!report.data) return;
        const lines: string[] = ['Section,Account,Name,Amount'];
        for (const row of report.data.revenue)
          lines.push(`Revenue,${row.account_code},"${row.account_name}",${row.amount}`);
        for (const row of report.data.cogs)
          lines.push(`COGS,${row.account_code},"${row.account_name}",${row.amount}`);
        for (const row of report.data.operating_expenses)
          lines.push(`Opex,${row.account_code},"${row.account_name}",${row.amount}`);
        const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `income-statement-${periodEnd}.csv`;
        a.click();
        URL.revokeObjectURL(url);
      }}
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4 max-w-2xl no-print">
        <div>
          <Label htmlFor="ps">Period start</Label>
          <Input id="ps" type="date" value={periodStart} onChange={(e) => setPeriodStart(e.target.value)} />
        </div>
        <div>
          <Label htmlFor="pe">Period end</Label>
          <Input id="pe" type="date" value={periodEnd} onChange={(e) => setPeriodEnd(e.target.value)} />
        </div>
        <div>
          <Label htmlFor="cmp">Compare to</Label>
          <Select
            value={compareTo || 'none'}
            onValueChange={(v) => setCompareTo(v === 'none' ? '' : (v as CompareTo))}
          >
            <SelectTrigger id="cmp">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="none">No comparison</SelectItem>
              <SelectItem value="prior_period">Prior period</SelectItem>
              <SelectItem value="prior_year">Prior year</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
      {report.isLoading ? (
        <Skeleton className="h-96" />
      ) : report.isError ? (
        <p className="text-red-700">Could not load the income statement.</p>
      ) : !report.data ? (
        <p className="text-slate">No data for this period.</p>
      ) : (
        <div className="space-y-6">
          <Section label="Revenue" rows={report.data.revenue} subtotal={report.data.revenue_total} />
          <Section label="Cost of Goods Sold" rows={report.data.cogs} subtotal={report.data.cogs_total} />
          <Row
            label="Gross Profit"
            amount={report.data.gross_profit}
            bold
            extra={
              report.data.gross_margin_pct !== null
                ? formatPercent(report.data.gross_margin_pct, 1)
                : undefined
            }
          />
          <Section
            label="Operating Expenses"
            rows={report.data.operating_expenses}
            subtotal={report.data.operating_expenses_total}
          />
          <div className="border-t-2 border-deep-navy pt-3">
            <Row label="Net Income" amount={report.data.net_income} bold large />
          </div>
          {report.data.comparison && <ComparisonBlock body={report.data.comparison} />}
        </div>
      )}
    </ReportShell>
  );
}

function Section({
  label,
  rows,
  subtotal,
}: {
  label: string;
  rows: IncomeStatementRow[];
  subtotal: number;
}) {
  return (
    <div>
      <div className="text-sm font-semibold text-deep-navy uppercase tracking-wider mb-2">
        {label}
      </div>
      {rows.length === 0 ? (
        <div className="text-sm text-slate py-1.5">No activity in this period.</div>
      ) : (
        <div className="divide-y divide-border">
          {rows.map((r) => (
            <div key={r.account_code} className="flex justify-between py-1.5 text-sm">
              <span className="text-ink">
                <span className="text-slate font-mono mr-2">{r.account_code}</span>
                {r.account_name}
              </span>
              <span className="tabular-nums text-ink">{formatMoney(r.amount)}</span>
            </div>
          ))}
        </div>
      )}
      <div className="flex justify-between py-2 font-semibold border-t border-deep-navy text-deep-navy">
        <span>Total {label}</span>
        <span className="tabular-nums">{formatMoney(subtotal)}</span>
      </div>
    </div>
  );
}

function Row({
  label,
  amount,
  bold,
  large,
  extra,
}: {
  label: string;
  amount: number;
  bold?: boolean;
  large?: boolean;
  extra?: string;
}) {
  return (
    <div
      className={`flex justify-between py-2 ${bold ? 'font-semibold' : ''} ${large ? 'text-xl text-deep-navy' : 'text-sm'}`}
    >
      <span>
        {label}
        {extra && <span className="text-slate ml-2 text-sm">({extra})</span>}
      </span>
      <span className="tabular-nums">{formatMoney(amount)}</span>
    </div>
  );
}

function ComparisonBlock({ body }: { body: IncomeStatementBody }) {
  return (
    <div className="border-t border-border pt-4">
      <div className="text-sm font-semibold text-slate uppercase tracking-wider mb-2">
        Comparison period
      </div>
      <dl className="grid grid-cols-2 gap-2 text-sm max-w-md">
        <dt className="text-slate">Revenue</dt>
        <dd className="tabular-nums text-ink">{formatMoney(body.revenue_total)}</dd>
        <dt className="text-slate">COGS</dt>
        <dd className="tabular-nums text-ink">{formatMoney(body.cogs_total)}</dd>
        <dt className="text-slate">Gross profit</dt>
        <dd className="tabular-nums text-ink">{formatMoney(body.gross_profit)}</dd>
        <dt className="text-slate">Operating expenses</dt>
        <dd className="tabular-nums text-ink">{formatMoney(body.operating_expenses_total)}</dd>
        <dt className="text-slate font-semibold">Net income</dt>
        <dd className="tabular-nums text-ink font-semibold">{formatMoney(body.net_income)}</dd>
      </dl>
    </div>
  );
}
