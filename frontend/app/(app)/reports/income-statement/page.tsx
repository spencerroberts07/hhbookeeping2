'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { getIncomeStatement, type IncomeStatementSection } from '@/lib/api/reports';
import { formatMoney, formatMonthLabel } from '@/lib/utils';

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

  const report = useQuery({
    queryKey: ['income-statement', entityCode, periodStart, periodEnd],
    enabled: !!entityCode,
    queryFn: () =>
      getIncomeStatement({
        entity_code: entityCode!,
        period_start: periodStart,
        period_end: periodEnd,
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
        const rows: string[] = ['Section,Account,Name,Amount'];
        for (const s of [report.data.revenue, report.data.cogs, report.data.operating_expenses]) {
          for (const r of s.rows) {
            rows.push(`${s.label},${r.account_code},"${r.account_name}",${r.amount}`);
          }
        }
        const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `income-statement-${periodEnd}.csv`;
        a.click();
        URL.revokeObjectURL(url);
      }}
    >
      <div className="grid grid-cols-2 gap-3 mb-4 max-w-md no-print">
        <div>
          <Label htmlFor="ps">Period start</Label>
          <Input id="ps" type="date" value={periodStart} onChange={(e) => setPeriodStart(e.target.value)} />
        </div>
        <div>
          <Label htmlFor="pe">Period end</Label>
          <Input id="pe" type="date" value={periodEnd} onChange={(e) => setPeriodEnd(e.target.value)} />
        </div>
      </div>
      {report.isLoading ? (
        <Skeleton className="h-96" />
      ) : !report.data ? (
        <p className="text-slate">No data for this period.</p>
      ) : (
        <div className="space-y-6">
          <Section section={report.data.revenue} />
          <Section section={report.data.cogs} />
          <Row label="Gross Profit" amount={report.data.gross_profit} bold />
          <Section section={report.data.operating_expenses} />
          <div className="border-t-2 border-deep-navy pt-3">
            <Row label="Net Income" amount={report.data.net_income} bold large />
          </div>
        </div>
      )}
    </ReportShell>
  );
}

function Section({ section }: { section: IncomeStatementSection }) {
  return (
    <div>
      <div className="text-sm font-semibold text-deep-navy uppercase tracking-wider mb-2">
        {section.label}
      </div>
      <div className="divide-y divide-border">
        {section.rows.map((r) => (
          <div key={r.account_code} className="flex justify-between py-1.5 text-sm">
            <span className="text-ink">
              <span className="text-slate font-mono mr-2">{r.account_code}</span>
              {r.account_name}
            </span>
            <span className="tabular-nums text-ink">{formatMoney(r.amount)}</span>
          </div>
        ))}
        <div className="flex justify-between py-2 font-semibold border-t border-deep-navy text-deep-navy">
          <span>Total {section.label}</span>
          <span className="tabular-nums">{formatMoney(section.subtotal)}</span>
        </div>
      </div>
    </div>
  );
}

function Row({
  label,
  amount,
  bold,
  large,
}: {
  label: string;
  amount: number;
  bold?: boolean;
  large?: boolean;
}) {
  return (
    <div
      className={`flex justify-between py-2 ${bold ? 'font-semibold' : ''} ${large ? 'text-xl text-deep-navy' : 'text-sm'}`}
    >
      <span>{label}</span>
      <span className="tabular-nums">{formatMoney(amount)}</span>
    </div>
  );
}
