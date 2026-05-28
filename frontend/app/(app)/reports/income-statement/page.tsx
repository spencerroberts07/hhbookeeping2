'use client';

import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import {
  getIncomeStatement,
  getIncomeStatementPeriods,
  type IncomeStatementPreset,
  type IncomeStatementSection,
} from '@/lib/api/reports';
import { formatMoney } from '@/lib/utils';
import { cn } from '@/lib/utils';

type SelectorValue =
  | { kind: 'month'; period_end: string }
  | { kind: 'ytd' | 'rolling12' | 'qtd' | 'trailing3' | 'last6' }
  | { kind: 'custom' };

function encodeSelector(v: SelectorValue): string {
  return v.kind === 'month' ? `month:${v.period_end}` : v.kind;
}

function decodeSelector(s: string): SelectorValue {
  if (s.startsWith('month:')) {
    return { kind: 'month', period_end: s.slice('month:'.length) };
  }
  if (
    s === 'ytd' ||
    s === 'rolling12' ||
    s === 'qtd' ||
    s === 'trailing3' ||
    s === 'last6' ||
    s === 'custom'
  ) {
    return { kind: s };
  }
  return { kind: 'custom' };
}

export default function IncomeStatementPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const periodsQ = useQuery({
    queryKey: ['is-periods', entityCode],
    enabled: !!entityCode,
    queryFn: () => getIncomeStatementPeriods(entityCode!),
  });

  const closedPeriods = periodsQ.data?.periods ?? [];
  const mostRecent = closedPeriods[0];

  const defaultSelector: SelectorValue = mostRecent
    ? { kind: 'month', period_end: mostRecent.period_end }
    : { kind: 'custom' };

  const [selector, setSelector] = useState<SelectorValue>(defaultSelector);
  // When periods load after mount, jump to most-recent month if user hasn't touched the selector yet.
  const [touched, setTouched] = useState(false);
  if (!touched && selector.kind === 'custom' && mostRecent) {
    setSelector({ kind: 'month', period_end: mostRecent.period_end });
  }

  const today = new Date();
  const [customFrom, setCustomFrom] = useState(
    new Date(today.getFullYear(), today.getMonth() - 1, 1).toISOString().slice(0, 10),
  );
  const [customTo, setCustomTo] = useState(
    new Date(today.getFullYear(), today.getMonth(), 0).toISOString().slice(0, 10),
  );
  // Anchor for ytd/rolling12/qtd/trailing3/last6 presets — defaults to
  // the most recent closed period_end.
  const presetAnchor = mostRecent?.period_end;

  const [showCompare, setShowCompare] = useState(true);

  const params = useMemo(() => {
    if (!entityCode) return null;
    if (selector.kind === 'month') {
      return { entity_code: entityCode, preset: 'month' as const, period_end: selector.period_end };
    }
    if (selector.kind === 'custom') {
      return {
        entity_code: entityCode,
        preset: 'custom' as const,
        date_from: customFrom,
        date_to: customTo,
      };
    }
    if (!presetAnchor) return null;
    return {
      entity_code: entityCode,
      preset: selector.kind satisfies IncomeStatementPreset,
      period_end: presetAnchor,
    };
  }, [entityCode, selector, customFrom, customTo, presetAnchor]);

  const report = useQuery({
    queryKey: ['income-statement', params],
    enabled: !!params,
    queryFn: () => getIncomeStatement(params!),
  });

  const subtitle = report.data
    ? showCompare
      ? `${report.data.period_label} vs ${report.data.prior_label}`
      : report.data.period_label
    : '';

  return (
    <ReportShell
      title="Income Statement"
      subtitle={subtitle}
      onExportCsv={() => {
        if (!report.data) return;
        const header = showCompare
          ? 'Section,Account Code,Account Name,Current,Prior Year,Current %,Prior Year %'
          : 'Section,Account Code,Account Name,Current,Current %';
        const lines: string[] = [header];
        for (const sec of report.data.sections) {
          for (const a of sec.accounts) {
            const row = showCompare
              ? `"${sec.section}",${a.account_code},"${a.account_name}",${a.current_amount},${a.prior_amount},${a.current_pct ?? ''},${a.prior_pct ?? ''}`
              : `"${sec.section}",${a.account_code},"${a.account_name}",${a.current_amount},${a.current_pct ?? ''}`;
            lines.push(row);
          }
          const totalRow = showCompare
            ? `"${sec.section} — Total",,,"${sec.section_total}",${sec.prior_total},${sec.section_pct ?? ''},${sec.prior_pct ?? ''}`
            : `"${sec.section} — Total",,,"${sec.section_total}",${sec.section_pct ?? ''}`;
          lines.push(totalRow);
        }
        const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `income-statement-${report.data.period_end}.csv`;
        a.click();
        URL.revokeObjectURL(url);
      }}
    >
      <div className="flex flex-wrap items-end gap-3 mb-4 no-print">
        <div className="min-w-[280px]">
          <Label htmlFor="period">Period</Label>
          <Select
            value={encodeSelector(selector)}
            onValueChange={(v) => {
              setTouched(true);
              setSelector(decodeSelector(v));
            }}
          >
            <SelectTrigger id="period">
              <SelectValue placeholder="Pick a period" />
            </SelectTrigger>
            <SelectContent>
              {closedPeriods.length > 0 && (
                <SelectGroup>
                  <SelectLabel>Monthly</SelectLabel>
                  {closedPeriods.map((p) => (
                    <SelectItem
                      key={p.period_end}
                      value={`month:${p.period_end}`}
                    >
                      {p.period_label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              )}
              <SelectGroup>
                <SelectLabel>Period ranges</SelectLabel>
                <SelectItem value="ytd">Year to date (fiscal)</SelectItem>
                <SelectItem value="rolling12">Rolling 12 months</SelectItem>
                <SelectItem value="qtd">Quarter to date (fiscal)</SelectItem>
                <SelectItem value="trailing3">Last 3 months</SelectItem>
                <SelectItem value="last6">Last 6 months</SelectItem>
              </SelectGroup>
              <SelectGroup>
                <SelectLabel>Custom</SelectLabel>
                <SelectItem value="custom">Custom date range…</SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
        {selector.kind === 'custom' && (
          <>
            <div>
              <Label htmlFor="from">From</Label>
              <Input
                id="from"
                type="date"
                value={customFrom}
                onChange={(e) => setCustomFrom(e.target.value)}
              />
            </div>
            <div>
              <Label htmlFor="to">To</Label>
              <Input
                id="to"
                type="date"
                value={customTo}
                onChange={(e) => setCustomTo(e.target.value)}
              />
            </div>
          </>
        )}
        <label className="flex items-center gap-2 text-sm text-ink cursor-pointer pb-2">
          <input
            type="checkbox"
            checked={showCompare}
            onChange={(e) => setShowCompare(e.target.checked)}
            className="h-4 w-4 rounded border-input text-ledger-blue"
          />
          Compare to prior year
        </label>
      </div>

      {report.isLoading ? (
        <Skeleton className="h-96" />
      ) : report.isError ? (
        <p className="text-red-700">Could not load the income statement.</p>
      ) : !report.data ? (
        <p className="text-slate">No data for this period.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud sticky top-0">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">
                  Account
                </th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">
                  {report.data.period_label}
                </th>
                {showCompare && (
                  <th className="text-right font-semibold text-deep-navy px-4 py-2">
                    {report.data.prior_label}
                  </th>
                )}
                <th className="text-right font-semibold text-deep-navy px-4 py-2">
                  %
                </th>
                {showCompare && (
                  <th className="text-right font-semibold text-deep-navy px-4 py-2">
                    PY %
                  </th>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {report.data.sections.map((sec) => (
                <SectionRows
                  key={sec.section}
                  section={sec}
                  showCompare={showCompare}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}

function SectionRows({
  section,
  showCompare,
}: {
  section: IncomeStatementSection;
  showCompare: boolean;
}) {
  const isSummary =
    section.section === 'Gross Profit' || section.section === 'Net Income';
  const summaryClass = section.section === 'Net Income'
    ? 'bg-cloud font-bold text-deep-navy border-t-2 border-deep-navy'
    : 'bg-cloud/60 font-semibold text-deep-navy border-t border-deep-navy';
  if (isSummary) {
    return (
      <tr className={summaryClass}>
        <td className="px-4 py-2 uppercase tracking-wider text-xs">
          {section.section}
        </td>
        <td className="px-4 py-2 text-right tabular-nums">
          {formatAmt(section.section_total)}
        </td>
        {showCompare && (
          <td className="px-4 py-2 text-right tabular-nums">
            {formatAmt(section.prior_total)}
          </td>
        )}
        <td className="px-4 py-2 text-right tabular-nums">
          {formatPct(section.section_pct)}
        </td>
        {showCompare && (
          <td className="px-4 py-2 text-right tabular-nums">
            {formatPct(section.prior_pct)}
          </td>
        )}
      </tr>
    );
  }

  return (
    <>
      <tr className="bg-cloud/40">
        <td
          className="px-4 py-1.5 text-xs uppercase tracking-wider font-semibold text-deep-navy"
          colSpan={showCompare ? 5 : 3}
        >
          {section.section}
        </td>
      </tr>
      {section.accounts.length === 0 ? (
        <tr>
          <td
            className="px-4 py-1.5 text-xs text-slate italic"
            colSpan={showCompare ? 5 : 3}
          >
            No activity.
          </td>
        </tr>
      ) : (
        section.accounts.map((a) => (
          <tr key={a.account_code} className="hover:bg-cloud">
            <td className="px-4 py-1.5">
              <span className="text-slate font-mono text-xs mr-2">
                {a.account_code}
              </span>
              <span className="text-ink">{a.account_name}</span>
            </td>
            <td className="px-4 py-1.5 text-right tabular-nums">
              {formatAmt(a.current_amount)}
            </td>
            {showCompare && (
              <td className="px-4 py-1.5 text-right tabular-nums">
                {formatAmt(a.prior_amount)}
              </td>
            )}
            <td className="px-4 py-1.5 text-right tabular-nums">
              {formatPct(a.current_pct)}
            </td>
            {showCompare && (
              <td className="px-4 py-1.5 text-right tabular-nums">
                {formatPct(a.prior_pct)}
              </td>
            )}
          </tr>
        ))
      )}
      <tr className="font-semibold text-deep-navy border-t border-border">
        <td className="px-4 py-1.5">Total {section.section}</td>
        <td className="px-4 py-1.5 text-right tabular-nums">
          {formatAmt(section.section_total)}
        </td>
        {showCompare && (
          <td className="px-4 py-1.5 text-right tabular-nums">
            {formatAmt(section.prior_total)}
          </td>
        )}
        <td className="px-4 py-1.5 text-right tabular-nums">
          {formatPct(section.section_pct)}
        </td>
        {showCompare && (
          <td className="px-4 py-1.5 text-right tabular-nums">
            {formatPct(section.prior_pct)}
          </td>
        )}
      </tr>
    </>
  );
}

function formatAmt(n: number): React.ReactNode {
  if (!n) return <span className="text-slate">—</span>;
  if (n < 0) {
    return (
      <span className="text-red-700">
        ({formatMoney(Math.abs(n))})
      </span>
    );
  }
  return formatMoney(n);
}

function formatPct(n: number | null): React.ReactNode {
  if (n === null || n === undefined) return <span className="text-slate">—</span>;
  if (n === 0) return <span className="text-slate">—</span>;
  const cls = cn(n < 0 && 'text-red-700');
  return <span className={cls}>{n.toFixed(1)}%</span>;
}
