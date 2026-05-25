'use client';

import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import {
  getCashBalancingDays,
  type CashBalancingDay,
  type CashBalancingStatus,
} from '@/lib/api/cash_balancing';
import { formatDate, formatMoney, cn } from '@/lib/utils';

function defaultRange(): { from: string; to: string } {
  const today = new Date();
  const from = new Date(today.getFullYear(), today.getMonth(), 1)
    .toISOString()
    .slice(0, 10);
  const to = today.toISOString().slice(0, 10);
  return { from, to };
}

const STATUS_LABEL: Record<CashBalancingStatus, string> = {
  balanced: 'Balanced',
  over: 'Over',
  short: 'Short',
};
const STATUS_VARIANT: Record<
  CashBalancingStatus,
  'complete' | 'warning' | 'error'
> = {
  balanced: 'complete',
  over: 'warning',
  short: 'error',
};

export function CashBalancingDailyTab() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [{ from, to }, setRange] = useState(defaultRange);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const q = useQuery({
    queryKey: ['cash-balancing-days', entityCode, from, to],
    enabled: !!entityCode,
    queryFn: () =>
      getCashBalancingDays({
        entity_code: entityCode!,
        date_from: from,
        date_to: to,
      }),
  });

  const toggle = (id: string) =>
    setExpanded((s) => {
      const next = new Set(s);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const summary = q.data?.summary;

  return (
    <div className="space-y-4">
      <Card>
        <CardContent className="p-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
            <div>
              <Label htmlFor="cb-from">From</Label>
              <Input
                id="cb-from"
                type="date"
                value={from}
                onChange={(e) =>
                  setRange((r) => ({ ...r, from: e.target.value }))
                }
              />
            </div>
            <div>
              <Label htmlFor="cb-to">To</Label>
              <Input
                id="cb-to"
                type="date"
                value={to}
                onChange={(e) =>
                  setRange((r) => ({ ...r, to: e.target.value }))
                }
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {summary && <SummaryStrip summary={summary} />}

      <Card>
        <CardContent className="p-0">
          {q.isLoading ? (
            <Skeleton className="h-64 m-4" />
          ) : !q.data || q.data.days.length === 0 ? (
            <div className="p-8 text-center text-slate">
              No cash balancing data in that window. Trigger a sync from the
              Sync History tab.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="w-8 px-2 py-2"></th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Date
                    </th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Day
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Sales
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      HST
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Paid Outs
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Over / (Short)
                    </th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Status
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {q.data.days.map((d) => (
                    <DayRow
                      key={d.id}
                      day={d}
                      expanded={expanded.has(d.id)}
                      onToggle={() => toggle(d.id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SummaryStrip({
  summary,
}: {
  summary: NonNullable<
    Awaited<ReturnType<typeof getCashBalancingDays>>
  >['summary'];
}) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
      <SummaryCard label="Days" value={String(summary.day_count)} />
      <SummaryCard label="Sales" value={formatMoney(summary.total_sales)} />
      <SummaryCard label="HST" value={formatMoney(summary.total_hst)} />
      <SummaryCard
        label="Net variance"
        value={formatMoney(summary.net_variance, { signed: true })}
        tone={
          summary.net_variance > 10
            ? 'positive'
            : summary.net_variance < -10
              ? 'negative'
              : 'neutral'
        }
      />
      <SummaryCard
        label="Balanced / Over / Short"
        value={`${summary.balanced_days} / ${summary.over_days} / ${summary.short_days}`}
      />
    </div>
  );
}

function SummaryCard({
  label,
  value,
  tone = 'neutral',
}: {
  label: string;
  value: string;
  tone?: 'neutral' | 'positive' | 'negative';
}) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-xs uppercase tracking-wide text-slate">
          {label}
        </div>
        <div
          className={cn(
            'mt-1 text-lg font-semibold tabular-nums',
            tone === 'positive' && 'text-amber-700',
            tone === 'negative' && 'text-red-600',
            tone === 'neutral' && 'text-deep-navy',
          )}
        >
          {value}
        </div>
      </CardContent>
    </Card>
  );
}

function DayRow({
  day,
  expanded,
  onToggle,
}: {
  day: CashBalancingDay;
  expanded: boolean;
  onToggle: () => void;
}) {
  const overTone =
    day.over_short > 10
      ? 'text-amber-700'
      : day.over_short < -10
        ? 'text-red-600'
        : 'text-ink';

  return (
    <>
      <tr className="hover:bg-cloud cursor-pointer" onClick={onToggle}>
        <td className="px-2 py-2 text-slate">
          {expanded ? (
            <ChevronDown className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
        </td>
        <td className="px-4 py-2 text-ink whitespace-nowrap">
          {formatDate(day.business_date)}
        </td>
        <td className="px-4 py-2 text-slate">{day.day_of_week}</td>
        <td className="px-4 py-2 tabular-nums text-right text-ink">
          {formatMoney(day.total_sales)}
        </td>
        <td className="px-4 py-2 tabular-nums text-right text-slate">
          {formatMoney(day.total_hst)}
        </td>
        <td className="px-4 py-2 tabular-nums text-right text-slate">
          {formatMoney(day.paid_outs)}
        </td>
        <td
          className={cn(
            'px-4 py-2 tabular-nums text-right font-medium',
            overTone,
          )}
        >
          {formatMoney(day.over_short, { signed: true })}
        </td>
        <td className="px-4 py-2">
          <Badge variant={STATUS_VARIANT[day.status]}>
            {STATUS_LABEL[day.status]}
          </Badge>
        </td>
      </tr>
      {expanded && (
        <tr className="bg-cloud/40">
          <td colSpan={8} className="px-4 py-3">
            <TenderBreakdown day={day} />
          </td>
        </tr>
      )}
    </>
  );
}

function TenderBreakdown({ day }: { day: CashBalancingDay }) {
  const grouped = useMemo(() => {
    const tenders = day.lines.filter(
      (l) => !(l.mapped_account_code || '').startsWith('6'),
    );
    const paidOuts = day.lines.filter((l) =>
      (l.mapped_account_code || '').startsWith('6'),
    );
    return { tenders, paidOuts };
  }, [day.lines]);

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <div>
        <div className="text-xs uppercase tracking-wide text-slate mb-2">
          Tender lines
        </div>
        {grouped.tenders.length === 0 ? (
          <div className="text-sm text-slate">—</div>
        ) : (
          <table className="w-full text-sm">
            <tbody className="divide-y divide-border">
              {grouped.tenders.map((l, i) => (
                <tr key={`t-${i}`}>
                  <td className="py-1 text-ink">{l.line_label}</td>
                  <td className="py-1 text-slate text-xs">
                    {l.mapped_account_code ?? '—'}
                  </td>
                  <td className="py-1 tabular-nums text-right">
                    {formatMoney(l.amount, { signed: true })}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      <div>
        <div className="text-xs uppercase tracking-wide text-slate mb-2">
          Paid outs (6xxx)
        </div>
        {grouped.paidOuts.length === 0 ? (
          <div className="text-sm text-slate">—</div>
        ) : (
          <table className="w-full text-sm">
            <tbody className="divide-y divide-border">
              {grouped.paidOuts.map((l, i) => (
                <tr key={`p-${i}`}>
                  <td className="py-1 text-ink">{l.line_label}</td>
                  <td className="py-1 text-slate text-xs">
                    {l.mapped_account_code ?? '—'}
                  </td>
                  <td className="py-1 tabular-nums text-right">
                    {formatMoney(l.amount, { signed: true })}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
