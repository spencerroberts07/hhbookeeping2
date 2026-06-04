'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useQuery } from '@tanstack/react-query';
import {
  Bar,
  ComposedChart,
  Line,
  LineChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { ArrowLeft, TrendingUp, TrendingDown } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatPercent, formatMonthLabel, parseLocalDate } from '@/lib/utils';
import {
  getSalesMonthly,
  getSalesRolling12,
  getSalesDaily,
  getSalesMtd,
} from '@/lib/api/dashboard';

function SourcePill({ source }: { source: 'gl_net' | 'pos_gross' }) {
  const isGl = source === 'gl_net';
  return (
    <span
      className={
        'rounded-full px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide ' +
        (isGl ? 'bg-ledger-blue/10 text-ledger-blue' : 'bg-bw-teal/10 text-bw-teal')
      }
      title={isGl ? 'From the general ledger (reconciles to the income statement)' : 'From daily cash balancing (POS gross sales)'}
    >
      {isGl ? 'GL net' : 'POS gross'}
    </span>
  );
}

function GrowthChip({ pct }: { pct: number | null }) {
  if (pct === null || pct === undefined) return <span className="text-slate">—</span>;
  const up = pct >= 0;
  return (
    <span className={'inline-flex items-center gap-1 text-sm font-medium ' + (up ? 'text-green-700' : 'text-red-700')}>
      {up ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
      {up ? '+' : ''}{pct.toFixed(1)}%
    </span>
  );
}

const AXIS = '#64748B';
const THIS_YR = '#1454C8';
const LAST_YR = '#0B2E72';
const ACCENT = '#13B8B4';
const moneyTick = (v: number) => `$${((v ?? 0) / 1000).toFixed(0)}k`;
const moneyTip = (value: unknown) => {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? formatMoney(n) : '—';
};

export default function SalesAnalyticsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [days, setDays] = useState(90);

  const mtd = useQuery({
    queryKey: ['sales-mtd', entityCode],
    enabled: !!entityCode,
    queryFn: () => getSalesMtd(entityCode!),
  });
  const monthly = useQuery({
    queryKey: ['sales-monthly', entityCode, 24],
    enabled: !!entityCode,
    queryFn: () => getSalesMonthly(entityCode!, 24),
  });
  const rolling = useQuery({
    queryKey: ['sales-rolling12', entityCode, 24],
    enabled: !!entityCode,
    queryFn: () => getSalesRolling12(entityCode!, 24),
  });
  const daily = useQuery({
    queryKey: ['sales-daily', entityCode, days],
    enabled: !!entityCode,
    queryFn: () => getSalesDaily(entityCode!, days),
  });

  const monthlyData = (monthly.data?.series ?? []).map((p) => ({
    label: formatMonthLabel(parseLocalDate(p.period_end)),
    sales: p.sales,
    py_sales: p.py_sales,
    margin_pct: p.margin_pct,
  }));
  const latest = monthly.data?.series.at(-1);

  const rollingData = (rolling.data?.series ?? []).map((p) => ({
    label: formatMonthLabel(parseLocalDate(p.period_end)),
    rolling12: p.rolling12_sales,
    py_rolling12: p.py_rolling12_sales,
  }));

  const dailyData = (daily.data?.series ?? []).map((p) => ({
    label: parseLocalDate(p.date).toLocaleDateString('en-CA', { month: 'short', day: 'numeric' }),
    sales: p.sales,
    py_sales: p.py_sales,
  }));

  return (
    <div>
      <Topbar title="Sales analytics" />
      <div className="p-6 space-y-6">
        <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-ledger-blue hover:underline">
          <ArrowLeft className="h-4 w-4" /> Back to dashboard
        </Link>

        {/* MTD — POS gross, same source as the dashboard Sales card */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              Month to date <SourcePill source="pos_gross" />
            </CardTitle>
            {mtd.data && (
              <span className="text-xs text-slate">
                {mtd.data.month_label} · {mtd.data.days_elapsed} days · vs same {mtd.data.days_elapsed} days last year
              </span>
            )}
          </CardHeader>
          <CardContent>
            {mtd.isLoading || !mtd.data ? (
              <Skeleton className="h-16" />
            ) : (
              <div className="flex flex-wrap items-end gap-8">
                <div>
                  <div className="text-3xl font-bold text-deep-navy tabular-nums">
                    {formatMoney(mtd.data.mtd_sales)}
                  </div>
                  <div className="text-xs text-slate">this month to date</div>
                </div>
                <div>
                  <div className="text-xl font-semibold text-slate tabular-nums">
                    {formatMoney(mtd.data.py_mtd_sales)}
                  </div>
                  <div className="text-xs text-slate">same period last year</div>
                </div>
                <div>
                  <GrowthChip pct={mtd.data.yoy_growth_pct} />
                  <div className="text-xs text-slate">YoY</div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Monthly trend + margin — GL net */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              Monthly sales &amp; margin <SourcePill source="gl_net" />
            </CardTitle>
            {latest && (
              <div className="flex items-center gap-4 text-xs text-slate">
                <span>YoY <GrowthChip pct={latest.yoy_growth_pct} /></span>
                <span>MoM <GrowthChip pct={latest.mom_growth_pct} /></span>
              </div>
            )}
          </CardHeader>
          <CardContent>
            {monthly.isLoading ? (
              <Skeleton className="h-[300px]" />
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={monthlyData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="label" stroke={AXIS} fontSize={12} />
                  <YAxis yAxisId="left" stroke={AXIS} fontSize={12} tickFormatter={moneyTick} />
                  <YAxis yAxisId="right" orientation="right" stroke={ACCENT} fontSize={12} tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
                  <Tooltip contentStyle={{ borderRadius: 12, border: '1px solid #E2E8F0', fontSize: 12 }} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar yAxisId="left" dataKey="sales" name="This year" fill={THIS_YR} radius={[6, 6, 0, 0]} />
                  <Bar yAxisId="left" dataKey="py_sales" name="Last year" fill={LAST_YR} radius={[6, 6, 0, 0]} />
                  <Line yAxisId="right" type="monotone" dataKey="margin_pct" name="Margin %" stroke={ACCENT} strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Rolling 12 — GL net */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              Rolling 12-month sales <SourcePill source="gl_net" />
            </CardTitle>
          </CardHeader>
          <CardContent>
            {rolling.isLoading ? (
              <Skeleton className="h-[260px]" />
            ) : (
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={rollingData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="label" stroke={AXIS} fontSize={12} />
                  <YAxis stroke={AXIS} fontSize={12} tickFormatter={moneyTick} />
                  <Tooltip contentStyle={{ borderRadius: 12, border: '1px solid #E2E8F0', fontSize: 12 }} formatter={moneyTip} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Line type="monotone" dataKey="rolling12" name="This year" stroke={THIS_YR} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="py_rolling12" name="Last year" stroke={LAST_YR} strokeWidth={2} strokeDasharray="4 4" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Daily — POS gross, current vs same calendar day prior year */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              Daily sales <SourcePill source="pos_gross" />
            </CardTitle>
            <div className="flex gap-1">
              {[30, 90, 180].map((d) => (
                <button
                  key={d}
                  onClick={() => setDays(d)}
                  className={
                    'rounded-md px-2 py-1 text-xs ' +
                    (days === d ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud')
                  }
                >
                  {d}d
                </button>
              ))}
            </div>
          </CardHeader>
          <CardContent>
            {daily.isLoading ? (
              <Skeleton className="h-[260px]" />
            ) : (
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={dailyData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="label" stroke={AXIS} fontSize={11} minTickGap={24} />
                  <YAxis stroke={AXIS} fontSize={12} tickFormatter={moneyTick} />
                  <Tooltip contentStyle={{ borderRadius: 12, border: '1px solid #E2E8F0', fontSize: 12 }} formatter={moneyTip} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Line type="monotone" dataKey="sales" name="This year" stroke={THIS_YR} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="py_sales" name="Same day last year" stroke={LAST_YR} strokeWidth={1.5} strokeDasharray="4 4" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        <p className="text-xs text-slate">
          <SourcePill source="pos_gross" /> daily &amp; MTD are gross sales from daily cash balancing.{' '}
          <SourcePill source="gl_net" /> monthly &amp; rolling-12 are net revenue from the general ledger and reconcile to the income statement. The two differ by tax, returns, and timing.
        </p>
      </div>
    </div>
  );
}
