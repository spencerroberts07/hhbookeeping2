'use client';

import { useQuery } from '@tanstack/react-query';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { getSalesHistory } from '@/lib/api/dashboard';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, parseLocalDate } from '@/lib/utils';
import { Skeleton } from '@/components/ui/skeleton';

// This-year vs last-year monthly sales, pulled from
// /api/dashboard/sales-history (24 months of posted/approved
// journal_lines on 4xxx accounts). Bars are bucketed by fiscal year
// + calendar month — each bar is exactly one period row, never a
// sum across periods. Bridlewood fiscal year ends Sep 30, so months
// Oct → Sep span one fiscal year.
const MONTH_NAMES = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

const FY_END_MONTH = 8;
const FY_MONTH_ORDER = [9, 10, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8];

function fiscalYearOf(date: Date): number {
  return date.getMonth() > FY_END_MONTH
    ? date.getFullYear() + 1
    : date.getFullYear();
}

interface ChartPoint {
  month: string;
  this_year: number | null;
  last_year: number | null;
}

export function SalesChart() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const history = useQuery({
    queryKey: ['sales-history', entityCode, 24],
    enabled: !!entityCode,
    queryFn: () => getSalesHistory(entityCode!, 24),
  });

  if (history.isLoading || !history.data) {
    return <Skeleton className="h-[280px] w-full" />;
  }

  const thisFy = fiscalYearOf(new Date());
  const lastFy = thisFy - 1;

  // Index every row by (fiscalYear, calendarMonth). If two rows ever
  // hit the same cell, the later iteration wins — we never sum.
  const byFyMonth = new Map<string, number>();
  for (const p of history.data.series) {
    const d = parseLocalDate(p.period_end);
    byFyMonth.set(`${fiscalYearOf(d)}-${d.getMonth()}`, p.sales);
  }

  const data: ChartPoint[] = FY_MONTH_ORDER.map((monthIdx) => ({
    month: MONTH_NAMES[monthIdx]!,
    this_year: byFyMonth.get(`${thisFy}-${monthIdx}`) ?? null,
    last_year: byFyMonth.get(`${lastFy}-${monthIdx}`) ?? null,
  }));

  if (data.every((p) => !p.this_year && !p.last_year)) {
    return (
      <div className="h-[280px] grid place-items-center text-sm text-slate">
        No posted sales in the last 24 months yet.
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
        <XAxis dataKey="month" stroke="#64748B" fontSize={12} />
        <YAxis
          stroke="#64748B"
          fontSize={12}
          tickFormatter={(v) => `$${((v ?? 0) / 1000).toFixed(0)}k`}
        />
        <Tooltip
          contentStyle={{
            borderRadius: 12,
            border: '1px solid #E2E8F0',
            fontSize: 12,
          }}
          formatter={(value) => {
            const n = typeof value === 'number' ? value : Number(value);
            return Number.isFinite(n) ? formatMoney(n) : '—';
          }}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Bar
          dataKey="this_year"
          name="This year"
          fill="#1454C8"
          radius={[6, 6, 0, 0]}
        />
        <Bar
          dataKey="last_year"
          name="Last year"
          fill="#0B2E72"
          radius={[6, 6, 0, 0]}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
