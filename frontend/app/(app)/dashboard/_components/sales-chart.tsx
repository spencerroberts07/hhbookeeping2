'use client';

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
import { formatMoney } from '@/lib/utils';

// TODO: backend endpoint not built — month-vs-yoy sales time series.
// Replace with real series when /api/reports/sales-by-month lands.
const DATA = [
  { month: 'Sep', this_year: 524_300, last_year: 498_120 },
  { month: 'Oct', this_year: 612_840, last_year: 580_200 },
  { month: 'Nov', this_year: 588_220, last_year: 562_412 },
  { month: 'Dec', this_year: 642_180, last_year: 605_990 },
  { month: 'Jan', this_year: 482_550, last_year: 459_220 },
  { month: 'Feb', this_year: 521_140, last_year: 490_330 },
];

export function SalesChart() {
  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={DATA} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
        <XAxis dataKey="month" stroke="#64748B" fontSize={12} />
        <YAxis
          stroke="#64748B"
          fontSize={12}
          tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
        />
        <Tooltip
          contentStyle={{
            borderRadius: 12,
            border: '1px solid #E2E8F0',
            fontSize: 12,
          }}
          formatter={(value: number) => formatMoney(value)}
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
