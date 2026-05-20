'use client';

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { formatMoney } from '@/lib/utils';
import type { HHAPSummary } from '@/lib/api/hh_ap';

interface Props {
  aging: HHAPSummary['aging'];
  total: number;
}

export function ApAgingChart({ aging, total }: Props) {
  const data = [
    { bucket: 'Current', amount: aging.current },
    { bucket: '30+', amount: aging.over_30 },
    { bucket: '60+', amount: aging.over_60 },
    { bucket: '90+', amount: aging.over_90 },
  ];

  return (
    <div>
      <div className="text-2xl font-extrabold text-deep-navy tabular-nums mb-2">
        {formatMoney(total)}
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
          <XAxis dataKey="bucket" stroke="#64748B" fontSize={11} />
          <YAxis
            stroke="#64748B"
            fontSize={11}
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
          <Bar dataKey="amount" fill="#1454C8" radius={[6, 6, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
