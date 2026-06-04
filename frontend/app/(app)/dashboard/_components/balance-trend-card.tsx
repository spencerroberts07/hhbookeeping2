'use client';

import { useQuery } from '@tanstack/react-query';
import {
  Line,
  LineChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getAccountTrend } from '@/lib/api/dashboard';
import { formatMoney, formatMonthLabel, parseLocalDate } from '@/lib/utils';
import { SourcePill, GrowthChip, AXIS, THIS_YR, LAST_YR, moneyTick, moneyTip } from './analytics-ui';

/** Month-end balance of one account over time, current vs prior year.
 *  Cutover-aware on the backend (reconciles to the balance sheet). */
export function BalanceTrendCard({
  title,
  accountCode,
  months = 24,
}: {
  title: string;
  accountCode: string;
  months?: number;
}) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const q = useQuery({
    queryKey: ['account-trend', entityCode, accountCode, months],
    enabled: !!entityCode,
    queryFn: () => getAccountTrend(entityCode!, accountCode, months),
  });

  const data = (q.data?.series ?? []).map((p) => ({
    label: formatMonthLabel(parseLocalDate(p.period_end)),
    balance: p.balance,
    py_balance: p.py_balance,
  }));
  const latest = q.data?.series.at(-1);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          {title} <span className="font-mono text-xs text-slate">{accountCode}</span>
          <SourcePill source="gl_net" />
        </CardTitle>
        {latest && (
          <div className="flex items-center gap-3 text-xs text-slate">
            <span className="tabular-nums text-deep-navy font-semibold">{formatMoney(latest.balance)}</span>
            <span>YoY <GrowthChip pct={latest.yoy_growth_pct} /></span>
          </div>
        )}
      </CardHeader>
      <CardContent>
        {q.isLoading ? (
          <Skeleton className="h-[260px]" />
        ) : data.length === 0 ? (
          <p className="text-sm text-slate">No data.</p>
        ) : (
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="label" stroke={AXIS} fontSize={12} minTickGap={20} />
              <YAxis stroke={AXIS} fontSize={12} tickFormatter={moneyTick} />
              <Tooltip contentStyle={{ borderRadius: 12, border: '1px solid #E2E8F0', fontSize: 12 }} formatter={moneyTip} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line type="monotone" dataKey="balance" name="This year" stroke={THIS_YR} strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="py_balance" name="Last year" stroke={LAST_YR} strokeWidth={1.5} strokeDasharray="4 4" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
