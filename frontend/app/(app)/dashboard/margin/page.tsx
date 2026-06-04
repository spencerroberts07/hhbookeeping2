'use client';

import Link from 'next/link';
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
import { ArrowLeft } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getMarginTrend } from '@/lib/api/dashboard';
import { formatMonthLabel, parseLocalDate } from '@/lib/utils';
import { SourcePill, AXIS, THIS_YR, LAST_YR, ACCENT } from '../_components/analytics-ui';
import { AnalyticsNav } from '../_components/analytics-nav';

const pctTip = (v: unknown) => {
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? `${n.toFixed(1)}%` : '—';
};

export default function MarginAnalyticsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const q = useQuery({
    queryKey: ['margin-trend', entityCode, 24],
    enabled: !!entityCode,
    queryFn: () => getMarginTrend(entityCode!, 24),
  });

  const data = (q.data?.series ?? []).map((p) => ({
    label: formatMonthLabel(parseLocalDate(p.period_end)),
    gross: p.gross_margin_pct,
    operating: p.operating_margin_pct,
    net: p.net_margin_pct,
    py_net: p.py_net_margin_pct,
  }));
  const unclosed = (q.data?.series ?? [])
    .filter((p) => !p.closed)
    .map((p) => formatMonthLabel(parseLocalDate(p.period_end)));

  return (
    <div>
      <Topbar title="Margin analytics" />
      <div className="p-6 space-y-6">
        <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-ledger-blue hover:underline">
          <ArrowLeft className="h-4 w-4" /> Back to dashboard
        </Link>
        <AnalyticsNav />
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              Margin trend (gross / operating / net) <SourcePill source="gl_net" />
            </CardTitle>
          </CardHeader>
          <CardContent>
            {q.isLoading ? (
              <Skeleton className="h-[300px]" />
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="label" stroke={AXIS} fontSize={12} minTickGap={20} />
                  <YAxis stroke={AXIS} fontSize={12} tickFormatter={(v) => `${v}%`} />
                  <Tooltip contentStyle={{ borderRadius: 12, border: '1px solid #E2E8F0', fontSize: 12 }} formatter={pctTip} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Line type="monotone" dataKey="gross" name="Gross %" stroke={THIS_YR} strokeWidth={2} dot={false} connectNulls={false} />
                  <Line type="monotone" dataKey="operating" name="Operating %" stroke={ACCENT} strokeWidth={2} dot={false} connectNulls={false} />
                  <Line type="monotone" dataKey="net" name="Net %" stroke={LAST_YR} strokeWidth={2} dot={false} connectNulls={false} />
                  <Line type="monotone" dataKey="py_net" name="Net % (last yr)" stroke={LAST_YR} strokeWidth={1.5} strokeDasharray="4 4" dot={false} connectNulls={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
            {unclosed.length > 0 && (
              <p className="mt-2 text-xs text-slate">
                Not yet closed (excluded): {unclosed.join(', ')}.
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
