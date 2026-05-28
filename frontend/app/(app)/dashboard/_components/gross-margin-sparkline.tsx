'use client';

import { useQuery } from '@tanstack/react-query';
import { Line, LineChart, ResponsiveContainer } from 'recharts';
import { getSalesHistory } from '@/lib/api/dashboard';
import { useEntityStore } from '@/lib/store/entity';

// 12-point margin sparkline backed by /api/dashboard/sales-history.
// Each point is one accounting period; only periods with non-zero
// sales contribute, so empty months don't drag the line to 0.
export function GrossMarginSparkline() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const history = useQuery({
    queryKey: ['sales-history', entityCode, 12],
    enabled: !!entityCode,
    queryFn: () => getSalesHistory(entityCode!, 12),
  });

  const points = (history.data?.series ?? [])
    .filter((p) => p.sales > 0)
    .map((p, i) => ({ m: i, v: p.margin_pct }));

  if (points.length < 2) {
    return <div className="h-8" />;
  }

  return (
    <ResponsiveContainer width="100%" height={32}>
      <LineChart data={points}>
        <Line
          type="monotone"
          dataKey="v"
          stroke="#13B8B4"
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
