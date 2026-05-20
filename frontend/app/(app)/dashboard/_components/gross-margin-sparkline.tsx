'use client';

import { Line, LineChart, ResponsiveContainer } from 'recharts';

// TODO: backend endpoint not built — gross margin trend series.
const SPARK = [
  { m: 1, v: 27.2 },
  { m: 2, v: 27.5 },
  { m: 3, v: 28.0 },
  { m: 4, v: 27.9 },
  { m: 5, v: 28.4 },
  { m: 6, v: 28.6 },
  { m: 7, v: 28.7 },
];

export function GrossMarginSparkline() {
  return (
    <ResponsiveContainer width="100%" height={32}>
      <LineChart data={SPARK}>
        <Line
          type="monotone"
          dataKey="v"
          stroke="#13B8B4"
          strokeWidth={2}
          dot={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
