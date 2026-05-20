'use client';

import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { getRevenueSnapshot } from '@/lib/api/admin';
import { formatMoney } from '@/lib/utils';

export default function RevenuePage() {
  const rev = useQuery({
    queryKey: ['admin-revenue'],
    queryFn: () => getRevenueSnapshot(),
  });

  return (
    <>
      <Topbar title="Revenue (admin)" />
      <main className="p-6">
        {rev.isLoading ? (
          <Skeleton className="h-32" />
        ) : rev.data ? (
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            <Stat label="MRR" value={formatMoney(rev.data.mrr)} />
            <Stat label="ARR" value={formatMoney(rev.data.arr)} />
            <Stat label="Active dealers" value={rev.data.active_dealers.toString()} />
            <Stat label="Trialing" value={rev.data.trialing_dealers.toString()} />
            <Stat
              label="Signups (30d)"
              value={rev.data.new_signups_last_30d.toString()}
            />
            <Stat label="Churn (30d)" value={rev.data.churn_last_30d.toString()} />
          </div>
        ) : null}
      </main>
    </>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-xs uppercase tracking-wider text-slate">
          {label}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-extrabold text-deep-navy tabular-nums">
          {value}
        </div>
      </CardContent>
    </Card>
  );
}
