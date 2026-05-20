'use client';

import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { listDealers } from '@/lib/api/admin';
import { formatMoney, formatDate } from '@/lib/utils';

export default function DealersPage() {
  const dealers = useQuery({
    queryKey: ['admin-dealers'],
    queryFn: () => listDealers(),
  });

  return (
    <>
      <Topbar title="Dealers (admin)" />
      <main className="p-6">
        <Card>
          <CardContent className="p-0">
            {dealers.isLoading ? (
              <Skeleton className="h-64 m-4" />
            ) : (
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Store</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Province</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Plan</th>
                    <th className="text-right font-semibold text-deep-navy px-4 py-2">MRR</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Last active</th>
                    <th className="px-4 py-2">Month-end</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {dealers.data?.dealers.map((d) => (
                    <tr key={d.entity_code} className="hover:bg-cloud">
                      <td className="px-4 py-2 text-ink">
                        <span className="font-mono text-xs text-slate mr-2">
                          {d.entity_code}
                        </span>
                        {d.store_name}
                      </td>
                      <td className="px-4 py-2 text-slate">{d.province}</td>
                      <td className="px-4 py-2">
                        <Badge
                          variant={d.plan_tier === 'professional' ? 'complete' : 'info'}
                        >
                          {d.plan_tier}
                        </Badge>
                      </td>
                      <td className="px-4 py-2 tabular-nums text-right">
                        {formatMoney(d.mrr)}
                      </td>
                      <td className="px-4 py-2 text-slate">
                        {formatDate(d.last_active)}
                      </td>
                      <td className="px-4 py-2">
                        <Badge
                          variant={
                            d.month_end_status === 'closed'
                              ? 'complete'
                              : d.month_end_status === 'in_progress'
                                ? 'warning'
                                : 'info'
                          }
                        >
                          {d.month_end_status}
                        </Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </main>
    </>
  );
}
