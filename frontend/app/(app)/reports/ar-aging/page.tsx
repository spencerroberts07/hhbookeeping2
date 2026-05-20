'use client';

import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getLatestAgedAr } from '@/lib/api/pos';
import { formatMoney, formatDate } from '@/lib/utils';

export default function ArAgingPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const aging = useQuery({
    queryKey: ['ar-aging', entityCode],
    enabled: !!entityCode,
    queryFn: () => getLatestAgedAr(entityCode!),
  });

  const snapshotDate = aging.data?.snapshot_date;

  return (
    <ReportShell
      title="AR Aging"
      subtitle={
        snapshotDate
          ? `As of ${formatDate(snapshotDate)} (POS snapshot)`
          : 'No snapshot uploaded'
      }
    >
      <p className="text-xs text-slate mb-3 no-print">
        AR aging is derived from the last uploaded Aged AR POS report. Live
        aging from journal data is not yet available.
      </p>
      {aging.isLoading ? (
        <Skeleton className="h-64" />
      ) : !aging.data || !aging.data.customers?.length ? (
        <p className="text-slate">No customers to age.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Customer</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Current</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">30+</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">60+</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">90+</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Total</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {aging.data.customers.map((c) => (
                <tr key={c.customer_name} className="hover:bg-cloud">
                  <td className="px-4 py-2 text-ink">{c.customer_name}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(c.current)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(c.over_30)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(c.over_60)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(c.over_90)}</td>
                  <td className="px-4 py-2 tabular-nums text-right font-semibold text-deep-navy">
                    {formatMoney(c.total)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}
