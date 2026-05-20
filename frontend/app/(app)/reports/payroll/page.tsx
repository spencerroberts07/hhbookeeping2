'use client';

import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import { listPayrollRuns } from '@/lib/api/payroll';
import { formatMoney, formatDate } from '@/lib/utils';

export default function PayrollReportPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const runs = useQuery({
    queryKey: ['payroll-runs', entityCode],
    enabled: !!entityCode,
    queryFn: () => listPayrollRuns({ entity_code: entityCode!, limit: 24 }),
  });

  return (
    <ReportShell title="Payroll Summary" subtitle="By pay period">
      {runs.isLoading ? (
        <Skeleton className="h-64" />
      ) : !runs.data?.runs.length ? (
        <p className="text-slate">No payroll runs uploaded yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Pay run</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Period</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Pay date</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Gross</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">CRA</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Net</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Vac. liab.</th>
                <th className="px-4 py-2">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {runs.data.runs.map((r) => (
                <tr key={r.id} className="hover:bg-cloud">
                  <td className="px-4 py-2 text-ink font-mono text-xs">{r.pay_run_number}</td>
                  <td className="px-4 py-2 text-ink">
                    {formatDate(r.period_start)} – {formatDate(r.period_end)}
                  </td>
                  <td className="px-4 py-2 text-ink">{formatDate(r.pay_date)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(r.gross_total)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(r.cra_total)}</td>
                  <td className="px-4 py-2 tabular-nums text-right font-semibold">{formatMoney(r.net_total)}</td>
                  <td className="px-4 py-2 tabular-nums text-right">{formatMoney(r.vacation_payable_total)}</td>
                  <td className="px-4 py-2">
                    <Badge
                      variant={
                        r.status === 'posted'
                          ? 'complete'
                          : r.status === 'approved'
                            ? 'complete'
                            : r.status === 'voided'
                              ? 'error'
                              : 'pending'
                      }
                    >
                      {r.status}
                    </Badge>
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
