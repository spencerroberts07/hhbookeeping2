'use client';

import { useQuery } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getHHAPSummary, listHHAPInvoices } from '@/lib/api/hh_ap';
import { formatMoney, formatDate } from '@/lib/utils';

export default function ApAgingPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const summary = useQuery({
    queryKey: ['hh-ap-summary', entityCode],
    enabled: !!entityCode,
    queryFn: () => getHHAPSummary(entityCode!),
  });

  const invoices = useQuery({
    queryKey: ['hh-ap-invoices', entityCode],
    enabled: !!entityCode,
    queryFn: () => listHHAPInvoices({ entity_code: entityCode!, limit: 200 }),
  });

  return (
    <ReportShell title="AP Aging — HH" subtitle="Live from HH AP module">
      {summary.isLoading ? (
        <Skeleton className="h-32 mb-6" />
      ) : summary.data ? (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
          <Bucket label="Current" amount={summary.data.aging.current} />
          <Bucket label="30+" amount={summary.data.aging.over_30} />
          <Bucket label="60+" amount={summary.data.aging.over_60} />
          <Bucket label="90+" amount={summary.data.aging.over_90} />
          <Bucket
            label="Total outstanding"
            amount={summary.data.current_balance}
            highlight
          />
        </div>
      ) : null}
      <div className="text-sm font-semibold text-deep-navy mb-2">
        Outstanding invoices
      </div>
      {invoices.isLoading ? (
        <Skeleton className="h-64" />
      ) : !invoices.data?.invoices.length ? (
        <p className="text-slate">No outstanding invoices.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">#</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Amount</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {invoices.data.invoices.map((i) => (
                <tr key={i.id} className="hover:bg-cloud">
                  <td className="px-4 py-2 text-ink">{formatDate(i.document_date)}</td>
                  <td className="px-4 py-2 text-slate font-mono text-xs">{i.document_type}</td>
                  <td className="px-4 py-2 text-slate font-mono text-xs">
                    {i.document_number ?? '—'}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {formatMoney(i.amount)}
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

function Bucket({
  label,
  amount,
  highlight,
}: {
  label: string;
  amount: number;
  highlight?: boolean;
}) {
  return (
    <div
      className={`rounded-xl border p-3 ${highlight ? 'bg-deep-navy text-white border-deep-navy' : 'border-border bg-white'}`}
    >
      <div className={`text-xs uppercase tracking-wide ${highlight ? 'text-white/70' : 'text-slate'}`}>
        {label}
      </div>
      <div className="text-lg font-bold tabular-nums">{formatMoney(amount)}</div>
    </div>
  );
}
