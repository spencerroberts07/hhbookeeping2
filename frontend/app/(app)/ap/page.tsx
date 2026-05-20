'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { getHHAPSummary, listHHAPInvoices } from '@/lib/api/hh_ap';
import { formatMoney, formatDate } from '@/lib/utils';
import { Upload } from 'lucide-react';
import Link from 'next/link';

type Tab = 'hh' | 'vendor';

export default function ApPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [tab, setTab] = useState<Tab>('hh');

  const summary = useQuery({
    queryKey: ['hh-ap-summary', entityCode],
    enabled: !!entityCode && tab === 'hh',
    queryFn: () => getHHAPSummary(entityCode!),
  });

  const invoices = useQuery({
    queryKey: ['hh-ap-invoices', entityCode, 'recent'],
    enabled: !!entityCode && tab === 'hh',
    queryFn: () => listHHAPInvoices({ entity_code: entityCode!, limit: 100 }),
  });

  return (
    <>
      <Topbar title="Accounts Payable" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <nav className="flex gap-1 bg-white border border-border rounded-xl p-1" role="tablist">
            <button
              role="tab"
              aria-selected={tab === 'hh'}
              onClick={() => setTab('hh')}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                tab === 'hh' ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              HH AP
            </button>
            <button
              role="tab"
              aria-selected={tab === 'vendor'}
              onClick={() => setTab('vendor')}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                tab === 'vendor' ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              Other vendors
            </button>
          </nav>
          {tab === 'hh' && (
            <Link href="/month-end">
              <Button variant="accent">
                <Upload className="h-4 w-4" strokeWidth={1.5} />
                Upload statement
              </Button>
            </Link>
          )}
        </div>

        {tab === 'hh' && (
          <>
            <section className="grid grid-cols-2 md:grid-cols-5 gap-3">
              {summary.isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-20" />
                ))
              ) : summary.data ? (
                <>
                  <Bucket label="Current" amount={summary.data.aging.current} />
                  <Bucket label="30+" amount={summary.data.aging.over_30} />
                  <Bucket label="60+" amount={summary.data.aging.over_60} />
                  <Bucket label="90+" amount={summary.data.aging.over_90} />
                  <Bucket
                    label="Total outstanding"
                    amount={summary.data.current_balance}
                    highlight
                  />
                </>
              ) : null}
            </section>

            <Card>
              <CardHeader>
                <CardTitle>Recent invoices</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                {invoices.isLoading ? (
                  <Skeleton className="h-64 m-4" />
                ) : !invoices.data?.invoices.length ? (
                  <div className="p-8 text-center text-slate">No invoices.</div>
                ) : (
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
                          <td className="px-4 py-2">
                            <Badge variant="info">{i.document_type}</Badge>
                          </td>
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
                )}
              </CardContent>
            </Card>
          </>
        )}

        {tab === 'vendor' && (
          <Card>
            <CardHeader>
              <CardTitle>Other vendors</CardTitle>
            </CardHeader>
            <CardContent>
              {/* TODO: backend endpoint not built — vendor-master list
                   with balances. /api/direct-vendor-ap has cheque/transfer
                   tracking but not a vendor master view. */}
              <p className="text-sm text-slate">
                Non-HH vendor balances and payment history will appear here
                once the vendor-master endpoint lands. For now, see{' '}
                <Link href="/bank" className="text-ledger-blue underline">
                  Bank module
                </Link>{' '}
                for outstanding payments.
              </p>
            </CardContent>
          </Card>
        )}
      </main>
    </>
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
    <Card
      className={`p-3 ${highlight ? 'bg-deep-navy text-white border-deep-navy' : ''}`}
    >
      <div className={`text-xs uppercase tracking-wide ${highlight ? 'text-white/70' : 'text-slate'}`}>
        {label}
      </div>
      <div className="text-xl font-bold tabular-nums mt-1">{formatMoney(amount)}</div>
    </Card>
  );
}
