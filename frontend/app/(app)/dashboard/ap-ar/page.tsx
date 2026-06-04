'use client';

import Link from 'next/link';
import { useQuery } from '@tanstack/react-query';
import { ArrowLeft } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getHHAPSummary } from '@/lib/api/hh_ap';
import { formatMoney } from '@/lib/utils';
import { BalanceTrendCard } from '../_components/balance-trend-card';
import { SourcePill } from '../_components/analytics-ui';
import { AnalyticsNav } from '../_components/analytics-nav';

function Bucket({ label, amount, highlight }: { label: string; amount: number; highlight?: boolean }) {
  return (
    <div className={`rounded-xl border p-3 ${highlight ? 'bg-deep-navy text-white border-deep-navy' : 'border-border bg-white'}`}>
      <div className={`text-xs uppercase tracking-wide ${highlight ? 'text-white/70' : 'text-slate'}`}>{label}</div>
      <div className="text-lg font-bold tabular-nums">{formatMoney(amount)}</div>
    </div>
  );
}

export default function ApArAnalyticsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const ap = useQuery({
    queryKey: ['hh-ap-summary', entityCode],
    enabled: !!entityCode,
    queryFn: () => getHHAPSummary(entityCode!),
  });

  return (
    <div>
      <Topbar title="AP / AR analytics" />
      <div className="p-6 space-y-6">
        <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-ledger-blue hover:underline">
          <ArrowLeft className="h-4 w-4" /> Back to dashboard
        </Link>
        <AnalyticsNav />

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              Accounts payable — HH aging <SourcePill source="gl_net" />
            </CardTitle>
          </CardHeader>
          <CardContent>
            {ap.isLoading || !ap.data ? (
              <Skeleton className="h-24" />
            ) : (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
                <Bucket label="Current" amount={ap.data.aging.current} />
                <Bucket label="30+" amount={ap.data.aging.over_30} />
                <Bucket label="60+" amount={ap.data.aging.over_60} />
                <Bucket label="90+" amount={ap.data.aging.over_90} />
                <Bucket label="Total outstanding" amount={ap.data.current_balance} highlight />
              </div>
            )}
          </CardContent>
        </Card>

        <BalanceTrendCard title="Accounts receivable" accountCode="1090" />

        <p className="text-xs text-slate">
          AP aging from the HH AP module (open invoices, account 2030). AR is the month-end balance
          of GL account 1090, cutover-aware, vs the same month-end last year.
        </p>
      </div>
    </div>
  );
}
