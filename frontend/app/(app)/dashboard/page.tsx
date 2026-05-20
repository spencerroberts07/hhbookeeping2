'use client';

import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatPercent, formatMonthLabel } from '@/lib/utils';
import { getQuickbooksStatus } from '@/lib/api/dashboard';
import { getHHAPSummary } from '@/lib/api/hh_ap';
import { getPeriodStatus } from '@/lib/api/month_end';
import { getLatestPosFinancial } from '@/lib/api/pos';
import { SalesChart } from './_components/sales-chart';
import { ApAgingChart } from './_components/ap-aging-chart';
import { GrossMarginSparkline } from './_components/gross-margin-sparkline';
import { QuickActions } from './_components/quick-actions';
import { AlertsFeed } from './_components/alerts-feed';
import Link from 'next/link';

export default function DashboardPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  // Default period (q10) = current calendar month, period_end = today.
  const today = useMemo(() => new Date(), []);
  const periodEnd = today.toISOString().slice(0, 10);
  const periodLabel = formatMonthLabel(today);

  const qbo = useQuery({
    queryKey: ['qbo-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getQuickbooksStatus(entityCode!),
  });
  const apSummary = useQuery({
    queryKey: ['hh-ap-summary', entityCode],
    enabled: !!entityCode,
    queryFn: () => getHHAPSummary(entityCode!),
  });
  const periodStatus = useQuery({
    queryKey: ['period-status', entityCode, periodEnd],
    enabled: !!entityCode,
    queryFn: () => getPeriodStatus(entityCode!, periodEnd),
  });
  const posLatest = useQuery({
    queryKey: ['pos-latest', entityCode],
    enabled: !!entityCode,
    queryFn: () => getLatestPosFinancial(entityCode!),
  });

  if (!entityCode) {
    return (
      <>
        <Topbar title="Dashboard" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">
              No entity selected. Pick one from the switcher to load your
              dashboard.
            </p>
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title="Dashboard" periodLabel={periodLabel} />
      <main className="p-6 space-y-6">
        <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          {/* Cash position */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                Cash position
              </CardTitle>
            </CardHeader>
            <CardContent>
              {qbo.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : qbo.data?.is_connected ? (
                <>
                  <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                    {/* TODO: backend endpoint not built — exact bank balance.
                         For now showing connection status only. */}
                    {qbo.data.company_name ?? 'QBO connected'}
                  </div>
                  <p className="text-xs text-slate mt-1">
                    Last synced{' '}
                    {qbo.data.last_synced_at
                      ? new Date(qbo.data.last_synced_at).toLocaleString()
                      : '—'}
                  </p>
                </>
              ) : (
                <>
                  <div className="text-2xl font-bold text-slate">
                    Not connected
                  </div>
                  <Link
                    href="/settings/store"
                    className="text-xs text-ledger-blue hover:underline"
                  >
                    Connect QuickBooks →
                  </Link>
                </>
              )}
            </CardContent>
          </Card>

          {/* Sales this month */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                Sales — last month closed
              </CardTitle>
            </CardHeader>
            <CardContent>
              {posLatest.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : posLatest.data?.snapshot_period_end ? (
                <>
                  <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                    {formatMoney(posLatest.data.total_sales)}
                  </div>
                  <p className="text-xs text-slate mt-1">
                    Period ending {posLatest.data.snapshot_period_end}
                  </p>
                </>
              ) : (
                <p className="text-sm text-slate">No POS data yet</p>
              )}
            </CardContent>
          </Card>

          {/* Gross margin */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                Gross margin (12 mo)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                {/* TODO: backend endpoint not built — gross margin time series */}
                {formatPercent(28.7)}
              </div>
              <GrossMarginSparkline />
            </CardContent>
          </Card>

          {/* Month-end status */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                Month-end status
              </CardTitle>
            </CardHeader>
            <CardContent>
              {periodStatus.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : (
                <>
                  <div className="flex items-center gap-2">
                    <Badge
                      variant={
                        periodStatus.data?.status === 'closed'
                          ? 'complete'
                          : periodStatus.data?.status === 'submitted_for_close'
                            ? 'pending'
                            : 'warning'
                      }
                    >
                      {periodStatus.data?.status ?? 'open'}
                    </Badge>
                  </div>
                  <Link
                    href="/month-end"
                    className="text-xs text-ledger-blue hover:underline mt-3 inline-block"
                  >
                    Open month-end workflow →
                  </Link>
                </>
              )}
            </CardContent>
          </Card>
        </section>

        <section className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Sales — this month vs last year</CardTitle>
            </CardHeader>
            <CardContent>
              <SalesChart />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>HH AP — aging</CardTitle>
            </CardHeader>
            <CardContent>
              {apSummary.isLoading ? (
                <Skeleton className="h-48" />
              ) : apSummary.data ? (
                <ApAgingChart
                  aging={apSummary.data.aging}
                  total={apSummary.data.current_balance}
                />
              ) : (
                <p className="text-sm text-slate">No HH AP data yet</p>
              )}
            </CardContent>
          </Card>
        </section>

        <section className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Alerts</CardTitle>
            </CardHeader>
            <CardContent>
              <AlertsFeed />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Quick actions</CardTitle>
            </CardHeader>
            <CardContent>
              <QuickActions />
            </CardContent>
          </Card>
        </section>
      </main>
    </>
  );
}
