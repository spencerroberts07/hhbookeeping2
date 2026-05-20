'use client';

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
import { getCurrentPeriod, getPeriodStatus } from '@/lib/api/month_end';
import { getLatestPosFinancial } from '@/lib/api/pos';
import { getLatestCashBalancing } from '@/lib/api/cash_balancing';
import { AssistantWidget } from '@/components/assistant/assistant-widget';
import { SalesChart } from './_components/sales-chart';
import { ApAgingChart } from './_components/ap-aging-chart';
import { GrossMarginSparkline } from './_components/gross-margin-sparkline';
import { QuickActions } from './_components/quick-actions';
import { AlertsFeed } from './_components/alerts-feed';
import { CalendarPlus } from 'lucide-react';
import type { AxiosError } from 'axios';
import Link from 'next/link';

export default function DashboardPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  // Find the period the dashboard should land on by querying the backend
  // for the most recent open-or-closed period. A 404 means no periods
  // exist yet — the dashboard renders an empty state with a link to
  // /month-end. This replaces the previous "default to today" behaviour
  // which 500'd on any entity without a same-day accounting_periods row.
  const currentPeriod = useQuery({
    queryKey: ['current-period', entityCode],
    enabled: !!entityCode,
    retry: false,
    queryFn: () => getCurrentPeriod(entityCode!),
  });

  const periodEnd = currentPeriod.data?.period_end ?? null;
  const periodLabel = periodEnd ? formatMonthLabel(periodEnd) : undefined;

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
    enabled: !!entityCode && !!periodEnd,
    queryFn: () => getPeriodStatus(entityCode!, periodEnd!),
  });
  const posLatest = useQuery({
    queryKey: ['pos-latest', entityCode],
    enabled: !!entityCode,
    queryFn: () => getLatestPosFinancial(entityCode!),
  });
  const cashLatest = useQuery({
    queryKey: ['cash-balancing-latest', entityCode],
    enabled: !!entityCode,
    retry: false,
    queryFn: () => getLatestCashBalancing(entityCode!),
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

  // No periods exist yet — first-run empty state. We render this BEFORE the
  // main dashboard so the user isn't shown half-loaded cards next to a
  // "start your first month-end" prompt.
  const noPeriods =
    !currentPeriod.isLoading &&
    !currentPeriod.data &&
    (currentPeriod.error as AxiosError | undefined)?.response?.status === 404;

  if (noPeriods) {
    return (
      <>
        <Topbar title="Dashboard" />
        <main className="p-6">
          <Card className="p-10 text-center max-w-xl mx-auto">
            <div className="grid h-12 w-12 place-items-center rounded-full bg-cloud text-ledger-blue mx-auto mb-4">
              <CalendarPlus className="h-6 w-6" strokeWidth={1.5} />
            </div>
            <h2 className="text-h2 text-deep-navy mb-2">
              No periods found
            </h2>
            <p className="text-slate mb-6">
              Get started by running your first month-end close. BookWize will
              walk you through document uploads, classification, journals,
              validation, and closing the period.
            </p>
            <Link href="/month-end">
              <Button variant="accent" size="lg">
                Start your first month-end →
              </Button>
            </Link>
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
              {cashLatest.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : cashLatest.data ? (
                <>
                  <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                    {formatMoney(cashLatest.data.closing_balance)}
                  </div>
                  <p className="text-xs text-slate mt-1">
                    Closing cash · {cashLatest.data.business_date}
                    {cashLatest.data.variance !== null && (
                      <>
                        {' · '}
                        <span
                          className={
                            cashLatest.data.status === 'balanced'
                              ? 'text-bw-teal'
                              : 'text-amber-700'
                          }
                        >
                          {cashLatest.data.status === 'balanced'
                            ? 'Balanced'
                            : `Variance ${formatMoney(cashLatest.data.variance, { signed: true })}`}
                        </span>
                      </>
                    )}
                  </p>
                </>
              ) : qbo.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : qbo.data?.is_connected ? (
                <>
                  <div className="text-2xl font-bold text-slate">
                    No cash data yet
                  </div>
                  <p className="text-xs text-slate mt-1">
                    QBO connected{' '}
                    {qbo.data.last_synced_at
                      ? `· last synced ${new Date(qbo.data.last_synced_at).toLocaleString()}`
                      : ''}
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
              {currentPeriod.isLoading || periodStatus.isLoading ? (
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
                    {periodLabel && (
                      <span className="text-xs text-slate">{periodLabel}</span>
                    )}
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
      <AssistantWidget />
    </>
  );
}
