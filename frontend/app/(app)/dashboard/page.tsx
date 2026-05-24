'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatPercent, formatMonthLabel } from '@/lib/utils';
import { getQuickbooksStatus, getGrossMargin } from '@/lib/api/dashboard';
import { getQboBankBalances } from '@/lib/api/qbo';
import { getOnboardingStatus } from '@/lib/api/onboarding';
import { getHHAPSummary } from '@/lib/api/hh_ap';
import { getCurrentPeriod, getPeriodStatus } from '@/lib/api/month_end';
import { AssistantWidget } from '@/components/assistant/assistant-widget';
import { SalesChart } from './_components/sales-chart';
import { ApAgingChart } from './_components/ap-aging-chart';
import { GrossMarginSparkline } from './_components/gross-margin-sparkline';
import { QuickActions } from './_components/quick-actions';
import { AlertsFeed } from './_components/alerts-feed';
import { InsightsCard } from './_components/insights-card';
import { CalendarPlus, Sparkles } from 'lucide-react';
import type { AxiosError } from 'axios';
import Link from 'next/link';

export default function DashboardPage() {
  const router = useRouter();
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  // Onboarding gate. Three states:
  //   1. onboarding_complete = true            → render dashboard normally
  //   2. !complete && entity has zero data     → redirect to /onboarding
  //   3. !complete && entity has some data     → render dashboard + banner
  // We don't redirect dealers with partial data (Bridlewood-style) so
  // they can keep using the app while finishing setup.
  const onboarding = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getOnboardingStatus(entityCode!),
  });
  useEffect(() => {
    if (!onboarding.data) return;
    if (
      !onboarding.data.onboarding_complete &&
      !onboarding.data.has_chart_of_accounts &&
      onboarding.data.journal_line_count === 0
    ) {
      router.replace('/onboarding');
    }
  }, [onboarding.data, router]);

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
  // Cash card now reads from QBO live (sum of every active bank
  // account's CurrentBalance). The cash_balancing_days snapshot from
  // last night isn't a real-time view — QBO is.
  const qboBalances = useQuery({
    queryKey: ['qbo-bank-balances', entityCode],
    enabled: !!entityCode,
    queryFn: () => getQboBankBalances(entityCode!),
    // Live-ish: stale after 60s, refetch on focus.
    staleTime: 60 * 1000,
  });
  const grossMargin = useQuery({
    queryKey: ['gross-margin', entityCode],
    enabled: !!entityCode,
    queryFn: () => getGrossMargin(entityCode!),
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

  const showOnboardingBanner =
    onboarding.data &&
    !onboarding.data.onboarding_complete &&
    (onboarding.data.has_chart_of_accounts || onboarding.data.journal_line_count > 0);

  return (
    <>
      <Topbar title="Dashboard" periodLabel={periodLabel} />
      <main className="p-6 space-y-6">
        {showOnboardingBanner && (
          <div className="rounded-xl border-2 border-ledger-blue/30 bg-ledger-blue/5 p-4 flex items-center gap-3">
            <Sparkles className="h-5 w-5 text-ledger-blue shrink-0" />
            <div className="flex-1">
              <div className="font-semibold text-deep-navy">
                Complete your setup to unlock all features
              </div>
              <div className="text-xs text-slate">
                Finish your onboarding checklist — opening balances, GL history,
                and HH AP statements give the AI assistant the context it needs.
              </div>
            </div>
            <Link href="/onboarding">
              <Button variant="accent" size="sm">
                Continue setup →
              </Button>
            </Link>
          </div>
        )}
        <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          {/* Cash position */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                Cash position
              </CardTitle>
            </CardHeader>
            <CardContent>
              {qboBalances.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : qboBalances.data?.connected ? (
                <>
                  <div className="space-y-1 mb-2">
                    {qboBalances.data.accounts.map((a) => {
                      // Credit-line subtypes carry a credit-natural
                      // balance; QBO sometimes returns the outstanding
                      // draw as a positive number, so we colour by
                      // subtype rather than by sign.
                      const isCredit = /credit|loan/i.test(a.account_subtype);
                      const isNegative = a.current_balance < 0;
                      const color =
                        isCredit || isNegative ? 'text-amber-700' : 'text-ink';
                      return (
                        <div
                          key={`${a.account_code}-${a.account_name}`}
                          className="flex justify-between text-sm tabular-nums"
                        >
                          <span className="text-slate truncate pr-2">
                            {a.account_name}
                          </span>
                          <span className={`font-semibold ${color}`}>
                            {formatMoney(a.current_balance, { signed: isCredit || isNegative })}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                  <div className="border-t border-border my-2" />
                  <div className="flex justify-between items-baseline">
                    <span className="text-xs uppercase tracking-wider text-slate">
                      Net position
                    </span>
                    <span
                      className={
                        'text-2xl font-extrabold tabular-nums ' +
                        (qboBalances.data.total_balance < 0
                          ? 'text-red-600'
                          : 'text-bw-teal')
                      }
                    >
                      {formatMoney(qboBalances.data.total_balance)}
                    </span>
                  </div>
                  <p className="text-[10px] text-slate mt-1">
                    Live from QuickBooks
                    {qboBalances.data.fetched_at &&
                      ` · ${new Date(qboBalances.data.fetched_at).toLocaleTimeString()}`}
                  </p>
                </>
              ) : (
                <>
                  <div className="text-2xl font-bold text-slate">
                    Not connected
                  </div>
                  <Link
                    href="/settings"
                    className="text-xs text-ledger-blue hover:underline"
                  >
                    Connect QuickBooks to see live balance →
                  </Link>
                </>
              )}
            </CardContent>
          </Card>

          {/* Sales — current period MTD. Pulled from journal_lines on
              4xxx accounts via the gross-margin endpoint (which already
              returns the per-period sales sum). Label switches to
              "(closed)" when the period is closed, "(open)" otherwise. */}
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                {grossMargin.data?.period_label
                  ? `Sales — ${grossMargin.data.period_label}`
                  : 'Sales — current period'}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {grossMargin.isLoading ? (
                <Skeleton className="h-9 w-32" />
              ) : grossMargin.data && grossMargin.data.period_end ? (
                <>
                  <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                    {formatMoney(grossMargin.data.sales)}
                  </div>
                  <p className="text-xs text-slate mt-1">
                    From journal_lines (4xxx) ·{' '}
                    {periodStatus.data?.status === 'closed'
                      ? 'closed'
                      : 'open'}
                  </p>
                </>
              ) : (
                <p className="text-sm text-slate">No journal data yet</p>
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
                {grossMargin.isLoading ? (
                  <Skeleton className="h-9 w-24" />
                ) : grossMargin.data && grossMargin.data.period_end ? (
                  formatPercent(grossMargin.data.margin_pct)
                ) : (
                  <span className="text-slate text-base">No data</span>
                )}
              </div>
              {grossMargin.data?.period_label && (
                <p className="text-xs text-slate mt-1">
                  {grossMargin.data.period_label}
                </p>
              )}
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

        <section>
          <InsightsCard />
        </section>
      </main>
      <AssistantWidget />
    </>
  );
}
