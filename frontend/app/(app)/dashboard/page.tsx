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
import { formatMoney, formatPercent, formatMonthLabel, formatDate } from '@/lib/utils';
import { getGlCashBalance, getGrossMargin, getSalesMtd, getAsOf } from '@/lib/api/dashboard';
import { getQboBankBalances } from '@/lib/api/qbo';
import { getOnboardingStatus } from '@/lib/api/onboarding';
import { getHHAPSummary } from '@/lib/api/hh_ap';
import { getCurrentPeriod, getPeriodStatus } from '@/lib/api/month_end';
import { getLatestInventoryValue } from '@/lib/api/pos';
import { getRatios, type RatioRow } from '@/lib/api/ratios';
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
  // Cash card primarily reads QBO live (sum of every active bank
  // account's CurrentBalance) — QBO is the real-time view, not the
  // cash_balancing_days snapshot from last night. When QBO is
  // disconnected or errors, fall back to the GL balance on account
  // 1020 so the card still shows a number.
  const qboBalances = useQuery({
    queryKey: ['qbo-bank-balances', entityCode],
    enabled: !!entityCode,
    queryFn: () => getQboBankBalances(entityCode!),
    // Live-ish: stale after 60s, refetch on focus.
    staleTime: 60 * 1000,
  });
  const qboDisconnected = qboBalances.data && !qboBalances.data.connected;
  const glCash = useQuery({
    queryKey: ['gl-cash-balance', entityCode],
    enabled: !!entityCode && qboDisconnected,
    queryFn: () => getGlCashBalance(entityCode!),
  });
  const grossMargin = useQuery({
    queryKey: ['gross-margin', entityCode],
    enabled: !!entityCode,
    queryFn: () => getGrossMargin(entityCode!),
  });
  // Last closed_locked period — single source for AP / Margin / Ratios "as of" labels.
  const asOf = useQuery({
    queryKey: ['dashboard-as-of', entityCode],
    enabled: !!entityCode,
    queryFn: () => getAsOf(entityCode!),
    staleTime: 5 * 60 * 1000,
  });
  // Inventory latest snapshot (POS import).
  const inventoryValue = useQuery({
    queryKey: ['inventory-value-latest', entityCode],
    enabled: !!entityCode,
    queryFn: () => getLatestInventoryValue(entityCode!),
  });
  // Ratios — compact subset for the dashboard card.
  const ratiosData = useQuery({
    queryKey: ['ratios', entityCode],
    enabled: !!entityCode,
    queryFn: () => getRatios(entityCode!),
    // Ratios can 500 when GL data is thin — keep the card from crashing.
    retry: false,
  });
  // Sales MTD comes from cash balancing (POS gross) — the SAME source as the
  // sales drill-down's MTD, so the card and the drill always agree.
  const salesMtd = useQuery({
    queryKey: ['sales-mtd', entityCode],
    enabled: !!entityCode,
    queryFn: () => getSalesMtd(entityCode!),
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
        <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
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
                  {glCash.isLoading ? (
                    <Skeleton className="h-9 w-32" />
                  ) : glCash.data ? (
                    <>
                      <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                        {formatMoney(glCash.data.balance)}
                      </div>
                      <p className="text-[10px] text-slate mt-1">
                        Book balance (GL · acct 1020)
                      </p>
                    </>
                  ) : (
                    <div className="text-2xl font-bold text-slate">
                      Not connected
                    </div>
                  )}
                  <Link
                    href="/settings"
                    className="text-xs text-ledger-blue hover:underline mt-2 inline-block"
                  >
                    Connect QuickBooks for live balance →
                  </Link>
                </>
              )}
              <Link
                href="/dashboard/cash"
                className="text-xs text-ledger-blue hover:underline mt-3 inline-block"
              >
                Cash trend →
              </Link>
            </CardContent>
          </Card>

          {/* Sales — current period MTD. Pulled from journal_lines on
              4xxx accounts via the gross-margin endpoint (which already
              returns the per-period sales sum). Label switches to
              "(closed)" when the period is closed, "(open)" otherwise. */}
          <Link href="/dashboard/sales" className="block transition hover:ring-2 hover:ring-ledger-blue/30 rounded-xl">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                  {salesMtd.data ? `Sales — ${salesMtd.data.month_label} MTD` : 'Sales — month to date'}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {salesMtd.isLoading || !salesMtd.data ? (
                  <Skeleton className="h-9 w-32" />
                ) : (
                  <>
                    <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                      {formatMoney(salesMtd.data.mtd_sales)}
                    </div>
                    <p className="text-xs text-slate mt-1">
                      POS gross · vs {formatMoney(salesMtd.data.py_mtd_sales)} same period last year
                      {salesMtd.data.yoy_growth_pct !== null && (
                        <span className={salesMtd.data.yoy_growth_pct >= 0 ? ' text-green-700' : ' text-red-700'}>
                          {' '}({salesMtd.data.yoy_growth_pct >= 0 ? '+' : ''}{salesMtd.data.yoy_growth_pct.toFixed(1)}%)
                        </span>
                      )}
                    </p>
                  </>
                )}
              </CardContent>
            </Card>
          </Link>

          {/* Gross margin — rolling 12 months. ttm_margin_pct is computed
              server-side from posted/approved batches across the trailing
              12 periods ending at the current period_end. Falls back to
              the single-period margin if the TTM field is absent. */}
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
                  formatPercent(
                    grossMargin.data.ttm_margin_pct ??
                      grossMargin.data.margin_pct,
                  )
                ) : (
                  <span className="text-slate text-base">No data</span>
                )}
              </div>
              {(grossMargin.data?.period_label || asOf.data?.last_closed_period_label) && (
                <p className="text-[10px] text-slate mt-1">
                  Trailing 12 months · as of{' '}
                  {asOf.data?.last_closed_period_label ?? grossMargin.data?.period_label}
                </p>
              )}
              <GrossMarginSparkline />
              <Link
                href="/dashboard/margin"
                className="text-xs text-ledger-blue hover:underline mt-2 inline-block"
              >
                Margin trend →
              </Link>
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

          {/* Inventory — latest POS snapshot value */}
          <Link href="/dashboard/inventory" className="block transition hover:ring-2 hover:ring-ledger-blue/30 rounded-xl">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                  Inventory
                </CardTitle>
              </CardHeader>
              <CardContent>
                {inventoryValue.isLoading ? (
                  <Skeleton className="h-9 w-32" />
                ) : inventoryValue.data?.inventory_value != null ? (
                  <>
                    <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
                      {formatMoney(inventoryValue.data.inventory_value)}
                    </div>
                    <p className="text-[10px] text-slate mt-1">
                      {inventoryValue.data.snapshot_date
                        ? `POS snapshot · as of ${formatDate(inventoryValue.data.snapshot_date)}`
                        : 'POS snapshot'}
                    </p>
                  </>
                ) : (
                  <p className="text-sm text-slate">No snapshot yet</p>
                )}
              </CardContent>
            </Card>
          </Link>

          {/* Key ratios — headline DSCR / FCCR from the ratio engine */}
          <Link href="/settings/ratios" className="block transition hover:ring-2 hover:ring-ledger-blue/30 rounded-xl">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm font-semibold uppercase tracking-wider text-slate">
                  Key ratios
                </CardTitle>
              </CardHeader>
              <CardContent>
                {ratiosData.isLoading ? (
                  <Skeleton className="h-9 w-32" />
                ) : ratiosData.data ? (
                  <>
                    <div className="space-y-1">
                      {ratiosData.data.ratios
                        .filter((r: RatioRow) => r.enabled && r.value !== null)
                        .slice(0, 3)
                        .map((r: RatioRow) => (
                          <div key={r.key} className="flex justify-between items-baseline text-sm">
                            <span className="text-slate truncate pr-2">{r.label}</span>
                            <span className={
                              'font-semibold tabular-nums ' +
                              (r.breached ? 'text-red-600' : 'text-ink')
                            }>
                              {r.format === 'percent'
                                ? `${(r.value! * 100).toFixed(1)}%`
                                : r.format === 'dollar'
                                  ? formatMoney(r.value!)
                                  : r.format === 'days'
                                    ? `${r.value!.toFixed(0)}d`
                                    : r.value!.toFixed(2) + 'x'}
                            </span>
                          </div>
                        ))}
                    </div>
                    <p className="text-[10px] text-slate mt-2">
                      as of {ratiosData.data.period_label}
                    </p>
                  </>
                ) : (
                  <p className="text-sm text-slate">No ratio data yet</p>
                )}
              </CardContent>
            </Card>
          </Link>
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
                <>
                  <ApAgingChart
                    aging={apSummary.data.aging}
                    total={apSummary.data.current_balance}
                  />
                  {asOf.data?.last_closed_period_label && (
                    <p className="text-[10px] text-slate mt-2">
                      as of {asOf.data.last_closed_period_label}
                    </p>
                  )}
                </>
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
    </>
  );
}
