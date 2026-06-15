'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { X, AlertTriangle, Download } from 'lucide-react';
import {
  Bar,
  ComposedChart,
  Line,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
  Legend,
} from 'recharts';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { formatMoney } from '@/lib/utils';
import {
  getWagePlannerDashboardSummary,
  getLatestSnapshot,
  type WagePlannerDashboardSummary,
  type DashboardSummaryTrendPoint,
} from '@/lib/api/wage_planner';

// ---------------------------------------------------------------------------
// Chart constants (match dashboard/analytics-ui)
// ---------------------------------------------------------------------------
const THIS_YR = '#1454C8';
const LAST_YR = '#0B2E72';
const ACCENT  = '#13B8B4';
const AXIS    = '#64748B';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function pct(v: string | null | undefined, digits = 2): string {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (!Number.isFinite(n)) return '—';
  return `${(n * 100).toFixed(digits)}%`;
}

function money(v: string | null | undefined): string {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (!Number.isFinite(n)) return '—';
  return formatMoney(n);
}

function variance(v: string | null | undefined, positiveIsGood = true): string {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (!Number.isFinite(n)) return '—';
  const prefix = n >= 0 ? '+' : '';
  return `${prefix}${formatMoney(n)}`;
}

function varColor(v: string | null | undefined, positiveIsGood = true): string {
  if (v == null) return 'text-muted-foreground';
  const n = parseFloat(v);
  if (!Number.isFinite(n) || n === 0) return 'text-muted-foreground';
  const good = positiveIsGood ? n > 0 : n < 0;
  return good ? 'text-emerald-600' : 'text-red-600';
}

function hoursNum(v: string | null | undefined): string {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (!Number.isFinite(n)) return '—';
  return n.toLocaleString('en-CA', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

const BASIS_LABELS: Record<string, string> = {
  gl_6120: 'Wages & Benefits (incl. employer CPP/EI & vacation accrual)',
  runline_gross: 'Gross wages, non-management (no employer burden)',
  none: 'No payroll data for this period',
};

const BASIS_TOOLTIP: Record<string, string> = {
  gl_6120: 'GL 6120',
  runline_gross: 'Gross-basis proxy',
  none: 'No data',
};

// ---------------------------------------------------------------------------
// Health badge
// ---------------------------------------------------------------------------
function HealthBadge({ health }: { health: 'green' | 'yellow' | 'red' }) {
  const map = {
    green:  { cls: 'bg-emerald-100 text-emerald-700', label: 'On track' },
    yellow: { cls: 'bg-yellow-100 text-yellow-700',   label: 'Near target' },
    red:    { cls: 'bg-red-100 text-red-700',          label: 'Over target' },
  };
  const { cls, label } = map[health];
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${cls}`}>
      {label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Custom recharts tooltip
// ---------------------------------------------------------------------------
interface TooltipPayloadItem {
  name: string;
  value: number | null | undefined;
  payload: ChartRow;
}

interface TooltipProps {
  active?: boolean;
  payload?: TooltipPayloadItem[];
  label?: string;
}

interface ChartRow {
  label: string;
  key: string;
  fy: number;
  actual: number | null;
  target: number | null;
  prior: number | null;
  wages: number;
  sales: number;
  basis: string;
}

function WageTrendTooltip({ active, payload }: TooltipProps) {
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload as ChartRow;
  if (!row) return null;
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3 text-xs shadow-lg min-w-[200px]">
      <p className="font-semibold text-deep-navy mb-2">{row.label} (FY{row.fy})</p>
      <div className="space-y-1">
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Wage %</span>
          <span className="font-medium">{row.actual != null ? `${row.actual.toFixed(2)}%` : '—'}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Target %</span>
          <span>{row.target != null ? `${row.target.toFixed(2)}%` : '—'}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Prior year %</span>
          <span>{row.prior != null ? `${row.prior.toFixed(2)}%` : '—'}</span>
        </div>
        <hr className="my-1 border-slate-100" />
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Wages</span>
          <span>{formatMoney(row.wages)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Sales</span>
          <span>{formatMoney(row.sales)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Basis</span>
          <span className="text-slate-500">{BASIS_TOOLTIP[row.basis] ?? row.basis}</span>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Card 1 — Headline KPI
// ---------------------------------------------------------------------------
function Card1Headline({ d }: { d: WagePlannerDashboardSummary }) {
  const c1 = d.card1_headline;
  const ytd = d.ytd;
  const lowData = ytd.periods_completed < 6;

  return (
    <Card className="md:col-span-2">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground flex items-center justify-between">
          Wage % of Sales — YTD
          <HealthBadge health={c1.health} />
        </CardTitle>
      </CardHeader>
      <CardContent>
        {lowData && (
          <div className="mb-3 flex items-start gap-2 rounded-md bg-amber-50 border border-amber-200 px-3 py-2 text-xs text-amber-800">
            <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
            YTD wages may be understated — not all payroll periods have been imported yet.
          </div>
        )}
        <div className="flex flex-wrap items-end gap-8">
          <div>
            <div className="text-4xl font-extrabold text-deep-navy tabular-nums">
              {pct(c1.ytd_managed_wage_pct)}
            </div>
            <div className="mt-1 text-xs text-muted-foreground">
              vs target <span className="font-medium text-foreground">{pct(c1.target_wage_pct)}</span>
            </div>
            <div className="mt-1 text-[11px] text-muted-foreground">{BASIS_LABELS[c1.wage_basis]}</div>
          </div>
          {c1.prior_year_basis !== 'none' && (
            <div>
              <div className="text-2xl font-semibold text-slate-500 tabular-nums">
                {pct(c1.prior_year_same_period_pct)}
              </div>
              <div className="mt-1 text-xs text-muted-foreground">prior year same period</div>
              <div className="text-[11px] text-muted-foreground">{BASIS_LABELS[c1.prior_year_basis]}</div>
            </div>
          )}
          <div className="ml-auto text-right text-xs text-muted-foreground">
            <div>{ytd.start} – {ytd.end}</div>
            <div>{ytd.periods_completed} periods completed · {ytd.periods_remaining} remaining</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Card 2 — Forward-looking target
// ---------------------------------------------------------------------------
function Card2ForwardTarget({ d }: { d: WagePlannerDashboardSummary }) {
  const c2 = d.card2_forward_target;
  const cumColor = c2.color === 'emerald' ? 'text-emerald-600' : c2.color === 'red' ? 'text-red-600' : 'text-muted-foreground';
  const hasTarget = c2.adjusted_target_hours != null;

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">Go-Forward Target</CardTitle>
      </CardHeader>
      <CardContent>
        {hasTarget ? (
          <>
            <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
              {hoursNum(c2.adjusted_target_hours)} <span className="text-base font-normal text-muted-foreground">hrs</span>
            </div>
            <div className="mt-1 text-xs text-muted-foreground">
              per remaining period to hit annual target
            </div>
            {c2.next_unlocked_period_number && (
              <div className="mt-1 text-xs text-muted-foreground">
                Next: Period {c2.next_unlocked_period_number}
              </div>
            )}
            {c2.cum_over_under != null && (
              <div className="mt-3 text-xs">
                Cumulative over/(under):{' '}
                <span className={`font-semibold ${cumColor}`}>
                  {parseFloat(c2.cum_over_under) > 0 ? '+' : ''}{hoursNum(c2.cum_over_under)} hrs
                </span>
              </div>
            )}
          </>
        ) : (
          <p className="text-xs text-muted-foreground">All periods locked.</p>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Card 3 — YTD Actuals
// ---------------------------------------------------------------------------
function Card3YtdActuals({ d }: { d: WagePlannerDashboardSummary }) {
  const c3 = d.card3_ytd_actuals;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">YTD Actuals</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-xs">
        <div>
          <div className="text-muted-foreground mb-1 font-medium">Wages</div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Actual</span>
            <span className="font-semibold">{money(c3.actual_wages_ytd)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Target</span>
            <span>{money(c3.target_wages_ytd)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Variance</span>
            <span className={varColor(c3.wages_variance, false)}>
              {variance(c3.wages_variance)}
            </span>
          </div>
        </div>
        <hr className="border-border/40" />
        <div>
          <div className="text-muted-foreground mb-1 font-medium">Sales</div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Actual</span>
            <span className="font-semibold">{money(c3.actual_sales_ytd)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Forecast</span>
            <span>{money(c3.forecast_sales_ytd)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Variance</span>
            <span className={varColor(c3.sales_variance, true)}>
              {variance(c3.sales_variance)}
            </span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Card 4 — Salaried Cost
// ---------------------------------------------------------------------------
function Card4Salaried({ d }: { d: WagePlannerDashboardSummary }) {
  const c4 = d.card4_salaried;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          Salaried Cost
          <span className="ml-2 text-[10px] font-normal text-muted-foreground">Fixed — independent of scheduling</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-xs">
        <div className="flex gap-6">
          <div>
            <div className="text-muted-foreground">Per period</div>
            <div className="text-lg font-bold text-deep-navy tabular-nums">{money(c4.per_period)}</div>
          </div>
          <div>
            <div className="text-muted-foreground">Annual</div>
            <div className="text-lg font-bold text-deep-navy tabular-nums">{money(c4.annual)}</div>
          </div>
          {c4.pct_of_annual_target && (
            <div>
              <div className="text-muted-foreground">% of wage target</div>
              <div className="text-lg font-bold text-deep-navy tabular-nums">
                {(parseFloat(c4.pct_of_annual_target) * 100).toFixed(1)}%
              </div>
            </div>
          )}
        </div>
        {c4.staff.length > 0 && (
          <div className="space-y-1">
            {c4.staff.map((s) => (
              <div key={s.employee_name} className="flex justify-between text-[11px]">
                <span className="text-muted-foreground">{s.employee_name}</span>
                <span className="font-medium">{money(s.annual_cost)}/yr</span>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Card 5 — Minimum wage alert (dismissible)
// ---------------------------------------------------------------------------
function Card5MinWage({
  d,
  entityCode,
}: {
  d: WagePlannerDashboardSummary;
  entityCode: string;
}) {
  const storageKey = `wp:minwage:dismissed:${entityCode}`;
  const [dismissed, setDismissed] = useState(() => {
    try { return !!localStorage.getItem(storageKey); } catch { return false; }
  });

  const c5 = d.card5_min_wage;
  const employees = c5.near_min_employees;
  if (employees.length === 0 || dismissed) return null;

  const minWage = parseFloat(c5.ontario_min_wage);
  const belowMin = employees.filter((e) => parseFloat(e.gap_to_min) > 0);
  const nearMin  = employees.filter((e) => parseFloat(e.gap_to_min) <= 0);

  function dismiss() {
    try { localStorage.setItem(storageKey, '1'); } catch { /* ignore */ }
    setDismissed(true);
  }

  return (
    <Card className="border-yellow-300 bg-yellow-50 col-span-full">
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center justify-between">
          <span className="flex items-center gap-2 text-sm font-medium text-yellow-800">
            <AlertTriangle className="h-4 w-4 text-yellow-600" />
            Ontario Minimum Wage Alert — ${c5.ontario_min_wage}/hr
          </span>
          <button onClick={dismiss} className="text-yellow-600 hover:text-yellow-800">
            <X className="h-4 w-4" />
          </button>
        </CardTitle>
      </CardHeader>
      <CardContent className="text-xs space-y-3">
        {belowMin.length > 0 && (
          <div>
            <p className="font-semibold text-red-700 mb-1">
              {belowMin.length} employee{belowMin.length !== 1 ? 's' : ''} below minimum wage
            </p>
            <div className="space-y-1">
              {belowMin.map((e) => (
                <div key={e.employee_id} className="flex justify-between">
                  <span className="text-red-700">{e.full_name}</span>
                  <span className="text-red-700">
                    ${e.current_rate}/hr — ${e.gap_to_min} below minimum · est. {money(e.est_annual_raise_cost)}/yr raise
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
        {nearMin.length > 0 && (
          <div>
            <p className="font-semibold text-yellow-800 mb-1">
              {nearMin.length} employee{nearMin.length !== 1 ? 's' : ''} within ${c5.alert_band} of minimum wage
            </p>
            <div className="space-y-1">
              {nearMin.map((e) => (
                <div key={e.employee_id} className="flex justify-between">
                  <span className="text-yellow-800">{e.full_name}</span>
                  <span className="text-yellow-700">${e.current_rate}/hr</span>
                </div>
              ))}
            </div>
          </div>
        )}
        {parseFloat(c5.total_delta_annual_est) > 0 && (
          <p className="text-muted-foreground">
            Estimated annual cost to bring all below-minimum to ${c5.ontario_min_wage}:{' '}
            <span className="font-medium text-foreground">{money(c5.total_delta_annual_est)}</span>
          </p>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Trend chart
// ---------------------------------------------------------------------------
function buildChartData(trend: DashboardSummaryTrendPoint[]): ChartRow[] {
  return trend.map((t) => {
    const actualRaw  = parseFloat(t.actual_wage_pct);
    const priorRaw   = parseFloat(t.prior_year_pct);
    const targetRaw  = t.target_pct != null ? parseFloat(t.target_pct) : null;
    return {
      label:  t.label,
      key:    `FY${String(t.fy).slice(-2)}-P${String(t.period_number).padStart(2, '0')}`,
      fy:     t.fy,
      actual: Number.isFinite(actualRaw) && actualRaw > 0 ? actualRaw * 100 : null,
      target: targetRaw != null && Number.isFinite(targetRaw) ? targetRaw * 100 : null,
      prior:  Number.isFinite(priorRaw) && priorRaw > 0 ? priorRaw * 100 : null,
      wages:  parseFloat(t.actual_wages) || 0,
      sales:  parseFloat(t.actual_sales) || 0,
      basis:  t.basis,
    };
  });
}

function TrendChart({ trend }: { trend: DashboardSummaryTrendPoint[] }) {
  const data = buildChartData(trend);
  const hasPrior = data.some((r) => r.prior != null);

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          Wage % of Sales — Multi-Period Trend
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={280}>
          <ComposedChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
            <XAxis
              dataKey="key"
              stroke={AXIS}
              fontSize={10}
              angle={-45}
              textAnchor="end"
              interval={2}
              tick={{ fill: AXIS }}
            />
            <YAxis
              stroke={AXIS}
              fontSize={11}
              tickFormatter={(v: number) => `${v.toFixed(0)}%`}
              domain={[0, 'auto']}
            />
            <RechartsTooltip content={<WageTrendTooltip />} />
            <Legend wrapperStyle={{ fontSize: 11, paddingTop: 8 }} />
            <Bar
              dataKey="actual"
              name="Wage %"
              fill={THIS_YR}
              radius={[4, 4, 0, 0]}
              maxBarSize={20}
            />
            {hasPrior && (
              <Line
                type="monotone"
                dataKey="prior"
                name="Prior year %"
                stroke={LAST_YR}
                strokeWidth={1.5}
                strokeDasharray="4 4"
                dot={false}
                connectNulls={false}
              />
            )}
            <Line
              type="monotone"
              dataKey="target"
              name="Target %"
              stroke={ACCENT}
              strokeWidth={1.5}
              strokeDasharray="2 4"
              dot={false}
              connectNulls={true}
            />
          </ComposedChart>
        </ResponsiveContainer>
        <p className="mt-1 text-[11px] text-muted-foreground">
          Bars: actual wage % per period (GL 6120 when available, gross-basis proxy otherwise).
          Periods with no payroll data show no bar.
        </p>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Snapshot row
// ---------------------------------------------------------------------------
function SnapshotRow({ entityCode, fiscalYear }: { entityCode: string; fiscalYear?: number }) {
  const [downloading, setDownloading] = useState(false);

  const { data: snap } = useQuery({
    queryKey: ['wage-planner-snapshot-latest', entityCode, fiscalYear],
    enabled: !!entityCode,
    queryFn: () => getLatestSnapshot(entityCode, fiscalYear),
  });

  async function handleDownload() {
    if (!snap?.id) return;
    setDownloading(true);
    try {
      const { getSnapshotDownloadUrl } = await import('@/lib/api/wage_planner');
      const res = await getSnapshotDownloadUrl(snap.id, entityCode);
      const url = res.download_url ?? res.fallback;
      if (url) window.open(url, '_blank');
    } catch {
      /* silent — user can use the main Download Excel button */
    } finally {
      setDownloading(false);
    }
  }

  if (!snap) return null;

  return (
    <div className="flex items-center justify-between rounded-lg border border-border/50 bg-muted/20 px-4 py-2.5 text-xs">
      <div className="text-muted-foreground">
        {snap.status === 'ready'
          ? `Latest snapshot: Period ${snap.pay_period_number}${snap.generated_at ? ` · ${new Date(snap.generated_at).toLocaleDateString('en-CA')}` : ''}`
          : `Snapshot ${snap.status}`}
      </div>
      <div className="flex items-center gap-3">
        {snap.status === 'ready' && (
          <Button size="sm" variant="outline" className="h-7 text-xs gap-1" onClick={handleDownload} disabled={downloading}>
            <Download className="h-3 w-3" />
            {downloading ? 'Opening…' : 'Download'}
          </Button>
        )}
        <a href="/payroll/planner/snapshots" className="text-ledger-blue hover:underline">
          View archive
        </a>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Skeleton loading state
// ---------------------------------------------------------------------------
function DashboardSummarySkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i} className={i === 0 ? 'md:col-span-2' : ''}>
            <CardContent className="p-4 space-y-2">
              <Skeleton className="h-3 w-24" />
              <Skeleton className="h-8 w-36" />
              <Skeleton className="h-3 w-48" />
            </CardContent>
          </Card>
        ))}
      </div>
      <Card>
        <CardContent className="p-4">
          <Skeleton className="h-[280px] w-full" />
        </CardContent>
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------
export function DashboardSummary({
  entityCode,
  fiscalYear,
}: {
  entityCode: string;
  fiscalYear?: number;
}) {
  const { data, isLoading } = useQuery({
    queryKey: ['wage-planner-summary', entityCode, fiscalYear],
    enabled: !!entityCode,
    queryFn: () => getWagePlannerDashboardSummary(entityCode, fiscalYear),
  });

  if (isLoading) return <DashboardSummarySkeleton />;
  if (!data || !data.settings_present) return null;

  return (
    <div className="space-y-3">
      {/* 5 KPI cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        <Card1Headline d={data} />
        <Card2ForwardTarget d={data} />
        <Card3YtdActuals d={data} />
        <Card4Salaried d={data} />
        <Card5MinWage d={data} entityCode={entityCode} />
      </div>

      {/* Trend chart */}
      {data.trend.length > 0 && <TrendChart trend={data.trend} />}

      {/* Snapshot row */}
      <SnapshotRow entityCode={entityCode} fiscalYear={fiscalYear} />
    </div>
  );
}
