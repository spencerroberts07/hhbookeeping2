'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import {
  getWagePlannerPlan,
  downloadFreshExcel,
  applyPeriodOverride,
  type WagePlannerPeriod,
  type WagePlannerSettings,
} from '@/lib/api/wage_planner';
import { DashboardSummary } from './_components/dashboard-summary';

const CURRENT_FY = new Date().getMonth() >= 9 ? new Date().getFullYear() + 1 : new Date().getFullYear();

function fmt(v: string | null | undefined, prefix = '', suffix = '') {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return `${prefix}${n.toLocaleString('en-CA', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${suffix}`;
}

function fmtHours(v: string | null | undefined) {
  if (v == null || v === '') return '—';
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return n.toLocaleString('en-CA', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

function OverUnderBadge({ v }: { v: string | null }) {
  if (!v) return <span className="text-muted-foreground">—</span>;
  const n = parseFloat(v);
  if (isNaN(n)) return <span>—</span>;
  const color = n > 0 ? 'text-red-600' : n < 0 ? 'text-emerald-600' : 'text-muted-foreground';
  return <span className={`font-medium ${color}`}>{n > 0 ? '+' : ''}{fmtHours(v)}</span>;
}

interface OverrideState {
  actual_sales?: string;
  actual_gross_wages?: string;
  actual_hours?: string;
}

function PeriodRow({
  period,
  entityCode,
  fiscalYear,
  onRefresh,
}: {
  period: WagePlannerPeriod;
  entityCode: string;
  fiscalYear: number;
  onRefresh: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [overrides, setOverrides] = useState<OverrideState>({});
  const [saving, setSaving] = useState(false);

  async function handleSaveOverride() {
    setSaving(true);
    try {
      await applyPeriodOverride({
        entity_code: entityCode,
        fiscal_year: fiscalYear,
        period_number: period.period_number,
        actual_sales: overrides.actual_sales ? parseFloat(overrides.actual_sales) : undefined,
        actual_gross_wages: overrides.actual_gross_wages ? parseFloat(overrides.actual_gross_wages) : undefined,
        actual_hours: overrides.actual_hours ? parseFloat(overrides.actual_hours) : undefined,
      });
      toast.success(`Period ${period.period_number} overrides applied`);
      setEditing(false);
      onRefresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Override failed');
    } finally {
      setSaving(false);
    }
  }

  const lockedBadge = period.locked
    ? <Badge variant="secondary" className="text-xs bg-slate-100 text-slate-600">Locked</Badge>
    : null;

  if (editing) {
    return (
      <tr className="bg-blue-50">
        <td className="px-2 py-1 text-center font-medium">{period.period_number}</td>
        <td className="px-2 py-1 text-xs text-muted-foreground">{period.period_start}</td>
        <td className="px-2 py-1 text-xs text-muted-foreground">{period.period_end}</td>
        <td colSpan={5} className="px-2 py-1">
          <div className="flex gap-2 items-center text-xs">
            <Input placeholder="Actual Sales $" value={overrides.actual_sales ?? ''} onChange={(e) => setOverrides((p) => ({ ...p, actual_sales: e.target.value }))} className="h-6 text-xs w-28" />
            <Input placeholder="Actual Wages $" value={overrides.actual_gross_wages ?? ''} onChange={(e) => setOverrides((p) => ({ ...p, actual_gross_wages: e.target.value }))} className="h-6 text-xs w-28" />
            <Input placeholder="Actual Hours" value={overrides.actual_hours ?? ''} onChange={(e) => setOverrides((p) => ({ ...p, actual_hours: e.target.value }))} className="h-6 text-xs w-24" />
            <Button size="sm" className="h-6 text-xs px-2" onClick={handleSaveOverride} disabled={saving}>{saving ? '…' : 'Save'}</Button>
            <Button size="sm" variant="ghost" className="h-6 text-xs px-2" onClick={() => setEditing(false)}>Cancel</Button>
          </div>
        </td>
        <td colSpan={4}></td>
      </tr>
    );
  }

  return (
    <tr className={`border-b border-border/40 hover:bg-muted/20 ${period.locked ? '' : 'opacity-90'}`}>
      <td className="px-2 py-1.5 text-center text-sm font-medium">{period.period_number} {lockedBadge}</td>
      <td className="px-2 py-1.5 text-xs text-muted-foreground">{period.period_start}</td>
      <td className="px-2 py-1.5 text-xs text-muted-foreground">{period.period_end}</td>
      <td className="px-2 py-1.5 text-right text-xs">{fmt(period.forecast_sales, '$')}</td>
      <td className="px-2 py-1.5 text-right text-xs font-medium">{fmtHours(period.target_hours)}</td>
      <td className="px-2 py-1.5 text-right text-xs">{fmt(period.actual_sales, '$')}</td>
      <td className="px-2 py-1.5 text-right text-xs">{fmtHours(period.actual_hours)}</td>
      <td className="px-2 py-1.5 text-right text-xs"><OverUnderBadge v={period.hours_over_under} /></td>
      <td className="px-2 py-1.5 text-right text-xs font-semibold text-bw-navy">
        {period.locked ? '—' : fmtHours(period.adjusted_target_hours)}
      </td>
      <td className="px-2 py-1.5 text-right text-xs">{fmt(period.actual_sales_per_hour, '$')}</td>
      <td className="px-2 py-1.5 text-right text-xs text-muted-foreground">{fmt(period.py_sales_per_hour, '$')}</td>
      <td className="px-2 py-1.5 text-center">
        {!period.locked && (
          <button onClick={() => setEditing(true)} className="text-xs text-blue-600 hover:underline">override</button>
        )}
      </td>
    </tr>
  );
}

export default function WagePlannerPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();
  const [fiscalYear, setFiscalYear] = useState(CURRENT_FY);
  const [downloading, setDownloading] = useState(false);

  const { data, isLoading } = useQuery({
    queryKey: ['wage-planner-plan', entityCode, fiscalYear],
    enabled: !!entityCode,
    queryFn: () => getWagePlannerPlan(entityCode!, fiscalYear),
  });

  function refresh() {
    qc.invalidateQueries({ queryKey: ['wage-planner-plan', entityCode, fiscalYear] });
  }

  async function handleDownload() {
    if (!entityCode) return;
    setDownloading(true);
    try {
      const result = await downloadFreshExcel(entityCode, fiscalYear);
      if (result.url) {
        window.open(result.url, '_blank');
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Download failed');
    } finally {
      setDownloading(false);
    }
  }

  const settings = data?.settings as WagePlannerSettings | null | undefined;
  const periods = data?.periods ?? [];
  const summary = data?.summary;

  if (!entityCode) {
    return <p className="text-sm text-muted-foreground p-4">Select an entity first.</p>;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Wage Cost Planner</h1>
          <p className="text-sm text-muted-foreground">
            Track payroll cost vs. target % of sales. Go-forward adjusted targets update as actuals arrive.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1">
            <span className="text-xs text-muted-foreground">FY</span>
            <Input
              type="number"
              value={fiscalYear}
              onChange={(e) => setFiscalYear(parseInt(e.target.value))}
              className="w-20 h-8 text-sm"
              min={2020}
              max={2099}
            />
          </div>
          <Button variant="outline" size="sm" onClick={handleDownload} disabled={downloading}>
            {downloading ? 'Generating…' : 'Download Excel'}
          </Button>
          <Button variant="ghost" size="sm" asChild>
            <a href="/payroll/planner/snapshots">Archive</a>
          </Button>
        </div>
      </div>

      {settings && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
          <div className="bg-muted/30 rounded p-3">
            <div className="text-muted-foreground mb-0.5">Target Wage %</div>
            <div className="font-semibold">{(parseFloat(settings.target_wage_pct) * 100).toFixed(2)}%</div>
          </div>
          <div className="bg-muted/30 rounded p-3">
            <div className="text-muted-foreground mb-0.5">Forecast Annual Sales</div>
            <div className="font-semibold">{fmt(summary?.forecast_annual_sales ?? null, '$')}</div>
          </div>
          <div className="bg-muted/30 rounded p-3">
            <div className="text-muted-foreground mb-0.5">Target Annual Wage $</div>
            <div className="font-semibold">{fmt(summary?.target_annual_wage_dollars ?? null, '$')}</div>
          </div>
          <div className="bg-muted/30 rounded p-3">
            <div className="text-muted-foreground mb-0.5">Cumulative Hours Over/(Under)</div>
            <div className="font-semibold">
              {summary?.cum_over_under != null ? (
                <OverUnderBadge v={summary.cum_over_under} />
              ) : '—'}
            </div>
          </div>
        </div>
      )}

      <DashboardSummary entityCode={entityCode} fiscalYear={fiscalYear} />

      {!settings && !isLoading && (
        <Card>
          <CardContent className="py-8 text-center space-y-2">
            <p className="text-muted-foreground">No settings configured for FY{fiscalYear}.</p>
            <p className="text-sm text-muted-foreground">
              Go to <a href="/settings/wage-planner" className="text-blue-600 hover:underline">Settings → Wage Planner</a> to enter your annual assumptions.
            </p>
          </CardContent>
        </Card>
      )}

      {periods.length === 0 && settings && !isLoading && (
        <Card>
          <CardContent className="py-8 text-center">
            <p className="text-muted-foreground">No pay-period calendar for FY{fiscalYear}.</p>
            <p className="text-sm text-muted-foreground mt-1">
              Use the Backfill button in <a href="/settings/wage-planner" className="text-blue-600 hover:underline">Settings → Wage Planner</a> to populate from existing payroll runs.
            </p>
          </CardContent>
        </Card>
      )}

      {isLoading && (
        <Card>
          <CardContent className="p-4 space-y-2">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-5/6" />
            <Skeleton className="h-4 w-4/5" />
          </CardContent>
        </Card>
      )}

      {periods.length > 0 && (
        <Card>
          <CardContent className="p-0 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-deep-navy text-white text-xs">
                  <th className="px-2 py-2 text-left">Period</th>
                  <th className="px-2 py-2 text-left">Start</th>
                  <th className="px-2 py-2 text-left">End</th>
                  <th className="px-2 py-2 text-right">Forecast Sales</th>
                  <th className="px-2 py-2 text-right">Target Hrs</th>
                  <th className="px-2 py-2 text-right">Actual Sales</th>
                  <th className="px-2 py-2 text-right">Actual Hrs</th>
                  <th className="px-2 py-2 text-right">Over/(Under)</th>
                  <th className="px-2 py-2 text-right">Adj. Target Hrs</th>
                  <th className="px-2 py-2 text-right">$/Hr (Act)</th>
                  <th className="px-2 py-2 text-right">$/Hr (LY)</th>
                  <th className="px-2 py-2"></th>
                </tr>
              </thead>
              <tbody>
                {periods.map((p) => (
                  <PeriodRow
                    key={p.period_number}
                    period={p}
                    entityCode={entityCode}
                    fiscalYear={fiscalYear}
                    onRefresh={refresh}
                  />
                ))}
              </tbody>
              {summary && (
                <tfoot>
                  <tr className="bg-muted/40 font-semibold text-xs border-t">
                    <td className="px-2 py-2" colSpan={3}>Total / Cumulative</td>
                    <td className="px-2 py-2 text-right">{fmt(summary.forecast_annual_sales ?? null, '$')}</td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td className="px-2 py-2 text-right">
                      {summary.cum_over_under != null ? <OverUnderBadge v={summary.cum_over_under} /> : '—'}
                    </td>
                    <td colSpan={4}></td>
                  </tr>
                </tfoot>
              )}
            </table>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
