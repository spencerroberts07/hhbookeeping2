'use client';

import { useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  getWagePlannerSettings,
  saveWagePlannerSettings,
  backfillPayPeriods,
  type SalariedStaffItem,
} from '@/lib/api/wage_planner';

const CURRENT_FY = new Date().getMonth() >= 9 ? new Date().getFullYear() + 1 : new Date().getFullYear();

function pct(v: string | number) {
  return (parseFloat(String(v)) * 100).toFixed(2);
}

export default function WagePlannerSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const [fiscalYear, setFiscalYear] = useState(CURRENT_FY);
  const [saving, setSaving] = useState(false);
  const [backfilling, setBackfilling] = useState(false);

  // Form state
  const [targetWagePct, setTargetWagePct] = useState('11.00');
  const [forecastChange, setForecastChange] = useState('0.00');
  const [avgHourlyWage, setAvgHourlyWage] = useState('17.66');
  const [benefitsPct, setBenefitsPct] = useState('4.00');
  const [distributionBasis, setDistributionBasis] = useState<'prior_year' | 'national_average'>('prior_year');
  const [notes, setNotes] = useState('');
  const [salariedStaff, setSalariedStaff] = useState<SalariedStaffItem[]>([]);
  const [dirty, setDirty] = useState(false);

  const { isLoading, data: settingsData } = useQuery({
    queryKey: ['wage-planner-settings', entityCode, fiscalYear],
    enabled: !!entityCode,
    queryFn: () => getWagePlannerSettings(entityCode!, fiscalYear),
  });

  useEffect(() => {
    const s = settingsData?.settings;
    if (s) {
      setTargetWagePct(pct(s.target_wage_pct));
      setForecastChange(pct(s.forecast_sales_change));
      setAvgHourlyWage(parseFloat(s.avg_hourly_wage).toFixed(2));
      setBenefitsPct(pct(s.benefits_pct));
      setDistributionBasis(s.distribution_basis);
      setNotes(s.notes ?? '');
      setSalariedStaff(s.salaried_staff.map((emp: SalariedStaffItem) => ({
        employee_name: emp.employee_name,
        annual_salary: parseFloat(String(emp.annual_salary)),
        bonus: parseFloat(String(emp.bonus)),
        assumed_hours_per_period: emp.assumed_hours_per_period,
      })));
      setDirty(false);
    }
  }, [settingsData]);

  function mark() { setDirty(true); }

  async function handleSave() {
    if (!entityCode) return;
    setSaving(true);
    try {
      await saveWagePlannerSettings({
        entity_code: entityCode,
        fiscal_year: fiscalYear,
        target_wage_pct: parseFloat(targetWagePct) / 100,
        forecast_sales_change: parseFloat(forecastChange) / 100,
        avg_hourly_wage: parseFloat(avgHourlyWage),
        benefits_pct: parseFloat(benefitsPct) / 100,
        distribution_basis: distributionBasis,
        notes: notes || null,
        salaried_staff: salariedStaff,
      });
      qc.invalidateQueries({ queryKey: ['wage-planner-settings', entityCode] });
      qc.invalidateQueries({ queryKey: ['wage-planner-plan', entityCode] });
      toast.success('Settings saved');
      setDirty(false);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Save failed';
      toast.error(msg);
    } finally {
      setSaving(false);
    }
  }

  async function handleBackfill() {
    if (!entityCode) return;
    setBackfilling(true);
    try {
      const result = await backfillPayPeriods(entityCode);
      qc.invalidateQueries({ queryKey: ['wage-planner-plan', entityCode] });
      toast.success(result.message);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Backfill failed';
      toast.error(msg);
    } finally {
      setBackfilling(false);
    }
  }

  function addSalaried() {
    setSalariedStaff((prev) => [
      ...prev,
      { employee_name: '', annual_salary: 0, bonus: 0, assumed_hours_per_period: 80 },
    ]);
    mark();
  }

  function removeSalaried(idx: number) {
    setSalariedStaff((prev) => prev.filter((_, i) => i !== idx));
    mark();
  }

  function updateSalaried(idx: number, field: keyof SalariedStaffItem, value: string | number) {
    setSalariedStaff((prev) => prev.map((e, i) => i === idx ? { ...e, [field]: value } : e));
    mark();
  }

  if (!entityCode) return <p className="text-sm text-muted-foreground p-4">Select an entity first.</p>;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">Wage Cost Planner — Settings</h2>
          <p className="text-sm text-muted-foreground">
            Annual assumptions for the wage cost tracker. Changes affect forecast calculations for the selected fiscal year.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Label className="text-xs text-muted-foreground">Fiscal year:</Label>
          <Input
            type="number"
            value={fiscalYear}
            onChange={(e) => setFiscalYear(parseInt(e.target.value))}
            className="w-24 h-8 text-sm"
            min={2020}
            max={2099}
          />
        </div>
      </div>

      {isLoading ? (
        <Card><CardContent className="p-4 space-y-3"><Skeleton className="h-4 w-1/2" /><Skeleton className="h-4 w-3/4" /></CardContent></Card>
      ) : (
        <>
          <Card>
            <CardHeader><CardTitle className="text-base">Annual Assumptions</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <div>
                <Label>Target Wage % of Sales</Label>
                <div className="flex items-center gap-1">
                  <Input value={targetWagePct} onChange={(e) => { setTargetWagePct(e.target.value); mark(); }} className="h-8" disabled={!isAdmin} />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </div>
              <div>
                <Label>Forecast Sales Change vs LY</Label>
                <div className="flex items-center gap-1">
                  <Input value={forecastChange} onChange={(e) => { setForecastChange(e.target.value); mark(); }} className="h-8" disabled={!isAdmin} />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </div>
              <div>
                <Label>Avg Hourly Wage (excl. salaried)</Label>
                <div className="flex items-center gap-1">
                  <span className="text-sm text-muted-foreground">$</span>
                  <Input value={avgHourlyWage} onChange={(e) => { setAvgHourlyWage(e.target.value); mark(); }} className="h-8" disabled={!isAdmin} />
                </div>
              </div>
              <div>
                <Label>Benefits %</Label>
                <div className="flex items-center gap-1">
                  <Input value={benefitsPct} onChange={(e) => { setBenefitsPct(e.target.value); mark(); }} className="h-8" disabled={!isAdmin} />
                  <span className="text-sm text-muted-foreground">%</span>
                </div>
              </div>
              <div>
                <Label>Distribution Basis</Label>
                <select
                  className="h-8 w-full border rounded px-2 text-sm disabled:opacity-50"
                  value={distributionBasis}
                  onChange={(e) => { setDistributionBasis(e.target.value as 'prior_year' | 'national_average'); mark(); }}
                  disabled={!isAdmin}
                >
                  <option value="prior_year">Prior Year</option>
                  <option value="national_average">National Average</option>
                </select>
              </div>
              <div className="col-span-2 md:col-span-3">
                <Label>Notes</Label>
                <Input value={notes} onChange={(e) => { setNotes(e.target.value); mark(); }} className="h-8" disabled={!isAdmin} placeholder="Optional notes" />
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between">
              <CardTitle className="text-base">Salaried Employees</CardTitle>
              {isAdmin && (
                <Button variant="outline" size="sm" onClick={addSalaried}>+ Add</Button>
              )}
            </CardHeader>
            <CardContent>
              {salariedStaff.length === 0 ? (
                <p className="text-sm text-muted-foreground">No salaried employees configured.</p>
              ) : (
                <div className="space-y-2">
                  <div className="grid grid-cols-5 gap-2 text-xs font-medium text-muted-foreground px-1">
                    <span className="col-span-1">Name</span>
                    <span>Annual Salary</span>
                    <span>Bonus</span>
                    <span>Hrs/Period</span>
                    <span></span>
                  </div>
                  {salariedStaff.map((emp, idx) => (
                    <div key={idx} className="grid grid-cols-5 gap-2 items-center">
                      <Input
                        value={emp.employee_name}
                        onChange={(e) => updateSalaried(idx, 'employee_name', e.target.value)}
                        className="h-7 text-sm col-span-1"
                        placeholder="Name"
                        disabled={!isAdmin}
                      />
                      <Input
                        type="number"
                        value={emp.annual_salary}
                        onChange={(e) => updateSalaried(idx, 'annual_salary', parseFloat(e.target.value) || 0)}
                        className="h-7 text-sm"
                        disabled={!isAdmin}
                      />
                      <Input
                        type="number"
                        value={emp.bonus}
                        onChange={(e) => updateSalaried(idx, 'bonus', parseFloat(e.target.value) || 0)}
                        className="h-7 text-sm"
                        disabled={!isAdmin}
                      />
                      <Input
                        type="number"
                        value={emp.assumed_hours_per_period}
                        onChange={(e) => updateSalaried(idx, 'assumed_hours_per_period', parseInt(e.target.value) || 80)}
                        className="h-7 text-sm"
                        disabled={!isAdmin}
                      />
                      {isAdmin && (
                        <Button variant="ghost" size="sm" className="h-7 w-7 p-0 text-destructive" onClick={() => removeSalaried(idx)}>×</Button>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {isAdmin && (
            <div className="flex gap-2">
              <Button onClick={handleSave} disabled={saving || !dirty}>
                {saving ? 'Saving…' : dirty ? 'Save Changes' : 'Saved'}
              </Button>
              <Button variant="outline" onClick={handleBackfill} disabled={backfilling}>
                {backfilling ? 'Backfilling…' : 'Backfill Pay-Period Calendar from Payroll Runs'}
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
