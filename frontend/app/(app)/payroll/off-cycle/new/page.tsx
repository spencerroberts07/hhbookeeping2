'use client';

/**
 * Off-cycle pay run wizard — covers correction, bonus, and
 * retroactive runs. All three share the same /runs/create-correction
 * endpoint; they differ only in which fields are filled per employee.
 *
 *   correction  — override_gross per employee
 *   bonus       — override_gross per employee (CRA bonus-method tax)
 *   retroactive — retro_old_rate / retro_new_rate / retro_periods /
 *                 hours_per_period (engine computes the delta)
 */

import { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useQuery, useMutation } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { ArrowLeft, ChevronRight, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  listEmployees,
  calculateRetro,
  createCorrectionRun,
  type Employee,
  type CorrectionEmployeeSpec,
  type RetroCalcResponse,
} from '@/lib/api/payroll';
import { formatMoney } from '@/lib/utils';

export const dynamic = 'force-dynamic';

type RunType = 'correction' | 'bonus' | 'retroactive';

export default function OffCycleNewPage() {
  const router = useRouter();
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? 'unknown';

  const [runType, setRunType] = useState<RunType>('correction');
  const [description, setDescription] = useState('');
  const today = new Date().toISOString().slice(0, 10);
  const [periodStart, setPeriodStart] = useState(today);
  const [periodEnd, setPeriodEnd] = useState(today);
  const [payDate, setPayDate] = useState(today);
  const [rows, setRows] = useState<EmployeeRowState[]>([]);

  const employees = useQuery({
    queryKey: ['payroll-employees', entityCode],
    enabled: !!entityCode,
    queryFn: () => listEmployees(entityCode!),
  });

  const create = useMutation({
    mutationFn: () =>
      createCorrectionRun({
        entity_code: entityCode!,
        actor_email: actorEmail,
        run_type: runType,
        description,
        period_start: periodStart,
        period_end: periodEnd,
        pay_date: payDate,
        employees: buildSpecs(rows, runType),
      }),
    onSuccess: (res) => {
      toast.success(`Off-cycle run ${res.pay_run_number} created`);
      router.push(`/payroll/runs/${res.payroll_run_id}`);
    },
    onError: (err) => {
      const detailMsg = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detailMsg ?? 'Could not create off-cycle run');
    },
  });

  if (!entityCode) {
    return (
      <>
        <Topbar title="Off-cycle run" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">Pick an entity to start an off-cycle run.</p>
          </Card>
        </main>
      </>
    );
  }

  const canSubmit =
    rows.length > 0 &&
    rows.every((r) => isRowReady(r, runType)) &&
    !!description.trim() &&
    !!periodStart &&
    !!periodEnd &&
    !!payDate;

  return (
    <>
      <Topbar title="Off-cycle run" />
      <main className="p-6 max-w-4xl space-y-4">
        <div>
          <Link
            href="/payroll"
            className="text-xs text-slate hover:text-deep-navy inline-flex items-center gap-1"
          >
            <ArrowLeft className="h-3 w-3" /> Back to payroll
          </Link>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>1. Type and dates</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-3 gap-2">
              {(['correction', 'bonus', 'retroactive'] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setRunType(t)}
                  className={`rounded-md border p-3 text-left text-sm ${
                    runType === t
                      ? 'border-deep-navy bg-deep-navy/5'
                      : 'border-input hover:border-deep-navy/40'
                  }`}
                >
                  <div className="font-semibold capitalize">{t}</div>
                  <div className="text-xs text-slate">{RUN_TYPE_HELP[t]}</div>
                </button>
              ))}
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div>
                <Label htmlFor="ps">Period start</Label>
                <Input
                  id="ps"
                  type="date"
                  value={periodStart}
                  onChange={(e) => setPeriodStart(e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="pe">Period end</Label>
                <Input
                  id="pe"
                  type="date"
                  value={periodEnd}
                  onChange={(e) => setPeriodEnd(e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="pd">Pay date</Label>
                <Input
                  id="pd"
                  type="date"
                  value={payDate}
                  onChange={(e) => setPayDate(e.target.value)}
                />
              </div>
            </div>

            <div>
              <Label htmlFor="desc">Description</Label>
              <Input
                id="desc"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder={DESC_PLACEHOLDER[runType]}
              />
              <p className="text-[11px] text-slate mt-1">
                This text appears on the journal entry, pay stub, and audit log.
              </p>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <span>2. Employees</span>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  const list = employees.data?.employees ?? [];
                  const first = list.find(
                    (e) => e.is_active && !rows.some((r) => r.employee_id === e.id),
                  );
                  if (first) setRows((prev) => [...prev, blankRow(first)]);
                }}
                disabled={!employees.data}
              >
                + Add employee
              </Button>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {rows.length === 0 ? (
              <p className="text-xs text-slate">
                Add at least one employee to continue.
              </p>
            ) : (
              <div className="space-y-3">
                {rows.map((row, idx) => (
                  <EmployeeRowEditor
                    key={idx}
                    row={row}
                    runType={runType}
                    employees={employees.data?.employees ?? []}
                    actorEntityCode={entityCode}
                    onChange={(next) =>
                      setRows((prev) => prev.map((r, i) => (i === idx ? next : r)))
                    }
                    onRemove={() =>
                      setRows((prev) => prev.filter((_, i) => i !== idx))
                    }
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <div className="flex justify-end gap-2">
          <Button variant="ghost" onClick={() => router.push('/payroll')}>
            Cancel
          </Button>
          <Button
            onClick={() => create.mutate()}
            disabled={!canSubmit || create.isPending}
          >
            {create.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            Create off-cycle run
            <ChevronRight className="h-4 w-4 ml-1" />
          </Button>
        </div>
      </main>
    </>
  );
}

// ----------------------------------------------------------------------

const RUN_TYPE_HELP: Record<RunType, string> = {
  correction:
    'Fix an under-or-overpayment from a prior run. You enter the gross amount owed.',
  bonus:
    'One-time bonus payout. Uses CRA bonus-method tax withholding.',
  retroactive:
    'Back-pay from a wage increase effective on a prior date. We compute the delta.',
};

const DESC_PLACEHOLDER: Record<RunType, string> = {
  correction: 'e.g. Missed hours from Period 5 — Smith J.',
  bonus: 'e.g. 2026 Q1 retention bonus',
  retroactive: 'e.g. Rate increase $22→$24 effective 2026-04-01',
};

interface EmployeeRowState {
  employee_id: string;
  employee_name: string;
  // correction / bonus
  override_gross: string;
  // retroactive
  retro_old_rate: string;
  retro_new_rate: string;
  retro_effective_date: string;
  // last preview from /calculate-retro for the retroactive flow.
  retro_preview: RetroCalcResponse | null;
}

function blankRow(emp: Employee): EmployeeRowState {
  const today = new Date().toISOString().slice(0, 10);
  return {
    employee_id: emp.id,
    employee_name:
      emp.full_name || `${emp.first_name ?? ''} ${emp.last_name ?? ''}`.trim() || `EE# ${emp.employee_number}`,
    override_gross: '',
    retro_old_rate: emp.hourly_rate ? String(emp.hourly_rate) : '',
    retro_new_rate: '',
    retro_effective_date: today,
    retro_preview: null,
  };
}

function isRowReady(row: EmployeeRowState, runType: RunType): boolean {
  if (runType === 'retroactive') {
    return (
      !!row.retro_preview && Math.abs(row.retro_preview.retro_amount_gross) > 0.0001
    );
  }
  return Number(row.override_gross) > 0;
}

function buildSpecs(
  rows: EmployeeRowState[],
  runType: RunType,
): CorrectionEmployeeSpec[] {
  if (runType === 'retroactive') {
    return rows.map((r) => {
      const periods = r.retro_preview?.periods ?? [];
      const totalHours = periods.reduce((s, p) => s + p.hours, 0);
      const avgHoursPerPeriod = periods.length > 0 ? totalHours / periods.length : 0;
      return {
        employee_id: r.employee_id,
        retro_old_rate: Number(r.retro_old_rate),
        retro_new_rate: Number(r.retro_new_rate),
        retro_periods: periods.length,
        hours_per_period: avgHoursPerPeriod,
      };
    });
  }
  return rows.map((r) => ({
    employee_id: r.employee_id,
    override_gross: Number(r.override_gross),
  }));
}

function EmployeeRowEditor({
  row,
  runType,
  employees,
  actorEntityCode,
  onChange,
  onRemove,
}: {
  row: EmployeeRowState;
  runType: RunType;
  employees: Employee[];
  actorEntityCode: string;
  onChange: (next: EmployeeRowState) => void;
  onRemove: () => void;
}) {
  const retro = useMutation({
    mutationFn: () =>
      calculateRetro({
        entity_code: actorEntityCode,
        employee_id: row.employee_id,
        old_rate: Number(row.retro_old_rate),
        new_rate: Number(row.retro_new_rate),
        effective_date: row.retro_effective_date,
      }),
    onSuccess: (res) => {
      onChange({ ...row, retro_preview: res });
    },
    onError: (err) => {
      const detailMsg = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detailMsg ?? 'Retro calculation failed');
    },
  });

  return (
    <div className="rounded-md border border-input p-3 space-y-3">
      <div className="flex items-center justify-between gap-2">
        <select
          value={row.employee_id}
          onChange={(e) => {
            const emp = employees.find((x) => x.id === e.target.value);
            if (emp) onChange({ ...blankRow(emp) });
          }}
          className="flex-1 text-sm rounded-md border border-input px-2 py-1.5"
        >
          {employees
            .filter((e) => e.is_active)
            .map((e) => (
              <option key={e.id} value={e.id}>
                #{e.employee_number} —{' '}
                {e.full_name || `${e.first_name ?? ''} ${e.last_name ?? ''}`.trim()}
              </option>
            ))}
        </select>
        <button
          type="button"
          onClick={onRemove}
          className="text-xs text-slate hover:text-red-600"
        >
          Remove
        </button>
      </div>

      {runType !== 'retroactive' ? (
        <div className="grid grid-cols-2 gap-2 items-end">
          <div>
            <Label htmlFor={`og-${row.employee_id}`}>Gross amount</Label>
            <Input
              id={`og-${row.employee_id}`}
              type="number"
              step="0.01"
              min="0"
              value={row.override_gross}
              onChange={(e) =>
                onChange({ ...row, override_gross: e.target.value })
              }
              placeholder="0.00"
            />
          </div>
          <p className="text-xs text-slate">
            Engine will withhold CPP/EI/tax automatically. Net deposit shows in the
            register once the run opens.
          </p>
        </div>
      ) : (
        <div className="space-y-2">
          <div className="grid grid-cols-3 gap-2">
            <div>
              <Label htmlFor={`old-${row.employee_id}`}>Old rate /hr</Label>
              <Input
                id={`old-${row.employee_id}`}
                type="number"
                step="0.01"
                value={row.retro_old_rate}
                onChange={(e) =>
                  onChange({ ...row, retro_old_rate: e.target.value, retro_preview: null })
                }
              />
            </div>
            <div>
              <Label htmlFor={`new-${row.employee_id}`}>New rate /hr</Label>
              <Input
                id={`new-${row.employee_id}`}
                type="number"
                step="0.01"
                value={row.retro_new_rate}
                onChange={(e) =>
                  onChange({ ...row, retro_new_rate: e.target.value, retro_preview: null })
                }
              />
            </div>
            <div>
              <Label htmlFor={`eff-${row.employee_id}`}>Effective date</Label>
              <Input
                id={`eff-${row.employee_id}`}
                type="date"
                value={row.retro_effective_date}
                onChange={(e) =>
                  onChange({
                    ...row,
                    retro_effective_date: e.target.value,
                    retro_preview: null,
                  })
                }
              />
            </div>
          </div>
          <Button
            size="sm"
            variant="outline"
            onClick={() => retro.mutate()}
            disabled={
              retro.isPending ||
              !Number(row.retro_old_rate) ||
              !Number(row.retro_new_rate) ||
              !row.retro_effective_date
            }
          >
            {retro.isPending && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Calculate retro
          </Button>
          {row.retro_preview && (
            <div className="rounded-md bg-cloud/40 p-2 text-xs space-y-1">
              <div className="font-semibold text-deep-navy">
                Retro owed: {formatMoney(row.retro_preview.retro_amount_gross)} gross
                · est. net {formatMoney(row.retro_preview.estimated_net)}
              </div>
              <div className="text-slate">
                {row.retro_preview.periods.length} period(s) ·{' '}
                {row.retro_preview.note}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
