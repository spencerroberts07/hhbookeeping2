'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  listEmployees,
  getEmployeeDetail,
  updateEmployee,
  type Employee,
  type UpdateEmployeeInput,
} from '@/lib/api/payroll';
import { formatMoney } from '@/lib/utils';
import { ArrowLeft, Pencil } from 'lucide-react';

export const dynamic = 'force-dynamic';

function maskAccount(transit: string | null | undefined,
                     institution: string | null | undefined,
                     account: string | null | undefined): string {
  if (!transit || !institution || !account) return '—';
  const last4 = account.slice(-4);
  return `${'*'.repeat(transit.length)}-${'*'.repeat(institution.length)}-****${last4}`;
}

export default function PayrollEmployeesPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [editingId, setEditingId] = useState<string | null>(null);

  const q = useQuery({
    queryKey: ['employees', entityCode],
    enabled: !!entityCode,
    queryFn: () => listEmployees(entityCode!),
  });

  if (!entityCode) {
    return (
      <>
        <Topbar title="Employees" />
        <main className="p-6">
          <Card className="p-6 text-center text-slate">
            No entity selected.
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title="Employees" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between">
          <Link
            href="/payroll"
            className="inline-flex items-center gap-1 text-sm text-slate hover:text-ledger-blue"
          >
            <ArrowLeft className="h-4 w-4" /> Back to payroll
          </Link>
        </div>

        <Card>
          <CardContent className="p-0">
            {q.isLoading ? (
              <Skeleton className="h-64 m-4" />
            ) : !q.data?.employees.length ? (
              <p className="p-8 text-sm text-slate text-center">
                No employees yet.
              </p>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                        #
                      </th>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                        Name
                      </th>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                        Type
                      </th>
                      <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                        Rate
                      </th>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                        Bank
                      </th>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                        Status
                      </th>
                      <th className="px-4 py-2" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {q.data.employees.map((e) => (
                      <EmployeeRow
                        key={e.id}
                        employee={e}
                        onEdit={() => setEditingId(e.id)}
                      />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </main>

      {editingId && (
        <EmployeeEditor
          entityCode={entityCode}
          employeeId={editingId}
          onClose={() => setEditingId(null)}
        />
      )}
    </>
  );
}

function EmployeeRow({
  employee,
  onEdit,
}: {
  employee: Employee;
  onEdit: () => void;
}) {
  const rate =
    employee.employment_type === 'salary'
      ? employee.biweekly_salary
        ? `${formatMoney(employee.biweekly_salary)} / pp`
        : '—'
      : employee.hourly_rate
        ? `${formatMoney(employee.hourly_rate)} / hr`
        : '—';
  return (
    <tr className="hover:bg-cloud">
      <td className="px-4 py-2 text-ink font-mono text-xs">
        {employee.employee_number}
      </td>
      <td className="px-4 py-2 text-ink">
        {employee.first_name && employee.last_name
          ? `${employee.first_name} ${employee.last_name}`
          : (employee as { full_name?: string }).full_name ?? '—'}
      </td>
      <td className="px-4 py-2 text-slate text-xs uppercase">
        {employee.employment_type ?? '—'}
      </td>
      <td className="px-4 py-2 tabular-nums text-right">{rate}</td>
      <td className="px-4 py-2 text-slate font-mono text-xs">
        {maskAccount(
          employee.bank_transit,
          employee.bank_institution,
          employee.bank_account,
        )}
      </td>
      <td className="px-4 py-2">
        <Badge variant={employee.is_active ? 'complete' : 'locked'}>
          {employee.is_active ? 'Active' : 'Inactive'}
        </Badge>
      </td>
      <td className="px-4 py-2 text-right">
        <Button size="sm" variant="ghost" onClick={onEdit}>
          <Pencil className="h-3 w-3" />
          Edit
        </Button>
      </td>
    </tr>
  );
}

function EmployeeEditor({
  entityCode,
  employeeId,
  onClose,
}: {
  entityCode: string;
  employeeId: string;
  onClose: () => void;
}) {
  const { user } = useUser();
  const qc = useQueryClient();
  const [showBank, setShowBank] = useState(false);
  const [form, setForm] = useState<Partial<UpdateEmployeeInput>>({});

  const detail = useQuery({
    queryKey: ['employee-detail', entityCode, employeeId],
    queryFn: () => getEmployeeDetail(entityCode, employeeId),
  });

  const save = useMutation({
    mutationFn: () =>
      updateEmployee(employeeId, {
        entity_code: entityCode,
        actor_email: user?.primaryEmailAddress?.emailAddress ?? 'unknown',
        ...form,
      }),
    onSuccess: () => {
      toast.success('Employee updated');
      qc.invalidateQueries({ queryKey: ['employees', entityCode] });
      qc.invalidateQueries({ queryKey: ['employee-detail', entityCode, employeeId] });
      onClose();
    },
    onError: () => toast.error('Update failed'),
  });

  const e = detail.data;
  const v = <K extends keyof UpdateEmployeeInput>(k: K): UpdateEmployeeInput[K] | undefined =>
    (form[k] !== undefined ? form[k] : (e as Record<string, unknown> | undefined)?.[k as string]) as UpdateEmployeeInput[K] | undefined;
  const set = <K extends keyof UpdateEmployeeInput>(k: K, val: UpdateEmployeeInput[K]) =>
    setForm((prev) => ({ ...prev, [k]: val }));

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>
            Edit employee — EE#{e?.employee_number ?? '…'}
          </DialogTitle>
          <DialogDescription>
            Changes save when you click Save. Bank fields are masked by
            default; click "Show bank info" to edit.
          </DialogDescription>
        </DialogHeader>

        {detail.isLoading || !e ? (
          <Skeleton className="h-64" />
        ) : (
          <div className="space-y-5">
            {/* Personal info */}
            <Section title="Personal">
              <Row>
                <Field label="First name">
                  <Input
                    value={(v('first_name') ?? '') as string}
                    onChange={(ev) => set('first_name', ev.target.value)}
                  />
                </Field>
                <Field label="Last name">
                  <Input
                    value={(v('last_name') ?? '') as string}
                    onChange={(ev) => set('last_name', ev.target.value)}
                  />
                </Field>
              </Row>
              <Row>
                <Field label="Province">
                  <Input
                    value={(v('province') ?? 'ON') as string}
                    onChange={(ev) => set('province', ev.target.value.toUpperCase())}
                    maxLength={2}
                  />
                </Field>
                <Field label="Start date">
                  <Input
                    type="date"
                    value={(v('start_date') ?? '') as string}
                    onChange={(ev) => set('start_date', ev.target.value)}
                  />
                </Field>
              </Row>
              <Field label="Address">
                <Input
                  value={(v('address') ?? '') as string}
                  onChange={(ev) => set('address', ev.target.value)}
                />
              </Field>
              <Field label="Status">
                <select
                  value={v('is_active') === false ? 'inactive' : 'active'}
                  onChange={(ev) => set('is_active', ev.target.value === 'active')}
                  className="h-10 rounded-md border border-border px-3 text-sm w-full"
                >
                  <option value="active">Active</option>
                  <option value="inactive">Inactive</option>
                </select>
              </Field>
            </Section>

            {/* Pay info */}
            <Section title="Pay">
              <Field label="Pay type">
                <select
                  value={(v('employment_type') ?? 'hourly') as string}
                  onChange={(ev) => set('employment_type', ev.target.value)}
                  className="h-10 rounded-md border border-border px-3 text-sm w-full"
                >
                  <option value="hourly">Hourly</option>
                  <option value="salary">Salary</option>
                </select>
              </Field>
              {v('employment_type') === 'salary' ? (
                <Field label="Bi-weekly salary">
                  <Input
                    type="number"
                    step="0.01"
                    value={String(v('biweekly_salary') ?? '')}
                    onChange={(ev) => set('biweekly_salary', Number(ev.target.value))}
                  />
                </Field>
              ) : (
                <Field label="Hourly rate">
                  <Input
                    type="number"
                    step="0.0001"
                    value={String(v('hourly_rate') ?? '')}
                    onChange={(ev) => set('hourly_rate', Number(ev.target.value))}
                  />
                </Field>
              )}
              <Field label="Vacation rate (decimal — 0.04 = 4%)">
                <Input
                  type="number"
                  step="0.0001"
                  value={String(v('vacation_rate') ?? '')}
                  onChange={(ev) => set('vacation_rate', Number(ev.target.value))}
                />
              </Field>
            </Section>

            {/* Bank info (masked by default) */}
            <Section title="Bank info">
              {!showBank ? (
                <div className="flex items-center justify-between">
                  <code className="text-xs text-slate">
                    {maskAccount(
                      e.bank_transit,
                      e.bank_institution,
                      e.bank_account,
                    )}
                  </code>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setShowBank(true)}
                  >
                    Show / edit
                  </Button>
                </div>
              ) : (
                <>
                  <Row>
                    <Field label="Transit (5 digits)">
                      <Input
                        value={(v('bank_transit') ?? '') as string}
                        onChange={(ev) => set('bank_transit', ev.target.value)}
                        maxLength={5}
                      />
                    </Field>
                    <Field label="Institution (3 digits)">
                      <Input
                        value={(v('bank_institution') ?? '') as string}
                        onChange={(ev) => set('bank_institution', ev.target.value)}
                        maxLength={3}
                      />
                    </Field>
                  </Row>
                  <Field label="Account">
                    <Input
                      value={(v('bank_account') ?? '') as string}
                      onChange={(ev) => set('bank_account', ev.target.value)}
                      maxLength={12}
                    />
                  </Field>
                </>
              )}
            </Section>

            {/* TD1 + flags */}
            <Section title="Tax setup">
              <Row>
                <Field label="Federal TD1 claim code (0–10)">
                  <Input
                    type="number"
                    min={0}
                    max={10}
                    value={String(v('federal_td1_claim_code') ?? '')}
                    onChange={(ev) =>
                      set('federal_td1_claim_code', Number(ev.target.value))
                    }
                  />
                </Field>
                <Field label="Provincial TD1 claim code (0–10)">
                  <Input
                    type="number"
                    min={0}
                    max={10}
                    value={String(v('provincial_td1_claim_code') ?? '')}
                    onChange={(ev) =>
                      set('provincial_td1_claim_code', Number(ev.target.value))
                    }
                  />
                </Field>
              </Row>
              <Row>
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={Boolean(v('cpp_exempt'))}
                    onChange={(ev) => set('cpp_exempt', ev.target.checked)}
                  />
                  CPP exempt
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={Boolean(v('ei_exempt'))}
                    onChange={(ev) => set('ei_exempt', ev.target.checked)}
                  />
                  EI exempt
                </label>
              </Row>
            </Section>

            <div className="flex justify-end gap-2 pt-2 border-t border-border">
              <Button variant="ghost" onClick={onClose}>
                Cancel
              </Button>
              <Button onClick={() => save.mutate()} disabled={save.isPending}>
                {save.isPending ? 'Saving…' : 'Save'}
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <h3 className="text-xs uppercase tracking-wider text-slate font-semibold">
        {title}
      </h3>
      <div className="space-y-2">{children}</div>
    </div>
  );
}

function Row({ children }: { children: React.ReactNode }) {
  return <div className="grid grid-cols-2 gap-3">{children}</div>;
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <Label className="text-xs text-slate">{label}</Label>
      {children}
    </div>
  );
}
