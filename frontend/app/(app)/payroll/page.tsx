'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { listEmployees, listPayrollRuns } from '@/lib/api/payroll';
import { getCraRemittance } from '@/lib/api/payroll_cra';
import { formatMoney, formatDate } from '@/lib/utils';
import { Plus, Upload, FileCheck2 } from 'lucide-react';

type Tab = 'runs' | 'employees' | 'cra';

export default function PayrollPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [tab, setTab] = useState<Tab>('runs');

  const runs = useQuery({
    queryKey: ['payroll-runs', entityCode],
    enabled: !!entityCode && tab === 'runs',
    queryFn: () => listPayrollRuns({ entity_code: entityCode!, limit: 24 }),
  });

  const craYear = new Date().getFullYear();
  const cra = useQuery({
    queryKey: ['cra-remittance', entityCode, craYear],
    enabled: !!entityCode && tab === 'cra',
    queryFn: () => getCraRemittance(entityCode!, craYear),
  });

  const employees = useQuery({
    queryKey: ['employees', entityCode],
    enabled: !!entityCode && tab === 'employees',
    queryFn: () => listEmployees(entityCode!),
  });

  return (
    <>
      <Topbar title="Payroll" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <nav className="flex gap-1 bg-white border border-border rounded-xl p-1" role="tablist">
            <TabButton active={tab === 'runs'} onClick={() => setTab('runs')}>
              Pay runs
            </TabButton>
            <TabButton active={tab === 'employees'} onClick={() => setTab('employees')}>
              Employees
            </TabButton>
            <TabButton active={tab === 'cra'} onClick={() => setTab('cra')}>
              CRA remittance
            </TabButton>
          </nav>
          {tab === 'runs' && (
            <Link href="/payroll/new">
              <Button variant="accent">
                <Plus className="h-4 w-4" strokeWidth={1.5} />
                New pay run
              </Button>
            </Link>
          )}
        </div>

        {tab === 'runs' && (
          <Card>
            <CardContent className="p-0">
              {runs.isLoading ? (
                <Skeleton className="h-64 m-4" />
              ) : !runs.data?.runs.length ? (
                <div className="p-8 text-center text-slate">
                  No pay runs yet.{' '}
                  <Link href="/payroll/new" className="text-ledger-blue underline">
                    Start your first pay run →
                  </Link>
                </div>
              ) : (
                <table className="min-w-full text-sm">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">#</th>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">Period</th>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">Pay date</th>
                      <th className="text-right font-semibold text-deep-navy px-4 py-2">Gross</th>
                      <th className="text-right font-semibold text-deep-navy px-4 py-2">Net</th>
                      <th className="px-4 py-2">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {runs.data.runs.map((r) => (
                      <tr
                        key={r.id}
                        className="hover:bg-cloud cursor-pointer"
                        onClick={() => { window.location.href = `/payroll/runs/${r.id}`; }}
                      >
                        <td className="px-4 py-2 text-ink font-mono text-xs">
                          <Link href={`/payroll/runs/${r.id}`} className="hover:text-ledger-blue">
                            {r.pay_run_number}
                          </Link>
                        </td>
                        <td className="px-4 py-2 text-ink">
                          {formatDate(r.period_start)} – {formatDate(r.period_end)}
                        </td>
                        <td className="px-4 py-2 text-ink">{formatDate(r.pay_date)}</td>
                        <td className="px-4 py-2 tabular-nums text-right">{formatMoney(r.gross_total)}</td>
                        <td className="px-4 py-2 tabular-nums text-right font-semibold">
                          {formatMoney(r.net_total)}
                        </td>
                        <td className="px-4 py-2">
                          <Badge
                            variant={
                              r.status === 'posted' || r.status === 'approved'
                                ? 'complete'
                                : r.status === 'voided'
                                  ? 'error'
                                  : 'pending'
                            }
                          >
                            {r.status}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardContent>
          </Card>
        )}

        {tab === 'employees' && (
          <Card>
            <CardContent className="p-0">
              {employees.isLoading ? (
                <Skeleton className="h-64 m-4" />
              ) : !employees.data?.employees.length ? (
                <div className="p-8 text-center text-slate">No employees yet.</div>
              ) : (
                <table className="min-w-full text-sm">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">#</th>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">Name</th>
                      <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                      <th className="text-right font-semibold text-deep-navy px-4 py-2">Rate</th>
                      <th className="text-right font-semibold text-deep-navy px-4 py-2">Vac %</th>
                      <th className="px-4 py-2">Active</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {employees.data.employees.map((e) => (
                      <tr key={e.id} className="hover:bg-cloud">
                        <td className="px-4 py-2 text-ink font-mono text-xs">{e.employee_number}</td>
                        <td className="px-4 py-2 text-ink">
                          {e.first_name} {e.last_name}
                        </td>
                        <td className="px-4 py-2 text-slate">{e.employment_type ?? '—'}</td>
                        <td className="px-4 py-2 tabular-nums text-right">
                          {e.hourly_rate ? formatMoney(e.hourly_rate) : '—'}
                        </td>
                        <td className="px-4 py-2 tabular-nums text-right">
                          {e.vacation_rate ? `${(Number(e.vacation_rate) * 100).toFixed(1)}%` : '—'}
                        </td>
                        <td className="px-4 py-2">
                          <Badge variant={e.is_active ? 'complete' : 'locked'}>
                            {e.is_active ? 'Active' : 'Inactive'}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              <div className="p-3 border-t border-border bg-cloud/40 text-right">
                <Link
                  href="/payroll/employees"
                  className="text-xs text-ledger-blue hover:underline"
                >
                  Manage employees (edit / add) →
                </Link>
              </div>
            </CardContent>
          </Card>
        )}

        {tab === 'cra' && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span>CRA remittance — {craYear}</span>
                {cra.data && (
                  <Badge variant="info">
                    Outstanding: {formatMoney(cra.data.total_outstanding)}
                  </Badge>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {cra.isLoading ? (
                <div className="p-6 space-y-2">
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                  <Skeleton className="h-8 w-full" />
                </div>
              ) : !cra.data || cra.data.remittances.length === 0 ? (
                <p className="p-6 text-sm text-slate">
                  No CRA-payable activity recorded for {craYear} yet.
                </p>
              ) : (
                <table className="min-w-full text-sm">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">Period</th>
                      <th className="text-right px-4 py-2 font-semibold text-deep-navy">Owing</th>
                      <th className="text-left px-4 py-2 font-semibold text-deep-navy">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {cra.data.remittances.map((r) => (
                      <tr key={r.period_end} className="hover:bg-cloud">
                        <td className="px-4 py-2 text-ink">{r.period_label}</td>
                        <td className="px-4 py-2 text-right tabular-nums">
                          {formatMoney(r.total_owing)}
                        </td>
                        <td className="px-4 py-2">
                          <Badge variant={r.status === 'remitted' ? 'complete' : 'warning'}>
                            {r.status}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              <p className="p-4 text-xs text-slate">
                Built from journal_lines on account {cra.data?.cra_account_code ?? '2320'}.
                Periods marked closed in /month-end count as remitted.
              </p>
            </CardContent>
          </Card>
        )}
      </main>
    </>
  );
}

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={`rounded-lg px-3 py-1.5 text-sm font-semibold transition ${
        active
          ? 'bg-deep-navy text-white'
          : 'text-slate hover:bg-cloud'
      }`}
    >
      {children}
    </button>
  );
}
