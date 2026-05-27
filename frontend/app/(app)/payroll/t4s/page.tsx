'use client';

/**
 * T4 slips — calendar-year statement of remuneration paid.
 *
 * The user picks a calendar year, hits Generate, the backend recomputes
 * every employee's T4 figures from payroll_run_lines and writes the
 * PDF to R2. The table then shows one row per employee with download
 * and "Mark filed with CRA" actions.
 */

import { useState } from 'react';
import Link from 'next/link';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, Download, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  listT4s,
  generateT4s,
  getT4Download,
  markT4Filed,
} from '@/lib/api/payroll';
import { formatMoney, formatDate } from '@/lib/utils';

export const dynamic = 'force-dynamic';

export default function T4Page() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? 'unknown';
  const qc = useQueryClient();
  // Default to the previous calendar year — T4s are filed in Feb
  // for the year that just closed.
  const [year, setYear] = useState<number>(new Date().getFullYear() - 1);

  const list = useQuery({
    queryKey: ['t4s', entityCode, year],
    enabled: !!entityCode,
    queryFn: () => listT4s(entityCode!, year),
  });

  const generate = useMutation({
    mutationFn: () =>
      generateT4s({
        entity_code: entityCode!,
        actor_email: actorEmail,
        calendar_year: year,
      }),
    onSuccess: (res) => {
      toast.success(
        `Generated ${res.employees_count} T4 slip(s)` +
          (res.r2_upload_failures > 0
            ? ` — ${res.r2_upload_failures} R2 upload(s) failed`
            : ''),
      );
      qc.invalidateQueries({ queryKey: ['t4s', entityCode, year] });
    },
    onError: (err) => {
      const detailMsg = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detailMsg ?? 'T4 generation failed');
    },
  });

  const markFiled = useMutation({
    mutationFn: (t4Id: string) =>
      markT4Filed(t4Id, { entity_code: entityCode!, actor_email: actorEmail }),
    onSuccess: () => {
      toast.success('Marked as filed with CRA');
      qc.invalidateQueries({ queryKey: ['t4s', entityCode, year] });
    },
    onError: () => toast.error('Mark filed failed'),
  });

  if (!entityCode) {
    return (
      <>
        <Topbar title="T4 slips" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">Pick an entity to view T4 slips.</p>
          </Card>
        </main>
      </>
    );
  }

  const rows = list.data?.t4s ?? [];
  const totals = rows.reduce(
    (acc, r) => ({
      box_14: acc.box_14 + r.box_14,
      box_16: acc.box_16 + r.box_16,
      box_18: acc.box_18 + r.box_18,
      box_22: acc.box_22 + r.box_22,
    }),
    { box_14: 0, box_16: 0, box_18: 0, box_22: 0 },
  );
  const yearOptions = (() => {
    const current = new Date().getFullYear();
    const out: number[] = [];
    for (let y = current; y >= current - 4; y--) out.push(y);
    return out;
  })();

  return (
    <>
      <Topbar title="T4 slips" />
      <main className="p-6 space-y-4 max-w-6xl">
        <div>
          <Link
            href="/payroll"
            className="inline-flex items-center gap-1 text-sm text-slate hover:text-deep-navy"
          >
            <ArrowLeft className="h-4 w-4" /> Back to payroll
          </Link>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between flex-wrap gap-3">
              <span>T4 — {year}</span>
              <div className="flex items-center gap-2">
                <label className="text-xs text-slate">Tax year</label>
                <select
                  value={year}
                  onChange={(e) => setYear(Number(e.target.value))}
                  className="h-9 rounded-md border border-input px-2 text-sm"
                >
                  {yearOptions.map((y) => (
                    <option key={y} value={y}>
                      {y}
                    </option>
                  ))}
                </select>
                <Button
                  onClick={() => generate.mutate()}
                  disabled={generate.isPending}
                >
                  {generate.isPending && (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  )}
                  {rows.length === 0 ? 'Generate T4s' : 'Regenerate T4s'}
                </Button>
              </div>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xs text-slate">
              T4 slips are calendar year (Jan 1 – Dec 31) regardless of your
              accounting fiscal year. Generate compiles from every approved/posted/paid
              run with a pay date in {year}. SIN is never printed — review
              every PDF against your CRA filing before distributing.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-0">
            {list.isLoading ? (
              <Skeleton className="h-64 m-4" />
            ) : rows.length === 0 ? (
              <div className="p-8 text-center text-slate text-sm">
                No T4s generated for {year} yet. Click "Generate T4s" above.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left px-3 py-2">#</th>
                      <th className="text-left px-3 py-2">Employee</th>
                      <th className="text-right px-3 py-2">Box 14 Empl. income</th>
                      <th className="text-right px-3 py-2">Box 16 CPP</th>
                      <th className="text-right px-3 py-2">Box 18 EI</th>
                      <th className="text-right px-3 py-2">Box 22 Fed tax</th>
                      <th className="text-left px-3 py-2">Generated</th>
                      <th className="text-left px-3 py-2">Status</th>
                      <th className="px-3 py-2" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {rows.map((r) => (
                      <tr key={r.id} className="hover:bg-cloud">
                        <td className="px-3 py-2 text-xs font-mono text-slate">
                          {r.employee_number ?? '—'}
                        </td>
                        <td className="px-3 py-2 text-ink">{r.employee_name}</td>
                        <td className="px-3 py-2 tabular-nums text-right">
                          {formatMoney(r.box_14)}
                        </td>
                        <td className="px-3 py-2 tabular-nums text-right">
                          {formatMoney(r.box_16)}
                        </td>
                        <td className="px-3 py-2 tabular-nums text-right">
                          {formatMoney(r.box_18)}
                        </td>
                        <td className="px-3 py-2 tabular-nums text-right">
                          {formatMoney(r.box_22)}
                        </td>
                        <td className="px-3 py-2 text-slate text-xs">
                          {r.generated_at ? formatDate(r.generated_at) : '—'}
                        </td>
                        <td className="px-3 py-2">
                          {r.filed_with_cra ? (
                            <Badge variant="complete">Filed</Badge>
                          ) : r.r2_uploaded ? (
                            <Badge>Ready</Badge>
                          ) : (
                            <Badge variant="locked">No PDF</Badge>
                          )}
                        </td>
                        <td className="px-3 py-2">
                          <T4Actions
                            entityCode={entityCode}
                            row={r}
                            onMarkFiled={() => markFiled.mutate(r.id)}
                            markFiledPending={markFiled.isPending}
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                  <tfoot className="bg-cloud/60">
                    <tr className="font-semibold">
                      <td colSpan={2} className="px-3 py-2 text-right">
                        Totals
                      </td>
                      <td className="px-3 py-2 tabular-nums text-right">
                        {formatMoney(totals.box_14)}
                      </td>
                      <td className="px-3 py-2 tabular-nums text-right">
                        {formatMoney(totals.box_16)}
                      </td>
                      <td className="px-3 py-2 tabular-nums text-right">
                        {formatMoney(totals.box_18)}
                      </td>
                      <td className="px-3 py-2 tabular-nums text-right">
                        {formatMoney(totals.box_22)}
                      </td>
                      <td colSpan={3} />
                    </tr>
                  </tfoot>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </main>
    </>
  );
}

function T4Actions({
  entityCode,
  row,
  onMarkFiled,
  markFiledPending,
}: {
  entityCode: string;
  row: {
    id: string;
    r2_uploaded: boolean;
    filed_with_cra: boolean;
  };
  onMarkFiled: () => void;
  markFiledPending: boolean;
}) {
  const [busy, setBusy] = useState(false);
  async function openPdf() {
    setBusy(true);
    try {
      const res = await getT4Download(entityCode, row.id);
      window.open(res.download_url, '_blank', 'noopener');
    } catch {
      toast.error('Could not get download link');
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="flex items-center gap-2 justify-end">
      <Button
        size="sm"
        variant="outline"
        disabled={busy || !row.r2_uploaded}
        onClick={openPdf}
      >
        <Download className="h-3 w-3" />
        {busy ? 'Opening…' : 'PDF'}
      </Button>
      {!row.filed_with_cra && (
        <Button
          size="sm"
          variant="ghost"
          onClick={onMarkFiled}
          disabled={markFiledPending}
        >
          Mark filed
        </Button>
      )}
    </div>
  );
}
