'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  getPayrollRun,
  buildPayrollJournal,
  submitPayrollRun,
  approvePayrollRun,
  generatePayrollEft,
  getPayrollEftDownload,
  type PayrollRunDetail,
} from '@/lib/api/payroll';
import { formatMoney, formatDate } from '@/lib/utils';
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Download,
  FileCheck2,
  Loader2,
} from 'lucide-react';

export const dynamic = 'force-dynamic';

export default function PayrollRunDetailPage() {
  // useParams returns Params | null in Next.js 15's types; accessing
  // .id directly on null would TypeError before any other code runs.
  // Use optional chaining and gate downstream work on runId presence.
  const params = useParams<{ id: string }>();
  const runId = params?.id ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const qc = useQueryClient();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? 'unknown';

  const q = useQuery({
    queryKey: ['payroll-run', entityCode, runId],
    enabled: !!entityCode && !!runId,
    queryFn: () => getPayrollRun(entityCode!, runId),
  });

  // Only fire the EFT-download lookup once we know the run could
  // plausibly have a file. EFT generation requires the run to be in
  // approved/approved_to_post/posted state — anything earlier and
  // no payroll_eft_files row will exist, so the endpoint would 404
  // every page load (and previously was 401-ing, which the axios
  // interceptor escalated to a /sign-in bounce). React Query strings
  // its `enabled` predicate, so this evaluates on every render once
  // q.data lands.
  const runStatus =
    q.data?.run.workflow_status || q.data?.run.status || '';
  const eftCouldExist = ['approved', 'approved_to_post', 'posted'].includes(
    runStatus,
  );
  const eftDownload = useQuery({
    queryKey: ['payroll-eft', entityCode, runId],
    enabled: !!entityCode && !!runId && eftCouldExist,
    retry: false,
    queryFn: () => getPayrollEftDownload(entityCode!, runId),
  });

  const buildJournal = useMutation({
    mutationFn: () =>
      buildPayrollJournal(runId, { entity_code: entityCode!, actor_email: actorEmail }),
    onSuccess: () => {
      toast.success('Journal entry built');
      qc.invalidateQueries({ queryKey: ['payroll-run'] });
    },
    onError: () => toast.error('Build journal failed'),
  });

  const submit = useMutation({
    mutationFn: () =>
      submitPayrollRun(runId, { entity_code: entityCode!, actor_email: actorEmail }),
    onSuccess: () => {
      toast.success('Submitted for approval');
      qc.invalidateQueries({ queryKey: ['payroll-run'] });
    },
    onError: () => toast.error('Submit failed'),
  });

  const approve = useMutation({
    mutationFn: () =>
      approvePayrollRun(runId, { entity_code: entityCode!, actor_email: actorEmail }),
    onSuccess: () => {
      toast.success('Approved');
      qc.invalidateQueries({ queryKey: ['payroll-run'] });
    },
    onError: () => toast.error('Approve failed'),
  });

  const generateEft = useMutation({
    mutationFn: () =>
      generatePayrollEft(runId, { entity_code: entityCode!, actor_email: actorEmail }),
    onSuccess: (res) => {
      toast.success(
        `EFT file generated: ${res.credit_count} credits, ${formatMoney(res.total_amount)}`,
      );
      qc.invalidateQueries({ queryKey: ['payroll-eft'] });
    },
    onError: (err) => {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detail ?? 'EFT generation failed');
    },
  });

  if (!entityCode || !runId) {
    return (
      <>
        <Topbar title="Pay run" />
        <main className="p-6">
          <Card className="p-6 text-slate text-center">
            {!entityCode ? 'No entity selected.' : 'Resolving pay run…'}
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title={q.data ? `Pay run ${q.data.run.pay_run_number}` : 'Pay run'} />
      <main className="p-6 space-y-4">
        <Link
          href="/payroll"
          className="inline-flex items-center gap-1 text-sm text-slate hover:text-ledger-blue"
        >
          <ArrowLeft className="h-4 w-4" /> Back to payroll
        </Link>

        {q.isLoading || !q.data ? (
          <Skeleton className="h-64" />
        ) : (
          <>
            <RunHeader detail={q.data} />

            <RegisterTable detail={q.data} />

            <CraSummary detail={q.data} />

            <Workflow
              detail={q.data}
              onBuildJournal={() => buildJournal.mutate()}
              onSubmit={() => submit.mutate()}
              onApprove={() => approve.mutate()}
              buildPending={buildJournal.isPending}
              submitPending={submit.isPending}
              approvePending={approve.isPending}
            />

            <EftStep
              detail={q.data}
              downloadQuery={eftDownload}
              onGenerate={() => generateEft.mutate()}
              generatePending={generateEft.isPending}
            />
          </>
        )}
      </main>
    </>
  );
}

function RunHeader({ detail }: { detail: PayrollRunDetail }) {
  const r = detail.run;
  const status = r.workflow_status || r.status;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between flex-wrap gap-2">
          <span>
            Pay run {r.pay_run_number}
            <span className="text-sm font-normal text-slate ml-2">
              {formatDate(r.period_start)} – {formatDate(r.period_end)} · pay {formatDate(r.pay_date)}
            </span>
          </span>
          <Badge variant={statusVariant(status)}>{status}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KPI label="Active employees" value={r.active_employees} />
          <KPI label="Paid employees" value={r.paid_employees} />
          <KPI label="Total gross" value={formatMoney(r.total_gross)} />
          <KPI label="Total net pay" value={formatMoney(r.total_net_pay)} />
        </div>
      </CardContent>
    </Card>
  );
}

function RegisterTable({ detail }: { detail: PayrollRunDetail }) {
  // `lines` is typed as PayrollRunLine[] but defensively coerce to []
  // — if the backend ever returns no `lines` key on a partial response
  // (e.g. a run with zero employees) we don't want .map to crash.
  const lines = detail.lines ?? [];
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Payroll register</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">#</th>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Employee</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Reg hrs</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Stat</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Gross</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Fed tax</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">CPP</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">EI</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Net pay</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {lines.map((l) => (
                <tr key={l.id} className="hover:bg-cloud">
                  <td className="px-3 py-1.5 text-ink font-mono text-xs">
                    {l.employee_number}
                  </td>
                  <td className="px-3 py-1.5 text-ink">{l.full_name}</td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-slate">
                    {l.total_hours}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-slate">
                    {formatMoney(l.stat_pay)}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-ink">
                    {formatMoney(l.gross_pay)}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-slate">
                    {formatMoney(l.fed_tax)}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-slate">
                    {formatMoney(l.cpp_ee)}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right text-slate">
                    {formatMoney(l.ei_ee)}
                  </td>
                  <td className="px-3 py-1.5 tabular-nums text-right font-semibold">
                    {formatMoney(l.net_pay)}
                  </td>
                </tr>
              ))}
            </tbody>
            <tfoot className="bg-cloud font-semibold">
              <tr>
                <td colSpan={4} className="px-3 py-2 text-right text-slate">Totals</td>
                <td className="px-3 py-2 tabular-nums text-right">
                  {formatMoney(detail.run.total_gross)}
                </td>
                <td className="px-3 py-2 tabular-nums text-right">
                  {formatMoney(detail.run.total_fed_tax)}
                </td>
                <td className="px-3 py-2 tabular-nums text-right">
                  {formatMoney(detail.run.total_cpp_ee)}
                </td>
                <td className="px-3 py-2 tabular-nums text-right">
                  {formatMoney(detail.run.total_ei_ee)}
                </td>
                <td className="px-3 py-2 tabular-nums text-right">
                  {formatMoney(detail.run.total_net_pay)}
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

function CraSummary({ detail }: { detail: PayrollRunDetail }) {
  const r = detail.run;
  // Backend ships numeric columns as strings; Number(undefined) = NaN
  // and NaN + NaN = NaN, which formatMoney handles, but be explicit.
  const safeNum = (v: unknown): number => {
    const n = Number(v ?? 0);
    return Number.isFinite(n) ? n : 0;
  };
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">CRA remittance breakdown</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <Stat label="Fed + Prov tax" value={formatMoney(r.total_fed_tax)} />
          <Stat label="CPP (ee + er)"
                value={formatMoney(safeNum(r.total_cpp_ee) + safeNum(r.total_cpp_er))} />
          <Stat label="EI (ee + er)"
                value={formatMoney(safeNum(r.total_ei_ee) + safeNum(r.total_ei_er))} />
          <Stat label="Total remittance" value={formatMoney(r.cra_remittance_amount)} highlight />
        </dl>
      </CardContent>
    </Card>
  );
}

function Workflow({
  detail,
  onBuildJournal,
  onSubmit,
  onApprove,
  buildPending,
  submitPending,
  approvePending,
}: {
  detail: PayrollRunDetail;
  onBuildJournal: () => void;
  onSubmit: () => void;
  onApprove: () => void;
  buildPending: boolean;
  submitPending: boolean;
  approvePending: boolean;
}) {
  const wf = detail.run.workflow_status || detail.run.status;
  const hasJournal = !!detail.run.journal_batch_id;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Workflow</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            onClick={onBuildJournal}
            disabled={buildPending}
          >
            {buildPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            {hasJournal ? 'Rebuild journal' : 'Build journal'}
          </Button>
          <Button
            variant="outline"
            onClick={onSubmit}
            disabled={submitPending || !hasJournal}
          >
            {submitPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            Submit for approval
          </Button>
          <Button
            onClick={onApprove}
            disabled={approvePending || wf !== 'submitted_for_review'}
          >
            {approvePending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            Approve
          </Button>
        </div>
        {!hasJournal && (
          <p className="text-xs text-slate">
            Build the journal first — submit and EFT generation both require it.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function EftStep({
  detail,
  downloadQuery,
  onGenerate,
  generatePending,
}: {
  detail: PayrollRunDetail;
  downloadQuery: ReturnType<typeof useQuery<Awaited<ReturnType<typeof getPayrollEftDownload>>>>;
  onGenerate: () => void;
  generatePending: boolean;
}) {
  const wf = detail.run.workflow_status || detail.run.status;
  const eligible = ['approved', 'approved_to_post', 'posted'].includes(wf);
  const has = !!downloadQuery.data;
  return (
    <Card className="border-ledger-blue/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <FileCheck2 className="h-4 w-4 text-ledger-blue" />
          Generate EFT file (CPA 005)
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {!eligible ? (
          <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 flex items-start gap-2">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            EFT generation is blocked until the run is approved.
          </div>
        ) : (
          <>
            <p className="text-sm text-slate">
              Builds a CPA-005 direct-deposit file for TD Business Banking.
              One credit per employee with bank info on file. Net pay total:{' '}
              <strong>{formatMoney(detail.run.total_net_pay)}</strong>.
            </p>
            <div className="flex gap-2">
              <Button onClick={onGenerate} disabled={generatePending}>
                {generatePending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                {has ? 'Regenerate EFT file' : 'Generate EFT file'}
              </Button>
              {has && downloadQuery.data && (
                <a
                  href={downloadQuery.data.download_url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 px-3 py-2 rounded-lg border border-border hover:bg-cloud text-sm font-semibold text-deep-navy"
                >
                  <Download className="h-4 w-4" />
                  Download .txt
                </a>
              )}
            </div>
            {has && downloadQuery.data && (
              <dl className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs pt-2 border-t border-border">
                <KV k="File name" v={downloadQuery.data.file_name} mono />
                <KV k="Credits" v={String(downloadQuery.data.record_count - 2)} />
                <KV k="Total" v={formatMoney(downloadQuery.data.total_amount)} />
                <KV
                  k="Generated"
                  v={formatDate(downloadQuery.data.generated_at, 'MMM dd, HH:mm')}
                />
              </dl>
            )}
            {has && (
              <div className="flex items-start gap-2 text-xs text-slate pt-2">
                <CheckCircle2 className="h-3 w-3 mt-0.5 text-bw-teal shrink-0" />
                <span>
                  Upload the downloaded .txt file in TD Business Banking →
                  Payments → File Upload. Then mark the run as paid in your
                  next month-end close.
                </span>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}

function KPI({ label, value }: { label: string; value: string | number }) {
  return (
    <div>
      <div className="text-xs uppercase tracking-wide text-slate">{label}</div>
      <div className="mt-1 text-lg font-semibold tabular-nums text-deep-navy">
        {value}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  highlight,
}: {
  label: string;
  value: string;
  highlight?: boolean;
}) {
  return (
    <div>
      <dt className="text-xs uppercase tracking-wide text-slate">{label}</dt>
      <dd
        className={
          'mt-1 tabular-nums font-semibold ' +
          (highlight ? 'text-lg text-ledger-blue' : 'text-base text-deep-navy')
        }
      >
        {value}
      </dd>
    </div>
  );
}

function KV({ k, v, mono }: { k: string; v: string; mono?: boolean }) {
  return (
    <div>
      <div className="text-slate">{k}</div>
      <div className={mono ? 'text-ink font-mono text-[11px] break-all' : 'text-ink'}>
        {v}
      </div>
    </div>
  );
}

function statusVariant(s: string): 'complete' | 'warning' | 'error' | 'pending' {
  if (s === 'posted' || s === 'approved_to_post' || s === 'approved') return 'complete';
  if (s === 'voided' || s === 'rejected') return 'error';
  if (s === 'submitted_for_review') return 'pending';
  return 'warning';
}
