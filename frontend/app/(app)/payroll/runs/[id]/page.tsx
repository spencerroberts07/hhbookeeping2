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
  generatePaystubs,
  listRunPaystubs,
  getPaystubDownload,
  analyzeRunVariances,
  listRunVariances,
  acknowledgeVariance,
  markEftSent,
  markEmployeesPaid,
  type PayrollRunDetail,
  type PayrollRunVariance,
  type VarianceSeverity,
} from '@/lib/api/payroll';
import { formatMoney, formatDate, cn } from '@/lib/utils';
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

            <VarianceBanner
              entityCode={entityCode}
              runId={runId}
              actorEmail={actorEmail}
            />

            <RegisterTable detail={q.data} />

            <CraSummary detail={q.data} />

            <Workflow
              detail={q.data}
              entityCode={entityCode}
              runId={runId}
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

            <EftSendStep
              entityCode={entityCode}
              runId={runId}
              actorEmail={actorEmail}
              detail={q.data}
            />

            <PaystubsStep
              entityCode={entityCode}
              runId={runId}
              eligible={['approved', 'approved_to_post', 'posted', 'eft_sent', 'paid'].includes(
                q.data.run.workflow_status || q.data.run.status,
              )}
              actorEmail={actorEmail}
            />
          </>
        )}
      </main>
    </>
  );
}

function PaystubsStep({
  entityCode,
  runId,
  eligible,
  actorEmail,
}: {
  entityCode: string;
  runId: string;
  eligible: boolean;
  actorEmail: string;
}) {
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ['run-paystubs', entityCode, runId],
    enabled: !!entityCode && !!runId && eligible,
    retry: false,
    queryFn: () => listRunPaystubs(entityCode, runId),
  });
  const generate = useMutation({
    mutationFn: () =>
      generatePaystubs(runId, { entity_code: entityCode, actor_email: actorEmail }),
    onSuccess: (res) => {
      toast.success(
        `Generated ${res.generated} stub${res.generated === 1 ? '' : 's'}` +
          (res.r2_upload_failures > 0
            ? ` (${res.r2_upload_failures} R2 upload failed)`
            : ''),
      );
      qc.invalidateQueries({ queryKey: ['run-paystubs'] });
    },
    onError: (err) => {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detail ?? 'Pay stub generation failed');
    },
  });

  return (
    <Card className="border-bw-teal/30">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <FileCheck2 className="h-4 w-4 text-bw-teal" />
          Pay Stubs
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {!eligible ? (
          <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 flex items-start gap-2">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            Pay stubs are generated after the run is approved.
          </div>
        ) : (
          <>
            <p className="text-sm text-slate">
              Generates a PDF pay stub for every employee on this run and
              archives them in R2. Each stub shows current period + YTD
              earnings and deductions, plus the vacation balance.
            </p>
            <div className="flex gap-2 items-center">
              <Button onClick={() => generate.mutate()} disabled={generate.isPending}>
                {generate.isPending && (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                )}
                {list.data && list.data.count > 0 ? 'Regenerate all stubs' : 'Generate pay stubs'}
              </Button>
              {list.data && list.data.count > 0 && (
                <span className="text-xs text-slate">
                  {list.data.count} stub{list.data.count === 1 ? '' : 's'} on file
                </span>
              )}
            </div>
            {list.data && list.data.paystubs.length > 0 && (
              <div className="border border-border rounded-md overflow-x-auto">
                <table className="min-w-full text-xs">
                  <thead className="bg-cloud">
                    <tr>
                      <th className="text-left px-2 py-1">#</th>
                      <th className="text-left px-2 py-1">Employee</th>
                      <th className="text-left px-2 py-1">Generated</th>
                      <th className="px-2 py-1"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {list.data.paystubs.map((p) => (
                      <tr key={p.id}>
                        <td className="px-2 py-1 text-slate font-mono">
                          {p.employee_number}
                        </td>
                        <td className="px-2 py-1 text-ink">{p.employee_name}</td>
                        <td className="px-2 py-1 text-slate">
                          {p.generated_at
                            ? formatDate(p.generated_at, 'MMM dd, HH:mm')
                            : '—'}
                        </td>
                        <td className="px-2 py-1 text-right">
                          {p.r2_uploaded ? (
                            <PaystubDownloadInlineLink
                              entityCode={entityCode}
                              paystubId={p.id}
                            />
                          ) : (
                            <span className="text-amber-700 text-[10px]">
                              R2 upload failed
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}

function PaystubDownloadInlineLink({
  entityCode,
  paystubId,
}: {
  entityCode: string;
  paystubId: string;
}) {
  const [busy, setBusy] = useState(false);
  return (
    <button
      type="button"
      disabled={busy}
      className="text-ledger-blue hover:underline disabled:opacity-50 inline-flex items-center gap-1"
      onClick={async () => {
        setBusy(true);
        try {
          const res = await getPaystubDownload(entityCode, paystubId);
          window.open(res.download_url, '_blank', 'noopener');
        } finally {
          setBusy(false);
        }
      }}
    >
      <Download className="h-3 w-3" />
      {busy ? 'Opening…' : 'PDF'}
    </button>
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
  entityCode,
  runId,
  onBuildJournal,
  onSubmit,
  onApprove,
  buildPending,
  submitPending,
  approvePending,
}: {
  detail: PayrollRunDetail;
  entityCode: string;
  runId: string;
  onBuildJournal: () => void;
  onSubmit: () => void;
  onApprove: () => void;
  buildPending: boolean;
  submitPending: boolean;
  approvePending: boolean;
}) {
  const wf = detail.run.workflow_status || detail.run.status;
  const hasJournal = !!detail.run.journal_batch_id;
  // Approve is locked while any 'block' variance is unacknowledged.
  const variances = useQuery({
    queryKey: ['run-variances', entityCode, runId],
    enabled: !!entityCode && !!runId,
    queryFn: () => listRunVariances(entityCode, runId),
    retry: false,
  });
  const blockingCount = (variances.data?.variances ?? []).filter(
    (v) => v.severity === 'block' && !v.acknowledged,
  ).length;
  const approveLocked = blockingCount > 0;
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
            disabled={approvePending || wf !== 'submitted_for_review' || approveLocked}
            title={approveLocked ? `${blockingCount} blocking variance(s) — acknowledge first` : ''}
          >
            {approvePending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            {approveLocked ? `🔒 Approve (${blockingCount} blocks)` : 'Approve'}
          </Button>
        </div>
        {!hasJournal && (
          <p className="text-xs text-slate">
            Build the journal first — submit and EFT generation both require it.
          </p>
        )}
        {approveLocked && (
          <p className="text-xs text-amber-700">
            {blockingCount} blocking variance{blockingCount === 1 ? '' : 's'} above — acknowledge each before approving.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

// ----------------------------------------------------------------------
// F1 — Variance alert banner
// ----------------------------------------------------------------------

function VarianceBanner({
  entityCode,
  runId,
  actorEmail,
}: {
  entityCode: string;
  runId: string;
  actorEmail: string;
}) {
  const qc = useQueryClient();
  const list = useQuery({
    queryKey: ['run-variances', entityCode, runId],
    enabled: !!entityCode && !!runId,
    queryFn: () => listRunVariances(entityCode, runId),
    retry: false,
  });
  const analyze = useMutation({
    mutationFn: () =>
      analyzeRunVariances(runId, { entity_code: entityCode, actor_email: actorEmail }),
    onSuccess: (res) => {
      toast.success(
        `Variance analysis: ${res.counts.block} block · ${res.counts.warn} warn · ${res.counts.info} info`,
      );
      qc.invalidateQueries({ queryKey: ['run-variances', entityCode, runId] });
    },
    onError: () => toast.error('Variance analysis failed'),
  });
  const ack = useMutation({
    mutationFn: (varianceId: string) =>
      acknowledgeVariance(runId, varianceId, {
        entity_code: entityCode,
        actor_email: actorEmail,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['run-variances', entityCode, runId] });
    },
  });

  const variances = list.data?.variances ?? [];
  const grouped: Record<VarianceSeverity, PayrollRunVariance[]> = {
    block: variances.filter((v) => v.severity === 'block'),
    warn: variances.filter((v) => v.severity === 'warn'),
    info: variances.filter((v) => v.severity === 'info'),
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span>Variance alerts</span>
          <Button
            size="sm"
            variant="outline"
            onClick={() => analyze.mutate()}
            disabled={analyze.isPending}
          >
            {analyze.isPending && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            {variances.length === 0 ? 'Analyze' : 'Re-analyze'}
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        {variances.length === 0 ? (
          <p className="text-xs text-slate">
            No analysis yet — click "Analyze" to scan this run against the previous one.
          </p>
        ) : (
          <div className="space-y-3">
            {(['block', 'warn', 'info'] as const).map((sev) =>
              grouped[sev].length === 0 ? null : (
                <VarianceGroup
                  key={sev}
                  severity={sev}
                  rows={grouped[sev]}
                  onAck={(id) => ack.mutate(id)}
                  ackPending={ack.isPending}
                />
              ),
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function VarianceGroup({
  severity,
  rows,
  onAck,
  ackPending,
}: {
  severity: VarianceSeverity;
  rows: PayrollRunVariance[];
  onAck: (id: string) => void;
  ackPending: boolean;
}) {
  const styles =
    severity === 'block'
      ? 'border-red-300 bg-red-50 text-red-900'
      : severity === 'warn'
        ? 'border-amber-300 bg-amber-50 text-amber-900'
        : 'border-ledger-blue/30 bg-ledger-blue/5 text-deep-navy';
  const label =
    severity === 'block'
      ? '🔴 Block — must acknowledge'
      : severity === 'warn'
        ? '🟡 Warn — review before approving'
        : 'ℹ️ Info';
  return (
    <div className={cn('rounded-md border p-2 space-y-1', styles)}>
      <div className="text-xs font-semibold">{label}</div>
      <ul className="space-y-1">
        {rows.map((r) => (
          <li
            key={r.id}
            className="flex items-start justify-between gap-2 text-xs"
          >
            <span className="flex-1">{r.message}</span>
            {r.acknowledged ? (
              <span className="text-bw-teal whitespace-nowrap">
                ✓ {r.acknowledged_by ?? ''}
              </span>
            ) : (
              <button
                type="button"
                onClick={() => onAck(r.id)}
                disabled={ackPending}
                className="text-ledger-blue underline disabled:opacity-50 whitespace-nowrap"
              >
                Acknowledge
              </button>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}

// ----------------------------------------------------------------------
// F2 — Mark EFT sent + Mark employees paid
// ----------------------------------------------------------------------

function EftSendStep({
  entityCode,
  runId,
  actorEmail,
  detail,
}: {
  entityCode: string;
  runId: string;
  actorEmail: string;
  detail: PayrollRunDetail;
}) {
  const qc = useQueryClient();
  const [notes, setNotes] = useState('');
  // We need eft_sent_at / employees_paid_at — not on PayrollRunDetail
  // type yet. Re-fetch the run via the existing query and read from
  // summary_json if backend mirrors it; otherwise we just optimistically
  // toggle on success.
  const sent = useMutation({
    mutationFn: () =>
      markEftSent(runId, { entity_code: entityCode, actor_email: actorEmail, notes }),
    onSuccess: () => {
      toast.success('EFT marked as sent to TD');
      qc.invalidateQueries({ queryKey: ['payroll-run', entityCode, runId] });
    },
    onError: (err) => {
      const detailMsg = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detailMsg ?? 'Mark sent failed');
    },
  });
  const paid = useMutation({
    mutationFn: () =>
      markEmployeesPaid(runId, { entity_code: entityCode, actor_email: actorEmail }),
    onSuccess: () => {
      toast.success('Employees marked as paid');
      qc.invalidateQueries({ queryKey: ['payroll-run', entityCode, runId] });
    },
    onError: (err) => {
      const detailMsg = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      toast.error(detailMsg ?? 'Mark paid failed');
    },
  });

  const wf = detail.run.workflow_status || detail.run.status;
  const isEftSent = wf === 'eft_sent' || wf === 'paid';
  const isPaid = wf === 'paid';
  const eligible = ['approved', 'approved_to_post', 'posted', 'eft_sent', 'paid'].includes(wf);

  if (!eligible) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Step 5 — TD upload + payment confirmation</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        {!isEftSent ? (
          <>
            <ol className="space-y-1 text-xs text-slate list-decimal list-inside">
              <li>Download the EFT file from Step 4 above.</li>
              <li>Log into TD Commercial Banking.</li>
              <li>File Transfer → upload the .txt file.</li>
              <li>Authorize payments in the TD portal.</li>
              <li>Come back here and click "Mark EFT as Sent".</li>
            </ol>
            <div>
              <label className="text-xs text-slate block mb-1">
                Notes (optional)
              </label>
              <input
                type="text"
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="e.g. uploaded to TD Web Business Banking 9:15am"
                className="w-full text-xs rounded-md border border-input px-2 py-1.5"
              />
            </div>
            <Button onClick={() => sent.mutate()} disabled={sent.isPending}>
              {sent.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              Mark EFT as Sent to TD
            </Button>
          </>
        ) : (
          <div className="rounded-md border border-bw-teal/30 bg-bw-teal/5 p-3 text-xs space-y-1">
            <div className="flex items-center gap-2 text-bw-teal font-semibold">
              <CheckCircle2 className="h-4 w-4" /> EFT sent to TD
            </div>
            <p className="text-slate">
              Funds typically arrive next business day. Click below once employees confirm receipt.
            </p>
            {!isPaid ? (
              <Button
                size="sm"
                onClick={() => paid.mutate()}
                disabled={paid.isPending}
              >
                {paid.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                Mark Employees as Paid
              </Button>
            ) : (
              <div className="text-bw-teal font-semibold flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4" /> Employees paid — run complete
              </div>
            )}
          </div>
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
