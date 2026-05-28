'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { Topbar } from '@/components/layout/topbar';
import {
  AlertTriangle,
  CheckCircle2,
  Database,
  FileSpreadsheet,
  Loader2,
  Upload,
} from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import Link from 'next/link';
import {
  confirmOpeningBalances,
  getGLHistoryProgress,
  getOnboardingStatus,
  getOpeningBalancesParseProgress,
  previewGLHistoryFile,
  startGLHistoryFromFile,
  uploadOpeningBalancesFile,
  type GLPreviewResponse,
  type GLProgressResponse,
  type SuspenseEntry,
  type TbPreviewLine,
} from '@/lib/api/onboarding';
import { formatMoney } from '@/lib/utils';

// Force dynamic — Zustand + React Query resolve client-side; static
// prerender would emit a route that 500s at request time once the
// auth/entity context isn't available.
export const dynamic = 'force-dynamic';

const PLUG_TOLERANCE = 1.0;
const OBE_ACCOUNT = '3900';
const PARSE_TIMEOUT_MS = 3 * 60 * 1000;
const GL_POLL_TIMEOUT_MS = 10 * 60 * 1000;

export default function DataImportPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  if (!entityCode) {
    return (
      <>
        <Topbar title="Settings" />
        <main className="p-6">
          <Card>
            <CardContent className="py-6">
              <Skeleton className="h-32 w-full" />
            </CardContent>
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title="Settings" />
      <main className="p-6 space-y-6">
        <div>
          <h1 className="text-h2 text-deep-navy">Data import</h1>
          <p className="text-sm text-slate mt-1 max-w-2xl">
            Upload trial balances or GL CSV exports outside the onboarding
            wizard. Useful for adding history after go-live or correcting an
            earlier import. Every import is scoped to the active store.
          </p>
        </div>

        <OpeningBalancesSection entityCode={entityCode} />
        <GLImportSection entityCode={entityCode} />
      </main>
    </>
  );
}

// =========================================================================
// Opening balances
// =========================================================================

interface Preview {
  tb_lines: TbPreviewLine[];
  total_debits: number;
  total_credits: number;
  variance: number;
  balanced: boolean;
}

function OpeningBalancesSection({ entityCode }: { entityCode: string }) {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });

  const [asOf, setAsOf] = useState<string>(() => defaultAsOf(status.data?.opening_balance_date));
  useEffect(() => {
    if (status.data?.opening_balance_date) {
      setAsOf(status.data.opening_balance_date);
    }
  }, [status.data?.opening_balance_date]);

  const [preview, setPreview] = useState<Preview | null>(null);
  const [parseJobId, setParseJobId] = useState<string | null>(null);
  const [parseStep, setParseStep] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };
  useEffect(() => () => stopPolling(), []);

  const startPolling = (jobId: string) => {
    stopPolling();
    const startedAt = Date.now();
    pollRef.current = setInterval(async () => {
      if (Date.now() - startedAt > PARSE_TIMEOUT_MS) {
        stopPolling();
        setParseJobId(null);
        setParseError(
          'Parsing is taking longer than expected. Try a CSV with columns: Account Code, Account Name, Debit, Credit.',
        );
        toast.error('Parse timed out');
        return;
      }
      try {
        const p = await getOpeningBalancesParseProgress(jobId);
        setParseStep(p.current_step);
        if (p.status === 'complete' && p.preview) {
          stopPolling();
          setParseJobId(null);
          setPreview(p.preview);
        } else if (p.status === 'error') {
          stopPolling();
          setParseJobId(null);
          setParseError(p.error || 'Parser failed');
          toast.error(p.error || 'Could not parse the trial balance');
        }
      } catch {
        // 404 / transient — try again
      }
    }, 3000);
  };

  const upload = useMutation({
    mutationFn: (file: File) =>
      uploadOpeningBalancesFile({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: asOf,
        file,
      }),
    onSuccess: (res) => {
      setParseError(null);
      setParseJobId(res.job_id);
      setParseStep('Queued');
      startPolling(res.job_id);
    },
    onError: (err: Error) => toast.error(err.message || 'Upload failed'),
  });

  const confirm = useMutation({
    mutationFn: (lines: TbPreviewLine[]) =>
      confirmOpeningBalances({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: asOf,
        tb_lines: lines,
      }),
    onSuccess: (res) => {
      toast.success(`Opening balances posted — ${res.line_count} lines`);
      // Backend filters out income-statement accounts on save and
      // returns a warning when any were skipped. Surface it so the
      // dealer can decide whether to re-export a post-close TB.
      if (res.warning) {
        toast.warning(res.warning, { duration: 10_000 });
      }
      setPreview(null);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
    },
    onError: (err: Error) => toast.error(err.message || 'Save failed'),
  });

  const existing = status.data?.has_opening_balances;
  const existingDate = status.data?.opening_balance_date;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FileSpreadsheet className="h-5 w-5 text-ledger-blue" />
          Opening balances
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {existing && !preview && !parseJobId && (
          <div className="rounded-md border border-bw-teal/30 bg-bw-teal/5 p-3 text-sm flex items-center gap-2">
            <CheckCircle2 className="h-4 w-4 text-bw-teal shrink-0" />
            <span className="text-deep-navy">
              Opening balances set as of <strong>{existingDate}</strong>.
              Re-uploading will replace the existing batch.
            </span>
          </div>
        )}

        <p className="text-sm text-slate">
          Upload your Trial Balance from QBO as of your cut-over date to set
          opening account balances. In QBO: <strong>Reports → Trial Balance</strong>,
          set the date, then export as CSV or Excel.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-[200px_1fr] items-end gap-3">
          <div>
            <Label htmlFor="as-of-date">Trial balance as of</Label>
            <Input
              id="as-of-date"
              type="date"
              value={asOf}
              onChange={(e) => setAsOf(e.target.value)}
              disabled={!!parseJobId || !!preview}
            />
          </div>
          {!preview && !parseJobId && !parseError && (
            <div>
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.xlsx,.xls,.txt"
                className="hidden"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) upload.mutate(f);
                  e.target.value = '';
                }}
              />
              <Button
                variant="outline"
                onClick={() => fileInputRef.current?.click()}
                disabled={!isAdmin || upload.isPending || !asOf}
              >
                {upload.isPending ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Upload className="h-4 w-4 mr-2" />
                )}
                Upload Trial Balance CSV
              </Button>
              <p className="text-xs text-slate mt-1">
                Accepts: .csv, .xlsx, .xls, .txt
              </p>
            </div>
          )}
        </div>

        {parseJobId && (
          <div className="rounded-md bg-cloud p-4 flex items-center gap-3">
            <Loader2 className="h-4 w-4 animate-spin text-ledger-blue" />
            <span className="text-sm text-deep-navy">
              {parseStep || 'Parsing…'}
            </span>
            <Button
              variant="ghost"
              size="sm"
              className="ml-auto"
              onClick={() => {
                stopPolling();
                setParseJobId(null);
                setParseStep(null);
              }}
            >
              Cancel
            </Button>
          </div>
        )}

        {parseError && (
          <div className="rounded-md border-2 border-amber-300 bg-amber-50 p-3 text-sm">
            <div className="flex items-center gap-2 font-semibold text-amber-900">
              <AlertTriangle className="h-4 w-4" />
              Couldn't parse the file
            </div>
            <p className="text-amber-900/80 mt-1">{parseError}</p>
            <Button
              variant="outline"
              size="sm"
              className="mt-2"
              onClick={() => {
                setParseError(null);
                fileInputRef.current?.click();
              }}
            >
              Try a different file
            </Button>
          </div>
        )}

        {preview && (
          <PreviewTable
            preview={preview}
            setPreview={setPreview}
            confirming={confirm.isPending}
            onSave={(lines) => confirm.mutate(lines)}
            onCancel={() => setPreview(null)}
          />
        )}

        {!isAdmin && (
          <p className="text-xs text-slate">
            Admin role required to upload or save opening balances.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function PreviewTable({
  preview,
  setPreview,
  confirming,
  onSave,
  onCancel,
}: {
  preview: Preview;
  setPreview: (p: Preview) => void;
  confirming: boolean;
  onSave: (lines: TbPreviewLine[]) => void;
  onCancel: () => void;
}) {
  const totals = useMemo(() => recompute(preview.tb_lines), [preview.tb_lines]);
  const balanced = Math.abs(totals.variance) < 0.005;
  const pluggable = !balanced && Math.abs(totals.variance) <= PLUG_TOLERANCE;
  const blocked = !balanced && !pluggable;

  // Detect income-statement accounts (4/5/6/7) before the user clicks
  // Save. The backend filters them out on insertion, but warning
  // up-front lets the dealer go back and re-export a post-close TB
  // instead of seeing a confusing variance error after submitting.
  const incomeStatementLines = useMemo(
    () =>
      preview.tb_lines.filter((l) => {
        const c = (l.account_code || '').trim();
        return c.length > 0 && ['4', '5', '6', '7'].includes(c[0]!);
      }),
    [preview.tb_lines],
  );

  const update = (idx: number, patch: Partial<TbPreviewLine>) => {
    const next = preview.tb_lines.map((l, i) => (i === idx ? { ...l, ...patch } : l));
    const t = recompute(next);
    setPreview({
      tb_lines: next,
      total_debits: t.total_debits,
      total_credits: t.total_credits,
      variance: t.variance,
      balanced: Math.abs(t.variance) < 0.005,
    });
  };

  const save = () => {
    let lines = preview.tb_lines;
    if (pluggable) {
      const plug = totals.variance;
      const existing = lines.findIndex((l) => l.account_code === OBE_ACCOUNT);
      if (existing >= 0) {
        lines = lines.map((l, i) =>
          i === existing
            ? {
                ...l,
                debit_balance: Number(l.debit_balance || 0) + (plug < 0 ? -plug : 0),
                credit_balance: Number(l.credit_balance || 0) + (plug > 0 ? plug : 0),
              }
            : l,
        );
      } else {
        lines = [
          ...lines,
          {
            account_code: OBE_ACCOUNT,
            account_name: 'Opening Balance Equity (rounding plug)',
            debit_balance: plug < 0 ? -plug : 0,
            credit_balance: plug > 0 ? plug : 0,
          },
        ];
      }
    }
    onSave(lines);
  };

  return (
    <div className="space-y-3">
      {incomeStatementLines.length > 0 && (
        <div className="rounded-md border-2 border-amber-300 bg-amber-50 p-3 text-sm flex items-start gap-2">
          <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5 text-amber-700" />
          <div className="text-amber-900">
            <div className="font-semibold">
              Your trial balance contains {incomeStatementLines.length}{' '}
              income-statement account
              {incomeStatementLines.length === 1 ? '' : 's'}{' '}
              (4xxx/5xxx/6xxx/7xxx).
            </div>
            <div className="text-amber-900/80 mt-1">
              These will be skipped on save — opening balances should only
              include balance-sheet accounts. Upload a post-close trial
              balance for accurate results.
            </div>
          </div>
        </div>
      )}

      <div
        className={
          'rounded-md border p-3 text-sm flex justify-between items-center ' +
          (balanced
            ? 'border-bw-teal/30 bg-bw-teal/5 text-deep-navy'
            : pluggable
              ? 'border-amber-300 bg-amber-50 text-amber-900'
              : 'border-red-300 bg-red-50 text-red-900')
        }
      >
        <span>
          Debits {formatMoney(totals.total_debits)} · Credits{' '}
          {formatMoney(totals.total_credits)}
        </span>
        <span className="font-semibold">
          {balanced ? (
            <>✅ Balanced</>
          ) : pluggable ? (
            <>⚠️ Plug {formatMoney(totals.variance, { signed: true })} to {OBE_ACCOUNT}</>
          ) : (
            <>Off by {formatMoney(totals.variance, { signed: true })}</>
          )}
        </span>
      </div>

      <div className="max-h-80 overflow-y-auto rounded-md border border-border">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud sticky top-0">
            <tr>
              <th className="text-left px-2 py-2 font-semibold text-deep-navy w-24">
                Account
              </th>
              <th className="text-left px-2 py-2 font-semibold text-deep-navy">
                Name
              </th>
              <th className="text-right px-2 py-2 font-semibold text-deep-navy w-28">
                Debit
              </th>
              <th className="text-right px-2 py-2 font-semibold text-deep-navy w-28">
                Credit
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {preview.tb_lines.map((l, i) => (
              <tr key={`${l.account_code}-${i}`}>
                <td className="px-2 py-1">
                  <Input
                    value={l.account_code}
                    onChange={(e) => update(i, { account_code: e.target.value })}
                    className="h-7 text-xs font-mono"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    value={l.account_name}
                    onChange={(e) => update(i, { account_name: e.target.value })}
                    className="h-7 text-xs"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    type="number"
                    step="0.01"
                    value={l.debit_balance || ''}
                    onChange={(e) =>
                      update(i, { debit_balance: Number(e.target.value) || 0 })
                    }
                    className="h-7 text-xs text-right tabular-nums"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    type="number"
                    step="0.01"
                    value={l.credit_balance || ''}
                    onChange={(e) =>
                      update(i, { credit_balance: Number(e.target.value) || 0 })
                    }
                    className="h-7 text-xs text-right tabular-nums"
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex justify-between">
        <Button variant="ghost" onClick={onCancel}>
          Cancel
        </Button>
        <Button onClick={save} disabled={blocked || confirming}>
          {confirming ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Posting…
            </>
          ) : (
            <>Save opening balances</>
          )}
        </Button>
      </div>
    </div>
  );
}

function recompute(lines: TbPreviewLine[]) {
  let dr = 0;
  let cr = 0;
  for (const l of lines) {
    dr += Number(l.debit_balance || 0);
    cr += Number(l.credit_balance || 0);
  }
  return { total_debits: dr, total_credits: cr, variance: dr - cr };
}

function defaultAsOf(existing?: string | null): string {
  if (existing) return existing;
  // Sensible default — last day of prior fiscal year for an HH dealer
  // (Sep 30 of the most recent year that's already past).
  const today = new Date();
  const year =
    today.getMonth() >= 9 ? today.getFullYear() : today.getFullYear() - 1;
  return `${year}-09-30`;
}

// =========================================================================
// GL import
// =========================================================================

function GLImportSection({ entityCode }: { entityCode: string }) {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });

  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  useEffect(() => {
    if (!dateFrom) {
      const d = new Date();
      d.setFullYear(d.getFullYear() - 1);
      setDateFrom(d.toISOString().slice(0, 10));
    }
    if (!dateTo) {
      setDateTo(new Date().toISOString().slice(0, 10));
    }
  }, [dateFrom, dateTo]);

  // Two-step flow: file -> preview (parse only, no write) -> confirm
  // -> background job + polling. The File reference has to survive
  // the preview round-trip so the same bytes drive both endpoints.
  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<GLPreviewResponse | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);
  const [progress, setProgress] = useState<GLProgressResponse | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };
  useEffect(() => () => stopPolling(), []);

  const startPolling = (id: string) => {
    stopPolling();
    const startedAt = Date.now();
    pollRef.current = setInterval(async () => {
      if (Date.now() - startedAt > GL_POLL_TIMEOUT_MS) {
        stopPolling();
        setProgress((p) => ({
          ...(p ?? {
            job_id: id,
            job_type: 'gl_import',
            pct_complete: 0,
            current_step: null,
            months_imported: 0,
            lines_created: 0,
            batches_created: 0,
            suspense_entries: [],
          }),
          status: 'error',
          error:
            'Import is taking longer than expected (10 minutes). The worker may be stuck. Try a shorter date range.',
        }));
        toast.error('GL import timed out');
        return;
      }
      try {
        const p = await getGLHistoryProgress(id);
        setProgress(p);
        if (p.status === 'complete' || p.status === 'error') {
          stopPolling();
          qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
          if (p.status === 'complete') {
            toast.success(
              `Imported ${p.lines_created} lines across ${p.months_imported} months`,
            );
          } else {
            toast.error(p.error || 'GL import failed');
          }
        }
      } catch {
        // transient — retry next tick
      }
    }, 3000);
  };

  const previewMutation = useMutation({
    mutationFn: (file: File) =>
      previewGLHistoryFile({
        entity_code: entityCode,
        actor_email: actor,
        file,
      }),
    onSuccess: (res) => setPreview(res),
    onError: (err: Error) => toast.error(err.message || 'Preview failed'),
  });

  const importMutation = useMutation({
    mutationFn: (file: File) =>
      startGLHistoryFromFile({
        entity_code: entityCode,
        actor_email: actor,
        date_from: dateFrom,
        date_to: dateTo,
        file,
      }),
    onSuccess: (res) => {
      setJobId(res.job_id);
      startPolling(res.job_id);
      setPreview(null);
      setPendingFile(null);
      toast.info('GL import started — this can take a few minutes');
    },
    onError: (err: Error) => toast.error(err.message || 'Upload failed'),
  });

  const onFilePicked = (file: File) => {
    setPendingFile(file);
    setPreview(null);
    previewMutation.mutate(file);
  };

  const cancelPreview = () => {
    setPreview(null);
    setPendingFile(null);
    previewMutation.reset();
  };

  const confirmImport = () => {
    if (!pendingFile) return;
    importMutation.mutate(pendingFile);
  };

  const reset = () => {
    stopPolling();
    setJobId(null);
    setProgress(null);
    setPreview(null);
    setPendingFile(null);
    previewMutation.reset();
    importMutation.reset();
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5 text-ledger-blue" />
          General ledger import
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-slate">
          Import historical GL transactions from QBO. In QBO:{' '}
          <strong>Reports → General Ledger</strong>, set the date range, then
          export as CSV. We'll show you a preview before anything is written.
        </p>

        {status.data?.has_gl_history && status.data.gl_history_from && (
          <div className="rounded-md border border-bw-teal/30 bg-bw-teal/5 p-3 text-sm flex items-start gap-2">
            <CheckCircle2 className="h-4 w-4 text-bw-teal shrink-0 mt-0.5" />
            <div>
              <div className="font-semibold text-deep-navy">
                {status.data.journal_line_count} GL lines already imported
              </div>
              <div className="text-xs text-slate mt-0.5">
                Covering <strong>{status.data.gl_history_from}</strong> →{' '}
                <strong>{status.data.gl_history_to}</strong>. Re-importing a
                period overwrites the existing batch for that month.
              </div>
            </div>
          </div>
        )}

        <div className="grid grid-cols-2 gap-3 max-w-md">
          <div>
            <Label htmlFor="gl-from">From</Label>
            <Input
              id="gl-from"
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              disabled={!!jobId || !!preview}
            />
          </div>
          <div>
            <Label htmlFor="gl-to">To</Label>
            <Input
              id="gl-to"
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              disabled={!!jobId || !!preview}
            />
          </div>
        </div>

        {!jobId && !preview && (
          <div>
            <input
              ref={fileRef}
              type="file"
              accept=".csv,.xlsx,.xls,.txt"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0];
                if (f) onFilePicked(f);
                e.target.value = '';
              }}
            />
            <Button
              variant="outline"
              onClick={() => fileRef.current?.click()}
              disabled={
                !isAdmin ||
                previewMutation.isPending ||
                !dateFrom ||
                !dateTo
              }
            >
              {previewMutation.isPending ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Upload className="h-4 w-4 mr-2" />
              )}
              Upload GL CSV
            </Button>
            <p className="text-xs text-slate mt-1">
              Accepts: .csv, .xlsx, .xls, .txt. We'll parse and preview
              before writing anything.
            </p>
          </div>
        )}

        {preview && !jobId && (
          <PreviewPanel
            preview={preview}
            confirming={importMutation.isPending}
            onCancel={cancelPreview}
            onConfirm={confirmImport}
          />
        )}

        {jobId && progress && progress.status !== 'complete' && progress.status !== 'error' && (
          <div className="rounded-md bg-cloud p-4 space-y-2">
            <div className="flex items-center gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin text-ledger-blue" />
              <span className="font-semibold text-deep-navy">
                {progress.current_step || 'Starting…'}
              </span>
            </div>
            <div className="h-2 bg-white rounded-full overflow-hidden">
              <div
                className="h-2 bg-ledger-blue transition-all"
                style={{ width: `${progress.pct_complete}%` }}
              />
            </div>
            <div className="text-xs text-slate flex justify-between">
              <span>
                {progress.lines_created} lines · {progress.batches_created} batches
              </span>
              <span>{progress.pct_complete}%</span>
            </div>
          </div>
        )}

        {jobId && progress?.status === 'complete' && (
          <>
            <div className="rounded-md border border-bw-teal/30 bg-bw-teal/5 p-4 space-y-1 text-sm">
              <div className="flex items-center gap-2 font-semibold text-deep-navy">
                <CheckCircle2 className="h-4 w-4 text-bw-teal" />
                Import complete
              </div>
              <div className="text-xs text-slate ml-6">
                {progress.months_imported} months · {progress.lines_created} lines ·{' '}
                {progress.batches_created} batches
              </div>
              <div className="ml-6 pt-1">
                <Button variant="outline" size="sm" onClick={reset}>
                  Import another range
                </Button>
              </div>
            </div>
            {progress.suspense_entries.length > 0 && (
              <SuspenseBreakdown entries={progress.suspense_entries} />
            )}
          </>
        )}

        {jobId && progress?.status === 'error' && (
          <div className="rounded-md border-2 border-amber-300 bg-amber-50 p-3 text-sm">
            <div className="flex items-center gap-2 font-semibold text-amber-900">
              <AlertTriangle className="h-4 w-4" />
              Import failed
            </div>
            <p className="text-amber-900/80 mt-1">
              {progress.error || 'Unknown error'}
            </p>
            <Button variant="outline" size="sm" className="mt-2" onClick={reset}>
              Try again
            </Button>
          </div>
        )}

        {!isAdmin && (
          <p className="text-xs text-slate">
            Admin role required to run GL imports.
          </p>
        )}
      </CardContent>
    </Card>
  );
}

function PreviewPanel({
  preview,
  confirming,
  onCancel,
  onConfirm,
}: {
  preview: GLPreviewResponse;
  confirming: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const hasUnmatched = preview.unmatched_accounts > 0;
  return (
    <div className="rounded-md border border-border p-4 space-y-3 text-sm">
      <div className="font-semibold text-deep-navy">File ready to import</div>

      {preview.periods_detected.length > 0 ? (
        <div>
          <div className="text-xs text-slate mb-1">Periods detected:</div>
          <ul className="space-y-0.5">
            {preview.periods_detected.map((p) => (
              <li key={`${p.year}-${p.month_num}`} className="text-ink">
                {p.month} — {p.transaction_count.toLocaleString()} transaction
                {p.transaction_count === 1 ? '' : 's'}
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className="text-slate">
          No transactions detected in the file. Check the format and try again.
        </p>
      )}

      <dl className="grid grid-cols-[140px_1fr] gap-x-3 gap-y-1 text-xs">
        <dt className="text-slate">Total transactions</dt>
        <dd className="text-ink font-semibold">
          {preview.total_transactions.toLocaleString()}
        </dd>
        {preview.date_range?.start && preview.date_range?.end && (
          <>
            <dt className="text-slate">Date range</dt>
            <dd className="text-ink">
              {preview.date_range.start} → {preview.date_range.end}
            </dd>
          </>
        )}
        <dt className="text-slate">Accounts found</dt>
        <dd className="text-ink">{preview.accounts_found}</dd>
      </dl>

      {hasUnmatched && (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-xs text-amber-900">
          <div className="flex items-start gap-2">
            <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
            <div>
              <div className="font-semibold">
                {preview.unmatched_accounts} unmatched account code
                {preview.unmatched_accounts === 1 ? '' : 's'}:{' '}
                <span className="font-mono">
                  {preview.unmatched_codes.slice(0, 6).join(', ')}
                  {preview.unmatched_codes.length > 6 ? '…' : ''}
                </span>
              </div>
              <div className="mt-1">
                These codes don't exist in your chart of accounts. Importing
                will route them to suspense (account 9999). You can add them
                to your chart via Sync chart of accounts on the Integrations
                page, then re-import.
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="flex justify-end gap-2 pt-1">
        <Button variant="ghost" size="sm" onClick={onCancel} disabled={confirming}>
          Cancel
        </Button>
        <Button
          size="sm"
          onClick={onConfirm}
          disabled={confirming || preview.total_transactions === 0}
        >
          {confirming ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              Starting…
            </>
          ) : (
            <>Import {preview.total_transactions.toLocaleString()} transactions</>
          )}
        </Button>
      </div>
    </div>
  );
}

function SuspenseBreakdown({ entries }: { entries: SuspenseEntry[] }) {
  return (
    <div className="rounded-md border-2 border-amber-300 bg-amber-50 p-4 text-sm">
      <div className="flex items-center gap-2 font-semibold text-amber-900">
        <AlertTriangle className="h-4 w-4" />
        {entries.length} account code{entries.length === 1 ? '' : 's'} routed
        to suspense (9999)
      </div>
      <div className="mt-3 overflow-x-auto rounded-md border border-amber-200 bg-white">
        <table className="min-w-full text-xs">
          <thead className="bg-amber-100/60">
            <tr>
              <th className="text-left px-2 py-1.5 font-semibold text-amber-900">
                Code
              </th>
              <th className="text-left px-2 py-1.5 font-semibold text-amber-900">
                Name (sample)
              </th>
              <th className="text-right px-2 py-1.5 font-semibold text-amber-900">
                Transactions
              </th>
              <th className="text-right px-2 py-1.5 font-semibold text-amber-900">
                Amount
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-amber-100">
            {entries.map((e) => (
              <tr key={e.account_code}>
                <td className="px-2 py-1 font-mono text-ink">{e.account_code}</td>
                <td className="px-2 py-1 text-ink truncate max-w-[200px]">
                  {e.sample_name || '—'}
                </td>
                <td className="px-2 py-1 text-right tabular-nums text-ink">
                  {e.transaction_count.toLocaleString()}
                </td>
                <td className="px-2 py-1 text-right tabular-nums text-ink">
                  {formatMoney(e.total_amount)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-3 text-xs text-amber-900/80">
        These codes don't exist in your chart of accounts. Add them and
        re-import to land the transactions on the right accounts, or
        manually reclassify the suspense entries.
      </p>
      <div className="mt-2">
        <Link
          href="/settings/integrations"
          className="inline-flex items-center gap-1 text-xs font-semibold text-ledger-blue hover:underline"
        >
          Go to chart of accounts →
        </Link>
      </div>
    </div>
  );
}
