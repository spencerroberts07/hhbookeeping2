'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import {
  confirmOpeningBalances,
  getOnboardingStatus,
  getOpeningBalancesParseProgress,
  pullOpeningBalancesFromQbo,
  uploadOpeningBalancesFile,
  type TbPreviewLine,
} from '@/lib/api/onboarding';
import { formatMoney } from '@/lib/utils';
import {
  AlertTriangle,
  CheckCircle2,
  Loader2,
  Pencil,
  Upload,
} from 'lucide-react';
import { toast } from 'sonner';

// The cutover date (e.g. Oct 1, 2025) is the first live day. The opening
// trial balance is dated the day BEFORE — Sep 30, 2025 — so equity rolls
// forward as the final entry of the prior fiscal year.
function asOfFromCutover(cutoverISO: string): string {
  const d = new Date(cutoverISO + 'T00:00:00');
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

function formatLongDate(iso: string): string {
  return new Date(iso + 'T00:00:00').toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
}

// Classify by first digit of account code (chart-of-accounts convention
// used throughout this codebase — services_onboarding._infer_type_from_code
// and routes/accounts._type_from_code do the same).
type Klass = 'Asset' | 'Liability' | 'Equity' | 'Revenue' | 'Expense' | 'Other';
function classifyByCode(code: string): Klass {
  const p = (code || '').trim().charAt(0);
  if (p === '1') return 'Asset';
  if (p === '2') return 'Liability';
  if (p === '3') return 'Equity';
  if (p === '4') return 'Revenue';
  if (p === '5' || p === '6' || p === '7' || p === '8' || p === '9')
    return p === '5' ? 'Expense' : 'Expense';
  return 'Other';
}

interface Preview {
  tb_lines: TbPreviewLine[];
  total_debits: number;
  total_credits: number;
  variance: number;
  balanced: boolean;
}

// Variance under this is small enough to plug to 3900 (Opening Balance
// Equity) and let the dealer proceed; over this we block and force a fix.
const PLUG_TOLERANCE = 1.0;
const OBE_ACCOUNT = '3900';

export function StepOpening() {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const path = useOnboardingStore((s) => s.connect_path);
  const cutover = useOnboardingStore((s) => s.cutover_date);
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);
  const qc = useQueryClient();

  const asOf = useMemo(() => asOfFromCutover(cutover), [cutover]);

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });
  const existing = status.data?.has_opening_balances;

  const [preview, setPreview] = useState<Preview | null>(null);
  const [manualMode, setManualMode] = useState(false);
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

  const CLIENT_TIMEOUT_MS = 3 * 60 * 1000;

  const startPolling = (jobId: string) => {
    stopPolling();
    const startedAt = Date.now();
    pollRef.current = setInterval(async () => {
      if (Date.now() - startedAt > CLIENT_TIMEOUT_MS) {
        stopPolling();
        setParseJobId(null);
        setParseError(
          'Parsing is taking longer than expected. Try a simpler file ' +
            'format (CSV with columns: Account Code, Account Name, Debit, Credit).',
        );
        toast.error('Parse timed out — try a different file format');
        return;
      }
      try {
        const p = await getOpeningBalancesParseProgress(jobId);
        setParseStep(p.current_step);
        if (p.status === 'complete' && p.preview) {
          stopPolling();
          setParseJobId(null);
          setPreview(p.preview);
          if (p.preview.balanced) {
            toast.success(
              `Trial balance parsed — ${p.preview.tb_lines.length} accounts, balanced`,
            );
          } else if (Math.abs(p.preview.variance) <= PLUG_TOLERANCE) {
            toast.warning(
              `Off by ${formatMoney(p.preview.variance, { signed: true })} — will plug to ${OBE_ACCOUNT}`,
            );
          } else {
            toast.warning(
              `Out of balance by ${formatMoney(p.preview.variance, { signed: true })}`,
            );
          }
        } else if (p.status === 'error') {
          stopPolling();
          setParseJobId(null);
          setParseError(p.error || 'Parser failed');
          toast.error(p.error || 'Could not parse the trial balance');
        }
      } catch {
        // 404 / transient — try again next tick.
      }
    }, 3000);
  };

  const qboMutation = useMutation({
    mutationFn: () =>
      pullOpeningBalancesFromQbo({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: asOf,
      }),
    onSuccess: (res) => {
      toast.success(`Opening balances posted — ${res.line_count} lines`);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
    },
    onError: (err: Error) =>
      toast.error(err.message || 'Could not pull trial balance from QuickBooks'),
  });

  const uploadMutation = useMutation({
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
    onError: (err: Error) =>
      toast.error(err.message || 'Could not start the parse'),
  });

  const confirmMutation = useMutation({
    mutationFn: (lines: TbPreviewLine[]) =>
      confirmOpeningBalances({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: asOf,
        tb_lines: lines,
      }),
    onSuccess: (res) => {
      toast.success(`Opening balance journal posted — ${res.line_count} lines`);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
      setPreview(null);
      setManualMode(false);
    },
    onError: (err: Error) => toast.error(err.message || 'Could not save'),
  });

  // STATE 3 — already loaded. Show a breakdown so the dealer can verify
  // the Sep 30 TB matches their old books before continuing.
  if (existing && !preview && !manualMode) {
    return (
      <DoneState
        onPrev={prev}
        onNext={next}
        fileInputRef={fileInputRef}
        onFile={(f) => uploadMutation.mutate(f)}
        entityCode={entityCode}
      />
    );
  }

  // STATE 2 — preview with inline editing
  if (preview) {
    return (
      <PreviewState
        preview={preview}
        setPreview={setPreview}
        asOf={asOf}
        confirming={confirmMutation.isPending}
        onCancel={() => {
          setPreview(null);
          setManualMode(false);
        }}
        onConfirm={(lines) => confirmMutation.mutate(lines)}
      />
    );
  }

  // QBO path (set when the user picked QBO in step 2)
  if (path === 'qbo' && !qboMutation.data) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Opening balances</h2>
          <p className="text-slate mt-1">
            We'll pull a trial balance from QuickBooks as of{' '}
            <strong>{formatLongDate(asOf)}</strong> — the last day before your
            cut-over.
          </p>
        </div>
        <div className="rounded-xl bg-cloud p-8 text-center">
          {qboMutation.isPending ? (
            <>
              <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
              <p className="text-sm text-slate mt-3">Pulling from QuickBooks…</p>
            </>
          ) : (
            <Button variant="accent" size="lg" onClick={() => qboMutation.mutate()}>
              Pull from QuickBooks
            </Button>
          )}
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button variant="ghost" onClick={next}>Skip →</Button>
        </div>
      </div>
    );
  }
  if (path === 'qbo' && qboMutation.data) {
    return (
      <div className="space-y-6">
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
          <CheckCircle2 className="h-6 w-6 text-bw-teal" />
          <div>
            <div className="font-semibold text-deep-navy">
              Opening balances posted — {qboMutation.data.line_count} lines
            </div>
            <div className="text-xs text-slate">
              Total {formatMoney(qboMutation.data.total_debits)}
            </div>
          </div>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button variant="accent" size="lg" onClick={next}>Continue →</Button>
        </div>
      </div>
    );
  }

  // Parsing in progress
  if (parseJobId) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Parsing your trial balance…</h2>
          <p className="text-slate mt-1">
            Large files take a minute. CSV exports parse instantly; unusual
            formats fall back to AI.
          </p>
        </div>
        <div className="rounded-xl bg-cloud p-8 text-center">
          <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
          <p className="text-sm text-slate mt-3">
            {parseStep || 'Working on it…'}
          </p>
        </div>
        <div className="flex justify-between pt-2">
          <Button
            variant="ghost"
            onClick={() => {
              stopPolling();
              setParseJobId(null);
              setParseStep(null);
            }}
          >
            Cancel
          </Button>
        </div>
      </div>
    );
  }

  if (parseError) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Couldn't parse that file</h2>
          <div className="rounded-xl border-2 border-amber-300 bg-amber-50 p-4 mt-3">
            <p className="text-sm text-amber-900">{parseError}</p>
          </div>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button
            variant="outline"
            onClick={() => {
              setParseError(null);
              fileInputRef.current?.click();
            }}
          >
            Try a different file
          </Button>
        </div>
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.xls,.txt"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) uploadMutation.mutate(f);
          }}
        />
      </div>
    );
  }

  // STATE 1B — manual entry form
  if (manualMode) {
    return (
      <ManualEntryState
        asOf={asOf}
        confirming={confirmMutation.isPending}
        onBack={() => setManualMode(false)}
        onSave={(lines) => confirmMutation.mutate(lines)}
      />
    );
  }

  // STATE 1A — file upload + instructions
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Opening balances</h2>
        <p className="text-slate mt-1">
          As of <strong>{formatLongDate(asOf)}</strong> — the last day before
          your cut-over to BookWize.
        </p>
      </div>

      <div className="rounded-xl border border-border bg-cloud/50 p-5 text-sm text-deep-navy">
        <div className="font-semibold mb-2">Export your Trial Balance from QuickBooks:</div>
        <ol className="list-decimal list-inside space-y-1 text-slate">
          <li>In QBO: <strong>Reports → Trial Balance</strong></li>
          <li>Set the date to <strong>{formatLongDate(asOf)}</strong></li>
          <li>Export as Excel or CSV</li>
          <li>Upload the file below</li>
        </ol>
      </div>

      <button
        type="button"
        onClick={() => fileInputRef.current?.click()}
        disabled={uploadMutation.isPending}
        className="w-full rounded-xl border-2 border-dashed border-border hover:border-ledger-blue p-10 text-center transition-colors"
      >
        {uploadMutation.isPending ? (
          <>
            <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
            <p className="text-sm text-slate mt-3">Uploading…</p>
          </>
        ) : (
          <>
            <Upload className="h-8 w-8 text-ledger-blue mx-auto" />
            <p className="font-semibold text-deep-navy mt-3">
              Upload Trial Balance File
            </p>
            <p className="text-xs text-slate mt-1">Accepts: .xlsx, .xls, .csv, .txt</p>
          </>
        )}
      </button>
      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,.xlsx,.xls,.txt"
        className="hidden"
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) uploadMutation.mutate(f);
        }}
      />

      <div className="flex items-center gap-3">
        <div className="flex-1 h-px bg-border" />
        <span className="text-xs text-slate uppercase tracking-wider">or</span>
        <div className="flex-1 h-px bg-border" />
      </div>

      <button
        type="button"
        onClick={() => setManualMode(true)}
        className="w-full rounded-xl border border-border hover:border-deep-navy p-4 text-center transition-colors flex items-center justify-center gap-2"
      >
        <Pencil className="h-4 w-4 text-deep-navy" />
        <span className="font-semibold text-deep-navy">Enter Balances Manually</span>
      </button>

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>← Back</Button>
        <Button variant="ghost" onClick={next}>Skip for now →</Button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Done state — show Asset/Liability/Equity breakdown so the dealer can
// eyeball the import against their prior books before moving on.
// ---------------------------------------------------------------------------

function DoneState({
  onPrev,
  onNext,
  fileInputRef,
  onFile,
  entityCode,
}: {
  onPrev: () => void;
  onNext: () => void;
  fileInputRef: React.RefObject<HTMLInputElement | null>;
  onFile: (f: File) => void;
  entityCode: string;
}) {
  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Opening balances</h2>
        <p className="text-slate mt-1">
          Posted as of <strong>{status.data?.opening_balance_date}</strong>.
        </p>
      </div>
      <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
        <CheckCircle2 className="h-6 w-6 text-bw-teal" />
        <div className="flex-1 font-semibold text-deep-navy">
          Opening balances posted
        </div>
        <Badge variant="complete">Done</Badge>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button variant="outline" onClick={() => fileInputRef.current?.click()}>
          Re-upload trial balance
        </Button>
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.xls,.txt"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) onFile(f);
          }}
        />
      </div>
      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={onPrev}>← Back</Button>
        <Button variant="accent" size="lg" onClick={onNext}>
          Continue →
        </Button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Preview state — inline-editable table with auto-plug for small variances.
// ---------------------------------------------------------------------------

function PreviewState({
  preview,
  setPreview,
  asOf,
  confirming,
  onCancel,
  onConfirm,
}: {
  preview: Preview;
  setPreview: (p: Preview) => void;
  asOf: string;
  confirming: boolean;
  onCancel: () => void;
  onConfirm: (lines: TbPreviewLine[]) => void;
}) {
  const totals = useMemo(() => recompute(preview.tb_lines), [preview.tb_lines]);
  const balanced = Math.abs(totals.variance) < 0.005;
  const pluggable = !balanced && Math.abs(totals.variance) <= PLUG_TOLERANCE;
  const blocked = !balanced && !pluggable;

  const classBreakdown = useMemo(() => {
    const acc: Record<Klass, number> = {
      Asset: 0, Liability: 0, Equity: 0, Revenue: 0, Expense: 0, Other: 0,
    };
    for (const l of preview.tb_lines) {
      const k = classifyByCode(l.account_code);
      // Assets/Expenses: debit-natural. Show as the debit balance.
      // Liabilities/Equity/Revenue: credit-natural. Show as credit balance.
      const dr = Number(l.debit_balance || 0);
      const cr = Number(l.credit_balance || 0);
      const net = dr - cr;
      acc[k] += net;
    }
    return acc;
  }, [preview.tb_lines]);

  const updateLine = (idx: number, patch: Partial<TbPreviewLine>) => {
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

  const handleConfirm = () => {
    let lines = preview.tb_lines;
    if (pluggable) {
      // Plug to 3900 (Opening Balance Equity). variance = dr - cr;
      // if variance > 0 → debits exceed credits → add credit to 3900.
      const plug = totals.variance;
      const existing = lines.findIndex((l) => l.account_code === OBE_ACCOUNT);
      if (existing >= 0) {
        const l = lines[existing]!;
        const newDr = Number(l.debit_balance || 0) + (plug < 0 ? -plug : 0);
        const newCr = Number(l.credit_balance || 0) + (plug > 0 ? plug : 0);
        lines = lines.map((x, i) =>
          i === existing
            ? { ...x, debit_balance: newDr, credit_balance: newCr }
            : x,
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
    onConfirm(lines);
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy">Review the trial balance</h2>
        <p className="text-slate mt-1">As of {formatLongDate(asOf)}</p>
      </div>

      <div
        className={
          'rounded-xl border-2 p-4 flex items-center gap-3 ' +
          (balanced
            ? 'border-bw-teal/30 bg-bw-teal/5'
            : pluggable
              ? 'border-amber-300 bg-amber-50'
              : 'border-red-300 bg-red-50')
        }
      >
        {balanced ? (
          <>
            <CheckCircle2 className="h-6 w-6 text-bw-teal" />
            <div>
              <div className="font-semibold text-deep-navy">Balanced</div>
              <div className="text-xs text-slate">
                Debits {formatMoney(totals.total_debits)} = Credits{' '}
                {formatMoney(totals.total_credits)}
              </div>
            </div>
          </>
        ) : pluggable ? (
          <>
            <AlertTriangle className="h-6 w-6 text-amber-700" />
            <div>
              <div className="font-semibold text-amber-900">
                Off by {formatMoney(totals.variance, { signed: true })} — within rounding
              </div>
              <div className="text-xs text-amber-900/80">
                Saving will plug the difference to account {OBE_ACCOUNT} (Opening Balance Equity).
              </div>
            </div>
          </>
        ) : (
          <>
            <AlertTriangle className="h-6 w-6 text-red-700" />
            <div>
              <div className="font-semibold text-red-900">
                Out of balance by {formatMoney(totals.variance, { signed: true })}
              </div>
              <div className="text-xs text-red-900/80">
                Fix the variance below before saving (must be within ${PLUG_TOLERANCE.toFixed(2)}).
              </div>
            </div>
          </>
        )}
      </div>

      <div className="grid grid-cols-3 gap-2 text-sm">
        <Tile label="Total Assets" value={classBreakdown.Asset} />
        <Tile label="Total Liabilities" value={-classBreakdown.Liability} />
        <Tile label="Total Equity" value={-classBreakdown.Equity} />
      </div>

      <div className="max-h-72 overflow-y-auto rounded-lg border border-border">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud sticky top-0">
            <tr>
              <th className="text-left px-3 py-2 font-semibold text-deep-navy">Code</th>
              <th className="text-left px-3 py-2 font-semibold text-deep-navy">Account</th>
              <th className="text-right px-3 py-2 font-semibold text-deep-navy w-28">Debit</th>
              <th className="text-right px-3 py-2 font-semibold text-deep-navy w-28">Credit</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {preview.tb_lines.map((l, i) => (
              <tr key={`${l.account_code}-${i}`}>
                <td className="px-2 py-1 font-mono">
                  <Input
                    value={l.account_code}
                    onChange={(e) => updateLine(i, { account_code: e.target.value })}
                    className="h-7 text-xs font-mono w-20"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    value={l.account_name}
                    onChange={(e) => updateLine(i, { account_name: e.target.value })}
                    className="h-7 text-xs"
                  />
                </td>
                <td className="px-2 py-1 text-right">
                  <Input
                    type="number"
                    step="0.01"
                    value={l.debit_balance || ''}
                    onChange={(e) =>
                      updateLine(i, { debit_balance: Number(e.target.value) || 0 })
                    }
                    className="h-7 text-xs text-right tabular-nums"
                  />
                </td>
                <td className="px-2 py-1 text-right">
                  <Input
                    type="number"
                    step="0.01"
                    value={l.credit_balance || ''}
                    onChange={(e) =>
                      updateLine(i, { credit_balance: Number(e.target.value) || 0 })
                    }
                    className="h-7 text-xs text-right tabular-nums"
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={onCancel}>
          Cancel
        </Button>
        <Button
          variant="accent"
          size="lg"
          disabled={blocked || confirming}
          onClick={handleConfirm}
        >
          {confirming ? 'Posting…' : 'Save Opening Balances'}
        </Button>
      </div>
    </div>
  );
}

function Tile({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-border bg-cloud/40 p-3">
      <div className="text-xs text-slate">{label}</div>
      <div className="text-base font-bold text-deep-navy tabular-nums">
        {formatMoney(value)}
      </div>
    </div>
  );
}

function recompute(lines: TbPreviewLine[]): {
  total_debits: number;
  total_credits: number;
  variance: number;
} {
  let dr = 0;
  let cr = 0;
  for (const l of lines) {
    dr += Number(l.debit_balance || 0);
    cr += Number(l.credit_balance || 0);
  }
  return { total_debits: dr, total_credits: cr, variance: dr - cr };
}

// ---------------------------------------------------------------------------
// Manual entry — same /confirm endpoint as file upload, just hand-keyed.
// ---------------------------------------------------------------------------

function ManualEntryState({
  asOf,
  confirming,
  onBack,
  onSave,
}: {
  asOf: string;
  confirming: boolean;
  onBack: () => void;
  onSave: (lines: TbPreviewLine[]) => void;
}) {
  const [lines, setLines] = useState<TbPreviewLine[]>(() =>
    Array.from({ length: 8 }, () => ({
      account_code: '',
      account_name: '',
      debit_balance: 0,
      credit_balance: 0,
    })),
  );

  const totals = useMemo(() => recompute(lines), [lines]);
  const balanced = Math.abs(totals.variance) < 0.005;
  const pluggable = !balanced && Math.abs(totals.variance) <= PLUG_TOLERANCE;
  const populated = lines.filter((l) => l.account_code.trim() && (l.debit_balance || l.credit_balance));
  const blocked = !balanced && !pluggable;

  const updateLine = (idx: number, patch: Partial<TbPreviewLine>) =>
    setLines((prev) => prev.map((l, i) => (i === idx ? { ...l, ...patch } : l)));

  const addRow = () =>
    setLines((prev) => [
      ...prev,
      { account_code: '', account_name: '', debit_balance: 0, credit_balance: 0 },
    ]);

  const handleSave = () => {
    let toSave = populated;
    if (pluggable) {
      const plug = totals.variance;
      const existing = toSave.findIndex((l) => l.account_code === OBE_ACCOUNT);
      if (existing >= 0) {
        toSave = toSave.map((l, i) =>
          i === existing
            ? {
                ...l,
                debit_balance: Number(l.debit_balance || 0) + (plug < 0 ? -plug : 0),
                credit_balance: Number(l.credit_balance || 0) + (plug > 0 ? plug : 0),
              }
            : l,
        );
      } else {
        toSave = [
          ...toSave,
          {
            account_code: OBE_ACCOUNT,
            account_name: 'Opening Balance Equity (rounding plug)',
            debit_balance: plug < 0 ? -plug : 0,
            credit_balance: plug > 0 ? plug : 0,
          },
        ];
      }
    }
    onSave(toSave);
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy">Enter opening balances</h2>
        <p className="text-slate mt-1">
          As of <strong>{formatLongDate(asOf)}</strong>. One row per account.
        </p>
      </div>

      <div
        className={
          'rounded-lg border p-3 text-sm flex justify-between ' +
          (balanced
            ? 'border-bw-teal/30 bg-bw-teal/5 text-deep-navy'
            : pluggable
              ? 'border-amber-300 bg-amber-50 text-amber-900'
              : 'border-border bg-cloud/40 text-slate')
        }
      >
        <span>
          Debits {formatMoney(totals.total_debits)} · Credits{' '}
          {formatMoney(totals.total_credits)}
        </span>
        <span className="font-semibold">
          {balanced
            ? 'Balanced'
            : pluggable
              ? `Plug ${formatMoney(totals.variance, { signed: true })} to ${OBE_ACCOUNT}`
              : `Off by ${formatMoney(totals.variance, { signed: true })}`}
        </span>
      </div>

      <div className="rounded-lg border border-border">
        <table className="min-w-full text-sm">
          <thead className="bg-cloud">
            <tr>
              <th className="text-left px-2 py-2 font-semibold text-deep-navy w-24">Code</th>
              <th className="text-left px-2 py-2 font-semibold text-deep-navy">Account</th>
              <th className="text-right px-2 py-2 font-semibold text-deep-navy w-28">Debit</th>
              <th className="text-right px-2 py-2 font-semibold text-deep-navy w-28">Credit</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {lines.map((l, i) => (
              <tr key={i}>
                <td className="px-2 py-1">
                  <Input
                    placeholder="1010"
                    value={l.account_code}
                    onChange={(e) => updateLine(i, { account_code: e.target.value })}
                    className="h-7 text-xs font-mono"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    placeholder="Cash — TD operating"
                    value={l.account_name}
                    onChange={(e) => updateLine(i, { account_name: e.target.value })}
                    className="h-7 text-xs"
                  />
                </td>
                <td className="px-2 py-1">
                  <Input
                    type="number"
                    step="0.01"
                    value={l.debit_balance || ''}
                    onChange={(e) =>
                      updateLine(i, { debit_balance: Number(e.target.value) || 0 })
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
                      updateLine(i, { credit_balance: Number(e.target.value) || 0 })
                    }
                    className="h-7 text-xs text-right tabular-nums"
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Button variant="outline" size="sm" onClick={addRow}>
        + Add row
      </Button>

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={onBack}>← Back</Button>
        <Button
          variant="accent"
          size="lg"
          disabled={blocked || confirming || populated.length === 0}
          onClick={handleSave}
        >
          {confirming ? 'Posting…' : 'Save Opening Balances'}
        </Button>
      </div>
    </div>
  );
}
