'use client';

import { useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  confirmOpeningBalances,
  getOnboardingStatus,
  pullOpeningBalancesFromQbo,
  uploadOpeningBalancesFile,
  type TbPreviewLine,
} from '@/lib/api/onboarding';
import { formatMoney } from '@/lib/utils';
import { AlertTriangle, CheckCircle2, Loader2, Upload } from 'lucide-react';
import { toast } from 'sonner';

interface Preview {
  tb_lines: TbPreviewLine[];
  total_debits: number;
  total_credits: number;
  variance: number;
  balanced: boolean;
}

export function StepOpening() {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const path = useOnboardingStore((s) => s.connect_path);
  const cutover = useOnboardingStore((s) => s.cutover_date);
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);
  const qc = useQueryClient();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });
  const existing = status.data?.has_opening_balances;

  const [preview, setPreview] = useState<Preview | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const qboMutation = useMutation({
    mutationFn: () =>
      pullOpeningBalancesFromQbo({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: cutover,
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
        as_of_date: cutover,
        file,
      }),
    onSuccess: (res) => {
      setPreview(res.preview);
      if (res.preview.balanced) {
        toast.success(`Trial balance parsed — ${res.preview.tb_lines.length} accounts, balanced`);
      } else {
        toast.warning(
          `Out of balance by ${formatMoney(res.preview.variance, { signed: true })}`,
        );
      }
    },
    onError: (err: Error) =>
      toast.error(err.message || 'Could not parse the trial balance'),
  });

  const confirmMutation = useMutation({
    mutationFn: (lines: TbPreviewLine[]) =>
      confirmOpeningBalances({
        entity_code: entityCode,
        actor_email: actor,
        as_of_date: cutover,
        tb_lines: lines,
      }),
    onSuccess: (res) => {
      toast.success(`Opening balance journal posted — ${res.line_count} lines`);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
      setPreview(null);
    },
    onError: (err: Error) => toast.error(err.message || 'Could not save'),
  });

  if (existing && !preview) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Opening balances</h2>
          <p className="text-slate mt-1">
            We already have opening balances loaded as of{' '}
            <strong>{status.data?.opening_balance_date}</strong>.
          </p>
        </div>
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
          <CheckCircle2 className="h-6 w-6 text-bw-teal" />
          <div className="flex-1 font-semibold text-deep-navy">
            Opening balances posted
          </div>
          <Badge variant="complete">Done</Badge>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>
            ← Back
          </Button>
          <Button variant="accent" size="lg" onClick={next}>
            Continue →
          </Button>
        </div>
      </div>
    );
  }

  if (preview) {
    return (
      <div className="space-y-5">
        <div>
          <h2 className="text-h2 text-deep-navy">Review the trial balance</h2>
          <p className="text-slate mt-1">As of {cutover}</p>
        </div>
        <div
          className={
            'rounded-xl border-2 p-4 flex items-center gap-3 ' +
            (preview.balanced
              ? 'border-bw-teal/30 bg-bw-teal/5'
              : 'border-amber-300 bg-amber-50')
          }
        >
          {preview.balanced ? (
            <>
              <CheckCircle2 className="h-6 w-6 text-bw-teal" />
              <div>
                <div className="font-semibold text-deep-navy">Balanced</div>
                <div className="text-xs text-slate">
                  Debits {formatMoney(preview.total_debits)} = Credits{' '}
                  {formatMoney(preview.total_credits)}
                </div>
              </div>
            </>
          ) : (
            <>
              <AlertTriangle className="h-6 w-6 text-amber-700" />
              <div>
                <div className="font-semibold text-amber-900">Out of balance</div>
                <div className="text-xs text-amber-900/80">
                  Variance {formatMoney(preview.variance, { signed: true })} —
                  fix the file and re-upload.
                </div>
              </div>
            </>
          )}
        </div>
        <div className="max-h-72 overflow-y-auto rounded-lg border border-border">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud sticky top-0">
              <tr>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Code</th>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Account</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Debit</th>
                <th className="text-right px-3 py-2 font-semibold text-deep-navy">Credit</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {preview.tb_lines.map((l, i) => (
                <tr key={`${l.account_code}-${i}`}>
                  <td className="px-3 py-1.5 font-mono">{l.account_code}</td>
                  <td className="px-3 py-1.5">{l.account_name}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">
                    {l.debit_balance ? formatMoney(l.debit_balance) : '—'}
                  </td>
                  <td className="px-3 py-1.5 text-right tabular-nums">
                    {l.credit_balance ? formatMoney(l.credit_balance) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={() => setPreview(null)}>
            Cancel
          </Button>
          <Button
            variant="accent"
            size="lg"
            disabled={!preview.balanced || confirmMutation.isPending}
            onClick={() => confirmMutation.mutate(preview.tb_lines)}
          >
            {confirmMutation.isPending ? 'Posting…' : 'Confirm opening balances'}
          </Button>
        </div>
      </div>
    );
  }

  // QBO path
  if (path === 'qbo' && !qboMutation.data) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Opening balances</h2>
          <p className="text-slate mt-1">
            We'll pull a trial balance from QuickBooks as of <strong>{cutover}</strong>.
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

  // File upload
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Upload your trial balance</h2>
        <p className="text-slate mt-1">
          As of <strong>{cutover}</strong>. QuickBooks, Sage, Excel, or CSV
          all work.
        </p>
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
            <p className="text-sm text-slate mt-3">Parsing with AI…</p>
          </>
        ) : (
          <>
            <Upload className="h-8 w-8 text-ledger-blue mx-auto" />
            <p className="font-semibold text-deep-navy mt-3">
              Click to upload your trial balance
            </p>
            <p className="text-xs text-slate mt-1">.csv, .xlsx, .xls, .txt</p>
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
      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>← Back</Button>
        <Button variant="ghost" onClick={next}>Skip for now →</Button>
      </div>
    </div>
  );
}
