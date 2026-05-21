'use client';

import { useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import {
  confirmChart,
  getOnboardingStatus,
  pullChartFromQbo,
  uploadChartFile,
  type ChartPreviewAccount,
} from '@/lib/api/onboarding';
import { CheckCircle2, Loader2, Upload } from 'lucide-react';
import { toast } from 'sonner';

export function StepChart() {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const path = useOnboardingStore((s) => s.connect_path);
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);
  const qc = useQueryClient();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });
  const existing = status.data?.has_chart_of_accounts;

  // QBO pull path
  const qboMutation = useMutation({
    mutationFn: () => pullChartFromQbo({ entity_code: entityCode, actor_email: actor }),
    onSuccess: (res) => {
      toast.success(`Imported ${res.account_count} accounts from QuickBooks`);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
    },
    onError: () => toast.error('QuickBooks chart pull failed'),
  });

  // File upload path — preview then confirm
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [preview, setPreview] = useState<ChartPreviewAccount[] | null>(null);
  const uploadMutation = useMutation({
    mutationFn: (file: File) =>
      uploadChartFile({ entity_code: entityCode, actor_email: actor, file }),
    onSuccess: (res) => {
      setPreview(res.preview.accounts);
      toast.success(`Parsed ${res.preview.count} accounts — review and confirm`);
    },
    onError: (err: Error) =>
      toast.error(err.message || 'Could not parse the file'),
  });
  const confirmMutation = useMutation({
    mutationFn: (accounts: ChartPreviewAccount[]) =>
      confirmChart({ entity_code: entityCode, actor_email: actor, accounts }),
    onSuccess: (res) => {
      toast.success(
        `Saved ${res.saved_count} accounts${
          res.conflicts.length ? ` (${res.conflicts.length} conflicts)` : ''
        }`,
      );
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
      setPreview(null);
    },
    onError: () => toast.error('Could not save the chart'),
  });

  // Already loaded — let the dealer skip or replace
  if (existing && !preview) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Chart of accounts</h2>
          <p className="text-slate mt-1">
            We found {status.data?.account_count} accounts already loaded for
            this store.
          </p>
        </div>
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
          <CheckCircle2 className="h-6 w-6 text-bw-teal" />
          <div className="flex-1">
            <div className="font-semibold text-deep-navy">
              {status.data?.account_count} accounts loaded
            </div>
            <div className="text-xs text-slate">
              Skip ahead, or replace with a new chart.
            </div>
          </div>
          <Badge variant="complete">Done</Badge>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            onClick={() => fileInputRef.current?.click()}
          >
            Replace with new chart
          </Button>
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

  // Preview after parse
  if (preview) {
    return (
      <div className="space-y-5">
        <div>
          <h2 className="text-h2 text-deep-navy">Confirm your accounts</h2>
          <p className="text-slate mt-1">
            We parsed {preview.length} accounts. Review and confirm.
          </p>
        </div>
        <div className="max-h-80 overflow-y-auto rounded-lg border border-border">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud sticky top-0">
              <tr>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Code</th>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Name</th>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Type</th>
                <th className="text-left px-3 py-2 font-semibold text-deep-navy">Normal</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {preview.map((a, i) => (
                <tr key={`${a.code}-${i}`}>
                  <td className="px-3 py-1.5 font-mono">{a.code}</td>
                  <td className="px-3 py-1.5">{a.name}</td>
                  <td className="px-3 py-1.5">{a.type}</td>
                  <td className="px-3 py-1.5">{a.normal_balance}</td>
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
            onClick={() => confirmMutation.mutate(preview)}
            disabled={confirmMutation.isPending}
          >
            {confirmMutation.isPending ? 'Saving…' : `Confirm ${preview.length} accounts`}
          </Button>
        </div>
      </div>
    );
  }

  // QBO path — auto-pull
  if (path === 'qbo') {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Chart of accounts</h2>
          <p className="text-slate mt-1">
            We'll pull your chart from QuickBooks now.
          </p>
        </div>
        <div className="rounded-xl bg-cloud p-8 text-center">
          {qboMutation.isPending ? (
            <>
              <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
              <p className="text-sm text-slate mt-3">
                Importing from QuickBooks…
              </p>
            </>
          ) : qboMutation.data ? (
            <>
              <CheckCircle2 className="h-10 w-10 text-bw-teal mx-auto" />
              <p className="text-lg font-bold text-deep-navy mt-3">
                {qboMutation.data.account_count} accounts imported
              </p>
              <p className="text-xs text-slate">
                {qboMutation.data.bank_account_count} bank-type accounts found
              </p>
            </>
          ) : (
            <Button
              variant="accent"
              size="lg"
              onClick={() => qboMutation.mutate()}
            >
              Pull from QuickBooks
            </Button>
          )}
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>
            ← Back
          </Button>
          <Button
            variant="accent"
            size="lg"
            onClick={next}
            disabled={!qboMutation.data && !existing}
          >
            Continue →
          </Button>
        </div>
      </div>
    );
  }

  // File path — upload
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Chart of accounts</h2>
        <p className="text-slate mt-1">
          Upload your existing chart of accounts. CSV, Excel, or QuickBooks
          export — we'll parse it.
        </p>
      </div>

      <button
        type="button"
        onClick={() => fileInputRef.current?.click()}
        className="w-full rounded-xl border-2 border-dashed border-border hover:border-ledger-blue p-10 text-center transition-colors"
        disabled={uploadMutation.isPending}
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
              Click to upload your chart
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
        <Button variant="ghost" onClick={prev}>
          ← Back
        </Button>
        <Button variant="ghost" onClick={next}>
          Skip for now →
        </Button>
      </div>
    </div>
  );
}
