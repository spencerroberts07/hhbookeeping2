'use client';

import { useEffect, useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
  getGLHistoryProgress,
  getOnboardingStatus,
  startGLHistoryFromFile,
  startGLHistoryFromQbo,
  type GLProgressResponse,
} from '@/lib/api/onboarding';
import { CheckCircle2, Loader2, Upload } from 'lucide-react';
import { toast } from 'sonner';

function shiftYears(iso: string, years: number): string {
  const d = new Date(iso);
  d.setFullYear(d.getFullYear() - years);
  return d.toISOString().slice(0, 10);
}

export function StepGLHistory() {
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const path = useOnboardingStore((s) => s.connect_path);
  const cutover = useOnboardingStore((s) => s.cutover_date);
  const dateFrom = useOnboardingStore((s) => s.gl_date_from);
  const dateTo = useOnboardingStore((s) => s.gl_date_to);
  const setField = useOnboardingStore((s) => s.setField);
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);
  const qc = useQueryClient();

  // Defaults: 2 years back from cutover, ending at cutover.
  useEffect(() => {
    if (!dateFrom) setField('gl_date_from', shiftYears(cutover, 2));
    if (!dateTo) setField('gl_date_to', cutover);
  }, [cutover, dateFrom, dateTo, setField]);

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });
  const existing = status.data?.has_gl_history;

  const [jobId, setJobId] = useState<string | null>(null);
  const [progress, setProgress] = useState<GLProgressResponse | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  const startPolling = (id: string) => {
    stopPolling();
    pollRef.current = setInterval(async () => {
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
        // 404 or transient error — try again next tick
      }
    }, 3000);
  };

  useEffect(() => () => stopPolling(), []);

  const qboMutation = useMutation({
    mutationFn: () =>
      startGLHistoryFromQbo({
        entity_code: entityCode,
        actor_email: actor,
        date_from: dateFrom,
        date_to: dateTo,
      }),
    onSuccess: (res) => {
      setJobId(res.job_id);
      startPolling(res.job_id);
      toast.info('GL import started — this can take a few minutes');
    },
    onError: () => toast.error('Could not start QBO GL import'),
  });

  const fileInputRef = useRef<HTMLInputElement>(null);
  const fileMutation = useMutation({
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
      toast.info('GL parse started — this can take a few minutes');
    },
    onError: (err: Error) => toast.error(err.message || 'Upload failed'),
  });

  if (existing && !jobId) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Historical GL imported</h2>
          <p className="text-slate mt-1">
            {status.data?.journal_line_count} transactions loaded from{' '}
            <strong>{status.data?.gl_history_from}</strong> to{' '}
            <strong>{status.data?.gl_history_to}</strong>.
          </p>
        </div>
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
          <CheckCircle2 className="h-6 w-6 text-bw-teal" />
          <div className="flex-1">
            <div className="font-semibold text-deep-navy">
              {status.data?.journal_line_count} GL lines
            </div>
            <div className="text-xs text-slate">
              The AI assistant has already learned from your history.
            </div>
          </div>
          <Badge variant="complete">Done</Badge>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button variant="accent" size="lg" onClick={next}>
            Continue →
          </Button>
        </div>
      </div>
    );
  }

  // Active progress view
  if (jobId && progress && progress.status !== 'complete' && progress.status !== 'error') {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">Importing GL history…</h2>
          <p className="text-slate mt-1">
            This can take a few minutes for large date ranges.
          </p>
        </div>
        <div className="rounded-xl bg-cloud p-6 space-y-3">
          <div className="flex items-center gap-2">
            <Loader2 className="h-5 w-5 text-ledger-blue animate-spin" />
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
      </div>
    );
  }

  if (jobId && progress?.status === 'complete') {
    return (
      <div className="space-y-6">
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5">
          <div className="flex items-center gap-3 mb-3">
            <CheckCircle2 className="h-6 w-6 text-bw-teal" />
            <div className="font-semibold text-deep-navy">Import complete</div>
          </div>
          <div className="text-sm text-slate ml-9 space-y-1">
            <div>{progress.months_imported} months imported</div>
            <div>{progress.lines_created} transaction lines</div>
            <div>{progress.batches_created} journal batches</div>
          </div>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button variant="accent" size="lg" onClick={next}>Continue →</Button>
        </div>
      </div>
    );
  }

  if (jobId && progress?.status === 'error') {
    return (
      <div className="space-y-6">
        <div className="rounded-xl border-2 border-amber-300 bg-amber-50 p-5">
          <div className="font-semibold text-amber-900">Import failed</div>
          <div className="text-sm text-amber-900/80 mt-1">
            {progress.error || 'Unknown error'}
          </div>
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>← Back</Button>
          <Button
            variant="outline"
            onClick={() => {
              setJobId(null);
              setProgress(null);
            }}
          >
            Try again
          </Button>
          <Button variant="ghost" onClick={next}>Skip →</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Import historical transactions</h2>
        <p className="text-slate mt-1">
          The more history you import, the smarter BookWize gets — the AI
          assistant learns your vendors, accounts, and patterns.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <Label htmlFor="gl_from">From</Label>
          <Input
            id="gl_from"
            type="date"
            value={dateFrom}
            onChange={(e) => setField('gl_date_from', e.target.value)}
          />
        </div>
        <div>
          <Label htmlFor="gl_to">To</Label>
          <Input
            id="gl_to"
            type="date"
            value={dateTo}
            onChange={(e) => setField('gl_date_to', e.target.value)}
          />
        </div>
      </div>

      {path === 'qbo' ? (
        <div className="rounded-xl bg-cloud p-6 text-center">
          <p className="text-sm text-slate mb-3">
            We'll pull each month one at a time and log progress.
          </p>
          <Button
            variant="accent"
            size="lg"
            onClick={() => qboMutation.mutate()}
            disabled={qboMutation.isPending}
          >
            {qboMutation.isPending ? 'Starting…' : 'Import from QuickBooks'}
          </Button>
        </div>
      ) : (
        <>
          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            disabled={fileMutation.isPending}
            className="w-full rounded-xl border-2 border-dashed border-border hover:border-ledger-blue p-10 text-center transition-colors"
          >
            {fileMutation.isPending ? (
              <>
                <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
                <p className="text-sm text-slate mt-3">Starting…</p>
              </>
            ) : (
              <>
                <Upload className="h-8 w-8 text-ledger-blue mx-auto" />
                <p className="font-semibold text-deep-navy mt-3">
                  Click to upload your GL export
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
              if (f) fileMutation.mutate(f);
            }}
          />
        </>
      )}

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>← Back</Button>
        <Button variant="ghost" onClick={next}>Skip for now →</Button>
      </div>
    </div>
  );
}
