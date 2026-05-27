'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import {
  completeOnboarding,
  getOnboardingStatus,
  type CompleteOnboardingResponse,
} from '@/lib/api/onboarding';
import { AlertTriangle, CheckCircle2, Sparkles } from 'lucide-react';
import { formatMoney as _formatMoney } from '@/lib/utils';

void _formatMoney; // currently unused — kept for future per-account totals on this screen

// Cap how long we wait for /api/onboarding/complete before surfacing
// an error UI with a retry button. Previously this view had only a
// skeleton fallback — if the call hung the user was stuck.
const COMPLETE_TIMEOUT_MS = 30_000;

export function StepComplete() {
  const router = useRouter();
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const reset = useOnboardingStore((s) => s.reset);

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });

  const completeRan = useRef(false);
  const completeMutation = useMutation<CompleteOnboardingResponse>({
    mutationFn: () =>
      completeOnboarding({ entity_code: entityCode, actor_email: actor }),
  });

  const [timedOut, setTimedOut] = useState(false);

  useEffect(() => {
    if (
      !completeRan.current &&
      entityCode &&
      actor &&
      !completeMutation.data &&
      !completeMutation.isPending
    ) {
      completeRan.current = true;
      setTimedOut(false);
      completeMutation.mutate();
    }
  }, [entityCode, actor, completeMutation]);

  // Pending-state timeout. If /complete hangs, show the error UI
  // instead of leaving the user on Skeleton tiles.
  useEffect(() => {
    if (!completeMutation.isPending) {
      setTimedOut(false);
      return;
    }
    const t = setTimeout(() => setTimedOut(true), COMPLETE_TIMEOUT_MS);
    return () => clearTimeout(t);
  }, [completeMutation.isPending]);

  const retry = () => {
    completeMutation.reset();
    setTimedOut(false);
    completeRan.current = true;
    completeMutation.mutate();
  };

  const summary = completeMutation.data;

  // Error or timeout state — give the user a real exit path.
  if (completeMutation.isError || (completeMutation.isPending && timedOut)) {
    const message =
      completeMutation.error instanceof Error
        ? completeMutation.error.message
        : 'The finishing-up step is taking longer than expected.';
    return (
      <div className="space-y-6">
        <div className="flex items-start gap-3 rounded-xl border-2 border-amber-300 bg-amber-50 p-5">
          <AlertTriangle className="h-6 w-6 text-amber-700 shrink-0 mt-0.5" />
          <div className="flex-1">
            <div className="font-semibold text-amber-900">
              Couldn't finish setting up
            </div>
            <div className="text-sm text-amber-900/80 mt-1">{message}</div>
          </div>
        </div>
        <div className="flex justify-between">
          <Button
            variant="outline"
            onClick={() => {
              reset();
              router.push('/dashboard');
            }}
          >
            Go to dashboard anyway
          </Button>
          <Button variant="accent" onClick={retry}>
            Try again
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="text-center py-4 space-y-6">
      <div className="flex justify-center">
        <div className="grid h-20 w-20 place-items-center rounded-full bg-bw-teal/10">
          <CheckCircle2 className="h-12 w-12 text-bw-teal" strokeWidth={1.5} />
        </div>
      </div>
      <div>
        <h1 className="text-h1 text-deep-navy">Your books are live</h1>
        <p className="text-slate mt-1">
          BookWize is ready. Here's what we set up.
        </p>
      </div>

      {completeMutation.isPending || !summary ? (
        <div className="grid grid-cols-2 gap-3 max-w-md mx-auto">
          <Skeleton className="h-20 w-full" />
          <Skeleton className="h-20 w-full" />
          <Skeleton className="h-20 w-full" />
          <Skeleton className="h-20 w-full" />
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3 max-w-md mx-auto">
          <SummaryTile
            label="Accounts loaded"
            value={`${summary.accounts_loaded}`}
          />
          <SummaryTile
            label="History imported"
            value={
              status.data?.gl_history_from && status.data?.gl_history_to
                ? `${monthsBetween(status.data.gl_history_from, status.data.gl_history_to)} months`
                : `${summary.journal_lines_loaded} lines`
            }
          />
          <SummaryTile
            label="Opening balance"
            value={
              status.data?.opening_balance_date
                ? new Date(status.data.opening_balance_date).toLocaleDateString()
                : '—'
            }
          />
          <SummaryTile
            label="AI ready"
            value={`${summary.vendors_learned} vendors`}
            icon={<Sparkles className="h-4 w-4 text-bw-teal" />}
          />
        </div>
      )}

      {summary && (
        <p className="text-sm text-slate max-w-md mx-auto">
          Your AI assistant has learned <strong>{summary.vendors_learned}</strong>{' '}
          vendor patterns from your history. It'll classify new transactions
          using what it learned.
        </p>
      )}

      <Button
        variant="accent"
        size="lg"
        className="min-w-48"
        onClick={() => {
          reset();
          router.push('/dashboard');
        }}
        disabled={completeMutation.isPending}
      >
        Go to dashboard →
      </Button>
    </div>
  );
}

function SummaryTile({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon?: React.ReactNode;
}) {
  return (
    <div className="rounded-xl border border-border bg-cloud p-4 text-left">
      <div className="text-xs text-slate flex items-center gap-1">
        {icon}
        {label}
      </div>
      <div className="text-lg font-bold text-deep-navy mt-1 tabular-nums">
        {value}
      </div>
    </div>
  );
}

function monthsBetween(fromISO: string, toISO: string): number {
  const f = new Date(fromISO);
  const t = new Date(toISO);
  return Math.max(
    1,
    (t.getFullYear() - f.getFullYear()) * 12 + (t.getMonth() - f.getMonth()) + 1,
  );
}
