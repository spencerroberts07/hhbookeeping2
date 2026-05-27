'use client';

import { Suspense, useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { useUser } from '@clerk/nextjs';
import { useQuery } from '@tanstack/react-query';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { useEntityStore } from '@/lib/store/entity';
import { getOnboardingStatus } from '@/lib/api/onboarding';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';
import { AlertTriangle } from 'lucide-react';
import { StepWelcome } from './_components/step-welcome';
import { StepConnect } from './_components/step-connect';
import { StepChart } from './_components/step-chart';
import { StepCutover } from './_components/step-cutover';
import { StepOpening } from './_components/step-opening';
import { StepGLHistory } from './_components/step-gl-history';
import { StepHHAP } from './_components/step-hh-ap';
import { StepComplete } from './_components/step-complete';

export default function OnboardingPage() {
  return (
    <Suspense fallback={<OnboardingSkeleton />}>
      <OnboardingContent />
    </Suspense>
  );
}

function OnboardingSkeleton() {
  return (
    <div className="space-y-3">
      <Skeleton className="h-8 w-1/2" />
      <Skeleton className="h-4 w-3/4" />
      <Skeleton className="h-32 w-full" />
    </div>
  );
}

// Hard cap on how long we'll keep the page-level skeleton visible
// before surfacing an error UI. React Query will keep retrying with
// backoff (~30s total across 3 attempts) — without this fallback the
// user can sit on a spinner for that whole window with no recourse.
const STATUS_SKELETON_TIMEOUT_MS = 15_000;

function OnboardingContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const force = searchParams.get('force') === 'true';
  const { isLoaded, isSignedIn } = useUser();
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const currentStep = useOnboardingStore((s) => s.currentStep);
  const goTo = useOnboardingStore((s) => s.goTo);

  useEffect(() => {
    if (isLoaded && !isSignedIn) router.replace('/sign-in');
  }, [isLoaded, isSignedIn, router]);

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode && isSignedIn === true,
    queryFn: () => getOnboardingStatus(entityCode!),
  });

  useEffect(() => {
    if (!status.data) return;
    if (status.data.onboarding_complete && !force) {
      router.replace('/dashboard');
      return;
    }
    const hasAnyData =
      status.data.has_chart_of_accounts ||
      status.data.journal_line_count > 0;
    if (currentStep === 'welcome' && hasAnyData) {
      if (!status.data.has_chart_of_accounts) goTo('connect');
      else if (!status.data.has_opening_balances) goTo('cutover');
      else if (!status.data.has_gl_history) goTo('gl-history');
      else goTo('hh-ap');
    }
  }, [status.data, currentStep, goTo, router, force]);

  // Surface an explicit error / timeout UI when the status query
  // can't resolve — otherwise users sit on the page-level skeleton
  // indefinitely while React Query retries silently.
  const [skeletonTimedOut, setSkeletonTimedOut] = useState(false);
  useEffect(() => {
    if (!status.isLoading) {
      setSkeletonTimedOut(false);
      return;
    }
    const t = setTimeout(() => setSkeletonTimedOut(true), STATUS_SKELETON_TIMEOUT_MS);
    return () => clearTimeout(t);
  }, [status.isLoading]);

  if (!isLoaded || (status.isLoading && !skeletonTimedOut)) {
    return <OnboardingSkeleton />;
  }

  if (status.isError || (status.isLoading && skeletonTimedOut)) {
    return <StatusErrorState onRetry={() => status.refetch()} timedOut={skeletonTimedOut} />;
  }

  if (!entityCode) {
    return (
      <div className="space-y-4">
        <h2 className="text-h2 text-deep-navy">No store selected</h2>
        <p className="text-slate">
          We can't find a store linked to your account yet. Try refreshing — if
          this persists, contact support.
        </p>
        <Button variant="outline" onClick={() => router.push('/dashboard')}>
          Go to dashboard
        </Button>
      </div>
    );
  }

  return (
    <>
      {currentStep === 'welcome' && <StepWelcome />}
      {currentStep === 'connect' && <StepConnect />}
      {currentStep === 'chart' && <StepChart />}
      {currentStep === 'cutover' && <StepCutover />}
      {currentStep === 'opening' && <StepOpening />}
      {currentStep === 'gl-history' && <StepGLHistory />}
      {currentStep === 'hh-ap' && <StepHHAP />}
      {currentStep === 'complete' && <StepComplete />}
    </>
  );
}

function StatusErrorState({
  onRetry,
  timedOut,
}: {
  onRetry: () => void;
  timedOut: boolean;
}) {
  const router = useRouter();
  return (
    <div className="space-y-5">
      <div className="flex items-start gap-3 rounded-xl border-2 border-amber-300 bg-amber-50 p-5">
        <AlertTriangle className="h-6 w-6 text-amber-700 shrink-0 mt-0.5" />
        <div className="flex-1">
          <div className="font-semibold text-amber-900">
            {timedOut
              ? "We're having trouble reaching the server"
              : "Couldn't load your setup status"}
          </div>
          <div className="text-sm text-amber-900/80 mt-1">
            {timedOut
              ? 'The request is taking longer than expected. Your connection may be slow, or the server may be busy.'
              : 'Something went wrong fetching your onboarding progress. Try again — if this keeps happening, contact support.'}
          </div>
        </div>
      </div>
      <div className="flex justify-between">
        <Button variant="outline" onClick={() => router.push('/dashboard')}>
          Go to dashboard
        </Button>
        <Button variant="accent" onClick={onRetry}>
          Try again
        </Button>
      </div>
    </div>
  );
}
