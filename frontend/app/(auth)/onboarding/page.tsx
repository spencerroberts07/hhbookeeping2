'use client';

import { useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { useUser } from '@clerk/nextjs';
import { useQuery } from '@tanstack/react-query';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { useEntityStore } from '@/lib/store/entity';
import { getOnboardingStatus } from '@/lib/api/onboarding';
import { Skeleton } from '@/components/ui/skeleton';
import { StepWelcome } from './_components/step-welcome';
import { StepConnect } from './_components/step-connect';
import { StepChart } from './_components/step-chart';
import { StepCutover } from './_components/step-cutover';
import { StepOpening } from './_components/step-opening';
import { StepGLHistory } from './_components/step-gl-history';
import { StepHHAP } from './_components/step-hh-ap';
import { StepComplete } from './_components/step-complete';

export default function OnboardingPage() {
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

  // Pre-fill the wizard's starting step based on what's already done.
  // Bridlewood-style dealers (partial setup) land at the first
  // unfinished step instead of being marched through completed steps.
  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode && isSignedIn === true,
    queryFn: () => getOnboardingStatus(entityCode!),
  });

  useEffect(() => {
    if (!status.data) return;
    // ?force=true bypasses the onboarding_complete redirect so admins
    // can re-enter the wizard after setup — e.g. to re-connect QBO to
    // a different company file or re-import the GL.
    if (status.data.onboarding_complete && !force) {
      router.replace('/dashboard');
      return;
    }
    // Pre-fill the cursor for dealers resuming with partial data
    // (Bridlewood-style). Brand-new dealers (no chart and no journal
    // lines) stay on welcome so they see the intro card first.
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

  if (!isLoaded || status.isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-8 w-1/2" />
        <Skeleton className="h-4 w-3/4" />
        <Skeleton className="h-32 w-full" />
      </div>
    );
  }

  if (!entityCode) {
    return (
      <div className="space-y-4">
        <h2 className="text-h2 text-deep-navy">No store selected</h2>
        <p className="text-slate">
          We can't find a store linked to your account yet. Try refreshing — if
          this persists, contact support.
        </p>
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
