'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useUser } from '@clerk/nextjs';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { OnboardingShell } from './_components/shell';
import { StepWelcome } from './_components/step-welcome';
import { StepBank } from './_components/step-bank';
import { StepHHAP } from './_components/step-hh-ap';
import { StepChart } from './_components/step-chart';
import { StepPayroll } from './_components/step-payroll';
import { StepInvite } from './_components/step-invite';
import { StepBilling } from './_components/step-billing';
import { StepComplete } from './_components/step-complete';

export default function OnboardingPage() {
  const router = useRouter();
  const { isLoaded, isSignedIn } = useUser();
  const currentStep = useOnboardingStore((s) => s.currentStep);

  useEffect(() => {
    if (isLoaded && !isSignedIn) router.replace('/sign-in');
  }, [isLoaded, isSignedIn, router]);

  if (!isLoaded) return null;

  return (
    <OnboardingShell>
      {currentStep === 'welcome' && <StepWelcome />}
      {currentStep === 'bank' && <StepBank />}
      {currentStep === 'hh-ap' && <StepHHAP />}
      {currentStep === 'chart' && <StepChart />}
      {currentStep === 'payroll' && <StepPayroll />}
      {currentStep === 'invite' && <StepInvite />}
      {currentStep === 'billing' && <StepBilling />}
      {currentStep === 'complete' && <StepComplete />}
    </OnboardingShell>
  );
}
