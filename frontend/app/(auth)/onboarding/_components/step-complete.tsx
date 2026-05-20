'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { CheckCircle2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useOnboardingStore } from '@/lib/store/onboarding';

export function StepComplete() {
  const router = useRouter();
  const reset = useOnboardingStore((s) => s.reset);

  useEffect(() => {
    const t = setTimeout(() => {
      reset();
      router.push('/dashboard');
    }, 2500);
    return () => clearTimeout(t);
  }, [reset, router]);

  return (
    <div className="text-center py-8 space-y-4">
      <CheckCircle2 className="h-16 w-16 text-bw-teal mx-auto" strokeWidth={1.5} />
      <h2 className="text-h2 text-deep-navy">You&apos;re all set</h2>
      <p className="text-slate">
        Taking you to your dashboard…
      </p>
      <Button
        type="button"
        onClick={() => {
          reset();
          router.push('/dashboard');
        }}
      >
        Go to dashboard now
      </Button>
    </div>
  );
}
