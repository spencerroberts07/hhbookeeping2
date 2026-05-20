'use client';

import Image from 'next/image';
import { Check } from 'lucide-react';
import { STEP_ORDER, useOnboardingStore } from '@/lib/store/onboarding';
import { cn } from '@/lib/utils';

const STEP_LABELS: Record<(typeof STEP_ORDER)[number], string> = {
  welcome: 'Store details',
  bank: 'Bank connection',
  'hh-ap': 'HH AP',
  chart: 'Chart of accounts',
  payroll: 'Payroll',
  invite: 'Invite team',
  billing: 'Choose plan',
  complete: 'Done',
};

export function OnboardingShell({ children }: { children: React.ReactNode }) {
  const current = useOnboardingStore((s) => s.currentStep);
  const currentIdx = STEP_ORDER.indexOf(current);

  return (
    <div className="min-h-screen flex flex-col bg-cloud">
      <header className="border-b border-border bg-white px-6 py-4">
        <Image
          src="/brand/bookwize-logo-primary.svg"
          alt="BookWize"
          width={140}
          height={36}
        />
      </header>
      <div className="flex-1 mx-auto w-full max-w-4xl px-4 py-8">
        <ol className="mb-8 grid grid-cols-4 md:grid-cols-8 gap-2">
          {STEP_ORDER.map((s, idx) => {
            const done = idx < currentIdx;
            const active = idx === currentIdx;
            return (
              <li key={s} className="flex flex-col items-center gap-1">
                <div
                  className={cn(
                    'grid h-7 w-7 place-items-center rounded-full border text-xs font-semibold',
                    done && 'bg-bw-teal border-bw-teal text-white',
                    active && 'bg-deep-navy border-deep-navy text-white',
                    !done && !active && 'border-border bg-white text-slate',
                  )}
                  aria-current={active ? 'step' : undefined}
                >
                  {done ? <Check className="h-4 w-4" strokeWidth={2} /> : idx + 1}
                </div>
                <div className="text-[10px] text-center text-slate hidden md:block">
                  {STEP_LABELS[s]}
                </div>
              </li>
            );
          })}
        </ol>
        <div className="bg-white rounded-2xl border border-border shadow-sm p-8">
          {children}
        </div>
      </div>
    </div>
  );
}
