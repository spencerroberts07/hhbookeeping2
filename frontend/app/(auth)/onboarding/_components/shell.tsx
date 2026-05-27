'use client';

import Image from 'next/image';
import Link from 'next/link';
import { Check, LogOut } from 'lucide-react';
import { STEP_ORDER, useOnboardingStore, type OnboardingStep } from '@/lib/store/onboarding';
import { cn } from '@/lib/utils';

const STEP_LABELS: Record<OnboardingStep, string> = {
  welcome: 'Welcome',
  connect: 'Connect',
  chart: 'Chart',
  cutover: 'Cut-over',
  opening: 'Opening',
  'gl-history': 'History',
  'hh-ap': 'HH AP',
  complete: 'Live',
};

export function OnboardingShell({ children }: { children: React.ReactNode }) {
  const current = useOnboardingStore((s) => s.currentStep);
  const goTo = useOnboardingStore((s) => s.goTo);
  const currentIdx = STEP_ORDER.indexOf(current);
  const pct =
    currentIdx >= 0 ? Math.round((currentIdx / (STEP_ORDER.length - 1)) * 100) : 0;

  return (
    <div className="min-h-screen bg-deep-navy text-white">
      <header className="border-b border-white/10 px-6 py-4 flex items-center gap-4">
        <Image
          src="/brand/bookwize-logo-reversed.svg"
          alt="BookWize"
          width={140}
          height={36}
          priority
        />
        <div className="flex-1" />
        <div className="text-xs text-white/60">
          Step {Math.min(currentIdx + 1, STEP_ORDER.length)} of {STEP_ORDER.length}
        </div>
        {/* Always-visible escape hatch. Onboarding state is persisted in
            zustand+localStorage, so navigating to /dashboard doesn't reset
            progress — the user can return and resume from the Setup
            sidebar entry. */}
        <Link
          href="/dashboard"
          className="ml-3 inline-flex items-center gap-1.5 rounded-md border border-white/20 px-2.5 py-1.5 text-xs font-medium text-white/80 hover:bg-white/10 hover:text-white transition-colors"
          aria-label="Exit to dashboard (progress is saved)"
        >
          <LogOut className="h-3.5 w-3.5" strokeWidth={1.5} />
          Exit to dashboard
        </Link>
      </header>

      {/* Progress bar */}
      <div className="h-1 bg-white/10">
        <div
          className="h-1 bg-bw-teal transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>

      {/* Step pills — completed steps are clickable; future steps locked.
          Horizontal scroll on narrow screens so the row never wraps. */}
      <div className="mx-auto max-w-3xl px-4 pt-8">
        <ol className="flex md:grid md:grid-cols-8 gap-2 mb-8 overflow-x-auto pb-2 md:overflow-visible">
          {STEP_ORDER.map((s, idx) => {
            const done = idx < currentIdx;
            const active = idx === currentIdx;
            const clickable = done;
            return (
              <li
                key={s}
                className="flex flex-col items-center gap-1 shrink-0 min-w-[64px] md:min-w-0"
              >
                <button
                  type="button"
                  onClick={clickable ? () => goTo(s) : undefined}
                  disabled={!clickable && !active}
                  aria-current={active ? 'step' : undefined}
                  aria-label={`Step ${idx + 1}: ${STEP_LABELS[s]}${done ? ' (completed — click to revisit)' : active ? ' (current)' : ' (locked)'}`}
                  className={cn(
                    'grid h-7 w-7 place-items-center rounded-full border text-xs font-semibold transition-colors',
                    done && 'bg-bw-teal border-bw-teal text-white cursor-pointer hover:ring-2 hover:ring-bw-teal/40',
                    active && 'bg-white border-white text-deep-navy cursor-default',
                    !done && !active && 'border-white/30 bg-transparent text-white/60 cursor-not-allowed',
                  )}
                >
                  {done ? <Check className="h-4 w-4" strokeWidth={2.5} /> : idx + 1}
                </button>
                <div
                  className={cn(
                    'text-[10px] text-center',
                    active ? 'text-white font-medium' : 'text-white/50',
                  )}
                >
                  {STEP_LABELS[s]}
                </div>
              </li>
            );
          })}
        </ol>
      </div>

      {/* Card */}
      <main className="mx-auto max-w-2xl px-4 pb-16">
        <div className="bg-white text-ink rounded-2xl border border-white/10 shadow-2xl p-8">
          {children}
        </div>
      </main>
    </div>
  );
}
