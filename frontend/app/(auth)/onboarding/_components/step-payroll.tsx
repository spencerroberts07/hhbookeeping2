'use client';

import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

export function StepPayroll() {
  const store = useOnboardingStore();

  const setUsesEnet = (uses: boolean) =>
    store.setField('uses_enetemployer', uses);

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">Payroll</h2>
        <p className="text-slate">
          We support ENetEmployer payroll registers directly — upload the PDF
          each pay run, and BookWize parses the per-employee deductions and
          builds the payroll journal automatically.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <button
          type="button"
          onClick={() => setUsesEnet(true)}
          className={cn(
            'rounded-xl border p-5 text-left transition',
            store.uses_enetemployer === true
              ? 'border-deep-navy ring-2 ring-ledger-blue bg-cloud'
              : 'border-border bg-white hover:border-deep-navy/50',
          )}
        >
          <div className="font-semibold text-deep-navy">
            Yes — we use ENetEmployer
          </div>
          <div className="text-sm text-slate mt-1">
            Upload your register PDF each pay period from the Payroll page.
          </div>
        </button>
        <button
          type="button"
          onClick={() => setUsesEnet(false)}
          className={cn(
            'rounded-xl border p-5 text-left transition',
            store.uses_enetemployer === false
              ? 'border-deep-navy ring-2 ring-ledger-blue bg-cloud'
              : 'border-border bg-white hover:border-deep-navy/50',
          )}
        >
          <div className="font-semibold text-deep-navy">
            No — manual entry
          </div>
          <div className="text-sm text-slate mt-1">
            Enter hours manually each pay run. BookWize calculates CPP, EI,
            and tax using the 2026 CRA rates.
          </div>
        </button>
      </div>
      <div className="flex justify-between pt-4">
        <Button type="button" variant="ghost" onClick={() => store.goTo('chart')}>
          Back
        </Button>
        <Button
          type="button"
          onClick={() => store.goTo('invite')}
          disabled={store.uses_enetemployer === null}
        >
          Continue
        </Button>
      </div>
    </div>
  );
}
