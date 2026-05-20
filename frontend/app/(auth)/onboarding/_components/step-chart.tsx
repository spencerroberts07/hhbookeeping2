'use client';

import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

export function StepChart() {
  const store = useOnboardingStore();

  const Choice = ({
    value,
    title,
    body,
  }: {
    value: 'standard' | 'custom';
    title: string;
    body: string;
  }) => {
    const active = store.chart_choice === value;
    return (
      <button
        type="button"
        onClick={() => store.setField('chart_choice', value)}
        className={cn(
          'w-full text-left rounded-xl border p-5 transition',
          active
            ? 'border-deep-navy ring-2 ring-ledger-blue bg-cloud'
            : 'border-border bg-white hover:border-deep-navy/50',
        )}
      >
        <div className="font-semibold text-deep-navy">{title}</div>
        <div className="text-sm text-slate mt-1">{body}</div>
      </button>
    );
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">Chart of accounts</h2>
        <p className="text-slate">
          Most dealers use the BookWize standard chart, which mirrors the
          Home Hardware dealer reporting structure. Pick standard unless you
          have a custom chart to import.
        </p>
      </div>
      <div className="space-y-3">
        <Choice
          value="standard"
          title="BookWize standard chart"
          body="Pre-built for HH dealers — lumber, hardware, paint, garden centre, payroll, HH AP, dating, etc."
        />
        <Choice
          value="custom"
          title="Custom chart"
          body="Upload your own chart of accounts later from Settings → Chart of accounts."
        />
      </div>
      <div className="flex justify-between pt-4">
        <Button type="button" variant="ghost" onClick={() => store.goTo('hh-ap')}>
          Back
        </Button>
        <Button
          type="button"
          onClick={() => store.goTo('payroll')}
          disabled={!store.chart_choice}
        >
          Continue
        </Button>
      </div>
    </div>
  );
}
