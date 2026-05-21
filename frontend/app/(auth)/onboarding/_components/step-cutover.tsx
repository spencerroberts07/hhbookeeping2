'use client';

import { useMemo } from 'react';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

function fiscalStartISO(): string {
  const today = new Date();
  // Default: most-recent Oct 1 (HH fiscal year start).
  const year = today.getMonth() >= 9 ? today.getFullYear() : today.getFullYear() - 1;
  return `${year}-10-01`;
}

function calendarStartISO(): string {
  return `${new Date().getFullYear()}-01-01`;
}

function diffMonths(from: string, to: string): number {
  const f = new Date(from);
  const t = new Date(to);
  return (
    (t.getFullYear() - f.getFullYear()) * 12 + (t.getMonth() - f.getMonth())
  );
}

export function StepCutover() {
  const cutover = useOnboardingStore((s) => s.cutover_date);
  const setField = useOnboardingStore((s) => s.setField);
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);

  const fy = fiscalStartISO();
  const cal = calendarStartISO();
  const today = new Date().toISOString().slice(0, 10);

  const choice: 'fy' | 'cal' | 'custom' = useMemo(() => {
    if (cutover === fy) return 'fy';
    if (cutover === cal) return 'cal';
    return 'custom';
  }, [cutover, fy, cal]);

  const months = diffMonths(cutover, today);

  const pick = (val: string) => setField('cutover_date', val);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Pick your cut-over date</h2>
        <p className="text-slate mt-1">
          When should BookWize's books start? Everything before this is
          historical; everything after is live.
        </p>
      </div>

      <div className="space-y-3">
        <Radio
          checked={choice === 'fy'}
          onClick={() => pick(fy)}
          title={`Start of current fiscal year (Oct 1, ${fy.slice(0, 4)})`}
          subtitle="Recommended — gives you a full year of comparatives"
        />
        <Radio
          checked={choice === 'cal'}
          onClick={() => pick(cal)}
          title={`Start of current calendar year (Jan 1, ${cal.slice(0, 4)})`}
        />
        <div
          className={
            'rounded-xl border-2 p-4 transition-colors ' +
            (choice === 'custom'
              ? 'border-ledger-blue bg-cloud/60'
              : 'border-border')
          }
        >
          <div className="flex items-center gap-3">
            <input
              type="radio"
              checked={choice === 'custom'}
              onChange={() => {
                /* date input below drives it */
              }}
              className="h-4 w-4"
              aria-label="Custom date"
            />
            <Label htmlFor="cutover" className="flex-1">
              Custom date
            </Label>
            <Input
              id="cutover"
              type="date"
              value={cutover}
              onChange={(e) => pick(e.target.value)}
              max={today}
              className="w-44"
            />
          </div>
        </div>
      </div>

      <div className="rounded-xl bg-cloud p-4 text-sm text-slate">
        You'll import history from <strong>{cutover}</strong> to today —
        approximately <strong>{Math.max(0, months)} months</strong> of data.
      </div>

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>
          ← Back
        </Button>
        <Button variant="accent" size="lg" onClick={next}>
          Continue →
        </Button>
      </div>
    </div>
  );
}

function Radio({
  checked,
  onClick,
  title,
  subtitle,
}: {
  checked: boolean;
  onClick: () => void;
  title: string;
  subtitle?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        'w-full text-left rounded-xl border-2 p-4 transition-colors flex items-center gap-3 ' +
        (checked ? 'border-ledger-blue bg-cloud/60' : 'border-border hover:border-ledger-blue/50')
      }
    >
      <span
        className={
          'h-4 w-4 rounded-full border-2 grid place-items-center ' +
          (checked ? 'border-ledger-blue' : 'border-border')
        }
      >
        {checked && <span className="h-2 w-2 rounded-full bg-ledger-blue" />}
      </span>
      <span className="flex-1">
        <span className="block font-semibold text-deep-navy">{title}</span>
        {subtitle && <span className="block text-xs text-slate mt-0.5">{subtitle}</span>}
      </span>
    </button>
  );
}
