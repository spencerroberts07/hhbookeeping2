'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { CalendarDays, ChevronRight } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import { MultiFileUpload } from '@/components/shared/multi-file-upload';
import { useUploadDefaults } from '@/lib/hooks/use-upload-defaults';
import { getStatDays } from '@/lib/api/payroll';
import { formatDate } from '@/lib/utils';

const STEPS = ['Period details', 'Hours (optional)', 'Register', 'Done'] as const;

export default function NewPayRunPage() {
  const router = useRouter();
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const uploadDefaults = useUploadDefaults();
  const [step, setStep] = useState(0);

  // Period details
  const today = new Date().toISOString().slice(0, 10);
  const [payRunNumber, setPayRunNumber] = useState('');
  const [periodNumber, setPeriodNumber] = useState<number | ''>('');
  const [periodStart, setPeriodStart] = useState('');
  const [periodEnd, setPeriodEnd] = useState('');
  const [payDate, setPayDate] = useState(today);

  const periodValid =
    !!payRunNumber &&
    !!periodNumber &&
    !!periodStart &&
    !!periodEnd &&
    !!payDate;

  if (!entityCode) {
    return (
      <>
        <Topbar title="New pay run" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">Pick an entity to start a pay run.</p>
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title="New pay run" />
      <main className="p-6 max-w-3xl space-y-4">
        <div className="flex items-center gap-2 text-sm flex-wrap">
          {STEPS.map((s, idx) => (
            <div key={s} className="flex items-center gap-2">
              <span
                className={`grid h-7 w-7 place-items-center rounded-full text-xs font-bold ${
                  idx === step
                    ? 'bg-deep-navy text-white'
                    : idx < step
                      ? 'bg-bw-teal text-white'
                      : 'bg-cloud text-slate'
                }`}
              >
                {idx + 1}
              </span>
              <span className={idx === step ? 'text-deep-navy font-semibold' : 'text-slate'}>
                {s}
              </span>
              {idx < STEPS.length - 1 && (
                <ChevronRight className="h-4 w-4 text-slate" strokeWidth={1.5} />
              )}
            </div>
          ))}
        </div>

        {periodStart && periodEnd && (
          <StatHolidayBanner periodStart={periodStart} periodEnd={periodEnd} />
        )}

        {step === 0 && (
          <Card>
            <CardHeader>
              <CardTitle>Period details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-sm text-slate">
                These details are used when parsing your hours / register files.
                Find them on your ENetEmployer Period Schedule for this run.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <Label htmlFor="prn">Pay run number</Label>
                  <Input
                    id="prn"
                    placeholder="e.g. P5"
                    value={payRunNumber}
                    onChange={(e) => setPayRunNumber(e.target.value)}
                  />
                </div>
                <div>
                  <Label htmlFor="pn">Period number</Label>
                  <Input
                    id="pn"
                    type="number"
                    min={1}
                    max={27}
                    placeholder="5"
                    value={periodNumber}
                    onChange={(e) =>
                      setPeriodNumber(e.target.value === '' ? '' : Number(e.target.value))
                    }
                  />
                </div>
                <div>
                  <Label htmlFor="ps">Period start</Label>
                  <Input
                    id="ps"
                    type="date"
                    value={periodStart}
                    onChange={(e) => setPeriodStart(e.target.value)}
                  />
                </div>
                <div>
                  <Label htmlFor="pe">Period end</Label>
                  <Input
                    id="pe"
                    type="date"
                    value={periodEnd}
                    onChange={(e) => setPeriodEnd(e.target.value)}
                  />
                </div>
                <div>
                  <Label htmlFor="pd">Pay date</Label>
                  <Input
                    id="pd"
                    type="date"
                    value={payDate}
                    onChange={(e) => setPayDate(e.target.value)}
                  />
                </div>
              </div>
              <div className="flex justify-end">
                <Button onClick={() => setStep(1)} disabled={!periodValid}>
                  Continue
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {step === 1 && (
          <Card>
            <CardHeader>
              <CardTitle>Hours file (optional)</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-sm text-slate">
                If your hours come from a scheduling system as an ODS file,
                upload it here. Otherwise skip — the register PDF carries
                everything we need.
              </p>
              <MultiFileUpload
                endpoint="/api/payroll/runs/upload-hours"
                fileKey="file"
                accept=".ods"
                extraFields={{
                  ...uploadDefaults,
                  pay_run_number: payRunNumber,
                  period_number: String(periodNumber),
                  period_start: periodStart,
                  period_end: periodEnd,
                  pay_date: payDate,
                }}
                label="Hours (ODS)"
                description="Scheduled hours by employee. Optional if the register has finalized numbers."
              />
              <div className="flex justify-between">
                <Button variant="ghost" onClick={() => setStep(0)}>
                  Back
                </Button>
                <Button onClick={() => setStep(2)}>Continue</Button>
              </div>
            </CardContent>
          </Card>
        )}

        {step === 2 && (
          <Card>
            <CardHeader>
              <CardTitle>Register PDF — primary input</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <p className="text-sm text-slate">
                Upload the ENetEmployer payroll register PDF. We&apos;ll parse
                per-employee deductions and build the payroll journal at
                status <code className="text-xs">draft_confirmed</code>.
              </p>
              <MultiFileUpload
                endpoint="/api/payroll/runs/upload-register"
                fileKey="file"
                accept=".pdf"
                extraFields={{
                  ...uploadDefaults,
                  pay_run_number: payRunNumber,
                  period_number: String(periodNumber),
                  pay_date: payDate,
                }}
                label="Register PDF"
                description="One file. Parses exact per-employee gross / deductions / net."
                onComplete={(results) => {
                  if (results.some((r) => r.status === 'success')) setStep(3);
                }}
              />
              <div className="flex justify-between">
                <Button variant="ghost" onClick={() => setStep(1)}>
                  Back
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {step === 3 && (
          <Card>
            <CardHeader>
              <CardTitle>Done</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-sm text-slate">
                Run is created as a draft. Open it from{' '}
                <Link href="/payroll" className="text-ledger-blue underline">
                  Pay runs
                </Link>{' '}
                to review line-by-line entries, build the journal, and submit
                for approval.
              </p>
              <Button onClick={() => router.push('/payroll')}>
                Go to pay runs
              </Button>
            </CardContent>
          </Card>
        )}
      </main>
    </>
  );
}

function StatHolidayBanner({
  periodStart,
  periodEnd,
}: {
  periodStart: string;
  periodEnd: string;
}) {
  // Pick the year of the period start. Cross-year periods are unusual
  // for bi-weekly payroll and we can ask the backend for both years if
  // needed later; for the common case one year is enough.
  const year = Number(periodStart.slice(0, 4));
  const q = useQuery({
    queryKey: ['stat-days', year],
    enabled: Number.isFinite(year),
    queryFn: () => getStatDays(year, 'ON'),
    staleTime: 24 * 60 * 60 * 1000,
  });
  const start = new Date(periodStart);
  const end = new Date(periodEnd);
  const matches = (q.data?.stat_days ?? []).filter((d) => {
    const od = new Date(d.observed_date);
    return od >= start && od <= end;
  });
  if (matches.length === 0) {
    return (
      <div className="rounded-md border border-border bg-cloud/40 px-3 py-2 text-xs text-slate flex items-center gap-2">
        <CalendarDays className="h-3 w-3" />
        No Ontario statutory holidays fall in this period.
      </div>
    );
  }
  return (
    <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900 flex items-start gap-2">
      <CalendarDays className="h-3 w-3 mt-0.5" />
      <div className="flex-1">
        <div className="font-semibold">
          {matches.length} stat day{matches.length === 1 ? '' : 's'} in this period:
        </div>
        <ul className="mt-1 space-y-0.5">
          {matches.map((d) => (
            <li key={d.observed_date} className="tabular-nums">
              {formatDate(d.observed_date)} — {d.holiday_name}
            </li>
          ))}
        </ul>
        <p className="mt-1 text-[10px] text-amber-900/80">
          Verify stat pay amounts in the register before approving.
        </p>
      </div>
    </div>
  );
}
