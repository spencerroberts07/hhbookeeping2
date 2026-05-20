'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Upload, ChevronRight } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import { uploadPayrollRegister } from '@/lib/api/payroll';
import { toast } from 'sonner';

const STEPS = ['Hours', 'Preview', 'Register', 'Review', 'Approve'] as const;

export default function NewPayRunPage() {
  const router = useRouter();
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const [step, setStep] = useState(0);
  const [register, setRegister] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);

  const onUploadRegister = async () => {
    if (!register || !entityCode) return;
    setUploading(true);
    try {
      const result = await uploadPayrollRegister({
        entity_code: entityCode,
        actor_email: actorEmail,
        file: register,
      });
      toast.success('Payroll register parsed');
      setStep(3);
      // eslint-disable-next-line no-console
      console.log('payroll parse result', result);
    } finally {
      setUploading(false);
    }
  };

  return (
    <>
      <Topbar title="New pay run" />
      <main className="p-6 max-w-3xl space-y-4">
        <div className="flex items-center gap-2 text-sm">
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

        <Card>
          <CardContent className="p-6">
            {step === 0 && (
              <div className="space-y-3">
                <h3 className="text-h2 text-deep-navy">Upload hours</h3>
                <p className="text-sm text-slate">
                  Upload your weekly hours ODS file from the scheduling system,
                  or enter hours manually. You can also skip this step if your
                  ENetEmployer register already contains the final numbers.
                </p>
                <div className="flex gap-2">
                  <Button variant="secondary">
                    <Upload className="h-4 w-4" strokeWidth={1.5} />
                    Upload ODS
                  </Button>
                  <Button onClick={() => setStep(2)}>Skip to register</Button>
                </div>
              </div>
            )}
            {step === 2 && (
              <div className="space-y-3">
                <h3 className="text-h2 text-deep-navy">Upload register PDF</h3>
                <p className="text-sm text-slate">
                  Drop your ENetEmployer payroll register PDF. We&apos;ll parse the
                  per-employee deductions and build the journal automatically.
                </p>
                <label className="flex items-center gap-3 rounded-xl border border-dashed border-input bg-cloud p-4 cursor-pointer hover:bg-white transition">
                  <Upload className="h-5 w-5 text-slate" strokeWidth={1.5} />
                  <span className="text-sm text-slate">
                    {register ? register.name : 'Click to upload PDF'}
                  </span>
                  <input
                    type="file"
                    accept="application/pdf"
                    className="hidden"
                    onChange={(e) => setRegister(e.target.files?.[0] ?? null)}
                  />
                </label>
                {register && (
                  <Button onClick={onUploadRegister} disabled={uploading}>
                    {uploading ? 'Parsing…' : 'Parse register'}
                  </Button>
                )}
              </div>
            )}
            {step === 3 && (
              <div className="space-y-3">
                <h3 className="text-h2 text-deep-navy">Review journal</h3>
                <p className="text-sm text-slate">
                  Review the generated payroll journal preview before approving
                  and posting.
                </p>
                <p className="text-xs text-slate">
                  Open the new run from{' '}
                  <Link href="/payroll" className="text-ledger-blue underline">
                    Pay runs
                  </Link>{' '}
                  to see line-by-line entries and approve.
                </p>
                <Button variant="primary" onClick={() => router.push('/payroll')}>
                  Done
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </main>
    </>
  );
}
