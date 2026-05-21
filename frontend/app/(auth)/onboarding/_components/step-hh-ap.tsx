'use client';

import { useRef, useState } from 'react';
import { useUser } from '@clerk/nextjs';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { getOnboardingStatus } from '@/lib/api/onboarding';
import { uploadHHAPDocuments } from '@/lib/api/hh_ap';
import { CheckCircle2, Loader2, Upload } from 'lucide-react';
import { toast } from 'sonner';

export function StepHHAP() {
  const { user } = useUser();
  const entityCode = useEntityStore((s) => s.activeEntityCode)!;
  const next = useOnboardingStore((s) => s.next);
  const prev = useOnboardingStore((s) => s.prev);
  const qc = useQueryClient();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    queryFn: () => getOnboardingStatus(entityCode),
  });

  const fileInputRef = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);

  const onUpload = async (files: File[]) => {
    if (!files.length) return;
    setBusy(true);
    try {
      await uploadHHAPDocuments({
        entity_code: entityCode,
        document_type: 'monthly_statement',
        files,
      });
      toast.success(`Uploaded ${files.length} statement${files.length === 1 ? '' : 's'}`);
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
    } catch (err) {
      toast.error('Statement upload failed');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">Home Hardware AP statements</h2>
        <p className="text-slate mt-1">
          Upload your monthly HH statements so your AP balance matches HH records.
          You can also add these later from <em>AP → HH</em>.
        </p>
      </div>

      {status.data?.has_hh_ap_history && (
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 flex items-center gap-3">
          <CheckCircle2 className="h-6 w-6 text-bw-teal" />
          <div className="flex-1">
            <div className="font-semibold text-deep-navy">
              {status.data.hh_ap_months_loaded} month
              {status.data.hh_ap_months_loaded === 1 ? '' : 's'} loaded
            </div>
            <div className="text-xs text-slate">You can add more or skip ahead.</div>
          </div>
          <Badge variant="complete">Done</Badge>
        </div>
      )}

      <button
        type="button"
        onClick={() => fileInputRef.current?.click()}
        disabled={busy}
        className="w-full rounded-xl border-2 border-dashed border-border hover:border-ledger-blue p-10 text-center transition-colors"
      >
        {busy ? (
          <>
            <Loader2 className="h-8 w-8 text-ledger-blue mx-auto animate-spin" />
            <p className="text-sm text-slate mt-3">Uploading…</p>
          </>
        ) : (
          <>
            <Upload className="h-8 w-8 text-ledger-blue mx-auto" />
            <p className="font-semibold text-deep-navy mt-3">
              Click to upload HH AP statement PDFs
            </p>
            <p className="text-xs text-slate mt-1">
              Drop multiple months at once — we parse each one.
            </p>
          </>
        )}
      </button>
      <input
        ref={fileInputRef}
        type="file"
        accept="application/pdf"
        multiple
        className="hidden"
        onChange={(e) => {
          const files = Array.from(e.target.files ?? []);
          if (files.length) onUpload(files);
          e.target.value = '';
        }}
      />

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>← Back</Button>
        <Button variant="accent" size="lg" onClick={next}>
          Continue →
        </Button>
      </div>
    </div>
  );
}
