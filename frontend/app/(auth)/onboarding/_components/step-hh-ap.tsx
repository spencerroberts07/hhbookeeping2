'use client';

import { useState } from 'react';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { uploadHHAPDocuments } from '@/lib/api/hh_ap';
import { toast } from 'sonner';
import { useUser } from '@clerk/nextjs';
import { Upload } from 'lucide-react';

export function StepHHAP() {
  const store = useOnboardingStore();
  const { user } = useUser();
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);

  const onUpload = async () => {
    if (!file || !store.entity_code) return;
    setUploading(true);
    try {
      await uploadHHAPDocuments({
        entity_code: store.entity_code,
        document_type: 'monthly_statement',
        files: [file],
      });
      store.setField('hh_ap_sample_uploaded', true);
      toast.success('HH AP statement uploaded');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">HH AP statement</h2>
        <p className="text-slate">
          Upload one Home Hardware AP statement. We&apos;ll confirm the parser
          handles your statement format. You can skip and do this later from
          Settings → Documents.
        </p>
      </div>
      <div>
        <Label>HH AP monthly statement (PDF)</Label>
        <label className="flex items-center gap-3 rounded-xl border border-dashed border-input bg-cloud p-4 cursor-pointer hover:bg-white transition">
          <Upload className="h-5 w-5 text-slate" strokeWidth={1.5} />
          <span className="text-sm text-slate">
            {file ? file.name : 'Drag-drop or click to upload'}
          </span>
          <input
            type="file"
            accept="application/pdf"
            className="hidden"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
        </label>
        {file && (
          <Button
            type="button"
            variant="secondary"
            size="sm"
            className="mt-2"
            onClick={onUpload}
            disabled={uploading || !user?.primaryEmailAddress}
          >
            {uploading ? 'Uploading…' : 'Upload sample'}
          </Button>
        )}
        {store.hh_ap_sample_uploaded && (
          <p className="text-xs text-bw-teal mt-2">Statement parsed successfully</p>
        )}
      </div>
      <div className="flex justify-between pt-4">
        <Button type="button" variant="ghost" onClick={() => store.goTo('bank')}>
          Back
        </Button>
        <Button type="button" onClick={() => store.goTo('chart')}>
          Continue
        </Button>
      </div>
    </div>
  );
}
