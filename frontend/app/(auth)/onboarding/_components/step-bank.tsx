'use client';

import { useState } from 'react';
import { useOnboardingStore, getPrevStep } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { previewBankPdf } from '@/lib/api/bank';
import { toast } from 'sonner';
import { Upload } from 'lucide-react';

const BANK_TYPES = [
  { code: 'td', label: 'TD Canada Trust' },
  { code: 'rbc', label: 'RBC' },
  { code: 'scotia', label: 'Scotiabank' },
  { code: 'bmo', label: 'BMO' },
  { code: 'cibc', label: 'CIBC' },
  { code: 'other', label: 'Other (CSV upload)' },
];

export function StepBank() {
  const store = useOnboardingStore();
  const [file, setFile] = useState<File | null>(null);
  const [previewing, setPreviewing] = useState(false);

  const onPreview = async () => {
    if (!file || !store.entity_code) return;
    setPreviewing(true);
    try {
      await previewBankPdf({
        entity_code: store.entity_code,
        file,
      });
      store.setField('bank_sample_uploaded', true);
      toast.success('Bank sample parsed');
    } catch {
      // Toast already surfaced by the API interceptor.
    } finally {
      setPreviewing(false);
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">Bank connection</h2>
        <p className="text-slate">
          Upload a sample PDF statement so we can confirm we parse your bank
          format correctly. You can skip and add this later.
        </p>
      </div>
      <div>
        <Label htmlFor="bank_type">Bank</Label>
        <Select
          value={store.bank_type ?? ''}
          onValueChange={(v) => store.setField('bank_type', v)}
        >
          <SelectTrigger id="bank_type">
            <SelectValue placeholder="Pick a bank" />
          </SelectTrigger>
          <SelectContent>
            {BANK_TYPES.map((b) => (
              <SelectItem key={b.code} value={b.code}>
                {b.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div>
        <Label>Sample statement (PDF)</Label>
        <label className="flex items-center gap-3 rounded-xl border border-dashed border-input bg-cloud p-4 cursor-pointer hover:bg-white transition">
          <Upload className="h-5 w-5 text-slate" strokeWidth={1.5} />
          <span className="text-sm text-slate">
            {file ? file.name : 'Drag-drop or click to upload a sample PDF'}
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
            onClick={onPreview}
            disabled={previewing}
          >
            {previewing ? 'Parsing…' : 'Parse sample'}
          </Button>
        )}
        {store.bank_sample_uploaded && (
          <p className="text-xs text-bw-teal mt-2">Sample parsed successfully</p>
        )}
      </div>
      <div className="flex justify-between pt-4">
        <Button
          type="button"
          variant="ghost"
          onClick={() => {
            const prev = getPrevStep('bank');
            if (prev) store.goTo(prev);
          }}
        >
          Back
        </Button>
        <Button type="button" onClick={() => store.goTo('hh-ap')}>
          Continue
        </Button>
      </div>
    </div>
  );
}
