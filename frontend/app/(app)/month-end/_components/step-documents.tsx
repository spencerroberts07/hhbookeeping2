'use client';

import { useState } from 'react';
import { Upload } from 'lucide-react';
import { useUser } from '@clerk/nextjs';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';
import { uploadBankPdf } from '@/lib/api/bank';
import { uploadHHAPDocuments } from '@/lib/api/hh_ap';
import { importPos } from '@/lib/api/pos';
import { uploadPayrollRegister } from '@/lib/api/payroll';
import { uploadGl } from '@/lib/api/gl';

type DocType =
  | 'bank_pdf'
  | 'hh_ap'
  | 'pos_financial'
  | 'inventory_adj'
  | 'payroll_p1'
  | 'payroll_p2'
  | 'aged_ar'
  | 'gl_export';

interface DocConfig {
  key: DocType;
  label: string;
  hint: string;
  required: boolean;
  accept: string;
}

const DOCS: DocConfig[] = [
  { key: 'bank_pdf', label: 'Bank PDF statement', hint: 'Monthly statement from your bank', required: true, accept: 'application/pdf' },
  { key: 'hh_ap', label: 'HH AP statement', hint: 'Home Hardware monthly statement', required: true, accept: 'application/pdf' },
  { key: 'pos_financial', label: 'POS Financial report', hint: 'Monthly POS Financial summary', required: true, accept: 'text/plain,application/pdf' },
  { key: 'inventory_adj', label: 'Inventory adjustment', hint: 'Cycle count adjustments', required: false, accept: 'text/plain,application/pdf' },
  { key: 'payroll_p1', label: 'Payroll register — P1', hint: 'ENetEmployer register PDF (first pay period)', required: true, accept: 'application/pdf' },
  { key: 'payroll_p2', label: 'Payroll register — P2', hint: 'Second pay period', required: true, accept: 'application/pdf' },
  { key: 'aged_ar', label: 'Aged AR report', hint: 'Customer AR balances', required: false, accept: 'text/plain,application/pdf' },
  { key: 'gl_export', label: 'GL export (optional)', hint: 'For trial balance comparison', required: false, accept: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' },
];

interface Props {
  entityCode: string;
  periodEnd: string;
}

export function StepDocuments({ entityCode, periodEnd }: Props) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const [uploaded, setUploaded] = useState<Partial<Record<DocType, { name: string; uploaded_at: string }>>>({});
  const [uploading, setUploading] = useState<Partial<Record<DocType, boolean>>>({});

  const handleUpload = async (doc: DocConfig, file: File) => {
    setUploading((u) => ({ ...u, [doc.key]: true }));
    try {
      switch (doc.key) {
        case 'bank_pdf':
          await uploadBankPdf({ entity_code: entityCode, actor_email: actorEmail, file });
          break;
        case 'hh_ap':
          await uploadHHAPDocuments({
            entity_code: entityCode,
            document_type: 'monthly_statement',
            document_date: periodEnd,
            files: [file],
          });
          break;
        case 'pos_financial':
          await importPos('pos-financial', { entity_code: entityCode, actor_email: actorEmail, file });
          break;
        case 'inventory_adj':
          await importPos('inventory-adjustment', { entity_code: entityCode, actor_email: actorEmail, file });
          break;
        case 'payroll_p1':
        case 'payroll_p2':
          await uploadPayrollRegister({ entity_code: entityCode, actor_email: actorEmail, file });
          break;
        case 'aged_ar':
          await importPos('aged-ar', { entity_code: entityCode, actor_email: actorEmail, file, snapshot_date: periodEnd });
          break;
        case 'gl_export':
          await uploadGl({ entity_code: entityCode, actor_email: actorEmail, file, period_end: periodEnd });
          break;
      }
      setUploaded((u) => ({
        ...u,
        [doc.key]: { name: file.name, uploaded_at: new Date().toISOString() },
      }));
      toast.success(`${doc.label} uploaded`);
    } finally {
      setUploading((u) => ({ ...u, [doc.key]: false }));
    }
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {DOCS.map((doc) => {
        const status = uploaded[doc.key];
        const isUploading = uploading[doc.key];
        return (
          <Card key={doc.key} className="p-4">
            <div className="flex items-start justify-between gap-2 mb-2">
              <div>
                <div className="font-semibold text-deep-navy">
                  {doc.label}
                  {doc.required && <span className="text-bw-teal ml-1">*</span>}
                </div>
                <div className="text-xs text-slate">{doc.hint}</div>
              </div>
              {status && (
                <span className="text-xs text-bw-teal font-semibold">
                  Uploaded
                </span>
              )}
            </div>
            <label className="flex items-center gap-2 rounded-lg border border-dashed border-input bg-cloud px-3 py-2 cursor-pointer hover:bg-white transition">
              <Upload className="h-4 w-4 text-slate" strokeWidth={1.5} />
              <span className="text-xs text-slate truncate flex-1">
                {status?.name ?? (isUploading ? 'Uploading…' : 'Click to upload')}
              </span>
              <input
                type="file"
                accept={doc.accept}
                className="hidden"
                disabled={!!isUploading || !actorEmail}
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) handleUpload(doc, f);
                }}
              />
            </label>
            {status && (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="mt-2 text-xs"
                onClick={() => setUploaded((u) => {
                  const next = { ...u };
                  delete next[doc.key];
                  return next;
                })}
              >
                Re-upload
              </Button>
            )}
          </Card>
        );
      })}
    </div>
  );
}
