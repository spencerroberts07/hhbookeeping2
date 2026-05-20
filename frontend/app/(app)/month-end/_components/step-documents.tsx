'use client';

import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { MultiFileUpload } from '@/components/shared/multi-file-upload';
import { useUploadDefaults } from '@/lib/hooks/use-upload-defaults';

interface Props {
  entityCode: string;
  periodEnd: string;
}

type DocStatus = 'not_uploaded' | 'uploaded' | 'error';

interface DocState {
  status: DocStatus;
  records?: number;
  fileName?: string;
}

interface DocConfig {
  key: string;
  label: string;
  description: string;
  endpoint: string;
  fileKey: 'file' | 'files';
  accept: string;
  required: boolean;
  /** Endpoint-specific extra fields (entity_code + actor_email added automatically). */
  buildExtra?: (periodEnd: string, snapshotDate: string) => Record<string, string>;
  /** Which React Query keys to invalidate after a successful upload. */
  invalidate?: string[];
}

const DOCS: DocConfig[] = [
  {
    key: 'bank_pdf',
    label: 'Bank PDF statement',
    description: 'Monthly statement from your bank.',
    endpoint: '/api/bank-pdf/upload',
    fileKey: 'file',
    accept: '.pdf',
    required: true,
    invalidate: ['bank-txns'],
  },
  {
    key: 'hh_ap_invoices',
    label: 'HH AP invoice batch',
    description:
      'All invoice PDFs for the period. The marquee upload — drop hundreds at once.',
    endpoint: '/api/hh-ap/invoices/upload-and-parse-batch',
    fileKey: 'files',
    accept: '.pdf',
    required: true,
    buildExtra: (periodEnd) => ({ document_date: periodEnd }),
    invalidate: ['hh-ap-summary', 'hh-ap-invoices'],
  },
  {
    key: 'hh_ap_documents',
    label: 'HH AP statement / documents',
    description: 'Monthly statement, remittances, credit notes.',
    endpoint: '/api/hh-ap/upload-documents',
    fileKey: 'files',
    accept: '.pdf',
    required: true,
    buildExtra: (periodEnd) => ({
      document_date: periodEnd,
      document_type: 'monthly_statement',
    }),
    invalidate: ['hh-ap-summary'],
  },
  {
    key: 'pos_financial',
    label: 'POS Financial report',
    description: 'Monthly POS Financial summary — used for validation.',
    endpoint: '/api/pos-import/pos-financial',
    fileKey: 'file',
    accept: '.pdf,.xlsx,.txt',
    required: true,
    invalidate: ['pos-runs', 'pos-latest'],
  },
  {
    key: 'inventory_adjustment',
    label: 'Inventory adjustment',
    description: 'Cycle count + shrinkage adjustments.',
    endpoint: '/api/pos-import/inventory-adjustment',
    fileKey: 'file',
    accept: '.pdf,.xlsx,.txt',
    required: false,
    invalidate: ['pos-runs'],
  },
  {
    key: 'payroll_p1',
    label: 'Payroll register — P1',
    description: 'First pay period of the month (ENetEmployer register PDF).',
    endpoint: '/api/payroll/runs/upload-register',
    fileKey: 'file',
    accept: '.pdf',
    required: true,
    invalidate: ['payroll-runs'],
  },
  {
    key: 'payroll_p2',
    label: 'Payroll register — P2',
    description: 'Second pay period.',
    endpoint: '/api/payroll/runs/upload-register',
    fileKey: 'file',
    accept: '.pdf',
    required: true,
    invalidate: ['payroll-runs'],
  },
  {
    key: 'aged_ar',
    label: 'Aged AR',
    description: 'Customer AR balances at period end.',
    endpoint: '/api/pos-import/aged-ar',
    fileKey: 'file',
    accept: '.pdf,.xlsx,.txt',
    required: false,
    buildExtra: (_periodEnd, snapshotDate) => ({ snapshot_date: snapshotDate }),
    invalidate: ['ar-aging'],
  },
  {
    key: 'gl_export',
    label: 'GL export (optional)',
    description: 'QuickBooks general ledger export for trial-balance comparison.',
    endpoint: '/api/gl-import/upload',
    fileKey: 'file',
    accept: '.xlsx',
    required: false,
    buildExtra: (periodEnd) => ({
      period_end: periodEnd,
      // period_start is optional on the backend — the GL parser derives it
      // from the file when omitted.
    }),
    invalidate: ['gl-runs'],
  },
  {
    key: 'outside_vendor_invoices',
    label: 'Outside-vendor invoices (optional)',
    description:
      'Any non-HH vendor invoices for the period. Auto-matched to bank transactions by amount + date.',
    endpoint: '/api/invoice-documents/upload',
    fileKey: 'files',
    accept: '.pdf',
    required: false,
    buildExtra: () => ({ invoice_type: 'outside_vendor' }),
    invalidate: ['unmatched-queue', 'invoice-documents'],
  },
];

export function StepDocuments({ entityCode, periodEnd }: Props) {
  const uploadDefaults = useUploadDefaults();
  const qc = useQueryClient();
  // Per-document local status. Source of truth for "uploaded" is the
  // backend's import-runs lists, which we invalidate so other pages pick
  // up the new data immediately.
  const [state, setState] = useState<Record<string, DocState>>({});
  // For aged-ar / gl endpoints we expose a separate snapshot-date input,
  // defaulted to period_end.
  const [snapshotDate, setSnapshotDate] = useState(periodEnd);

  // Defensive — the page already reads entity from the same Zustand store
  // as the upload defaults, so this should never trip. Surface clearly if
  // it ever does (e.g. mid-render entity switch).
  if (!uploadDefaults.entity_code || uploadDefaults.entity_code !== entityCode) {
    return (
      <p className="text-sm text-red-700">
        Entity mismatch — refresh the page or pick the right entity from the
        switcher.
      </p>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-end gap-3 max-w-md">
        <div className="flex-1">
          <Label htmlFor="snap-date">Snapshot date (for AR / GL only)</Label>
          <Input
            id="snap-date"
            type="date"
            value={snapshotDate}
            onChange={(e) => setSnapshotDate(e.target.value)}
          />
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {DOCS.map((doc) => {
          const cur = state[doc.key] ?? { status: 'not_uploaded' as const };
          const extra = doc.buildExtra
            ? doc.buildExtra(periodEnd, snapshotDate)
            : {};
          return (
            <Card key={doc.key} className="p-4 flex flex-col gap-3">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="font-semibold text-deep-navy">
                    {doc.label}
                    {doc.required && <span className="text-bw-teal ml-1">*</span>}
                  </div>
                  <p className="text-xs text-slate">{doc.description}</p>
                </div>
                <StatusBadge status={cur.status} records={cur.records} />
              </div>
              <MultiFileUpload
                endpoint={doc.endpoint}
                fileKey={doc.fileKey}
                accept={doc.accept}
                extraFields={{ ...uploadDefaults, ...extra }}
                label={doc.label}
                description={doc.description}
                onFileSuccess={(r) =>
                  setState((s) => ({
                    ...s,
                    [doc.key]: {
                      status: 'uploaded',
                      records: r.recordCount,
                      fileName: r.fileName,
                    },
                  }))
                }
                onComplete={(results) => {
                  for (const key of doc.invalidate ?? []) {
                    qc.invalidateQueries({ queryKey: [key] });
                  }
                  if (results.every((r) => r.status === 'error')) {
                    setState((s) => ({ ...s, [doc.key]: { status: 'error' } }));
                  }
                }}
              />
            </Card>
          );
        })}
      </div>
      <p className="text-xs text-slate">
        Required documents are marked <span className="text-bw-teal">*</span>.
        Re-uploads are idempotent on bank / HH AP / GL endpoints (matched by
        file hash) and supersede prior runs on POS / payroll endpoints.
      </p>
    </div>
  );
}

function StatusBadge({
  status,
  records,
}: {
  status: DocStatus;
  records?: number;
}) {
  if (status === 'uploaded')
    return (
      <Badge variant="complete">
        Uploaded{records !== undefined ? ` · ${records}` : ''}
      </Badge>
    );
  if (status === 'error') return <Badge variant="error">Error</Badge>;
  return <Badge variant="info">Not uploaded</Badge>;
}
