'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import { getHHAPSummary, listHHAPInvoices } from '@/lib/api/hh_ap';
import { formatMoney, formatDate } from '@/lib/utils';
import { MultiFileUpload } from '@/components/shared/multi-file-upload';
import { useUploadDefaults } from '@/lib/hooks/use-upload-defaults';
import { listInvoiceDocuments, getUnmatchedQueue } from '@/lib/api/invoices';
import Link from 'next/link';

type Tab = 'hh' | 'vendor' | 'unmatched';

const HH_AP_DOCUMENT_TYPES = [
  { value: 'monthly_statement', label: 'Monthly statement' },
  { value: 'remittance', label: 'Remittance' },
  { value: 'credit_note', label: 'Credit note' },
  { value: 'other', label: 'Other' },
];

export default function ApPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [tab, setTab] = useState<Tab>('hh');
  const qc = useQueryClient();
  const uploadDefaults = useUploadDefaults();
  const today = new Date().toISOString().slice(0, 10);
  const [docType, setDocType] = useState<string>('monthly_statement');
  const [docDate, setDocDate] = useState<string>(today);
  const refreshAp = () => {
    qc.invalidateQueries({ queryKey: ['hh-ap-summary'] });
    qc.invalidateQueries({ queryKey: ['hh-ap-invoices'] });
    qc.invalidateQueries({ queryKey: ['invoice-documents'] });
    qc.invalidateQueries({ queryKey: ['unmatched-queue'] });
  };

  // Count of unmatched invoices, used for the tab badge.
  const unmatchedCount = useQuery({
    queryKey: ['unmatched-queue', entityCode],
    enabled: !!entityCode,
    queryFn: () => getUnmatchedQueue({ entity_code: entityCode! }),
    select: (data) => data.total,
  });

  // Outside-vendor invoices (status excludes 'deleted').
  const outsideInvoices = useQuery({
    queryKey: ['invoice-documents', entityCode, 'outside_vendor'],
    enabled: !!entityCode && tab === 'vendor',
    queryFn: () =>
      listInvoiceDocuments({
        entity_code: entityCode!,
        invoice_type: 'outside_vendor',
        limit: 200,
      }),
  });

  const summary = useQuery({
    queryKey: ['hh-ap-summary', entityCode],
    enabled: !!entityCode && tab === 'hh',
    queryFn: () => getHHAPSummary(entityCode!),
  });

  const invoices = useQuery({
    queryKey: ['hh-ap-invoices', entityCode, 'recent'],
    enabled: !!entityCode && tab === 'hh',
    queryFn: () => listHHAPInvoices({ entity_code: entityCode!, limit: 100 }),
  });

  return (
    <>
      <Topbar title="Accounts Payable" />
      <main className="p-6 space-y-4">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <nav className="flex gap-1 bg-white border border-border rounded-xl p-1" role="tablist">
            <button
              role="tab"
              aria-selected={tab === 'hh'}
              onClick={() => setTab('hh')}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                tab === 'hh' ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              HH AP
            </button>
            <button
              role="tab"
              aria-selected={tab === 'vendor'}
              onClick={() => setTab('vendor')}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                tab === 'vendor' ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              Other vendors
            </button>
            <button
              role="tab"
              aria-selected={tab === 'unmatched'}
              onClick={() => setTab('unmatched')}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold flex items-center gap-2 ${
                tab === 'unmatched' ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              Unmatched
              {(unmatchedCount.data ?? 0) > 0 && (
                <Badge variant="warning">{unmatchedCount.data}</Badge>
              )}
            </button>
          </nav>
        </div>

        {tab === 'hh' && (
          <>
            <section className="grid grid-cols-2 md:grid-cols-5 gap-3">
              {summary.isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-20" />
                ))
              ) : summary.data ? (
                <>
                  <Bucket label="Current" amount={summary.data.aging.current} />
                  <Bucket label="30+" amount={summary.data.aging.over_30} />
                  <Bucket label="60+" amount={summary.data.aging.over_60} />
                  <Bucket label="90+" amount={summary.data.aging.over_90} />
                  <Bucket
                    label="Total outstanding"
                    amount={summary.data.current_balance}
                    highlight
                  />
                </>
              ) : null}
            </section>

            <Card>
              <CardHeader>
                <CardTitle>Upload HH invoices & documents</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3 max-w-2xl">
                  <div className="md:col-span-2">
                    <Label htmlFor="ap-doc-date">Document date</Label>
                    <input
                      id="ap-doc-date"
                      type="date"
                      className="flex h-10 w-full rounded-lg border border-input bg-white px-3 py-2 text-sm text-ink"
                      value={docDate}
                      onChange={(e) => setDocDate(e.target.value)}
                    />
                    <p className="text-xs text-slate mt-1">
                      The statement / invoice date. Used to bucket aging.
                    </p>
                  </div>
                </div>
                <MultiFileUpload
                  endpoint="/api/hh-ap/invoices/upload-and-parse-batch"
                  fileKey="files"
                  accept=".pdf"
                  extraFields={{
                    entity_code: uploadDefaults.entity_code,
                    document_date: docDate,
                  }}
                  label="Invoice batch — parse all at once"
                  description="The main HH AP upload. Drop every invoice PDF for the period in one go."
                  note="Upload all invoice PDFs for this period"
                  variant="prominent"
                  onComplete={refreshAp}
                />
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3 max-w-md">
                  <div>
                    <Label htmlFor="ap-doc-type">Document type</Label>
                    <Select value={docType} onValueChange={setDocType}>
                      <SelectTrigger id="ap-doc-type">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {HH_AP_DOCUMENT_TYPES.map((t) => (
                          <SelectItem key={t.value} value={t.value}>
                            {t.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <MultiFileUpload
                  endpoint="/api/hh-ap/upload-documents"
                  fileKey="files"
                  accept=".pdf"
                  extraFields={{
                    entity_code: uploadDefaults.entity_code,
                    document_type: docType,
                    document_date: docDate,
                  }}
                  label="HH AP documents (statements, remittances, credit notes)"
                  description="Non-invoice HH AP paperwork. Pick a document type above before uploading."
                  onComplete={refreshAp}
                />
                <MultiFileUpload
                  endpoint="/api/invoice-documents/upload"
                  fileKey="files"
                  accept=".pdf"
                  extraFields={{
                    entity_code: uploadDefaults.entity_code,
                    invoice_type: 'hh_ap',
                  }}
                  label="HH invoice PDFs (per-PO archive)"
                  description="Upload individual HH invoice PDFs — matched to statement rows by PO number for the audit trail."
                  onComplete={refreshAp}
                />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Recent invoices</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                {invoices.isLoading ? (
                  <Skeleton className="h-64 m-4" />
                ) : !invoices.data?.invoices.length ? (
                  <div className="p-8 text-center text-slate">No invoices.</div>
                ) : (
                  <table className="min-w-full text-sm">
                    <thead className="bg-cloud">
                      <tr>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">#</th>
                        <th className="text-right font-semibold text-deep-navy px-4 py-2">Amount</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {invoices.data.invoices.map((i) => (
                        <tr key={i.id} className="hover:bg-cloud">
                          <td className="px-4 py-2 text-ink">{formatDate(i.document_date)}</td>
                          <td className="px-4 py-2">
                            <Badge variant="info">{i.document_type}</Badge>
                          </td>
                          <td className="px-4 py-2 text-slate font-mono text-xs">
                            {i.document_number ?? '—'}
                          </td>
                          <td className="px-4 py-2 tabular-nums text-right text-ink">
                            {formatMoney(i.amount)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </CardContent>
            </Card>
          </>
        )}

        {tab === 'vendor' && (
          <>
            <Card>
              <CardHeader>
                <CardTitle>Upload outside-vendor invoices</CardTitle>
              </CardHeader>
              <CardContent>
                <MultiFileUpload
                  endpoint="/api/invoice-documents/upload"
                  fileKey="files"
                  accept=".pdf"
                  extraFields={{
                    entity_code: uploadDefaults.entity_code,
                    invoice_type: 'outside_vendor',
                  }}
                  label="Outside-vendor invoice PDFs"
                  description="Auto-matched to bank transactions by amount + date (±30 days). Unmatched invoices land in the Unmatched tab."
                  variant="prominent"
                  onComplete={refreshAp}
                />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Outside-vendor invoices</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                {outsideInvoices.isLoading ? (
                  <Skeleton className="h-64 m-4" />
                ) : !outsideInvoices.data?.invoices.length ? (
                  <div className="p-8 text-center text-slate">
                    No outside-vendor invoices uploaded yet.
                  </div>
                ) : (
                  <table className="min-w-full text-sm">
                    <thead className="bg-cloud">
                      <tr>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">Vendor</th>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">#</th>
                        <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                        <th className="text-right font-semibold text-deep-navy px-4 py-2">Amount</th>
                        <th className="px-4 py-2">Status</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {outsideInvoices.data.invoices.map((i) => (
                        <tr key={i.id} className="hover:bg-cloud">
                          <td className="px-4 py-2 text-ink">{i.vendor_name ?? '—'}</td>
                          <td className="px-4 py-2 text-slate font-mono text-xs">
                            {i.invoice_number ?? '—'}
                          </td>
                          <td className="px-4 py-2 text-ink">
                            {i.invoice_date ? formatDate(i.invoice_date) : '—'}
                          </td>
                          <td className="px-4 py-2 tabular-nums text-right text-ink">
                            {i.amount ? formatMoney(i.amount) : '—'}
                          </td>
                          <td className="px-4 py-2">
                            <Badge
                              variant={
                                i.status === 'posted_to_ap'
                                  ? 'complete'
                                  : i.status === 'matched'
                                    ? 'pending'
                                    : 'warning'
                              }
                            >
                              {i.status.replace('_', ' ')}
                            </Badge>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </CardContent>
            </Card>
          </>
        )}

        {tab === 'unmatched' && (
          <Card className="p-6 text-center">
            <p className="text-sm text-slate mb-3">
              The full unmatched queue lives on its own page — better-suited
              for the inline suggested-match / confirm-or-reject flow.
            </p>
            <Link
              href="/ap/unmatched"
              className="inline-block rounded-xl bg-bw-teal text-white font-semibold px-4 py-2"
            >
              Open the unmatched queue →
            </Link>
          </Card>
        )}
      </main>
    </>
  );
}

function Bucket({
  label,
  amount,
  highlight,
}: {
  label: string;
  amount: number;
  highlight?: boolean;
}) {
  return (
    <Card
      className={`p-3 ${highlight ? 'bg-deep-navy text-white border-deep-navy' : ''}`}
    >
      <div className={`text-xs uppercase tracking-wide ${highlight ? 'text-white/70' : 'text-slate'}`}>
        {label}
      </div>
      <div className="text-xl font-bold tabular-nums mt-1">{formatMoney(amount)}</div>
    </Card>
  );
}
