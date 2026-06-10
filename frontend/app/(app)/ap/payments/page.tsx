'use client';

import { useState, useMemo } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatDate } from '@/lib/utils';
import {
  listVendorInvoices,
  listVendors,
  listVendorEftFiles,
  previewVendorPaymentFile,
  generateVendorPaymentFile,
  setVendorBanking,
  type VendorInvoice,
  type Vendor,
  type PreviewResult,
} from '@/lib/api/vendor_ap';
import { toast } from 'sonner';
import {
  AlertTriangle,
  CheckCircle2,
  Clock,
  Download,
  FileText,
} from 'lucide-react';

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function daysUntilDue(dueDateStr: string | null): number | null {
  if (!dueDateStr) return null;
  const due = new Date(dueDateStr);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  due.setHours(0, 0, 0, 0);
  return Math.round((due.getTime() - today.getTime()) / 86_400_000);
}

function urgencyBadge(days: number | null) {
  if (days === null)
    return <Badge variant="secondary">No due date</Badge>;
  if (days < 0)
    return <Badge variant="error">{Math.abs(days)}d overdue</Badge>;
  if (days === 0)
    return <Badge variant="error">Due today</Badge>;
  if (days <= 3)
    return <Badge variant="warning">{days}d</Badge>;
  if (days <= 7)
    return <Badge variant="outline" className="border-amber-400 text-amber-700">{days}d</Badge>;
  return <Badge variant="outline">{days}d</Badge>;
}

function statusBadge(status: string) {
  if (status === 'payment_pending')
    return <Badge variant="outline" className="border-blue-400 text-blue-700">Pending</Badge>;
  if (status === 'paid')
    return <Badge className="bg-emerald-100 text-emerald-800">Paid</Badge>;
  if (status === 'approved')
    return <Badge className="bg-violet-100 text-violet-800">Approved</Badge>;
  return <Badge variant="secondary">{status}</Badge>;
}

// ---------------------------------------------------------------------------
// Banking entry dialog (inline, no external dialog lib)
// ---------------------------------------------------------------------------

interface BankingFormProps {
  vendor: Vendor;
  entityCode: string;
  onSaved: () => void;
  onCancel: () => void;
}

function BankingForm({ vendor, entityCode, onSaved, onCancel }: BankingFormProps) {
  const [transit, setTransit] = useState(vendor.bank_transit ?? '');
  const [institution, setInstitution] = useState(vendor.bank_institution ?? '');
  const [account, setAccount] = useState(vendor.bank_account ?? '');
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    if (!transit || !institution || !account) {
      toast.error('All three banking fields are required');
      return;
    }
    setSaving(true);
    try {
      await setVendorBanking(vendor.id, { entity_code: entityCode, transit, institution, account });
      toast.success(`Banking saved for ${vendor.vendor_name}`);
      onSaved();
    } catch {
      toast.error('Failed to save banking details');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-3 p-4 border rounded-lg bg-amber-50 border-amber-200">
      <p className="text-sm font-semibold text-amber-800">
        Enter EFT banking details for {vendor.vendor_name}
      </p>
      <div className="grid grid-cols-3 gap-2">
        <div>
          <label className="text-xs text-slate mb-1 block">Transit (5 digits)</label>
          <input
            className="h-8 w-full rounded border border-input bg-white px-2 text-sm"
            placeholder="12345"
            value={transit}
            maxLength={5}
            onChange={(e) => setTransit(e.target.value.replace(/\D/g, ''))}
          />
        </div>
        <div>
          <label className="text-xs text-slate mb-1 block">Institution (3 digits)</label>
          <input
            className="h-8 w-full rounded border border-input bg-white px-2 text-sm"
            placeholder="004"
            value={institution}
            maxLength={3}
            onChange={(e) => setInstitution(e.target.value.replace(/\D/g, ''))}
          />
        </div>
        <div>
          <label className="text-xs text-slate mb-1 block">Account number</label>
          <input
            className="h-8 w-full rounded border border-input bg-white px-2 text-sm"
            placeholder="12345678"
            value={account}
            onChange={(e) => setAccount(e.target.value.replace(/\D/g, ''))}
          />
        </div>
      </div>
      <div className="flex gap-2">
        <Button size="sm" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving…' : 'Save & continue'}
        </Button>
        <Button size="sm" variant="ghost" onClick={onCancel}>Cancel</Button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Payment file generate flow
// ---------------------------------------------------------------------------

interface GenerateFlowProps {
  entityCode: string;
  selectedIds: string[];
  onClose: () => void;
  onComplete: () => void;
}

function GenerateFlow({ entityCode, selectedIds, onClose, onComplete }: GenerateFlowProps) {
  const qc = useQueryClient();
  const [paymentDate, setPaymentDate] = useState(
    new Date().toISOString().slice(0, 10),
  );
  const [generating, setGenerating] = useState(false);
  const [bankingFor, setBankingFor] = useState<string | null>(null);

  const vendors = useQuery({
    queryKey: ['vendors', entityCode],
    queryFn: () => listVendors(entityCode),
    enabled: !!entityCode,
  });

  const preview = useQuery({
    queryKey: ['eft-preview', entityCode, selectedIds],
    enabled: selectedIds.length > 0,
    queryFn: () => previewVendorPaymentFile({ entity_code: entityCode, invoice_ids: selectedIds }),
  });

  const data: PreviewResult | undefined = preview.data;
  const missingBanking = data?.missing_banking ?? [];

  const handleGenerate = async () => {
    if (missingBanking.length > 0) {
      toast.error('Enter banking details for all vendors before generating');
      return;
    }
    setGenerating(true);
    try {
      const result = await generateVendorPaymentFile({
        entity_code: entityCode,
        invoice_ids: selectedIds,
        payment_date: paymentDate,
      });
      toast.success(`EFT file generated: ${result.file_name}`);
      if (result.download_url) {
        window.open(result.download_url, '_blank');
      }
      qc.invalidateQueries({ queryKey: ['vendor-invoices', entityCode] });
      qc.invalidateQueries({ queryKey: ['eft-files', entityCode] });
      onComplete();
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Generation failed';
      toast.error(msg);
    } finally {
      setGenerating(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-ink">Generate EFT payment file</h3>
        <Button size="sm" variant="ghost" onClick={onClose}>✕</Button>
      </div>

      {preview.isLoading && <Skeleton className="h-24" />}

      {data && (
        <div className="space-y-3">
          {/* Summary */}
          <div className="grid grid-cols-3 gap-3">
            <div className="rounded-lg border p-3 text-center">
              <p className="text-xl font-bold text-ink">{formatMoney(data.total_amount)}</p>
              <p className="text-xs text-slate">Total</p>
            </div>
            <div className="rounded-lg border p-3 text-center">
              <p className="text-xl font-bold text-ink">{data.vendor_count}</p>
              <p className="text-xs text-slate">Vendor{data.vendor_count !== 1 ? 's' : ''}</p>
            </div>
            <div className="rounded-lg border p-3 text-center">
              <p className="text-xl font-bold text-ink">{data.invoice_count}</p>
              <p className="text-xs text-slate">Invoice{data.invoice_count !== 1 ? 's' : ''}</p>
            </div>
          </div>

          {/* Missing banking */}
          {missingBanking.length > 0 && (
            <div className="space-y-2">
              <p className="text-sm font-semibold text-amber-700 flex items-center gap-1">
                <AlertTriangle className="h-4 w-4" />
                Banking required for {missingBanking.length} vendor{missingBanking.length !== 1 ? 's' : ''}
              </p>
              {missingBanking.map((v) => {
                const fullVendor = vendors.data?.find((vnd) => vnd.id === v.vendor_id);
                return bankingFor === v.vendor_id ? (
                  fullVendor ? (
                    <BankingForm
                      key={v.vendor_id}
                      vendor={fullVendor}
                      entityCode={entityCode}
                      onSaved={() => {
                        setBankingFor(null);
                        qc.invalidateQueries({ queryKey: ['vendors', entityCode] });
                        qc.invalidateQueries({ queryKey: ['eft-preview', entityCode, selectedIds] });
                      }}
                      onCancel={() => setBankingFor(null)}
                    />
                  ) : null
                ) : (
                  <div key={v.vendor_id} className="flex items-center justify-between rounded border px-3 py-2 bg-amber-50">
                    <span className="text-sm text-ink">{v.vendor_name}</span>
                    <Button size="sm" variant="outline" onClick={() => setBankingFor(v.vendor_id ?? null)}>
                      Enter banking
                    </Button>
                  </div>
                );
              })}
            </div>
          )}

          {/* Banking complete */}
          {data.banking_complete && (
            <div className="flex items-center gap-2 text-sm text-emerald-700">
              <CheckCircle2 className="h-4 w-4" />
              All vendors have banking details on file
            </div>
          )}

          {/* Payment date */}
          <div>
            <label className="text-xs text-slate mb-1 block">Payment date</label>
            <input
              type="date"
              className="h-9 rounded border border-input bg-white px-2 text-sm"
              value={paymentDate}
              onChange={(e) => setPaymentDate(e.target.value)}
            />
          </div>

          {/* Dry-run notice */}
          <p className="text-xs text-slate bg-cloud rounded p-2">
            <strong>Dry-run only.</strong> This file is for manual review and submission only.
            It will NOT be automatically submitted to your bank.
          </p>

          <Button
            onClick={handleGenerate}
            disabled={generating || !data.banking_complete}
            className="w-full"
          >
            {generating ? 'Generating…' : `Generate EFT file — ${formatMoney(data.total_amount)}`}
          </Button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

type Tab = 'invoices' | 'vendors' | 'files';

export default function ApPaymentsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [tab, setTab] = useState<Tab>('invoices');
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [showGenerate, setShowGenerate] = useState(false);

  // Date range: rolling 6 months back to today+90
  const today = new Date();
  const dateFrom = new Date(today.getFullYear(), today.getMonth() - 6, 1)
    .toISOString()
    .slice(0, 10);
  const dateTo = new Date(today.getTime() + 90 * 86_400_000)
    .toISOString()
    .slice(0, 10);

  const invoicesQ = useQuery({
    queryKey: ['vendor-invoices', entityCode, dateFrom, dateTo],
    enabled: !!entityCode && tab === 'invoices',
    queryFn: () =>
      listVendorInvoices({
        entity_code: entityCode!,
        date_from: dateFrom,
        date_to: dateTo,
      }),
  });

  const vendorsQ = useQuery({
    queryKey: ['vendors', entityCode],
    enabled: !!entityCode && tab === 'vendors',
    queryFn: () => listVendors(entityCode!),
  });

  const filesQ = useQuery({
    queryKey: ['eft-files', entityCode],
    enabled: !!entityCode && tab === 'files',
    queryFn: () => listVendorEftFiles(entityCode!),
  });

  const qc = useQueryClient();

  // Invoices eligible for payment selection (open / approved, not pending/paid)
  const payableInvoices = useMemo(
    () =>
      (invoicesQ.data?.invoices ?? []).filter(
        (inv) =>
          ['open', 'needs_review', 'approved'].includes(inv.status) &&
          inv.status !== 'payment_pending',
      ),
    [invoicesQ.data],
  );

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === payableInvoices.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(payableInvoices.map((i) => i.id)));
    }
  };

  if (!entityCode) return null;

  return (
    <>
      <Topbar title="Vendor Payments" />
      <main className="p-6 space-y-4">
        {/* Tab bar */}
        <nav className="flex gap-1 bg-white border border-border rounded-xl p-1 w-fit" role="tablist">
          {(['invoices', 'vendors', 'files'] as const).map((t) => (
            <button
              key={t}
              role="tab"
              aria-selected={tab === t}
              onClick={() => setTab(t)}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold capitalize ${
                tab === t ? 'bg-deep-navy text-white' : 'text-slate hover:bg-cloud'
              }`}
            >
              {t === 'files' ? 'EFT files' : t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>

        {/* ---------------------------------------------------------------- */}
        {/* INVOICES TAB                                                     */}
        {/* ---------------------------------------------------------------- */}
        {tab === 'invoices' && (
          <div className="space-y-3">
            {/* Summary buckets */}
            {invoicesQ.data?.summary && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <Card className="p-3">
                  <p className="text-xs text-slate">Open</p>
                  <p className="text-lg font-bold text-ink">
                    {formatMoney(invoicesQ.data.summary.total_open)}
                  </p>
                  <p className="text-xs text-slate">{invoicesQ.data.summary.count_open} invoice{invoicesQ.data.summary.count_open !== 1 ? 's' : ''}</p>
                </Card>
                <Card className="p-3">
                  <p className="text-xs text-slate">Pending payment</p>
                  <p className="text-lg font-bold text-ink">
                    {formatMoney(invoicesQ.data.summary.total_payment_pending)}
                  </p>
                  <p className="text-xs text-slate">{invoicesQ.data.summary.count_payment_pending} invoice{invoicesQ.data.summary.count_payment_pending !== 1 ? 's' : ''}</p>
                </Card>
                <Card className="p-3">
                  <p className="text-xs text-slate">Overdue</p>
                  <p className={`text-lg font-bold ${invoicesQ.data.summary.count_overdue > 0 ? 'text-red-600' : 'text-ink'}`}>
                    {invoicesQ.data.summary.count_overdue}
                  </p>
                  <p className="text-xs text-slate">invoice{invoicesQ.data.summary.count_overdue !== 1 ? 's' : ''}</p>
                </Card>
                <Card className="p-3 flex items-center justify-between">
                  <div>
                    <p className="text-xs text-slate">Selected</p>
                    <p className="text-lg font-bold text-ink">{selected.size}</p>
                  </div>
                  <Button
                    size="sm"
                    disabled={selected.size === 0}
                    onClick={() => setShowGenerate(true)}
                  >
                    Pay selected
                  </Button>
                </Card>
              </div>
            )}

            {/* Generate flow slide-over */}
            {showGenerate && (
              <Card>
                <CardContent className="pt-4">
                  <GenerateFlow
                    entityCode={entityCode}
                    selectedIds={Array.from(selected)}
                    onClose={() => setShowGenerate(false)}
                    onComplete={() => {
                      setShowGenerate(false);
                      setSelected(new Set());
                    }}
                  />
                </CardContent>
              </Card>
            )}

            {/* Invoice table */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base flex items-center gap-3">
                  <span>Outstanding invoices</span>
                  {payableInvoices.length > 0 && (
                    <Button size="sm" variant="ghost" onClick={toggleAll} className="font-normal text-xs">
                      {selected.size === payableInvoices.length ? 'Deselect all' : 'Select all payable'}
                    </Button>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {invoicesQ.isLoading ? (
                  <div className="space-y-2">
                    {Array.from({ length: 5 }).map((_, i) => (
                      <Skeleton key={i} className="h-10" />
                    ))}
                  </div>
                ) : (invoicesQ.data?.invoices?.length ?? 0) === 0 ? (
                  <p className="text-sm text-slate py-6 text-center">
                    No outside-vendor invoices in this period
                  </p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b text-left text-xs text-slate">
                          <th className="pb-2 w-8" />
                          <th className="pb-2">Vendor</th>
                          <th className="pb-2">Invoice #</th>
                          <th className="pb-2">Date</th>
                          <th className="pb-2 text-right">Amount</th>
                          <th className="pb-2 text-right">Open</th>
                          <th className="pb-2 text-center">Due</th>
                          <th className="pb-2 text-center">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {invoicesQ.data!.invoices.map((inv: VendorInvoice) => {
                          const days = daysUntilDue(inv.due_date);
                          const isPayable = ['open', 'needs_review', 'approved'].includes(inv.status) &&
                            inv.status !== 'payment_pending';
                          const isSelected = selected.has(inv.id);
                          return (
                            <tr
                              key={inv.id}
                              className={`border-b last:border-0 hover:bg-cloud/40 transition-colors ${
                                isSelected ? 'bg-sky-50' : ''
                              }`}
                            >
                              <td className="py-2">
                                {isPayable && (
                                  <input
                                    type="checkbox"
                                    checked={isSelected}
                                    onChange={() => toggleSelect(inv.id)}
                                    className="h-4 w-4 rounded border-input"
                                  />
                                )}
                              </td>
                              <td className="py-2 font-medium text-ink">{inv.vendor_name}</td>
                              <td className="py-2 text-slate">{inv.invoice_number}</td>
                              <td className="py-2 text-slate">{formatDate(inv.invoice_date)}</td>
                              <td className="py-2 text-right font-mono">{formatMoney(inv.total_amount)}</td>
                              <td className="py-2 text-right font-mono">{formatMoney(inv.open_amount)}</td>
                              <td className="py-2 text-center">{urgencyBadge(days)}</td>
                              <td className="py-2 text-center">{statusBadge(inv.status)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* VENDORS TAB                                                      */}
        {/* ---------------------------------------------------------------- */}
        {tab === 'vendors' && (
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Vendor master</CardTitle>
              <p className="text-xs text-slate">
                Vendors are auto-created from invoice uploads. Add banking details here
                so they can be included in EFT payment files.
              </p>
            </CardHeader>
            <CardContent>
              {vendorsQ.isLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 4 }).map((_, i) => (
                    <Skeleton key={i} className="h-12" />
                  ))}
                </div>
              ) : (vendorsQ.data?.length ?? 0) === 0 ? (
                <p className="text-sm text-slate py-6 text-center">
                  No vendors yet — they are auto-created when you upload outside-vendor invoices
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-xs text-slate">
                        <th className="pb-2">Vendor</th>
                        <th className="pb-2 text-center">Profile</th>
                        <th className="pb-2 text-center">Banking</th>
                        <th className="pb-2 text-center">Terms</th>
                        <th className="pb-2 text-center">Invoices</th>
                        <th className="pb-2">Last seen</th>
                      </tr>
                    </thead>
                    <tbody>
                      {vendorsQ.data!.map((v) => {
                        const conf = v.profile_confidence_computed;
                        const confPct = Math.round(conf * 100);
                        return (
                          <tr key={v.id} className="border-b last:border-0 hover:bg-cloud/40">
                            <td className="py-2 font-medium text-ink">{v.vendor_name}</td>
                            <td className="py-2 text-center">
                              <span
                                className={`text-xs font-semibold ${
                                  confPct >= 80
                                    ? 'text-emerald-700'
                                    : confPct >= 40
                                    ? 'text-amber-700'
                                    : 'text-slate'
                                }`}
                              >
                                {confPct}%
                              </span>
                            </td>
                            <td className="py-2 text-center">
                              {v.banking_complete ? (
                                <CheckCircle2 className="h-4 w-4 text-emerald-600 mx-auto" />
                              ) : (
                                <AlertTriangle className="h-4 w-4 text-amber-500 mx-auto" />
                              )}
                            </td>
                            <td className="py-2 text-center text-slate text-xs">
                              {v.payment_terms_days !== null ? `Net ${v.payment_terms_days}` : '—'}
                            </td>
                            <td className="py-2 text-center text-slate">{v.invoice_count}</td>
                            <td className="py-2 text-slate text-xs">
                              {formatDate(v.last_seen_at)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* EFT FILES TAB                                                    */}
        {/* ---------------------------------------------------------------- */}
        {tab === 'files' && (
          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <FileText className="h-4 w-4" />
                Generated EFT files
              </CardTitle>
            </CardHeader>
            <CardContent>
              {filesQ.isLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 3 }).map((_, i) => (
                    <Skeleton key={i} className="h-12" />
                  ))}
                </div>
              ) : (filesQ.data?.length ?? 0) === 0 ? (
                <p className="text-sm text-slate py-6 text-center">
                  No EFT payment files generated yet
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-left text-xs text-slate">
                        <th className="pb-2">File name</th>
                        <th className="pb-2">Payment date</th>
                        <th className="pb-2 text-right">Total</th>
                        <th className="pb-2 text-center">Vendors</th>
                        <th className="pb-2 text-center">FCN</th>
                        <th className="pb-2">Generated</th>
                        <th className="pb-2" />
                      </tr>
                    </thead>
                    <tbody>
                      {filesQ.data!.map((f) => (
                        <tr key={f.id} className="border-b last:border-0 hover:bg-cloud/40">
                          <td className="py-2 font-mono text-xs text-ink">{f.file_name}</td>
                          <td className="py-2 text-slate">{formatDate(f.payment_date)}</td>
                          <td className="py-2 text-right font-mono">{formatMoney(f.total_amount)}</td>
                          <td className="py-2 text-center text-slate">{f.vendor_count}</td>
                          <td className="py-2 text-center text-slate">{f.file_creation_number}</td>
                          <td className="py-2 text-slate text-xs">{formatDate(f.generated_at)}</td>
                          <td className="py-2 text-right">
                            <DownloadButton fileId={f.id} entityCode={entityCode} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        )}
      </main>
    </>
  );
}

// ---------------------------------------------------------------------------
// Download button (fetches presigned URL on demand)
// ---------------------------------------------------------------------------

function DownloadButton({ fileId, entityCode }: { fileId: string; entityCode: string }) {
  const [loading, setLoading] = useState(false);

  const handleDownload = async () => {
    setLoading(true);
    try {
      const { downloadVendorEftFile } = await import('@/lib/api/vendor_ap');
      const { download_url, file_name } = await downloadVendorEftFile(fileId, entityCode);
      if (download_url) {
        const a = document.createElement('a');
        a.href = download_url;
        a.download = file_name;
        a.click();
      } else {
        toast.error('Download URL not available');
      }
    } catch {
      toast.error('Failed to get download link');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Button size="sm" variant="outline" onClick={handleDownload} disabled={loading}>
      <Download className="h-3 w-3 mr-1" />
      {loading ? '…' : 'Download'}
    </Button>
  );
}
