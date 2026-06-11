'use client';

import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Download, PlusCircle } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Sheet,
  SheetContent,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatDate } from '@/lib/utils';
import {
  getArAging,
  postArWriteDown,
  getArAgingExcelUrl,
  type ArCustomer,
  type ArSnapshot,
} from '@/lib/api/ar';
import { toast } from 'sonner';

const BUCKET_KEYS = [
  'current',
  'over_30',
  'over_60',
  'over_90',
  'over_120',
] as const;
type BucketKey = (typeof BUCKET_KEYS)[number];

const BUCKET_TEXT: Record<BucketKey, string> = {
  current: 'text-green-700',
  over_30: 'text-amber-600',
  over_60: 'text-orange-600',
  over_90: 'text-red-600',
  over_120: 'text-rose-800',
};

const BUCKET_BG: Record<BucketKey, string> = {
  current: 'bg-green-50 border-green-200',
  over_30: 'bg-amber-50 border-amber-200',
  over_60: 'bg-orange-50 border-orange-200',
  over_90: 'bg-red-50 border-red-200',
  over_120: 'bg-rose-50 border-rose-200',
};

interface WriteDownForm {
  customer_name: string;
  customer_number: string;
  amount: string;
  memo: string;
}

export default function ArPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [form, setForm] = useState<WriteDownForm>({
    customer_name: '',
    customer_number: '',
    amount: '',
    memo: '',
  });
  const [excelLoading, setExcelLoading] = useState(false);

  const aging = useQuery({
    queryKey: ['ar-aging', entityCode],
    enabled: !!entityCode,
    queryFn: () => getArAging(entityCode!),
  });

  const writeDown = useMutation({
    mutationFn: postArWriteDown,
    onSuccess: (data) => {
      toast.success(
        `Write-down posted — ${data.period_label} · Dr ${data.dr_account} / Cr ${data.cr_account}`,
      );
      setDrawerOpen(false);
      setForm({ customer_name: '', customer_number: '', amount: '', memo: '' });
      qc.invalidateQueries({ queryKey: ['ar-aging', entityCode] });
    },
    onError: (err: { response?: { data?: { detail?: string } } }) => {
      toast.error(
        err?.response?.data?.detail ?? 'Write-down failed — check the console',
      );
    },
  });

  const handleWriteDown = () => {
    if (!entityCode) return;
    const amount = parseFloat(form.amount);
    if (!amount || amount <= 0) {
      toast.error('Enter a positive amount');
      return;
    }
    writeDown.mutate({
      entity_code: entityCode,
      amount,
      customer_name: form.customer_name || undefined,
      customer_number: form.customer_number || undefined,
      memo: form.memo || undefined,
      aged_ar_snapshot_id: aging.data?.current?.id ?? undefined,
    });
  };

  const handleExcelDownload = async () => {
    if (!entityCode) return;
    setExcelLoading(true);
    try {
      const { url } = await getArAgingExcelUrl(entityCode);
      window.open(url, '_blank');
    } catch {
      toast.error('Excel export failed');
    } finally {
      setExcelLoading(false);
    }
  };

  const snap = aging.data?.current ?? null;
  const prior = aging.data?.prior ?? null;
  const bucketLabels = aging.data?.bucket_labels ?? {};

  if (!entityCode) {
    return (
      <>
        <Topbar title="Accounts receivable" />
        <main className="p-6">
          <Card className="p-8 text-center">
            <p className="text-slate">Pick an entity from the switcher.</p>
          </Card>
        </main>
      </>
    );
  }

  return (
    <>
      <Topbar title="Accounts receivable" />
      <main className="p-6 space-y-6">
        {/* Header row */}
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-3xl font-extrabold text-deep-navy tabular-nums">
              {aging.isLoading ? (
                <Skeleton className="h-9 w-40" />
              ) : snap ? (
                formatMoney(snap.total_ar)
              ) : (
                <span className="text-slate text-base">No snapshot yet</span>
              )}
            </div>
            {snap?.snapshot_date && (
              <p className="text-[10px] text-slate mt-1">
                as of {formatDate(snap.snapshot_date)}
              </p>
            )}
          </div>
          <div className="flex gap-2 shrink-0">
            <Button
              variant="secondary"
              size="sm"
              onClick={handleExcelDownload}
              disabled={excelLoading || !snap}
            >
              <Download className="h-4 w-4 mr-1.5" />
              {excelLoading ? 'Generating…' : 'Export Excel'}
            </Button>
            <Button
              variant="primary"
              size="sm"
              onClick={() => setDrawerOpen(true)}
              disabled={!snap}
            >
              <PlusCircle className="h-4 w-4 mr-1.5" />
              Write-down
            </Button>
          </div>
        </div>

        {/* Bucket summary */}
        {aging.isLoading ? (
          <Skeleton className="h-20" />
        ) : snap ? (
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            {BUCKET_KEYS.map((key) => {
              const label = bucketLabels[key] ?? key;
              const value = snap.buckets[key];
              const pct = snap.total_ar > 0 ? (value / snap.total_ar) * 100 : 0;
              const priorValue = prior?.buckets[key] ?? null;
              const delta =
                priorValue !== null ? value - priorValue : null;
              return (
                <div
                  key={key}
                  className={`rounded-xl border p-3 ${BUCKET_BG[key]}`}
                >
                  <div
                    className={`text-xs font-semibold uppercase tracking-wide mb-1 ${BUCKET_TEXT[key]}`}
                  >
                    {label}
                  </div>
                  <div className={`text-lg font-bold tabular-nums ${BUCKET_TEXT[key]}`}>
                    {formatMoney(value)}
                  </div>
                  <div className="text-[10px] text-slate">
                    {pct.toFixed(1)}% of total
                  </div>
                  {delta !== null && (
                    <div
                      className={`text-[10px] mt-0.5 ${delta > 0 ? 'text-red-600' : delta < 0 ? 'text-green-700' : 'text-slate'}`}
                    >
                      {delta > 0 ? '+' : ''}
                      {formatMoney(delta)} vs prior
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <Card className="p-8 text-center">
            <p className="text-slate">
              No AR aging snapshot found. Upload one via Month-end → Document
              upload → Aged AR.
            </p>
          </Card>
        )}

        {/* Customer table */}
        {snap && snap.customers.length > 0 && (
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>By customer</CardTitle>
                {prior && (
                  <Badge variant="info">
                    Prior: {prior.snapshot_date ? formatDate(prior.snapshot_date) : 'unknown'}
                  </Badge>
                )}
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border bg-cloud">
                      <th className="text-left py-2 px-4 font-semibold text-slate">
                        Customer
                      </th>
                      <th className="text-left py-2 px-3 font-semibold text-slate hidden sm:table-cell">
                        #
                      </th>
                      {BUCKET_KEYS.map((key) => (
                        <th
                          key={key}
                          className={`text-right py-2 px-3 font-semibold ${BUCKET_TEXT[key]}`}
                        >
                          {bucketLabels[key] ?? key}
                        </th>
                      ))}
                      <th className="text-right py-2 px-4 font-semibold text-deep-navy">
                        Total
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {snap.customers
                      .slice()
                      .sort((a, b) => b.total - a.total)
                      .map((c: ArCustomer, idx: number) => (
                        <tr
                          key={idx}
                          className="border-b border-border/50 hover:bg-cloud/40 transition-colors"
                        >
                          <td className="py-2 px-4 text-deep-navy font-medium">
                            {c.customer_name ?? '—'}
                          </td>
                          <td className="py-2 px-3 text-slate hidden sm:table-cell">
                            {c.customer_number ?? '—'}
                          </td>
                          {BUCKET_KEYS.map((key) => (
                            <td
                              key={key}
                              className={`py-2 px-3 text-right tabular-nums ${
                                c[key] > 0 ? BUCKET_TEXT[key] : 'text-slate'
                              }`}
                            >
                              {c[key] > 0 ? formatMoney(c[key]) : '—'}
                            </td>
                          ))}
                          <td className="py-2 px-4 text-right tabular-nums font-semibold text-deep-navy">
                            {formatMoney(c.total)}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                  <tfoot>
                    <tr className="bg-cloud border-t-2 border-border font-semibold">
                      <td className="py-2 px-4 text-deep-navy">TOTAL</td>
                      <td className="py-2 px-3 hidden sm:table-cell" />
                      {BUCKET_KEYS.map((key) => (
                        <td
                          key={key}
                          className={`py-2 px-3 text-right tabular-nums ${BUCKET_TEXT[key]}`}
                        >
                          {formatMoney(snap.buckets[key])}
                        </td>
                      ))}
                      <td className="py-2 px-4 text-right tabular-nums text-deep-navy">
                        {formatMoney(snap.total_ar)}
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Write-down drawer */}
        <Sheet open={drawerOpen} onOpenChange={setDrawerOpen}>
          <SheetContent side="right" size="md" variant="light">
            <div className="p-6 space-y-5">
              <div>
                <SheetTitle className="text-deep-navy text-lg font-bold">
                  Post AR write-down
                </SheetTitle>
                <SheetDescription className="text-slate text-sm mt-1">
                  Posts Dr 6550 Bad Debt / Cr 1085 Accounts Receivable and
                  records an audit line. Requires an open accounting period.
                </SheetDescription>
              </div>

              <div className="space-y-4">
                <div className="space-y-1.5">
                  <Label htmlFor="wd-customer-name">Customer name</Label>
                  <Input
                    id="wd-customer-name"
                    value={form.customer_name}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, customer_name: e.target.value }))
                    }
                    placeholder="Optional"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="wd-customer-number">Customer number</Label>
                  <Input
                    id="wd-customer-number"
                    value={form.customer_number}
                    onChange={(e) =>
                      setForm((f) => ({
                        ...f,
                        customer_number: e.target.value,
                      }))
                    }
                    placeholder="Optional"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="wd-amount">
                    Amount <span className="text-bw-teal">*</span>
                  </Label>
                  <Input
                    id="wd-amount"
                    type="number"
                    min="0.01"
                    step="0.01"
                    value={form.amount}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, amount: e.target.value }))
                    }
                    placeholder="0.00"
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="wd-memo">Memo</Label>
                  <textarea
                    id="wd-memo"
                    value={form.memo}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, memo: e.target.value }))
                    }
                    placeholder="Reason for write-down (optional)"
                    rows={3}
                    className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-bw-teal/50 resize-none"
                  />
                </div>
              </div>

              <div className="flex gap-2 pt-2">
                <Button
                  variant="primary"
                  onClick={handleWriteDown}
                  disabled={writeDown.isPending || !form.amount}
                  className="flex-1"
                >
                  {writeDown.isPending ? 'Posting…' : 'Post write-down'}
                </Button>
                <Button
                  variant="secondary"
                  onClick={() => setDrawerOpen(false)}
                  disabled={writeDown.isPending}
                >
                  Cancel
                </Button>
              </div>
            </div>
          </SheetContent>
        </Sheet>
      </main>
    </>
  );
}
