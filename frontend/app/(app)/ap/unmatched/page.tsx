'use client';

import { useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { formatMoney, formatDate } from '@/lib/utils';
import { useIsAdmin } from '@/lib/store/user';
import {
  getUnmatchedQueue,
  manualMatchInvoice,
  postInvoiceToAp,
  softDeleteInvoice,
  updateInvoiceDocument,
  type SuggestedMatch,
  type UnmatchedQueueRow,
  type ApAccount,
} from '@/lib/api/invoices';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Check, X, Pencil } from 'lucide-react';
import { toast } from 'sonner';

type FilterType = 'all' | 'hh_ap' | 'outside_vendor';

export default function UnmatchedQueuePage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const today = new Date().toISOString().slice(0, 10);
  const [periodEnd] = useState(today);
  const [filter, setFilter] = useState<FilterType>('all');

  const queue = useQuery({
    queryKey: ['unmatched-queue', entityCode],
    enabled: !!entityCode,
    queryFn: () => getUnmatchedQueue({ entity_code: entityCode! }),
  });

  const refresh = () => {
    qc.invalidateQueries({ queryKey: ['unmatched-queue'] });
    qc.invalidateQueries({ queryKey: ['invoice-documents'] });
  };

  const filtered = useMemo<UnmatchedQueueRow[]>(() => {
    if (!queue.data) return [];
    if (filter === 'all') return queue.data.queue;
    return queue.data.queue.filter((r) => r.invoice.invoice_type === filter);
  }, [queue.data, filter]);

  const [editingId, setEditingId] = useState<string | null>(null);
  const editing = filtered.find((r) => r.invoice.id === editingId);

  if (!entityCode) {
    return (
      <>
        <Topbar title="Unmatched invoices" />
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
      <Topbar title="Unmatched invoices" />
      <main className="p-6 space-y-4">
        <section className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <Stat label="Unmatched" value={queue.data?.total ?? 0} highlight />
          <Stat
            label="With suggested match"
            value={
              queue.data?.queue.filter((r) => r.suggested_matches.length > 0)
                .length ?? 0
            }
          />
          <Stat
            label="HH AP"
            value={
              queue.data?.queue.filter(
                (r) => r.invoice.invoice_type === 'hh_ap',
              ).length ?? 0
            }
          />
        </section>

        <div className="flex flex-wrap items-center gap-2">
          {(['all', 'hh_ap', 'outside_vendor'] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
                filter === f
                  ? 'bg-deep-navy text-white'
                  : 'bg-white border border-border text-ink hover:bg-cloud'
              }`}
            >
              {f === 'all' ? 'All' : f === 'hh_ap' ? 'HH AP' : 'Outside vendor'}
            </button>
          ))}
        </div>

        <Card>
          <CardContent className="p-0">
            {queue.isLoading ? (
              <Skeleton className="h-64 m-4" />
            ) : !filtered.length ? (
              <div className="p-8 text-center text-slate">
                Nothing unmatched. Nice.
              </div>
            ) : (
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Vendor</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Invoice #</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                    <th className="text-right font-semibold text-deep-navy px-4 py-2">Amount</th>
                    <th className="px-4 py-2">Type</th>
                    <th className="px-4 py-2">Suggested match</th>
                    <th className="px-4 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {filtered.map((row) => {
                    const top = row.suggested_matches[0];
                    return (
                      <tr key={row.invoice.id} className="hover:bg-cloud">
                        <td className="px-4 py-2 text-ink">
                          {row.invoice.vendor_name ?? <span className="text-amber-700">— missing</span>}
                        </td>
                        <td className="px-4 py-2 text-slate font-mono text-xs">
                          {row.invoice.invoice_number ?? '—'}
                        </td>
                        <td className="px-4 py-2 text-ink">
                          {row.invoice.invoice_date
                            ? formatDate(row.invoice.invoice_date)
                            : <span className="text-amber-700">— missing</span>}
                        </td>
                        <td className="px-4 py-2 tabular-nums text-right text-ink">
                          {row.invoice.amount && Number(row.invoice.amount) > 0
                            ? formatMoney(row.invoice.amount)
                            : <span className="text-amber-700">$0.00</span>}
                        </td>
                        <td className="px-4 py-2">
                          <Badge variant="info">
                            {row.invoice.invoice_type === 'hh_ap' ? 'HH AP' : 'Outside'}
                          </Badge>
                        </td>
                        <td className="px-4 py-2 max-w-md">
                          {top ? (
                            <SuggestedInline
                              suggestion={top}
                              onConfirm={async () => {
                                await manualMatchInvoice(row.invoice.id, {
                                  entity_code: entityCode,
                                  actor_email: actorEmail,
                                  ...(top.type === 'bank'
                                    ? { bank_transaction_id: top.id }
                                    : top.type === 'journal'
                                      ? { journal_batch_id: top.id }
                                      : { hh_ap_invoice_id: top.id }),
                                });
                                toast.success('Matched');
                                refresh();
                              }}
                              onReject={() => {
                                // Reject = nothing to persist; we just don't auto-link.
                                toast.info('Suggestion dismissed for this session');
                              }}
                            />
                          ) : (
                            <span className="text-xs text-slate">no candidate</span>
                          )}
                        </td>
                        <td className="px-4 py-2 text-right">
                          <RowActions
                            row={row}
                            onEdit={() => setEditingId(row.invoice.id)}
                            onPost={async () => {
                              if (!row.invoice.amount || Number(row.invoice.amount) <= 0) {
                                toast.error('Edit the invoice first — amount is 0.');
                                return;
                              }
                              const ap_account: ApAccount =
                                (row.invoice.ap_account as ApAccount) ??
                                (row.invoice.invoice_type === 'hh_ap'
                                  ? '2030'
                                  : '2020');
                              await postInvoiceToAp(row.invoice.id, {
                                entity_code: entityCode,
                                actor_email: actorEmail,
                                ap_account,
                                period_end: periodEnd,
                              });
                              toast.success('Posted to AP');
                              refresh();
                            }}
                            onDelete={async () => {
                              const reason = window.prompt(
                                'Reason for deleting this invoice document?',
                              );
                              if (!reason) return;
                              await softDeleteInvoice(row.invoice.id, {
                                entity_code: entityCode,
                                reason,
                              });
                              toast.success('Invoice removed');
                              refresh();
                            }}
                            canPost={!!isAdmin || true}
                          />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </main>

      {editing && (
        <EditInvoiceDialog
          open
          row={editing}
          entityCode={entityCode}
          onClose={() => setEditingId(null)}
          onSaved={() => {
            setEditingId(null);
            refresh();
          }}
        />
      )}
    </>
  );
}

function Stat({
  label,
  value,
  highlight,
}: {
  label: string;
  value: number;
  highlight?: boolean;
}) {
  return (
    <Card className={`p-3 ${highlight ? 'bg-deep-navy text-white border-deep-navy' : ''}`}>
      <div className={`text-xs uppercase tracking-wide ${highlight ? 'text-white/70' : 'text-slate'}`}>
        {label}
      </div>
      <div className="text-xl font-bold tabular-nums">{value}</div>
    </Card>
  );
}

function SuggestedInline({
  suggestion,
  onConfirm,
  onReject,
}: {
  suggestion: SuggestedMatch;
  onConfirm: () => Promise<void>;
  onReject: () => void;
}) {
  const [busy, setBusy] = useState(false);
  return (
    <div className="flex items-center gap-2">
      <div className="text-xs">
        <div className="text-deep-navy font-semibold truncate" title={suggestion.description}>
          {suggestion.description}
        </div>
        <div className="text-slate">
          {formatMoney(suggestion.amount)}
          {suggestion.date ? ` · ${formatDate(suggestion.date)}` : ''}
          {' · '}
          <Badge
            variant={
              suggestion.confidence >= 95
                ? 'complete'
                : suggestion.confidence >= 70
                  ? 'pending'
                  : 'warning'
            }
            className="text-[10px]"
          >
            {suggestion.confidence.toFixed(0)}%
          </Badge>
        </div>
      </div>
      <Button
        size="sm"
        variant="accent"
        onClick={async () => {
          setBusy(true);
          try {
            await onConfirm();
          } finally {
            setBusy(false);
          }
        }}
        disabled={busy}
        aria-label="Confirm match"
      >
        <Check className="h-4 w-4" strokeWidth={2} />
      </Button>
      <Button
        size="sm"
        variant="ghost"
        onClick={onReject}
        disabled={busy}
        aria-label="Reject suggestion"
      >
        <X className="h-4 w-4" strokeWidth={2} />
      </Button>
    </div>
  );
}

function RowActions({
  row,
  onEdit,
  onPost,
  onDelete,
  canPost,
}: {
  row: UnmatchedQueueRow;
  onEdit: () => void;
  onPost: () => Promise<void>;
  onDelete: () => Promise<void>;
  canPost: boolean;
}) {
  const [busy, setBusy] = useState(false);
  return (
    <div className="flex justify-end gap-1">
      <Button size="sm" variant="ghost" onClick={onEdit} aria-label="Edit">
        <Pencil className="h-4 w-4" strokeWidth={1.5} />
      </Button>
      {canPost && (
        <Button
          size="sm"
          variant="secondary"
          onClick={async () => {
            setBusy(true);
            try {
              await onPost();
            } finally {
              setBusy(false);
            }
          }}
          disabled={busy}
        >
          Post to AP
        </Button>
      )}
      <Button
        size="sm"
        variant="ghost"
        onClick={async () => {
          setBusy(true);
          try {
            await onDelete();
          } finally {
            setBusy(false);
          }
        }}
        disabled={busy}
        aria-label="Delete"
      >
        <X className="h-4 w-4 text-red-700" strokeWidth={1.5} />
      </Button>
    </div>
  );
}

function EditInvoiceDialog({
  open,
  row,
  entityCode,
  onClose,
  onSaved,
}: {
  open: boolean;
  row: UnmatchedQueueRow;
  entityCode: string;
  onClose: () => void;
  onSaved: () => void;
}) {
  const inv = row.invoice;
  const [vendor, setVendor] = useState(inv.vendor_name ?? '');
  const [invNumber, setInvNumber] = useState(inv.invoice_number ?? '');
  const [invDate, setInvDate] = useState(inv.invoice_date ?? '');
  const [amount, setAmount] = useState(inv.amount ?? '');
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      await updateInvoiceDocument(inv.id, {
        entity_code: entityCode,
        vendor_name: vendor || undefined,
        invoice_number: invNumber || undefined,
        invoice_date: invDate || undefined,
        amount: amount === '' ? undefined : amount,
      });
      toast.success('Invoice updated');
      onSaved();
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Edit invoice</DialogTitle>
        </DialogHeader>
        <div className="space-y-3">
          <div>
            <Label htmlFor="v">Vendor</Label>
            <Input id="v" value={vendor} onChange={(e) => setVendor(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="n">Invoice number</Label>
            <Input id="n" value={invNumber} onChange={(e) => setInvNumber(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="d">Invoice date</Label>
            <Input id="d" type="date" value={invDate} onChange={(e) => setInvDate(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="a">Amount</Label>
            <Input
              id="a"
              type="number"
              inputMode="decimal"
              step="0.01"
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="ghost" onClick={onClose} disabled={saving}>
              Cancel
            </Button>
            <Button onClick={save} disabled={saving}>
              {saving ? 'Saving…' : 'Save'}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
