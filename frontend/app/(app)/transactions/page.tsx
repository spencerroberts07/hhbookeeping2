'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useEntityStore } from '@/lib/store/entity';
import { listGlRuns, getGlTransactions, type GlTransaction } from '@/lib/api/gl';
import { listInvoiceDocuments, type InvoiceDocument } from '@/lib/api/invoices';
import { formatMoney, formatDate } from '@/lib/utils';
import { Search, FileText } from 'lucide-react';
import Link from 'next/link';
import { cn } from '@/lib/utils';

export default function TransactionsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [selectedTxn, setSelectedTxn] = useState<GlTransaction | null>(null);

  const runs = useQuery({
    queryKey: ['gl-runs', entityCode],
    enabled: !!entityCode,
    queryFn: () => listGlRuns(entityCode!),
  });
  const latest = runs.data?.runs[0];

  // Pull *all* txns from the latest run, then derive the account tree
  // from the transactions themselves.
  const allTxns = useQuery({
    queryKey: ['all-txns', entityCode, latest?.id],
    enabled: !!latest && !!entityCode,
    queryFn: () => getGlTransactions(entityCode!, latest!.id, undefined, 5000),
  });

  const accountTree = useMemo(() => {
    const map = new Map<string, { code: string; count: number }>();
    for (const t of allTxns.data?.transactions ?? []) {
      const cur = map.get(t.account_code) ?? { code: t.account_code, count: 0 };
      cur.count += 1;
      map.set(t.account_code, cur);
    }
    return Array.from(map.values()).sort((a, b) => a.code.localeCompare(b.code));
  }, [allTxns.data]);

  const filteredTxns = useMemo(() => {
    const txns = allTxns.data?.transactions ?? [];
    const lower = search.toLowerCase();
    return txns.filter((t) => {
      if (selectedAccount && t.account_code !== selectedAccount) return false;
      if (lower && !t.description.toLowerCase().includes(lower)) return false;
      return true;
    });
  }, [allTxns.data, selectedAccount, search]);

  return (
    <>
      <Topbar
        title="Transactions"
        periodLabel={latest ? `From GL import ${formatDate(latest.imported_at)}` : ''}
      />
      <main className="p-6 grid grid-cols-1 lg:grid-cols-[260px_1fr] gap-4">
        {/* Account tree */}
        <aside>
          <Card>
            <CardContent className="p-3">
              <div className="text-xs uppercase tracking-wider text-slate font-semibold mb-2">
                Accounts
              </div>
              {runs.isLoading || allTxns.isLoading ? (
                <Skeleton className="h-48" />
              ) : !latest ? (
                <p className="text-xs text-slate">
                  Upload a GL export from Month-end to see accounts.
                </p>
              ) : (
                <ul className="space-y-1 max-h-[60vh] overflow-y-auto pr-1">
                  <li>
                    <button
                      onClick={() => setSelectedAccount(null)}
                      className={cn(
                        'w-full text-left rounded-md px-2 py-1 text-sm',
                        selectedAccount === null
                          ? 'bg-deep-navy text-white'
                          : 'text-ink hover:bg-cloud',
                      )}
                    >
                      All accounts
                    </button>
                  </li>
                  {accountTree.map((a) => (
                    <li key={a.code}>
                      <button
                        onClick={() => setSelectedAccount(a.code)}
                        className={cn(
                          'w-full text-left rounded-md px-2 py-1 text-sm flex justify-between',
                          selectedAccount === a.code
                            ? 'bg-deep-navy text-white'
                            : 'text-ink hover:bg-cloud',
                        )}
                      >
                        <span className="font-mono">{a.code}</span>
                        <span className="text-xs text-slate">{a.count}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </aside>

        {/* Txns table */}
        <section>
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center gap-3 mb-3 max-w-md">
                <div className="relative flex-1">
                  <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-4 w-4 text-slate" strokeWidth={1.5} />
                  <Input
                    className="pl-8"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    placeholder="Search description"
                  />
                </div>
              </div>
              {allTxns.isLoading ? (
                <Skeleton className="h-96" />
              ) : !filteredTxns.length ? (
                <p className="text-slate py-12 text-center">No transactions.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-sm">
                    <thead className="bg-cloud sticky top-0">
                      <tr>
                        <th className="text-left font-semibold text-deep-navy px-3 py-2">Date</th>
                        <th className="text-left font-semibold text-deep-navy px-3 py-2">Description</th>
                        <th className="text-left font-semibold text-deep-navy px-3 py-2">Account</th>
                        <th className="text-left font-semibold text-deep-navy px-3 py-2">Ref</th>
                        <th className="text-right font-semibold text-deep-navy px-3 py-2">Debit</th>
                        <th className="text-right font-semibold text-deep-navy px-3 py-2">Credit</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {filteredTxns.slice(0, 500).map((t, idx) => (
                        <tr
                          key={idx}
                          onClick={() => setSelectedTxn(t)}
                          className="hover:bg-cloud cursor-pointer"
                        >
                          <td className="px-3 py-2 text-ink whitespace-nowrap">{formatDate(t.date)}</td>
                          <td className="px-3 py-2 text-ink">{t.description}</td>
                          <td className="px-3 py-2 text-slate font-mono text-xs">{t.account_code}</td>
                          <td className="px-3 py-2 text-slate font-mono text-xs">{t.ref ?? '—'}</td>
                          <td className="px-3 py-2 tabular-nums text-right text-ink">
                            {t.debit ? formatMoney(t.debit) : '—'}
                          </td>
                          <td className="px-3 py-2 tabular-nums text-right text-ink">
                            {t.credit ? formatMoney(t.credit) : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {filteredTxns.length > 500 && (
                <p className="text-xs text-slate mt-2">
                  Showing first 500 of {filteredTxns.length}. Use search or
                  account filter to narrow.
                </p>
              )}
            </CardContent>
          </Card>
        </section>
      </main>

      {selectedTxn && entityCode && (
        <TransactionDetailDialog
          txn={selectedTxn}
          entityCode={entityCode}
          onClose={() => setSelectedTxn(null)}
        />
      )}
    </>
  );
}

function TransactionDetailDialog({
  txn,
  entityCode,
  onClose,
}: {
  txn: GlTransaction;
  entityCode: string;
  onClose: () => void;
}) {
  // Search uploaded invoices with the same amount as this GL row so the
  // dealer can attach a source document. The amount on the GL row is the
  // debit or credit value — we search by whichever is non-zero.
  const amount = Math.abs(txn.debit || txn.credit || 0);

  const candidates = useQuery({
    queryKey: ['invoice-search', entityCode, amount],
    enabled: amount > 0,
    queryFn: () =>
      listInvoiceDocuments({
        entity_code: entityCode,
        limit: 25,
        // Backend list doesn't filter by amount yet — we fetch a small
        // recent set and filter client-side. Acceptable for v1.
      }),
    select: (data) =>
      data.invoices.filter((i: InvoiceDocument) => {
        const a = i.amount ? Number(i.amount) : 0;
        return Math.abs(a - amount) < 0.01;
      }),
  });

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Transaction detail</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <dl className="grid grid-cols-2 gap-2 text-sm">
            <dt className="text-slate">Date</dt>
            <dd className="text-ink">{formatDate(txn.date)}</dd>
            <dt className="text-slate">Account</dt>
            <dd className="text-ink font-mono">{txn.account_code}</dd>
            <dt className="text-slate">Description</dt>
            <dd className="text-ink">{txn.description}</dd>
            <dt className="text-slate">Reference</dt>
            <dd className="text-ink font-mono text-xs">{txn.ref ?? '—'}</dd>
            <dt className="text-slate">Debit</dt>
            <dd className="text-ink tabular-nums">
              {txn.debit ? formatMoney(txn.debit) : '—'}
            </dd>
            <dt className="text-slate">Credit</dt>
            <dd className="text-ink tabular-nums">
              {txn.credit ? formatMoney(txn.credit) : '—'}
            </dd>
          </dl>

          <div className="border-t border-border pt-4">
            <div className="text-sm font-semibold text-deep-navy mb-2 flex items-center gap-2">
              <FileText className="h-4 w-4" strokeWidth={1.5} />
              Source documents
            </div>
            {candidates.isLoading ? (
              <Skeleton className="h-16" />
            ) : !candidates.data?.length ? (
              <div className="rounded-lg border border-border bg-cloud p-3 text-sm text-slate">
                No invoice attached.{' '}
                <Link href="/ap/unmatched" className="text-ledger-blue underline">
                  Link an invoice →
                </Link>
                <p className="text-xs mt-1">
                  GL-import rows can only be linked through the AP module.
                  Once an invoice is posted to AP via BookWize, the link
                  appears here automatically.
                </p>
              </div>
            ) : (
              <ul className="space-y-2">
                {candidates.data.map((inv) => (
                  <li
                    key={inv.id}
                    className="flex items-center justify-between gap-2 rounded-lg border border-border bg-white p-3 text-sm"
                  >
                    <div className="min-w-0 flex items-center gap-3">
                      {inv.file_url ? (
                        <a
                          href={inv.file_url}
                          target="_blank"
                          rel="noreferrer"
                          aria-label="View PDF"
                          title="View PDF"
                          className="shrink-0 rounded-md p-1 text-slate hover:text-deep-navy hover:bg-cloud"
                        >
                          <FileText className="h-4 w-4" strokeWidth={1.5} />
                        </a>
                      ) : (
                        <span
                          aria-disabled
                          title="PDF not available — file uploaded before storage was configured"
                          className="shrink-0 rounded-md p-1 text-slate/40"
                        >
                          <FileText className="h-4 w-4" strokeWidth={1.5} />
                        </span>
                      )}
                      <div className="min-w-0">
                        <div className="font-semibold text-deep-navy truncate">
                          {inv.vendor_name ?? 'Unknown vendor'}
                        </div>
                        <div className="text-xs text-slate">
                          {inv.invoice_number ?? '—'} ·{' '}
                          {inv.invoice_date ? formatDate(inv.invoice_date) : '—'}{' '}
                          · {inv.amount ? formatMoney(inv.amount) : '—'}
                        </div>
                      </div>
                    </div>
                    <Badge
                      variant={
                        inv.status === 'posted_to_ap'
                          ? 'complete'
                          : inv.status === 'matched'
                            ? 'pending'
                            : 'warning'
                      }
                    >
                      {inv.status.replace('_', ' ')}
                    </Badge>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
