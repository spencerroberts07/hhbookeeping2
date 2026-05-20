'use client';

import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { listGlRuns, getGlTransactions } from '@/lib/api/gl';
import { formatMoney, formatDate } from '@/lib/utils';
import { Search } from 'lucide-react';
import { cn } from '@/lib/utils';

export default function TransactionsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [selectedAccount, setSelectedAccount] = useState<string | null>(null);
  const [search, setSearch] = useState('');

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
                        <tr key={idx} className="hover:bg-cloud cursor-pointer">
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
    </>
  );
}
