'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useEntityStore } from '@/lib/store/entity';
import {
  listBankTransactions,
  type BankMatchState,
  type BankReviewStatus,
} from '@/lib/api/bank';
import { formatDate, formatMoney } from '@/lib/utils';

const MATCH_LABEL: Record<BankMatchState, string> = {
  matched: 'Matched',
  needs_review: 'Needs review',
  unmatched: 'Unmatched',
  ignored: 'Ignored',
};
const MATCH_VARIANT: Record<BankMatchState, 'complete' | 'warning' | 'error' | 'locked'> = {
  matched: 'complete',
  needs_review: 'warning',
  unmatched: 'error',
  ignored: 'locked',
};

export default function BankPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const today = new Date();
  const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1)
    .toISOString()
    .slice(0, 10);
  const endOfMonth = today.toISOString().slice(0, 10);

  const [dateFrom, setDateFrom] = useState(startOfMonth);
  const [dateTo, setDateTo] = useState(endOfMonth);
  const [matchFilter, setMatchFilter] = useState<BankMatchState | 'all'>('all');
  const [reviewFilter, setReviewFilter] = useState<BankReviewStatus | 'all'>(
    'all',
  );

  const txns = useQuery({
    queryKey: ['bank-txns', entityCode, dateFrom, dateTo, matchFilter, reviewFilter],
    enabled: !!entityCode,
    queryFn: () =>
      listBankTransactions({
        entity_code: entityCode!,
        date_from: dateFrom,
        date_to: dateTo,
        match_state: matchFilter === 'all' ? undefined : matchFilter,
        review_status: reviewFilter === 'all' ? undefined : reviewFilter,
        limit: 500,
      }),
  });

  return (
    <>
      <Topbar title="Bank" />
      <main className="p-6 space-y-4">
        <Card>
          <CardContent className="p-4">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
              <div>
                <Label htmlFor="from">From</Label>
                <Input id="from" type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
              </div>
              <div>
                <Label htmlFor="to">To</Label>
                <Input id="to" type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
              </div>
              <div>
                <Label htmlFor="match">Match state</Label>
                <Select value={matchFilter} onValueChange={(v) => setMatchFilter(v as BankMatchState | 'all')}>
                  <SelectTrigger id="match">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All</SelectItem>
                    <SelectItem value="matched">Matched</SelectItem>
                    <SelectItem value="needs_review">Needs review</SelectItem>
                    <SelectItem value="unmatched">Unmatched</SelectItem>
                    <SelectItem value="ignored">Ignored</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="review">Review</Label>
                <Select value={reviewFilter} onValueChange={(v) => setReviewFilter(v as BankReviewStatus | 'all')}>
                  <SelectTrigger id="review">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All</SelectItem>
                    <SelectItem value="pending">Pending</SelectItem>
                    <SelectItem value="reviewed">Reviewed</SelectItem>
                    <SelectItem value="flagged">Flagged</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-0">
            {txns.isLoading ? (
              <Skeleton className="h-64 m-4" />
            ) : !txns.data?.transactions.length ? (
              <div className="p-8 text-center text-slate">
                No transactions in that window. Upload a statement to import data.
              </div>
            ) : (
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                    <th className="text-left font-semibold text-deep-navy px-4 py-2">Description</th>
                    <th className="text-right font-semibold text-deep-navy px-4 py-2">Amount</th>
                    <th className="px-4 py-2">Match</th>
                    <th className="px-4 py-2">Review</th>
                    <th className="px-4 py-2" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {txns.data.transactions.map((t) => (
                    <tr key={t.id} className="hover:bg-cloud">
                      <td className="px-4 py-2 text-ink whitespace-nowrap">{formatDate(t.txn_date)}</td>
                      <td className="px-4 py-2 text-ink">{t.description}</td>
                      <td className="px-4 py-2 tabular-nums text-right">
                        <span className={t.direction === 'inflow' ? 'text-bw-teal' : 'text-ink'}>
                          {formatMoney(t.amount, { signed: t.direction === 'unknown' })}
                        </span>
                      </td>
                      <td className="px-4 py-2">
                        <Badge variant={MATCH_VARIANT[t.match_state]}>
                          {MATCH_LABEL[t.match_state]}
                        </Badge>
                      </td>
                      <td className="px-4 py-2">
                        <Badge variant="info">{t.review_status}</Badge>
                      </td>
                      <td className="px-4 py-2 text-right">
                        {t.match_state === 'needs_review' && (
                          <Button size="sm" variant="accent">
                            Classify
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardContent>
        </Card>
      </main>
    </>
  );
}
