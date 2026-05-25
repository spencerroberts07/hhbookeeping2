'use client';

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { AlertTriangle, CheckCircle2, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import { useUser } from '@clerk/nextjs';
import {
  fixCashBalancingImbalance,
  getCashBalancingMonthEndBatch,
} from '@/lib/api/cash_balancing';
import { formatMoney } from '@/lib/utils';

export function CashBalancingJournalTab() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ['cash-balancing-month-end', entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      getCashBalancingMonthEndBatch({ entity_code: entityCode! }),
  });

  const [offsetAccount, setOffsetAccount] = useState('1020');
  const [offsetDescription, setOffsetDescription] = useState(
    'Cash balancing close — balancing offset (float movement + misc reconciliation)',
  );

  const fix = useMutation({
    mutationFn: fixCashBalancingImbalance,
    onSuccess: (res) => {
      toast.success(
        `Posted balancing line for ${formatMoney(res.imbalance_resolved, { signed: true })}`,
      );
      qc.invalidateQueries({ queryKey: ['cash-balancing-month-end'] });
    },
    onError: () => toast.error('Fix-imbalance failed'),
  });

  if (q.isLoading) return <Skeleton className="h-64" />;

  const batch = q.data;
  if (!batch || !batch.batch_id) {
    return (
      <Card>
        <CardContent className="p-8 text-center text-slate">
          No cash balancing journal batch yet. Posts during month-end close.
        </CardContent>
      </Card>
    );
  }

  const canFix =
    batch.status === 'draft_unbalanced' && Math.abs(batch.imbalance) >= 0.01;

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>
              {batch.batch_label ?? 'Cash balancing month-end'}
              <span className="ml-2 text-xs font-normal text-slate">
                {batch.status}
              </span>
            </span>
            {batch.is_balanced ? (
              <Badge variant="complete" className="inline-flex items-center gap-1">
                <CheckCircle2 className="h-3 w-3" /> Balanced
              </Badge>
            ) : (
              <Badge variant="error" className="inline-flex items-center gap-1">
                <AlertTriangle className="h-3 w-3" /> Unbalanced
              </Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <div className="text-xs uppercase tracking-wide text-slate">
                Total debits
              </div>
              <div className="mt-1 text-lg font-semibold tabular-nums text-deep-navy">
                {formatMoney(batch.total_debits)}
              </div>
            </div>
            <div>
              <div className="text-xs uppercase tracking-wide text-slate">
                Total credits
              </div>
              <div className="mt-1 text-lg font-semibold tabular-nums text-deep-navy">
                {formatMoney(batch.total_credits)}
              </div>
            </div>
            <div>
              <div className="text-xs uppercase tracking-wide text-slate">
                Imbalance
              </div>
              <div
                className={`mt-1 text-lg font-semibold tabular-nums ${
                  batch.is_balanced ? 'text-deep-navy' : 'text-red-600'
                }`}
              >
                {formatMoney(batch.imbalance, { signed: true })}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {canFix && (
        <Card className="border-red-200">
          <CardHeader>
            <CardTitle className="text-base text-red-700 flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" /> Fix imbalance
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-slate mb-3">
              Post a one-line balancing entry to bring Dr = Cr. The journal
              flips to <code>draft</code> after posting and is ready for
              approval.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
              <div>
                <Label htmlFor="offset-acct">Offset account</Label>
                <Input
                  id="offset-acct"
                  value={offsetAccount}
                  onChange={(e) => setOffsetAccount(e.target.value)}
                  placeholder="1020"
                />
              </div>
              <div className="md:col-span-2">
                <Label htmlFor="offset-desc">Memo</Label>
                <Input
                  id="offset-desc"
                  value={offsetDescription}
                  onChange={(e) => setOffsetDescription(e.target.value)}
                />
              </div>
            </div>
            <div className="mt-4 flex justify-end">
              <Button
                onClick={() =>
                  fix.mutate({
                    entity_code: entityCode!,
                    period_id: batch.period_id!,
                    offset_account_code: offsetAccount,
                    offset_description: offsetDescription,
                    actor_email: user?.primaryEmailAddress?.emailAddress,
                  })
                }
                disabled={fix.isPending}
              >
                {fix.isPending && (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                )}
                Post balancing line
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Journal lines</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-cloud">
                <tr>
                  <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                    #
                  </th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                    Account
                  </th>
                  <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                    Memo
                  </th>
                  <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                    Debit
                  </th>
                  <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                    Credit
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {batch.lines.map((l) => (
                  <tr key={l.line_number} className="hover:bg-cloud">
                    <td className="px-4 py-2 text-right text-slate tabular-nums">
                      {l.line_number}
                    </td>
                    <td className="px-4 py-2 text-ink font-mono text-xs">
                      {l.account_code}
                    </td>
                    <td className="px-4 py-2 text-ink">{l.memo ?? '—'}</td>
                    <td className="px-4 py-2 tabular-nums text-right text-ink">
                      {l.debit_amount > 0 ? formatMoney(l.debit_amount) : ''}
                    </td>
                    <td className="px-4 py-2 tabular-nums text-right text-ink">
                      {l.credit_amount > 0 ? formatMoney(l.credit_amount) : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot className="bg-cloud font-semibold">
                <tr>
                  <td colSpan={3} className="px-4 py-2 text-right text-slate">
                    Totals
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right">
                    {formatMoney(batch.total_debits)}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right">
                    {formatMoney(batch.total_credits)}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
