'use client';

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { CheckCircle2, AlertTriangle, Lock, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  computeBankRec,
  getJournalCandidates,
  lockBankRec,
  type BankRec,
  type JournalCandidate,
} from '@/lib/api/bank-rec';
import { formatMoney } from '@/lib/utils';

export function ReconciliationTab() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();

  const [accountCode, setAccountCode] = useState('1020');
  const [periodEnd, setPeriodEnd] = useState('2026-02-28');
  const [statementDate, setStatementDate] = useState('2026-02-27');
  const [closing, setClosing] = useState('-616218.86');
  const [confirmedDit, setConfirmedDit] = useState('');
  const [rec, setRec] = useState<BankRec | null>(null);

  const compute = useMutation({
    mutationFn: () =>
      computeBankRec({
        entity_code: entityCode!,
        source_account_code: accountCode,
        period_end: periodEnd,
        statement_date: statementDate,
        statement_closing_balance: Number(closing),
        confirmed_deposits_in_transit: confirmedDit ? Number(confirmedDit) : null,
      }),
    onSuccess: (data) => {
      setRec(data);
      if (data.ties) toast.success('Reconciliation ties — variance $0.00');
      else toast.warning(`Off by ${formatMoney(data.variance, { signed: true })}`);
    },
    onError: () => toast.error('Compute failed'),
  });

  const candidatesQ = useQuery({
    queryKey: ['bank-rec-candidates', rec?.id],
    enabled: !!rec?.id,
    queryFn: () => getJournalCandidates(rec!.id),
  });

  const lock = useMutation({
    mutationFn: () => lockBankRec(rec!.id),
    onSuccess: () => {
      toast.success('Reconciliation locked');
      setRec((r) => (r ? { ...r, status: 'locked' } : r));
      qc.invalidateQueries({ queryKey: ['bank-rec'] });
    },
    onError: () => toast.error('Lock failed — does it tie?'),
  });

  return (
    <div className="space-y-4">
      {/* Picker */}
      <Card>
        <CardHeader>
          <CardTitle>Reconcile cash account to statement</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
            <div>
              <Label htmlFor="acct">Account</Label>
              <Input id="acct" value={accountCode} onChange={(e) => setAccountCode(e.target.value)} />
            </div>
            <div>
              <Label htmlFor="pend">Period end</Label>
              <Input id="pend" type="date" value={periodEnd} onChange={(e) => setPeriodEnd(e.target.value)} />
            </div>
            <div>
              <Label htmlFor="sdate">Statement date</Label>
              <Input id="sdate" type="date" value={statementDate} onChange={(e) => setStatementDate(e.target.value)} />
            </div>
            <div>
              <Label htmlFor="close">Statement closing</Label>
              <Input id="close" value={closing} onChange={(e) => setClosing(e.target.value)} />
            </div>
            <div>
              <Label htmlFor="dit">Confirmed DIT (opt.)</Label>
              <Input id="dit" value={confirmedDit} placeholder="auto" onChange={(e) => setConfirmedDit(e.target.value)} />
            </div>
          </div>
          <Button className="mt-4" onClick={() => compute.mutate()} disabled={!entityCode || compute.isPending}>
            {compute.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            Compute reconciliation
          </Button>
        </CardContent>
      </Card>

      {compute.isPending && <Skeleton className="h-72" />}

      {rec && (
        <>
          {/* Waterfall summary */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center justify-between">
                <span>Reconciliation summary</span>
                {rec.ties ? (
                  <Badge variant="complete" className="gap-1">
                    <CheckCircle2 className="h-3.5 w-3.5" /> Ties · variance {formatMoney(rec.variance)}
                  </Badge>
                ) : (
                  <Badge variant="error" className="gap-1">
                    <AlertTriangle className="h-3.5 w-3.5" /> Off by {formatMoney(rec.variance, { signed: true })}
                  </Badge>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <tbody>
                  <WaterfallRow label="Book balance (GL 1020, cutover-aware)" value={rec.book_balance} bold />
                  <WaterfallRow label="− Deposits in transit (clear next cycle)" value={-rec.deposits_in_transit} />
                  <WaterfallRow label="+ Outstanding cheques (term loan LN PYMT)" value={rec.outstanding_cheques} hint="signed — adds back the not-yet-cleared payment" />
                  <WaterfallRow label="− Payroll source deductions (bank-only, gross eNet draw)" value={-rec.payroll_deductions} />
                  <WaterfallRow label="+ Other bank-only items" value={rec.summary.named_items.other_bank_only} />
                  <tr className="border-t-2 border-ink/20">
                    <td className="py-2 font-semibold">= Expected closing</td>
                    <td className="py-2 text-right font-mono font-semibold">{formatMoney(rec.expected_closing)}</td>
                  </tr>
                  <tr>
                    <td className="py-1 text-slate">Statement closing (actual)</td>
                    <td className="py-1 text-right font-mono text-slate">{formatMoney(rec.statement_closing_balance)}</td>
                  </tr>
                </tbody>
              </table>
              <div className="mt-4 flex items-center gap-3">
                <Button
                  onClick={() => lock.mutate()}
                  disabled={!rec.ties || rec.status === 'locked' || lock.isPending}
                >
                  {lock.isPending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Lock className="mr-2 h-4 w-4" />}
                  {rec.status === 'locked' ? 'Locked' : 'Lock reconciliation'}
                </Button>
                {!rec.ties && <span className="text-sm text-error">Lock blocked until variance ≤ $0.01.</span>}
              </div>
            </CardContent>
          </Card>

          {/* Match panes */}
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            <PaneCard
              title="Cleared"
              count={(rec.summary.match.pre_cleared ?? 0) + (rec.summary.match.auto_cleared ?? 0)}
              variant="complete"
            >
              <p className="text-sm text-slate">
                {rec.summary.match.auto_cleared ?? 0} auto-cleared, {rec.summary.match.pre_cleared ?? 0} pre-linked
                of {rec.summary.match.bank_count ?? 0} bank lines.
              </p>
            </PaneCard>
            <PaneCard title="Suggested" count={rec.summary.match.suggested ?? 0} variant="warning">
              <p className="text-sm text-slate">Lower-confidence matches awaiting review.</p>
            </PaneCard>
            <PaneCard title="Outstanding / bank-only" count={rec.summary.outstanding_book.length + rec.summary.bank_only_items.length} variant="error">
              <ul className="space-y-1 text-sm">
                {rec.summary.outstanding_book.map((o, i) => (
                  <li key={`ob-${i}`} className="flex justify-between">
                    <span className="truncate text-slate">{o.memo || o.source_module}</span>
                    <span className="font-mono">{formatMoney(o.amount, { signed: true })}</span>
                  </li>
                ))}
                {rec.summary.bank_only_items.map((b, i) => (
                  <li key={`bo-${i}`} className="flex justify-between">
                    <span className="truncate text-slate">{b.description}</span>
                    <span className="font-mono">{formatMoney(b.amount, { signed: true })}</span>
                  </li>
                ))}
                {rec.summary.outstanding_book.length + rec.summary.bank_only_items.length === 0 && (
                  <li className="text-slate">None.</li>
                )}
              </ul>
            </PaneCard>
          </div>

          {/* 3D journal candidates */}
          {candidatesQ.data && candidatesQ.data.candidates.length > 0 && (
            <Card className="border-amber-300">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-amber-700">
                  <AlertTriangle className="h-4 w-4" /> Pending journal entries (confirm before posting)
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                {candidatesQ.data.candidates.map((c: JournalCandidate, i: number) => (
                  <div key={i} className="rounded-md border border-amber-200 bg-amber-50/50 p-4">
                    <div className="mb-2 flex items-center justify-between">
                      <span className="font-medium">{c.description}</span>
                      <Badge variant="warning">{c.status} · {c.post_to.replace(/_/g, ' ')}</Badge>
                    </div>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-slate">
                          <th className="py-1">Account</th>
                          <th className="py-1 text-right">Debit</th>
                          <th className="py-1 text-right">Credit</th>
                        </tr>
                      </thead>
                      <tbody>
                        {c.lines.map((l, j) => (
                          <tr key={j} className="border-t border-amber-100">
                            <td className="py-1">{l.account_code} · {l.account_name}</td>
                            <td className="py-1 text-right font-mono">{l.debit ? formatMoney(l.debit) : ''}</td>
                            <td className="py-1 text-right font-mono">{l.credit ? formatMoney(l.credit) : ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ))}
                <p className="text-xs text-slate">{candidatesQ.data.note}</p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}

function WaterfallRow({
  label,
  value,
  bold,
  hint,
}: {
  label: string;
  value: number;
  bold?: boolean;
  hint?: string;
}) {
  return (
    <tr>
      <td className={`py-1 ${bold ? 'font-medium' : ''}`}>
        {label}
        {hint && <span className="ml-2 text-xs text-slate">{hint}</span>}
      </td>
      <td className={`py-1 text-right font-mono ${bold ? 'font-medium' : ''}`}>
        {formatMoney(value, { signed: true })}
      </td>
    </tr>
  );
}

function PaneCard({
  title,
  count,
  variant,
  children,
}: {
  title: string;
  count: number;
  variant: 'complete' | 'warning' | 'error';
  children: React.ReactNode;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span>{title}</span>
          <Badge variant={variant}>{count}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}
