'use client';

import { useState } from 'react';
import { useQuery, useQueryClient, useMutation } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Topbar } from '@/components/layout/topbar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  getYearEndStatus,
  setYearEndStatus,
  postAdjustingEntry,
  type YearEndStatus,
} from '@/lib/api/year-end';

const STATUS_VARIANT: Record<string, 'complete' | 'warning' | 'error' | 'locked'> = {
  draft: 'warning',
  in_review: 'complete',
  final_locked: 'locked',
};

function statusLabel(s: YearEndStatus): string {
  if (!s) return 'not started';
  return s.replace(/_/g, ' ');
}

export default function YearEndPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const now = new Date();
  const defaultFy = now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
  const [fy, setFy] = useState(defaultFy);

  const q = useQuery({
    queryKey: ['year-end', fy, entityCode],
    enabled: !!entityCode,
    queryFn: () => getYearEndStatus(fy, entityCode!),
  });

  const transition = useMutation({
    mutationFn: (status: 'draft' | 'in_review' | 'final_locked') =>
      setYearEndStatus({ fy, entity_code: entityCode!, status, actor_email: actorEmail }),
    onSuccess: (d) => {
      toast.success(`Year-end FY${fy} → ${statusLabel(d.year_end_status)}`);
      qc.invalidateQueries({ queryKey: ['year-end'] });
    },
    onError: (e: unknown) => {
      const msg =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
        'Transition failed';
      toast.error(msg);
    },
  });

  const fyOptions = [defaultFy - 1, defaultFy, defaultFy + 1];

  return (
    <>
      <Topbar title="Year-end" />
      <main className="space-y-4 p-6">
        <div className="flex items-end gap-3">
          <div>
            <Label htmlFor="fy">Fiscal year (ending Sep 30)</Label>
            <Select value={String(fy)} onValueChange={(v) => setFy(Number(v))}>
              <SelectTrigger id="fy" className="w-48">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {fyOptions.map((y) => (
                  <SelectItem key={y} value={String(y)}>
                    FY{y} (Oct {y - 1} – Sep {y})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {q.isLoading ? (
          <Skeleton className="h-48" />
        ) : q.data ? (
          <>
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>FY{q.data.fy} year-end</span>
                  <Badge variant={STATUS_VARIANT[q.data.year_end_status ?? ''] ?? 'warning'}>
                    {statusLabel(q.data.year_end_status)}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
                  <Stat label="Period" value={`${q.data.fy_start} → ${q.data.fy_end}`} />
                  <Stat
                    label="Periods closed"
                    value={`${q.data.periods_closed}/${q.data.periods_total}`}
                  />
                  <Stat label="Sep period closed" value={q.data.september_period_closed ? 'Yes' : 'No'} />
                  <Stat label="Adjusting entries" value={String(q.data.adjusting_entry_count)} />
                </div>

                {!q.data.year_end_status && (
                  <p className="text-sm text-slate">
                    Year-end activates when the September period is closed. Close FY{q.data.fy}{' '}
                    September first.
                  </p>
                )}

                {isAdmin && q.data.year_end_status && (
                  <div className="flex flex-wrap gap-2">
                    {q.data.year_end_status === 'draft' && (
                      <Button onClick={() => transition.mutate('in_review')} disabled={transition.isPending}>
                        Open for review
                      </Button>
                    )}
                    {q.data.year_end_status === 'in_review' && (
                      <>
                        <Button variant="secondary" onClick={() => transition.mutate('draft')} disabled={transition.isPending}>
                          Back to draft
                        </Button>
                        <Button onClick={() => transition.mutate('final_locked')} disabled={transition.isPending}>
                          Finalize &amp; lock
                        </Button>
                      </>
                    )}
                    {q.data.year_end_status === 'final_locked' && (
                      <p className="text-sm text-slate">
                        Year-end is finalized and locked. All journal entries (including adjustments)
                        are blocked for this fiscal year.
                      </p>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>

            {q.data.year_end_status === 'in_review' && isAdmin && (
              <AdjustingEntryForm fy={fy} entityCode={entityCode!} actorEmail={actorEmail} />
            )}
          </>
        ) : (
          <Card>
            <CardContent className="p-8 text-center text-slate">
              Select an entity to view year-end status.
            </CardContent>
          </Card>
        )}
      </main>
    </>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs text-slate">{label}</div>
      <div className="font-medium">{value}</div>
    </div>
  );
}

function AdjustingEntryForm({
  fy,
  entityCode,
  actorEmail,
}: {
  fy: number;
  entityCode: string;
  actorEmail: string;
}) {
  const qc = useQueryClient();
  const [label, setLabel] = useState('');
  const [drAcct, setDrAcct] = useState('');
  const [crAcct, setCrAcct] = useState('');
  const [amount, setAmount] = useState('');
  const [memo, setMemo] = useState('');

  const post = useMutation({
    mutationFn: () =>
      postAdjustingEntry({
        fy,
        entity_code: entityCode,
        label,
        actor_email: actorEmail,
        lines: [
          { account_code: drAcct, debit: Number(amount), memo },
          { account_code: crAcct, credit: Number(amount), memo },
        ],
      }),
    onSuccess: (d) => {
      toast.success(`Adjusting entry posted (${d.line_count} lines, balanced)`);
      setLabel('');
      setDrAcct('');
      setCrAcct('');
      setAmount('');
      setMemo('');
      qc.invalidateQueries({ queryKey: ['year-end'] });
    },
    onError: (e: unknown) => {
      const msg =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
        'Post failed';
      toast.error(msg);
    },
  });

  const valid = label && drAcct && crAcct && Number(amount) > 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Post year-end adjusting entry</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-xs text-slate">
          Posts a balanced <code>year_end_adjustment</code> batch into the September period. Two-line
          entry; account 3900 is blocked.
        </p>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
          <div className="md:col-span-2">
            <Label htmlFor="ye-label">Description</Label>
            <Input id="ye-label" value={label} onChange={(e) => setLabel(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="ye-dr">Debit account</Label>
            <Input id="ye-dr" value={drAcct} onChange={(e) => setDrAcct(e.target.value)} placeholder="e.g. 6900" />
          </div>
          <div>
            <Label htmlFor="ye-cr">Credit account</Label>
            <Input id="ye-cr" value={crAcct} onChange={(e) => setCrAcct(e.target.value)} placeholder="e.g. 1610" />
          </div>
          <div>
            <Label htmlFor="ye-amt">Amount</Label>
            <Input id="ye-amt" value={amount} onChange={(e) => setAmount(e.target.value)} />
          </div>
        </div>
        <div>
          <Label htmlFor="ye-memo">Memo (optional)</Label>
          <Input id="ye-memo" value={memo} onChange={(e) => setMemo(e.target.value)} />
        </div>
        <Button onClick={() => post.mutate()} disabled={!valid || post.isPending}>
          Post adjusting entry
        </Button>
      </CardContent>
    </Card>
  );
}
