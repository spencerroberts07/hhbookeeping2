'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Loader2, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import {
  getCashBalancingHistory,
  triggerCashBalancingSync,
  type CashBalancingSyncRun,
} from '@/lib/api/cash_balancing';
import { formatDate } from '@/lib/utils';

export function CashBalancingSyncTab() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ['cash-balancing-history', entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      getCashBalancingHistory({ entity_code: entityCode!, limit: 20 }),
  });

  const sync = useMutation({
    mutationFn: () => triggerCashBalancingSync(entityCode!),
    onSuccess: (res) => {
      toast.success(
        `Sync complete — ${res.days_upserted ?? 0} days, ${res.lines_inserted ?? 0} lines`,
      );
      qc.invalidateQueries({ queryKey: ['cash-balancing-history'] });
      qc.invalidateQueries({ queryKey: ['cash-balancing-days'] });
    },
    onError: () => toast.error('Sync failed'),
  });

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between text-base">
            <span>Sync history</span>
            <Button
              size="sm"
              onClick={() => sync.mutate()}
              disabled={sync.isPending}
            >
              {sync.isPending ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4 mr-2" />
              )}
              Sync now
            </Button>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {q.isLoading ? (
            <Skeleton className="h-64 m-4" />
          ) : !q.data || q.data.runs.length === 0 ? (
            <div className="p-8 text-center text-slate">
              No sync runs recorded yet.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="bg-cloud">
                  <tr>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Started
                    </th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Type
                    </th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Status
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Duration
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Days
                    </th>
                    <th className="text-right px-4 py-2 font-semibold text-deep-navy">
                      Lines
                    </th>
                    <th className="text-left px-4 py-2 font-semibold text-deep-navy">
                      Notes
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {q.data.runs.map((r) => (
                    <RunRow key={r.id} run={r} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function RunRow({ run }: { run: CashBalancingSyncRun }) {
  const variant: 'complete' | 'warning' | 'error' | 'pending' =
    run.status === 'completed' || run.status === 'ok'
      ? 'complete'
      : run.status === 'failed' || run.status === 'error'
        ? 'error'
        : run.status === 'running'
          ? 'pending'
          : 'warning';
  return (
    <tr className="hover:bg-cloud">
      <td className="px-4 py-2 text-ink whitespace-nowrap">
        {run.started_at
          ? formatDate(run.started_at, 'MMM dd, HH:mm')
          : '—'}
      </td>
      <td className="px-4 py-2 text-slate text-xs uppercase">
        {run.run_type}
      </td>
      <td className="px-4 py-2">
        <Badge variant={variant}>{run.status}</Badge>
      </td>
      <td className="px-4 py-2 tabular-nums text-right text-slate">
        {run.duration_seconds != null ? `${run.duration_seconds}s` : '—'}
      </td>
      <td className="px-4 py-2 tabular-nums text-right text-ink">
        {run.days_upserted ?? '—'}
      </td>
      <td className="px-4 py-2 tabular-nums text-right text-ink">
        {run.lines_inserted ?? '—'}
      </td>
      <td className="px-4 py-2 text-slate text-xs max-w-[280px] truncate" title={run.error_text ?? undefined}>
        {run.error_text ?? '—'}
      </td>
    </tr>
  );
}
