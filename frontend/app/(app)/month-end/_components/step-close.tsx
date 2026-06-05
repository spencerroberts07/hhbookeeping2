'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useUser } from '@clerk/nextjs';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { useIsAdmin } from '@/lib/store/user';
import { getPeriodStatus, submitPeriodForClose, approvePeriodClose } from '@/lib/api/month_end';
import { toast } from 'sonner';
import { formatDate } from '@/lib/utils';
import { MonthEndDocumentCard } from './month-end-document-card';

interface Props {
  entityCode: string;
  periodEnd: string;
}

export function StepClose({ entityCode, periodEnd }: Props) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();
  const [busy, setBusy] = useState(false);

  const status = useQuery({
    queryKey: ['period-status', entityCode, periodEnd],
    queryFn: () => getPeriodStatus(entityCode, periodEnd),
  });

  if (status.isLoading) return <Skeleton className="h-24" />;
  if (!status.data) return null;

  const data = status.data;
  const isClosed = data.status === 'closed';
  const isSubmitted = data.status === 'submitted_for_close';
  const hasBlocking = (data.blocking_items?.length ?? 0) > 0;

  const onSubmit = async () => {
    setBusy(true);
    try {
      await submitPeriodForClose({
        entity_code: entityCode,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      qc.invalidateQueries({ queryKey: ['period-status'] });
      toast.success('Period submitted for close');
    } finally {
      setBusy(false);
    }
  };

  const onApprove = async () => {
    setBusy(true);
    try {
      await approvePeriodClose({
        entity_code: entityCode,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      qc.invalidateQueries({ queryKey: ['period-status'] });
      toast.success('Period closed');
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <span className="text-sm text-slate">Current status:</span>
        <Badge
          variant={
            isClosed ? 'complete' : isSubmitted ? 'pending' : hasBlocking ? 'error' : 'warning'
          }
        >
          {data.status}
        </Badge>
      </div>

      {data.approved_by && (
        <p className="text-xs text-slate">
          Approved by {data.approved_by} on {formatDate(data.approved_at)}
        </p>
      )}

      {hasBlocking && !isClosed && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3">
          <div className="text-sm font-semibold text-red-800 mb-1">
            Blocking items
          </div>
          <ul className="text-xs text-red-700 space-y-1">
            {data.blocking_items.map((item, idx) => (
              <li key={idx}>• {item.description} ({item.module})</li>
            ))}
          </ul>
        </div>
      )}

      {!isClosed && (
        <div className="flex gap-2">
          {!isSubmitted && (
            <Button
              variant="secondary"
              onClick={onSubmit}
              disabled={busy || hasBlocking || !actorEmail}
            >
              Submit for close
            </Button>
          )}
          {isAdmin ? (
            <Button
              variant="primary"
              onClick={onApprove}
              disabled={busy || hasBlocking || !isSubmitted}
            >
              Approve & close
            </Button>
          ) : (
            <p className="text-xs text-slate self-center">
              Only an admin can approve close.
            </p>
          )}
        </div>
      )}

      {/* Month-end financial package (Phase 4B) — auto-generated on close;
          manual (re)generate + email here. */}
      <MonthEndDocumentCard entityCode={entityCode} periodEnd={periodEnd} />
    </div>
  );
}
