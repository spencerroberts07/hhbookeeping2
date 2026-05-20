'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Wand2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { useUser } from '@clerk/nextjs';
import { runBankAutoJournal, listBankAutoJournalRuns } from '@/lib/api/bank';
import { runAutoMatch } from '@/lib/api/auto_match';
import { formatDate } from '@/lib/utils';
import { toast } from 'sonner';

interface Props {
  entityCode: string;
  periodEnd: string;
}

interface RunRecord {
  id: string;
  started_at: string;
  matched_count?: number;
  flagged_count?: number;
}

export function StepAutoClassify({ entityCode, periodEnd }: Props) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const [running, setRunning] = useState(false);

  const periodStart = (() => {
    const d = new Date(periodEnd);
    return new Date(d.getFullYear(), d.getMonth(), 1)
      .toISOString()
      .slice(0, 10);
  })();

  const runsQuery = useQuery({
    queryKey: ['auto-journal-runs', entityCode],
    queryFn: () => listBankAutoJournalRuns(entityCode, 5),
  });

  const onRun = async () => {
    setRunning(true);
    try {
      await runBankAutoJournal({
        entity_code: entityCode,
        period_start: periodStart,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      await runAutoMatch({
        entity_code: entityCode,
        period_start: periodStart,
        period_end: periodEnd,
        actor_email: actorEmail,
      });
      await runsQuery.refetch();
      toast.success('Auto-journal and auto-match complete');
    } finally {
      setRunning(false);
    }
  };

  const runs = (runsQuery.data as { runs?: RunRecord[] } | undefined)?.runs ?? [];
  const lastRun = runs[0];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="text-sm text-slate">
          {lastRun
            ? `Last run: ${formatDate(lastRun.started_at)}`
            : 'No runs yet for this entity.'}
        </div>
        <Button
          type="button"
          variant="accent"
          onClick={onRun}
          disabled={running || !actorEmail}
        >
          <Wand2 className="h-4 w-4" strokeWidth={1.5} />
          {running ? 'Running…' : 'Run auto-classify'}
        </Button>
      </div>
      {lastRun && (
        <div className="flex flex-wrap items-center gap-4 text-sm">
          {lastRun.matched_count !== undefined && (
            <Badge variant="complete">
              {lastRun.matched_count} matched
            </Badge>
          )}
          {lastRun.flagged_count !== undefined && (
            <Badge variant="warning">
              {lastRun.flagged_count} flagged for review
            </Badge>
          )}
        </div>
      )}
    </div>
  );
}
