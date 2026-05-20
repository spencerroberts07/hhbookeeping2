'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { useUser } from '@clerk/nextjs';
import { buildCogsJournal, getCogsStatus, getSuggestedDating } from '@/lib/api/cogs';
import { buildAccrualJournal } from '@/lib/api/accruals';
import { buildDepreciationJournal } from '@/lib/api/depreciation';
import { buildRemittanceClearing } from '@/lib/api/hh_ap';
import { approveBatch } from '@/lib/api/month_end';
import { toast } from 'sonner';
import { useQuery, useQueryClient } from '@tanstack/react-query';

interface JournalCardProps {
  title: string;
  description: string;
  onBuild: () => Promise<void>;
  onApprove?: () => Promise<void>;
  status?: 'not_built' | 'draft' | 'pending_approval' | 'approved' | 'posted';
}

function JournalCard({ title, description, onBuild, onApprove, status = 'not_built' }: JournalCardProps) {
  const [busy, setBusy] = useState(false);
  const variantByStatus = {
    not_built: 'info' as const,
    draft: 'pending' as const,
    pending_approval: 'warning' as const,
    approved: 'complete' as const,
    posted: 'complete' as const,
  };
  return (
    <div className="rounded-xl border border-border bg-white p-4 flex flex-col gap-3">
      <div>
        <div className="flex items-center justify-between gap-2 mb-1">
          <div className="font-semibold text-deep-navy">{title}</div>
          <Badge variant={variantByStatus[status]}>{status.replace('_', ' ')}</Badge>
        </div>
        <p className="text-xs text-slate">{description}</p>
      </div>
      <div className="flex gap-2">
        <Button
          size="sm"
          variant="secondary"
          onClick={async () => {
            setBusy(true);
            try { await onBuild(); } finally { setBusy(false); }
          }}
          disabled={busy || status === 'approved' || status === 'posted'}
        >
          {busy ? 'Working…' : status === 'not_built' ? 'Build' : 'Re-build'}
        </Button>
        {onApprove && (
          <Button
            size="sm"
            variant="accent"
            onClick={async () => {
              setBusy(true);
              try { await onApprove(); } finally { setBusy(false); }
            }}
            disabled={busy || status === 'approved' || status === 'posted' || status === 'not_built'}
          >
            Approve
          </Button>
        )}
      </div>
    </div>
  );
}

interface Props {
  entityCode: string;
  periodEnd: string;
}

export function StepJournals({ entityCode, periodEnd }: Props) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const qc = useQueryClient();

  const cogsStatus = useQuery({
    queryKey: ['cogs-status', entityCode, periodEnd],
    queryFn: () => getCogsStatus(entityCode, periodEnd),
  });

  const buildCogs = async () => {
    const suggested = await getSuggestedDating(entityCode, periodEnd);
    await buildCogsJournal({
      entity_code: entityCode,
      period_end: periodEnd,
      actor_email: actorEmail,
      dating_new_amount: suggested.suggested_new_amount,
      dating_reversal_amount: suggested.suggested_reversal_amount,
    });
    qc.invalidateQueries({ queryKey: ['cogs-status'] });
    toast.success('COGS journal built');
  };

  const approveBatchHelper = (source_module: string, batch_label: string) => async () => {
    await approveBatch({
      entity_code: entityCode,
      period_end: periodEnd,
      source_module,
      batch_label,
      actor_email: actorEmail,
    });
    toast.success(`${source_module} batch approved`);
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
      <JournalCard
        title="COGS"
        description="POS COGS + dating reversal + new dating"
        status={cogsStatus.data?.status ?? 'not_built'}
        onBuild={buildCogs}
        onApprove={approveBatchHelper('cogs', 'monthly')}
      />
      <JournalCard
        title="Payroll P1"
        description="First pay period of the month"
        onBuild={async () => {
          toast.info('Build payroll from the Payroll page (per-run flow).');
        }}
        onApprove={approveBatchHelper('payroll', 'p1')}
      />
      <JournalCard
        title="Payroll P2"
        description="Second pay period"
        onBuild={async () => {
          toast.info('Build payroll from the Payroll page (per-run flow).');
        }}
        onApprove={approveBatchHelper('payroll', 'p2')}
      />
      <JournalCard
        title="HH AP remittance"
        description="Weekly remittance clearing journal"
        onBuild={async () => {
          await buildRemittanceClearing({
            entity_code: entityCode,
            period_end: periodEnd,
            actor_email: actorEmail,
          });
          toast.success('Remittance clearing built');
        }}
        onApprove={approveBatchHelper('hh_ap_remittance_clearing', 'monthly')}
      />
      <JournalCard
        title="Accruals"
        description="Monthly recurring accruals"
        onBuild={async () => {
          // Builds nothing by itself — needs accrual codes list. Defer to
          // the dedicated Accruals page; here we just present the gate.
          toast.info('Build accruals from the Accruals workflow first.');
        }}
        onApprove={approveBatchHelper('accruals', 'monthly')}
      />
      <JournalCard
        title="Depreciation"
        description="Fixed asset depreciation for the period"
        onBuild={async () => {
          await buildDepreciationJournal({
            entity_code: entityCode,
            period_end: periodEnd,
            actor_email: actorEmail,
          });
          toast.success('Depreciation built');
        }}
        onApprove={approveBatchHelper('depreciation', 'monthly')}
      />
    </div>
  );
}
