'use client';

import { useQuery } from '@tanstack/react-query';
import Link from 'next/link';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { listPosRuns, validatePosFinancial } from '@/lib/api/pos';
import { listGlRuns, getTrialBalance } from '@/lib/api/gl';
import { formatMoney, formatPercent } from '@/lib/utils';

interface Props {
  entityCode: string;
  periodEnd: string;
}

export function StepValidation({ entityCode, periodEnd }: Props) {
  const posRuns = useQuery({
    queryKey: ['pos-runs', entityCode],
    queryFn: () => listPosRuns({ entity_code: entityCode }),
  });

  const posFinancialRun = posRuns.data?.runs?.find(
    (r) => r.report_type === 'pos_financial' && r.period_end <= periodEnd,
  );

  const posValidation = useQuery({
    queryKey: ['pos-validation', entityCode, posFinancialRun?.id],
    enabled: !!posFinancialRun,
    queryFn: () =>
      validatePosFinancial({
        entity_code: entityCode,
        import_run_id: posFinancialRun!.id,
      }) as Promise<{
        match_percent: number;
        total_variance: number;
      }>,
  });

  const glRuns = useQuery({
    queryKey: ['gl-runs', entityCode],
    queryFn: () => listGlRuns(entityCode),
  });

  const latestGlRun = glRuns.data?.runs[0];
  const tb = useQuery({
    queryKey: ['tb', entityCode, latestGlRun?.id],
    enabled: !!latestGlRun,
    queryFn: () =>
      getTrialBalance(entityCode, latestGlRun!.id, /* onlyVariance */ true),
  });

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="rounded-xl border border-border bg-white p-4">
        <div className="text-sm font-semibold text-deep-navy mb-2">
          POS validation
        </div>
        {posRuns.isLoading || posValidation.isLoading ? (
          <Skeleton className="h-10" />
        ) : !posFinancialRun ? (
          <p className="text-sm text-slate">
            No POS Financial report uploaded for this period.
          </p>
        ) : posValidation.data ? (
          <div className="flex items-center gap-2">
            <Badge
              variant={
                posValidation.data.match_percent > 99
                  ? 'complete'
                  : posValidation.data.match_percent > 95
                    ? 'warning'
                    : 'error'
              }
            >
              {formatPercent(posValidation.data.match_percent, 1)} match
            </Badge>
            <span className="text-sm text-slate">
              Variance: {formatMoney(posValidation.data.total_variance, { signed: true })}
            </span>
          </div>
        ) : null}
      </div>
      <div className="rounded-xl border border-border bg-white p-4">
        <div className="text-sm font-semibold text-deep-navy mb-2">
          Trial balance variance
        </div>
        {glRuns.isLoading || tb.isLoading ? (
          <Skeleton className="h-10" />
        ) : !latestGlRun ? (
          <p className="text-sm text-slate">
            Upload a GL export to enable trial-balance comparison.
          </p>
        ) : tb.data ? (
          <div className="text-sm">
            <div>
              <span className="text-slate">Accounts with variance:</span>{' '}
              <span className="font-semibold text-deep-navy">
                {tb.data.rows.length}
              </span>
            </div>
            <Link
              href={`/reports/trial-balance?run_id=${latestGlRun.id}`}
              className="text-xs text-ledger-blue hover:underline mt-2 inline-block"
            >
              View full trial balance →
            </Link>
          </div>
        ) : null}
      </div>
    </div>
  );
}
