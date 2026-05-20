'use client';

import { useQuery } from '@tanstack/react-query';
import Link from 'next/link';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { listPosRuns, validatePosFinancial } from '@/lib/api/pos';
import { listGlRuns, getTrialBalance } from '@/lib/api/gl';
import {
  getUnmatchedQueue,
  listInvoiceDocuments,
} from '@/lib/api/invoices';
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

  // Invoice match status — unmatched count is the main signal here.
  const unmatched = useQuery({
    queryKey: ['unmatched-queue', entityCode],
    queryFn: () => getUnmatchedQueue({ entity_code: entityCode }),
  });
  const matched = useQuery({
    queryKey: ['invoice-documents', entityCode, 'matched-or-posted'],
    queryFn: () =>
      listInvoiceDocuments({
        entity_code: entityCode,
        // Backend doesn't have a multi-status filter; we count manually
        // from the unfiltered list.
        limit: 500,
      }),
    select: (data) =>
      data.invoices.filter((i) =>
        ['matched', 'posted_to_ap'].includes(i.status),
      ).length,
  });

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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

      {/* Invoice matching — does not block period close per the spec, but
          warns when invoices are uploaded without a link. */}
      <div className="rounded-xl border border-border bg-white p-4">
        <div className="text-sm font-semibold text-deep-navy mb-2">
          Invoice matching
        </div>
        {unmatched.isLoading || matched.isLoading ? (
          <Skeleton className="h-10" />
        ) : (
          <div className="text-sm">
            <div className="flex items-center gap-2">
              <Badge
                variant={
                  (unmatched.data?.total ?? 0) === 0 ? 'complete' : 'warning'
                }
              >
                {matched.data ?? 0} matched · {unmatched.data?.total ?? 0} unmatched
              </Badge>
            </div>
            {(unmatched.data?.total ?? 0) > 0 && (
              <p className="text-xs text-slate mt-2">
                Unmatched invoices won&apos;t block close, but should be resolved.
              </p>
            )}
            <Link
              href="/ap/unmatched"
              className="text-xs text-ledger-blue hover:underline mt-2 inline-block"
            >
              Open unmatched queue →
            </Link>
          </div>
        )}
      </div>
    </div>
  );
}
