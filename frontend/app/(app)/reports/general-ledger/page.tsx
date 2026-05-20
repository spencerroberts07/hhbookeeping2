'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ReportShell } from '@/components/reports/report-shell';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { listGlRuns, getGlTransactions } from '@/lib/api/gl';
import { formatMoney, formatDate } from '@/lib/utils';
import { MultiFileUpload } from '@/components/shared/multi-file-upload';
import { useUploadDefaults } from '@/lib/hooks/use-upload-defaults';
import { ChevronDown, ChevronRight } from 'lucide-react';

export default function GeneralLedgerPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [accountCode, setAccountCode] = useState('');
  const [showUpload, setShowUpload] = useState(false);
  const [glPeriodStart, setGlPeriodStart] = useState('');
  const [glPeriodEnd, setGlPeriodEnd] = useState('');
  const uploadDefaults = useUploadDefaults();
  const qc = useQueryClient();

  // Use the latest GL import run as the data source (no live ledger yet).
  // TODO: backend endpoint not built — app-native /api/reports/general-ledger.
  const runs = useQuery({
    queryKey: ['gl-runs', entityCode],
    enabled: !!entityCode,
    queryFn: () => listGlRuns(entityCode!),
  });
  const latest = runs.data?.runs[0];

  const txns = useQuery({
    queryKey: ['gl-txns', entityCode, latest?.id, accountCode],
    enabled: !!latest && !!entityCode && !!accountCode,
    queryFn: () =>
      getGlTransactions(entityCode!, latest!.id, accountCode || undefined),
  });

  return (
    <ReportShell
      title="General Ledger"
      subtitle={
        latest
          ? `Data from import ${formatDate(latest.imported_at)} (${latest.file_name})`
          : 'No GL import on file'
      }
    >
      <div className="mb-4 no-print">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setShowUpload((s) => !s)}
        >
          {showUpload ? (
            <ChevronDown className="h-4 w-4" strokeWidth={1.5} />
          ) : (
            <ChevronRight className="h-4 w-4" strokeWidth={1.5} />
          )}
          {showUpload ? 'Hide GL upload' : 'Upload a new GL export'}
        </Button>
        {showUpload && (
          <div className="mt-3 space-y-3">
            <div className="grid grid-cols-2 gap-3 max-w-md">
              <div>
                <Label htmlFor="gl-ps">Period start (optional)</Label>
                <Input
                  id="gl-ps"
                  type="date"
                  value={glPeriodStart}
                  onChange={(e) => setGlPeriodStart(e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="gl-pe">Period end (optional)</Label>
                <Input
                  id="gl-pe"
                  type="date"
                  value={glPeriodEnd}
                  onChange={(e) => setGlPeriodEnd(e.target.value)}
                />
              </div>
            </div>
            <MultiFileUpload
              endpoint="/api/gl-import/upload"
              fileKey="file"
              accept=".xlsx"
              extraFields={{
                ...uploadDefaults,
                period_start: glPeriodStart || undefined,
                period_end: glPeriodEnd || undefined,
              }}
              label="GL export (xlsx)"
              description="Drop the QuickBooks GL export. Builds a trial-balance comparison automatically once imported."
              onComplete={() =>
                qc.invalidateQueries({ queryKey: ['gl-runs'] })
              }
            />
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-3 mb-4 max-w-md no-print">
        <div>
          <Label htmlFor="acct">Account code</Label>
          <Input
            id="acct"
            value={accountCode}
            onChange={(e) => setAccountCode(e.target.value)}
            placeholder="e.g. 1020"
          />
        </div>
      </div>
      {runs.isLoading ? (
        <Skeleton className="h-96" />
      ) : !latest ? (
        <p className="text-slate">
          No GL export available. Upload one from Month-end → Documents.
        </p>
      ) : !accountCode ? (
        <p className="text-slate">Enter an account code to view its transactions.</p>
      ) : txns.isLoading ? (
        <Skeleton className="h-96" />
      ) : !txns.data?.transactions.length ? (
        <p className="text-slate">No transactions for this account.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Date</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Description</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Ref</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Debit</th>
                <th className="text-right font-semibold text-deep-navy px-4 py-2">Credit</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {txns.data.transactions.map((t, idx) => (
                <tr key={idx} className="hover:bg-cloud">
                  <td className="px-4 py-2 text-ink">{formatDate(t.date)}</td>
                  <td className="px-4 py-2 text-ink">{t.description}</td>
                  <td className="px-4 py-2 text-slate font-mono text-xs">{t.ref ?? '—'}</td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {t.debit ? formatMoney(t.debit) : '—'}
                  </td>
                  <td className="px-4 py-2 tabular-nums text-right text-ink">
                    {t.credit ? formatMoney(t.credit) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </ReportShell>
  );
}
