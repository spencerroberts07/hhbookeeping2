'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getWagePlannerSnapshots, getSnapshotDownloadUrl } from '@/lib/api/wage_planner';

const CURRENT_FY = new Date().getMonth() >= 9 ? new Date().getFullYear() + 1 : new Date().getFullYear();

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, string> = {
    ready: 'bg-emerald-100 text-emerald-800',
    generating: 'bg-yellow-100 text-yellow-800',
    failed: 'bg-red-100 text-red-800',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${variants[status] ?? 'bg-gray-100 text-gray-700'}`}>
      {status}
    </span>
  );
}

export default function WagePlannerSnapshotsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [fiscalYear, setFiscalYear] = useState(CURRENT_FY);
  const [downloading, setDownloading] = useState<string | null>(null);

  const { data, isLoading } = useQuery({
    queryKey: ['wage-planner-snapshots', entityCode, fiscalYear],
    enabled: !!entityCode,
    queryFn: () => getWagePlannerSnapshots(entityCode!, fiscalYear),
  });

  async function handleDownload(snapshotId: string) {
    if (!entityCode) return;
    setDownloading(snapshotId);
    try {
      const result = await getSnapshotDownloadUrl(snapshotId, entityCode);
      if (result.download_url) {
        window.open(result.download_url, '_blank');
      } else if (result.fallback) {
        window.open(result.fallback, '_blank');
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Download failed');
    } finally {
      setDownloading(null);
    }
  }

  if (!entityCode) return <p className="text-sm text-muted-foreground p-4">Select an entity first.</p>;

  const snapshots = data?.snapshots ?? [];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Wage Planner — Snapshot Archive</h1>
          <p className="text-sm text-muted-foreground">
            Immutable Excel snapshots archived each time a pay period is approved.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">FY</span>
          <Input
            type="number"
            value={fiscalYear}
            onChange={(e) => setFiscalYear(parseInt(e.target.value))}
            className="w-20 h-8 text-sm"
            min={2020}
            max={2099}
          />
        </div>
      </div>

      <Card>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="p-4 space-y-2">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
            </div>
          ) : snapshots.length === 0 ? (
            <div className="py-10 text-center text-muted-foreground text-sm">
              No snapshots found for FY{fiscalYear}. Snapshots are created automatically when payroll runs are approved.
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/30 text-xs font-medium">
                  <th className="px-4 py-2 text-left">Period #</th>
                  <th className="px-4 py-2 text-left">Status</th>
                  <th className="px-4 py-2 text-left">Generated</th>
                  <th className="px-4 py-2 text-left">By</th>
                  <th className="px-4 py-2 text-left">Error</th>
                  <th className="px-4 py-2 text-right">Download</th>
                </tr>
              </thead>
              <tbody>
                {snapshots.map((snap) => (
                  <tr key={snap.id} className="border-b border-border/40 hover:bg-muted/20">
                    <td className="px-4 py-2 font-medium">P{snap.pay_period_number.toString().padStart(2, '0')}</td>
                    <td className="px-4 py-2"><StatusBadge status={snap.status} /></td>
                    <td className="px-4 py-2 text-xs text-muted-foreground">
                      {snap.generated_at ? new Date(snap.generated_at).toLocaleString('en-CA') : '—'}
                    </td>
                    <td className="px-4 py-2 text-xs text-muted-foreground">{snap.generated_by ?? '—'}</td>
                    <td className="px-4 py-2 text-xs text-red-600">{snap.error_msg ?? ''}</td>
                    <td className="px-4 py-2 text-right">
                      {snap.has_file && snap.status === 'ready' ? (
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-7 text-xs"
                          onClick={() => handleDownload(snap.id)}
                          disabled={downloading === snap.id}
                        >
                          {downloading === snap.id ? '…' : 'Download'}
                        </Button>
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>

      <div className="text-xs text-muted-foreground">
        Up to 26 snapshots per fiscal year (one per pay period). Snapshots are immutable once created.
        Re-approving a period overwrites only the current frontier period; past periods remain frozen.
      </div>
    </div>
  );
}
