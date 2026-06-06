'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Topbar } from '@/components/layout/topbar';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  listFixedAssets,
  listAssetClasses,
  generateDepreciationSchedule,
  getDepreciationSummary,
  downloadScheduleExcel,
  type FixedAsset,
  type AssetClass,
} from '@/lib/api/depreciation';

const BRIDLEWOOD_FY = 2025; // current fiscal year (Oct 2024 - Sep 2025 = FY2025)

function AssetRow({ asset }: { asset: FixedAsset }) {
  const nbv = parseFloat(asset.opening_nbv);
  const cost = parseFloat(asset.cost);

  return (
    <tr className="border-b border-cloud hover:bg-cloud/40 text-sm">
      <td className="py-2 px-3 font-medium">{asset.asset_code}</td>
      <td className="py-2 px-3">{asset.description}</td>
      <td className="py-2 px-3 text-slate">{asset.cca_class}</td>
      <td className="py-2 px-3 text-right">
        {cost.toLocaleString('en-CA', { style: 'currency', currency: 'CAD' })}
      </td>
      <td className="py-2 px-3 text-right">
        {nbv.toLocaleString('en-CA', { style: 'currency', currency: 'CAD' })}
      </td>
      <td className="py-2 px-3 text-right">
        {(parseFloat(asset.cca_rate) * 100).toFixed(0)}%
      </td>
      <td className="py-2 px-3 text-center">
        {asset.is_active ? (
          <Badge variant="secondary" className="text-xs">Active</Badge>
        ) : (
          <Badge variant="outline" className="text-xs text-slate">Disposed</Badge>
        )}
      </td>
    </tr>
  );
}

function DepreciationSummary({ entityCode }: { entityCode: string }) {
  const [generatingSchedule, setGeneratingSchedule] = useState(false);
  const [downloading, setDownloading] = useState(false);

  const summaryQ = useQuery({
    queryKey: ['depr-summary', entityCode],
    enabled: !!entityCode,
    queryFn: () => getDepreciationSummary(entityCode, `${BRIDLEWOOD_FY}-09-30`),
  });

  const summary = summaryQ.data as {
    grand_total_monthly?: string;
    by_class?: Array<{ cca_class: string; description: string; monthly_depreciation: string }>;
  } | null;

  const handleGenerateSchedule = async () => {
    setGeneratingSchedule(true);
    try {
      await generateDepreciationSchedule({
        entity_code: entityCode,
        fiscal_year: BRIDLEWOOD_FY,
        actor_email: 'spencer7roberts@gmail.com',
      });
      toast.success('Schedule generated');
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Generate failed: ${msg}`);
    } finally {
      setGeneratingSchedule(false);
    }
  };

  const handleDownloadExcel = async () => {
    setDownloading(true);
    try {
      const result = await downloadScheduleExcel(entityCode, BRIDLEWOOD_FY);
      window.open(result.url, '_blank');
      toast.success('Excel schedule opened in new tab');
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Download failed: ${msg}`);
    } finally {
      setDownloading(false);
    }
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>Depreciation Schedule — FY{BRIDLEWOOD_FY}</CardTitle>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={handleGenerateSchedule} disabled={generatingSchedule}>
            {generatingSchedule ? 'Generating...' : 'Regenerate'}
          </Button>
          <Button size="sm" onClick={handleDownloadExcel} disabled={downloading}>
            {downloading ? 'Preparing...' : '⬇ Excel'}
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {summaryQ.isLoading && <Skeleton className="h-24 w-full" />}
        {summary && (
          <div className="space-y-3">
            <div className="flex items-center gap-4 text-sm">
              <span className="text-slate">Total monthly depreciation:</span>
              <span className="font-semibold text-ink text-lg">
                ${parseFloat(summary.grand_total_monthly ?? '0').toLocaleString('en-CA', {
                  minimumFractionDigits: 2,
                })}
              </span>
            </div>
            <div className="space-y-1">
              {summary.by_class?.map((cls) => (
                <div key={cls.cca_class} className="flex justify-between text-sm border-b border-cloud pb-1">
                  <span>{cls.description}</span>
                  <span className="font-medium">
                    ${parseFloat(cls.monthly_depreciation).toLocaleString('en-CA', {
                      minimumFractionDigits: 2,
                    })}/mo
                  </span>
                </div>
              ))}
            </div>
            <p className="text-xs text-slate">
              Note: regenerate the schedule after adding or disposing assets.
              Run a dry-run via Recurring Entries before enabling auto-posting.
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export default function FixedAssetsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();

  const assetsQ = useQuery({
    queryKey: ['fixed-assets', entityCode],
    enabled: !!entityCode,
    queryFn: () => listFixedAssets(entityCode!),
  });

  if (!entityCode) {
    return (
      <>
        <Topbar title="Fixed Assets" />
        <div className="p-6 text-slate">Select an entity.</div>
      </>
    );
  }

  const assets = (assetsQ.data as { assets?: FixedAsset[] })?.assets ?? [];
  const activeAssets = assets.filter((a) => a.is_active);
  const disposedAssets = assets.filter((a) => !a.is_active);

  return (
    <>
      <Topbar title="Fixed Assets" />
      <div className="p-6 space-y-6 max-w-5xl">

        <DepreciationSummary entityCode={entityCode} />

        <Card>
          <CardHeader>
            <CardTitle>Asset Schedule</CardTitle>
          </CardHeader>
          <CardContent>
            {assetsQ.isLoading && <Skeleton className="h-40 w-full" />}
            {!assetsQ.isLoading && assets.length === 0 && (
              <div className="text-center py-8 text-slate">
                <p>No fixed assets found.</p>
                <p className="text-sm mt-1">
                  Go to <strong>Settings → Asset Classes</strong> to seed the standard Bridlewood classes first,
                  then add assets here.
                </p>
              </div>
            )}
            {assets.length > 0 && (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-xs text-slate border-b border-cloud">
                      <th className="text-left py-2 px-3">Code</th>
                      <th className="text-left py-2 px-3">Description</th>
                      <th className="text-left py-2 px-3">Class</th>
                      <th className="text-right py-2 px-3">Cost</th>
                      <th className="text-right py-2 px-3">Opening NBV</th>
                      <th className="text-right py-2 px-3">Rate</th>
                      <th className="text-center py-2 px-3">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activeAssets.map((a) => <AssetRow key={a.id} asset={a} />)}
                    {disposedAssets.length > 0 && (
                      <>
                        <tr><td colSpan={7} className="py-2 px-3 text-xs text-slate font-medium bg-cloud">Disposed</td></tr>
                        {disposedAssets.map((a) => <AssetRow key={a.id} asset={a} />)}
                      </>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Disposal notes</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-slate space-y-1">
            <p>
              To dispose an asset, use <code className="bg-cloud px-1 rounded text-xs">
              POST /api/depreciation/dispose</code> with <code className="bg-cloud px-1 rounded text-xs">dry_run=true</code> first
              to preview the gain/loss journal.
            </p>
            <p className="text-amber-700">
              ⚠ Disposal gain/loss accounts (4020 gain / 6950 loss) do not exist in the
              Bridlewood chart of accounts as of June 2026. Confirm the correct accounts with
              the bookkeeper before posting a disposal.
            </p>
          </CardContent>
        </Card>
      </div>
    </>
  );
}
