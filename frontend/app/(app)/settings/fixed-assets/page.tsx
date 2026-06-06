'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  listAssetClasses,
  upsertAssetClass,
  seedAssetClasses,
  linkAssetClasses,
  type AssetClass,
} from '@/lib/api/depreciation';

function ClassRow({
  cls,
  entityCode,
  isAdmin,
  onSaved,
}: {
  cls: AssetClass;
  entityCode: string;
  isAdmin: boolean;
  onSaved: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [rate, setRate] = useState(cls.cca_rate);
  const [expAcct, setExpAcct] = useState(cls.expense_account);
  const [accumAcct, setAccumAcct] = useState(cls.accum_account);
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      await upsertAssetClass({
        entity_code: entityCode,
        class_code: cls.class_code,
        class_name: cls.class_name,
        cca_rate: parseFloat(rate),
        expense_account: expAcct,
        accum_account: accumAcct,
        is_active: cls.is_active,
        display_order: cls.display_order,
      });
      toast.success(`${cls.class_name} updated`);
      setEditing(false);
      onSaved();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Save failed: ${msg}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="border border-cloud rounded-lg p-4">
      <div className="flex items-center justify-between mb-2">
        <div>
          <span className="font-medium">{cls.class_name}</span>
          <span className="text-xs text-slate ml-2">({cls.class_code})</span>
        </div>
        {isAdmin && !editing && (
          <Button variant="outline" size="sm" onClick={() => setEditing(true)}>Edit</Button>
        )}
      </div>

      {editing ? (
        <div className="grid grid-cols-3 gap-3 mt-2">
          <div>
            <Label className="text-xs">CCA Rate</Label>
            <Input
              className="mt-0.5 h-8 text-sm"
              value={rate}
              onChange={(e) => setRate(e.target.value)}
              placeholder="0.15"
            />
          </div>
          <div>
            <Label className="text-xs">Expense account</Label>
            <Input
              className="mt-0.5 h-8 text-sm"
              value={expAcct}
              onChange={(e) => setExpAcct(e.target.value)}
            />
          </div>
          <div>
            <Label className="text-xs">Accum. depr. account</Label>
            <Input
              className="mt-0.5 h-8 text-sm"
              value={accumAcct}
              onChange={(e) => setAccumAcct(e.target.value)}
            />
          </div>
          <div className="col-span-3 flex gap-2 justify-end">
            <Button variant="outline" size="sm" onClick={() => setEditing(false)}>Cancel</Button>
            <Button size="sm" onClick={handleSave} disabled={saving}>Save</Button>
          </div>
        </div>
      ) : (
        <div className="flex gap-6 text-sm text-slate">
          <span>Rate: <strong className="text-ink">{(parseFloat(cls.cca_rate) * 100).toFixed(0)}%</strong></span>
          <span>Expense: <strong className="text-ink">{cls.expense_account}</strong></span>
          <span>Accum. Depr.: <strong className="text-ink">{cls.accum_account}</strong></span>
        </div>
      )}
    </div>
  );
}

export default function FixedAssetClassesPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const classesQ = useQuery({
    queryKey: ['asset-classes', entityCode],
    enabled: !!entityCode,
    queryFn: () => listAssetClasses(entityCode!),
  });

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['asset-classes', entityCode] });
  };

  const handleSeed = async () => {
    if (!entityCode) return;
    try {
      const result = await seedAssetClasses(entityCode);
      const r = result as { inserted: number; skipped: number };
      toast.success(`Seeded: ${r.inserted} inserted, ${r.skipped} already existed`);
      invalidate();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Seed failed: ${msg}`);
    }
  };

  const handleLink = async () => {
    if (!entityCode) return;
    try {
      const result = await linkAssetClasses(entityCode);
      const r = result as { linked: number; skipped: number; class_not_found_for: string[] };
      if (r.class_not_found_for?.length > 0) {
        toast.warning(
          `Linked ${r.linked} assets. No class found for: ${r.class_not_found_for.join(', ')}`
        );
      } else {
        toast.success(`Linked ${r.linked} assets to classes (${r.skipped} already linked)`);
      }
      invalidate();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Link failed: ${msg}`);
    }
  };

  if (!entityCode) {
    return <p className="p-6 text-slate">Select an entity.</p>;
  }

  const classes = classesQ.data?.classes ?? [];

  return (
    <div className="space-y-6 max-w-3xl">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>Asset Classes</CardTitle>
            <p className="text-sm text-slate mt-1">
              CCA class configuration — rate, depreciation expense account, and accumulated
              depreciation account per class. Depreciation expense always rolls up to account
              6900 for Bridlewood; the split is on the balance sheet (accumulated depreciation only).
            </p>
          </div>
          {isAdmin && (
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={handleSeed}>
                Seed Bridlewood classes
              </Button>
              <Button variant="outline" size="sm" onClick={handleLink}>
                Link existing assets
              </Button>
            </div>
          )}
        </CardHeader>
        <CardContent className="space-y-3">
          {classesQ.isLoading && (
            <div className="space-y-3">
              {[1, 2, 3].map((n) => <Skeleton key={n} className="h-16 w-full" />)}
            </div>
          )}
          {!classesQ.isLoading && classes.length === 0 && (
            <div className="text-center py-8 text-slate">
              <p>No asset classes configured.</p>
              {isAdmin && (
                <p className="text-sm mt-1">Click <strong>Seed Bridlewood classes</strong> to create the 3 standard CCA classes.</p>
              )}
            </div>
          )}
          {classes.map((cls) => (
            <ClassRow
              key={cls.id}
              cls={cls}
              entityCode={entityCode}
              isAdmin={isAdmin}
              onSaved={invalidate}
            />
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">Setup sequence</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-slate space-y-1">
          <p>1. Click <strong>Seed Bridlewood classes</strong> to create the 3 standard classes.</p>
          <p>2. Click <strong>Link existing assets</strong> to attach the seeded fixed assets to their classes.</p>
          <p>3. Go to <strong>Fixed Assets</strong> in the main navigation to view the schedule and add new assets.</p>
          <p>4. Enable the <strong>Monthly Depreciation</strong> template in Recurring Entries once you&apos;ve verified the amounts with a dry-run.</p>
        </CardContent>
      </Card>
    </div>
  );
}
