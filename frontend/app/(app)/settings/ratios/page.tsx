'use client';

import { useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Topbar } from '@/components/layout/topbar';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  getRatios, getRatioRoles, getRatioTokens, listCustomRatios,
  upsertRatioConfig, setRatioInput, clearRatioInput, addRatioRole, removeRatioRole,
  upsertCustomRatio, deleteCustomRatio, previewCustomRatio,
  type RatioRow, type RatioFormat, type CustomRatio,
} from '@/lib/api/ratios';

const ROLES = [
  'cash', 'accounts_receivable', 'inventory', 'accounts_payable',
  'interest_bearing_debt', 'interest_expense', 'depreciation_amortization',
  'income_tax_expense', 'current_portion_ltd', 'equity_reclass',
  'rent_expense', 'percentage_rent_accrual',
];

function fmt(v: number | null | undefined, f: RatioFormat): string {
  if (v === null || v === undefined) return '—';
  if (f === 'percent') return `${v.toFixed(1)}%`;
  if (f === 'dollar') return v.toLocaleString('en-CA', { style: 'currency', currency: 'CAD', maximumFractionDigits: 0 });
  if (f === 'days') return `${v.toFixed(0)}d`;
  return v.toFixed(2);
}

export default function RatiosSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['ratios', entityCode] });
    qc.invalidateQueries({ queryKey: ['ratio-roles', entityCode] });
    qc.invalidateQueries({ queryKey: ['ratio-custom', entityCode] });
  };

  const ratios = useQuery({ queryKey: ['ratios', entityCode], enabled: !!entityCode, queryFn: () => getRatios(entityCode!) });
  const roles = useQuery({ queryKey: ['ratio-roles', entityCode], enabled: !!entityCode, queryFn: () => getRatioRoles(entityCode!) });
  const tokens = useQuery({ queryKey: ['ratio-tokens', entityCode], enabled: !!entityCode, queryFn: () => getRatioTokens(entityCode!) });
  const customs = useQuery({ queryKey: ['ratio-custom', entityCode], enabled: !!entityCode, queryFn: () => listCustomRatios(entityCode!) });

  if (!entityCode) return <div className="p-6 text-slate">Select an entity.</div>;

  const ctx = ratios.data?.context;
  const builtins = (ratios.data?.ratios ?? []).filter((r) => !r.custom);

  return (
    <div>
      <Topbar title="Ratio settings" />
      <div className="p-6 space-y-6 max-w-5xl">
        {!isAdmin && (
          <div className="rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
            You can view ratio settings, but only admins can change them.
          </div>
        )}

        {/* Derived inputs: debt service + fixed charges */}
        <Card>
          <CardHeader><CardTitle>Debt service &amp; fixed charges (GL-derived, overridable)</CardTitle></CardHeader>
          <CardContent className="space-y-4">
            {ratios.isLoading || !ctx ? <Skeleton className="h-24" /> : (
              <div className="grid md:grid-cols-2 gap-4">
                <OverrideInput
                  title="Annual debt service" inputKey="annual_debt_service"
                  derived={ctx.debt_service_breakdown.annual_debt_service as number}
                  source={ctx.annual_debt_service_source} current={ctx.annual_debt_service}
                  breakdown={ctx.debt_service_breakdown} entityCode={entityCode} canEdit={isAdmin} onDone={invalidate}
                />
                <OverrideInput
                  title="Annual fixed charges (rent)" inputKey="fixed_charges"
                  derived={ctx.fixed_charges_breakdown.annual_rent as number}
                  source={ctx.fixed_charges_source} current={ctx.fixed_charges}
                  breakdown={ctx.fixed_charges_breakdown} entityCode={entityCode} canEdit={isAdmin} onDone={invalidate}
                />
              </div>
            )}
          </CardContent>
        </Card>

        {/* Built-in ratios: enable + thresholds */}
        <Card>
          <CardHeader><CardTitle>Built-in ratios — enable &amp; thresholds</CardTitle></CardHeader>
          <CardContent>
            {ratios.isLoading ? <Skeleton className="h-64" /> : (
              <div className="space-y-1">
                {builtins.map((r) => (
                  <BuiltinRow key={r.key} row={r} entityCode={entityCode} canEdit={isAdmin} onDone={invalidate} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Account-role map */}
        <Card>
          <CardHeader><CardTitle>Account-role map</CardTitle></CardHeader>
          <CardContent>
            {roles.isLoading ? <Skeleton className="h-40" /> : (
              <RoleEditor rows={roles.data?.roles ?? []} entityCode={entityCode} canEdit={isAdmin} onDone={invalidate} />
            )}
          </CardContent>
        </Card>

        {/* Custom ratios */}
        <Card>
          <CardHeader><CardTitle>Custom ratios</CardTitle></CardHeader>
          <CardContent>
            <CustomRatios
              customs={customs.data?.custom ?? []}
              computed={(ratios.data?.ratios ?? []).filter((r) => r.custom)}
              tokens={tokens.data}
              entityCode={entityCode} canEdit={isAdmin} onDone={invalidate}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function OverrideInput({ title, inputKey, derived, source, current, breakdown, entityCode, canEdit, onDone }: {
  title: string; inputKey: string; derived: number; source: string; current: number;
  breakdown: Record<string, number | string>; entityCode: string; canEdit: boolean; onDone: () => void;
}) {
  const [val, setVal] = useState('');
  return (
    <div className="rounded-lg border border-border p-3">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-deep-navy">{title}</span>
        <Badge variant={source === 'override' ? 'warning' : 'complete'}>{source === 'override' ? 'override' : 'GL-derived'}</Badge>
      </div>
      <div className="mt-1 text-2xl font-bold tabular-nums text-deep-navy">
        {current.toLocaleString('en-CA', { style: 'currency', currency: 'CAD', maximumFractionDigits: 0 })}
      </div>
      <div className="mt-1 text-[11px] text-slate space-y-0.5">
        {Object.entries(breakdown).filter(([k]) => !k.startsWith('ttm_')).map(([k, v]) => (
          <div key={k} className="flex justify-between"><span>{k}</span><span className="tabular-nums">{typeof v === 'number' ? v.toLocaleString('en-CA', { maximumFractionDigits: 0 }) : v}</span></div>
        ))}
      </div>
      {canEdit && (
        <div className="mt-2 flex gap-2">
          <Input type="number" placeholder={`override (derived ${Math.round(derived).toLocaleString()})`} value={val} onChange={(e) => setVal(e.target.value)} />
          <Button size="sm" disabled={!val} onClick={async () => { await setRatioInput(entityCode, inputKey, Number(val)); setVal(''); toast.success('Override saved'); onDone(); }}>Set</Button>
          {source === 'override' && (
            <Button size="sm" variant="outline" onClick={async () => { await clearRatioInput(entityCode, inputKey); toast.success('Reverted to GL-derived'); onDone(); }}>Clear</Button>
          )}
        </div>
      )}
    </div>
  );
}

function BuiltinRow({ row, entityCode, canEdit, onDone }: { row: RatioRow; entityCode: string; canEdit: boolean; onDone: () => void }) {
  const [enabled, setEnabled] = useState(row.enabled);
  const [tmin, setTmin] = useState(row.threshold_min?.toString() ?? '');
  const [tmax, setTmax] = useState(row.threshold_max?.toString() ?? '');
  const [dir, setDir] = useState(row.threshold_direction ?? '');
  const save = async () => {
    await upsertRatioConfig(entityCode, {
      ratio_key: row.key, enabled,
      threshold_min: tmin === '' ? null : Number(tmin),
      threshold_max: tmax === '' ? null : Number(tmax),
      threshold_direction: dir || null,
    });
    toast.success(`${row.label} saved`); onDone();
  };
  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-border py-1.5 text-sm">
      <input type="checkbox" checked={enabled} disabled={!canEdit} onChange={(e) => setEnabled(e.target.checked)} className="h-4 w-4" />
      <span className="w-56 truncate text-ink">{row.label}</span>
      <span className="w-16 text-right tabular-nums text-slate text-xs">{row.category}</span>
      <span className={'w-20 text-right tabular-nums font-medium ' + (row.breached ? 'text-red-700' : 'text-deep-navy')}>{fmt(row.value, row.format)}</span>
      {row.breached && <Badge variant="error" className="text-[10px]">breach</Badge>}
      {canEdit && (
        <span className="ml-auto flex items-center gap-1">
          <select value={dir} onChange={(e) => setDir(e.target.value)} className="rounded border border-input px-1 py-1 text-xs">
            <option value="">no alert</option>
            <option value="min">alert if &lt; min</option>
            <option value="max">alert if &gt; max</option>
          </select>
          <Input className="w-20 h-8" placeholder="min" value={tmin} onChange={(e) => setTmin(e.target.value)} />
          <Input className="w-20 h-8" placeholder="max" value={tmax} onChange={(e) => setTmax(e.target.value)} />
          <Button size="sm" variant="outline" onClick={save}>Save</Button>
        </span>
      )}
    </div>
  );
}

function RoleEditor({ rows, entityCode, canEdit, onDone }: { rows: { role: string; account_code: string; account_name: string }[]; entityCode: string; canEdit: boolean; onDone: () => void }) {
  const [role, setRole] = useState<string>(ROLES[0]!);
  const [code, setCode] = useState('');
  const byRole = useMemo(() => {
    const m: Record<string, { account_code: string; account_name: string }[]> = {};
    for (const r of rows) (m[r.role] ??= []).push(r);
    return m;
  }, [rows]);
  return (
    <div className="space-y-3">
      {ROLES.filter((r) => byRole[r]?.length).map((r) => (
        <div key={r} className="text-sm">
          <span className="font-mono text-xs text-slate">{r}</span>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {(byRole[r] ?? []).map((a) => (
              <span key={a.account_code} className="inline-flex items-center gap-1 rounded-full bg-cloud px-2 py-0.5 text-xs">
                <span className="font-mono">{a.account_code}</span> {a.account_name}
                {canEdit && (
                  <button className="text-red-600 hover:text-red-800" onClick={async () => { await removeRatioRole(entityCode, r, a.account_code); toast.success('Removed'); onDone(); }}>×</button>
                )}
              </span>
            ))}
          </div>
        </div>
      ))}
      {canEdit && (
        <div className="flex items-end gap-2 border-t border-border pt-3">
          <div><Label className="text-xs">Role</Label>
            <select value={role} onChange={(e) => setRole(e.target.value)} className="block rounded border border-input px-2 py-1.5 text-sm">
              {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>
          <div><Label className="text-xs">Account code</Label><Input className="w-32" value={code} onChange={(e) => setCode(e.target.value)} placeholder="e.g. 1120" /></div>
          <Button size="sm" disabled={!code} onClick={async () => { await addRatioRole(entityCode, role, code); setCode(''); toast.success('Added'); onDone(); }}>Add</Button>
        </div>
      )}
    </div>
  );
}

interface TokensData {
  tokens: { totals: string[]; accounts: string[] };
  builtin_formulas: Record<string, { numerator_expr: string; denominator_expr: string; output_type: string }>;
}

function CustomRatios({ customs, computed, tokens, entityCode, canEdit, onDone }: {
  customs: CustomRatio[]; computed: RatioRow[]; tokens: TokensData | undefined;
  entityCode: string; canEdit: boolean; onDone: () => void;
}) {
  const blank = { key: '', label: '', numerator_expr: '', denominator_expr: '', output_type: 'ratio', enabled: true, threshold_min: null, threshold_max: null, threshold_direction: null } as CustomRatio;
  const [form, setForm] = useState<CustomRatio>(blank);
  const [preview, setPreview] = useState<string>('');
  const valueByKey = useMemo(() => Object.fromEntries(computed.map((c) => [c.key.replace('custom:', ''), c])), [computed]);

  const doPreview = async () => {
    const res = await previewCustomRatio(entityCode, { ...form });
    setPreview(res.ok ? `= ${res.value === null ? '—' : Number(res.value).toFixed(4)}` : `error: ${res.error}`);
  };
  const save = async () => {
    if (!form.key || !form.label || !form.numerator_expr) { toast.error('key, label and numerator are required'); return; }
    try { await upsertCustomRatio(entityCode, form); toast.success('Saved'); setForm(blank); setPreview(''); onDone(); }
    catch (e) { toast.error((e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? 'Save failed'); }
  };

  return (
    <div className="space-y-4">
      {customs.length > 0 && (
        <div className="space-y-1">
          {customs.map((c) => (
            <div key={c.key} className="flex items-center gap-2 border-b border-border py-1.5 text-sm">
              <span className="w-48 truncate text-ink">{c.label}</span>
              <span className="flex-1 truncate font-mono text-[11px] text-slate">{c.numerator_expr}{c.denominator_expr ? ` / ${c.denominator_expr}` : ''} · {c.output_type}</span>
              <span className="w-20 text-right tabular-nums font-medium text-deep-navy">{(() => { const cv = valueByKey[c.key]; return cv ? fmt(cv.value, cv.format) : '—'; })()}</span>
              {canEdit && <>
                <Button size="sm" variant="outline" onClick={() => setForm(c)}>Edit</Button>
                <button className="text-red-600 hover:text-red-800" onClick={async () => { await deleteCustomRatio(entityCode, c.key); toast.success('Deleted'); onDone(); }}>×</button>
              </>}
            </div>
          ))}
        </div>
      )}

      {canEdit && (
        <div className="rounded-lg border border-border p-3 space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-deep-navy">{form.key && customs.some((c) => c.key === form.key) ? 'Edit' : 'New'} custom ratio</span>
            {tokens && (
              <select className="ml-auto rounded border border-input px-2 py-1 text-xs"
                onChange={(e) => {
                  const f = tokens.builtin_formulas[e.target.value];
                  if (f) setForm({ ...form, numerator_expr: f.numerator_expr, denominator_expr: f.denominator_expr, output_type: f.output_type, label: form.label || e.target.value, key: form.key || `copy_${e.target.value}` });
                }} defaultValue="">
                <option value="">Clone a built-in…</option>
                {Object.keys(tokens.builtin_formulas).map((k) => <option key={k} value={k}>{k}</option>)}
              </select>
            )}
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div><Label className="text-xs">Key</Label><Input value={form.key} onChange={(e) => setForm({ ...form, key: e.target.value })} placeholder="gp_per_inventory" /></div>
            <div><Label className="text-xs">Label</Label><Input value={form.label} onChange={(e) => setForm({ ...form, label: e.target.value })} placeholder="GP per inventory $" /></div>
          </div>
          <div><Label className="text-xs">Numerator</Label><Input className="font-mono" value={form.numerator_expr} onChange={(e) => setForm({ ...form, numerator_expr: e.target.value })} placeholder="ttm_gross_profit" /></div>
          <div><Label className="text-xs">Denominator (optional)</Label><Input className="font-mono" value={form.denominator_expr ?? ''} onChange={(e) => setForm({ ...form, denominator_expr: e.target.value || null })} placeholder="inventory" /></div>
          <div className="flex items-end gap-2">
            <div><Label className="text-xs">Output</Label>
              <select value={form.output_type} onChange={(e) => setForm({ ...form, output_type: e.target.value })} className="block rounded border border-input px-2 py-1.5 text-sm">
                <option value="ratio">ratio</option><option value="percent">percent</option><option value="dollar">dollar</option>
              </select>
            </div>
            <Button size="sm" variant="outline" onClick={doPreview}>Preview</Button>
            {preview && <span className="text-sm tabular-nums text-deep-navy">{preview}</span>}
            <Button size="sm" className="ml-auto" onClick={save}>Save</Button>
          </div>
          {tokens && (
            <details className="text-xs text-slate">
              <summary className="cursor-pointer">Available tokens ({tokens.tokens.totals.length + tokens.tokens.accounts.length})</summary>
              <div className="mt-1 font-mono leading-5">
                <div><b>totals:</b> {tokens.tokens.totals.join(', ')}</div>
                <div className="mt-1"><b>accounts:</b> {tokens.tokens.accounts.join(', ')}</div>
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  );
}
