'use client';

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  listRecurringTemplates,
  toggleRecurringTemplate,
  seedRecurringTemplates,
  postRecurringTemplate,
  type RecurringTemplate,
} from '@/lib/api/recurring_entries';

const CALC_TYPE_LABELS: Record<string, string> = {
  fixed: 'Fixed',
  formula: 'Formula',
  schedule: 'Schedule',
};

const CALC_TYPE_DESCRIPTIONS: Record<string, string> = {
  fixed: 'Constant amount every period — auto-posts directly when enabled.',
  formula: 'Amount computed from GL account balances each period — requires approval.',
  schedule: 'Amount supplied by a feeder module at post time — requires approval.',
};

function TemplateBadge({ template }: { template: RecurringTemplate }) {
  const isStandard = !!template.standard_key;
  return (
    <Badge variant={isStandard ? 'secondary' : 'outline'} className="text-xs">
      {isStandard ? 'Standard' : 'Custom'}
    </Badge>
  );
}

function CalcTypeBadge({ calcType }: { calcType: string }) {
  const colors: Record<string, string> = {
    fixed: 'bg-green-100 text-green-800',
    formula: 'bg-blue-100 text-blue-800',
    schedule: 'bg-purple-100 text-purple-800',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${colors[calcType] ?? 'bg-gray-100'}`}>
      {CALC_TYPE_LABELS[calcType] ?? calcType}
    </span>
  );
}

function PostNowDrawer({
  template,
  entityCode,
  actorEmail,
  onClose,
}: {
  template: RecurringTemplate;
  entityCode: string;
  actorEmail: string;
  onClose: () => void;
}) {
  const [periodEnd, setPeriodEnd] = useState('');
  const [preview, setPreview] = useState<null | Record<string, unknown>>(null);
  const [loading, setLoading] = useState(false);

  const handlePreview = async () => {
    if (!periodEnd) { toast.error('Enter a period end date'); return; }
    setLoading(true);
    try {
      const result = await postRecurringTemplate({
        entity_code: entityCode,
        template_id: template.id,
        period_end: periodEnd,
        actor_email: actorEmail,
        dry_run: true,
      });
      setPreview(result as Record<string, unknown>);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Preview failed: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  const handlePost = async () => {
    if (!periodEnd) { toast.error('Enter a period end date'); return; }
    setLoading(true);
    try {
      const result = await postRecurringTemplate({
        entity_code: entityCode,
        template_id: template.id,
        period_end: periodEnd,
        actor_email: actorEmail,
        dry_run: false,
      });
      const r = result as Record<string, unknown>;
      if (r.auto_post) {
        toast.success(`Posted — batch ${r.journal_batch_id}`);
      } else {
        toast.success(`Draft created — batch ${r.journal_batch_id} awaiting approval`);
      }
      onClose();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Post failed: ${msg}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/40 sm:items-center">
      <div className="bg-white rounded-t-2xl sm:rounded-2xl shadow-xl w-full max-w-lg p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-lg">Post: {template.name}</h3>
          <button onClick={onClose} className="text-slate hover:text-ink text-xl">×</button>
        </div>

        <div className="space-y-2">
          <Label htmlFor="period_end">Period end date</Label>
          <Input
            id="period_end"
            type="date"
            value={periodEnd}
            onChange={(e) => { setPeriodEnd(e.target.value); setPreview(null); }}
          />
        </div>

        {preview && (
          <div className="bg-cloud rounded p-3 text-sm space-y-1">
            <p className="font-medium">Dry-run preview</p>
            <p>Grand total: <span className="font-semibold">${preview.grand_total as string}</span></p>
            <p className="text-slate">
              {template.auto_post
                ? 'Will post directly (auto_post=on)'
                : 'Will create a DRAFT for approval'}
            </p>
            <ul className="mt-1 space-y-0.5">
              {(preview.journal_lines as Array<Record<string, string>>)?.map((ln) => (
                <li key={ln.line_number} className="text-xs text-slate">
                  {ln.account_code} — Dr {ln.debit_amount} / Cr {ln.credit_amount}
                </li>
              ))}
            </ul>
          </div>
        )}

        <div className="flex gap-2 justify-end">
          <Button variant="outline" onClick={handlePreview} disabled={loading}>
            Preview
          </Button>
          <Button onClick={handlePost} disabled={loading || !periodEnd}>
            {template.auto_post ? 'Post Now' : 'Create Draft'}
          </Button>
        </div>
      </div>
    </div>
  );
}

function TemplateRow({
  template,
  entityCode,
  actorEmail,
  isAdmin,
  onToggle,
}: {
  template: RecurringTemplate;
  entityCode: string;
  actorEmail: string;
  isAdmin: boolean;
  onToggle: () => void;
}) {
  const [showPost, setShowPost] = useState(false);

  return (
    <div className="border border-cloud rounded-lg p-4 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-ink">{template.name}</span>
            <TemplateBadge template={template} />
            <CalcTypeBadge calcType={template.calc_type} />
            {!template.auto_post && (
              <span className="text-xs text-amber-700 bg-amber-50 px-2 py-0.5 rounded">
                Needs approval
              </span>
            )}
          </div>
          {template.description && (
            <p className="text-sm text-slate mt-1 leading-snug">{template.description}</p>
          )}
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {isAdmin && (
            <button
              onClick={onToggle}
              className={`relative inline-flex h-5 w-9 cursor-pointer rounded-full border-2 border-transparent transition-colors ${
                template.is_active ? 'bg-deep-navy' : 'bg-slate-200'
              }`}
              title={template.is_active ? 'Active — click to disable' : 'Inactive — click to enable'}
            >
              <span
                className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${
                  template.is_active ? 'translate-x-4' : 'translate-x-0'
                }`}
              />
            </button>
          )}
          {template.is_active && (
            <Button size="sm" variant="outline" onClick={() => setShowPost(true)}>
              Post
            </Button>
          )}
        </div>
      </div>

      <div className="flex flex-wrap gap-4 text-xs text-slate">
        {template.fixed_amount && (
          <span>Amount: <strong>${Number(template.fixed_amount).toLocaleString('en-CA', { minimumFractionDigits: 2 })}</strong></span>
        )}
        {template.formula_expr && (
          <span>Formula: <code className="bg-cloud px-1 rounded">{template.formula_expr}</code></span>
        )}
        {template.schedule_source && (
          <span>Source: <strong>{template.schedule_source}</strong></span>
        )}
        <span>Cadence: <strong>{template.cadence}</strong></span>
        {template.last_posted_period_end && (
          <span>Last posted: <strong>{template.last_posted_period_end}</strong></span>
        )}
        <span>Total posts: <strong>{template.total_postings}</strong></span>
      </div>

      {template.lines.length > 0 && (
        <div className="flex gap-2 flex-wrap text-xs">
          {template.lines.map((ln) => (
            <span key={ln.line_number} className="bg-cloud px-2 py-0.5 rounded text-slate">
              {ln.direction === 'debit' ? 'Dr' : 'Cr'} {ln.account_code}
              {ln.memo ? ` (${ln.memo})` : ''}
            </span>
          ))}
        </div>
      )}

      {showPost && (
        <PostNowDrawer
          template={template}
          entityCode={entityCode}
          actorEmail={actorEmail}
          onClose={() => setShowPost(false)}
        />
      )}
    </div>
  );
}

export default function RecurringEntriesSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();
  // Derive actor email from the user store (same pattern as other admin pages)
  const actorEmail = 'spencer7roberts@gmail.com'; // TODO: pull from useUserStore when available

  const templatesQ = useQuery({
    queryKey: ['recurring-templates', entityCode],
    enabled: !!entityCode,
    queryFn: () => listRecurringTemplates(entityCode!),
  });

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['recurring-templates', entityCode] });
  };

  const handleToggle = async (template: RecurringTemplate) => {
    if (!isAdmin) return;
    try {
      await toggleRecurringTemplate(template.id, entityCode!, !template.is_active);
      toast.success(`${template.name} ${template.is_active ? 'disabled' : 'enabled'}`);
      invalidate();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Failed: ${msg}`);
    }
  };

  const handleSeed = async () => {
    if (!entityCode) return;
    try {
      const result = await seedRecurringTemplates(entityCode);
      const r = result as { inserted: number; skipped: number };
      toast.success(`Seeded: ${r.inserted} inserted, ${r.skipped} already existed`);
      invalidate();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      toast.error(`Seed failed: ${msg}`);
    }
  };

  if (!entityCode) {
    return <p className="p-6 text-slate">Select an entity to manage recurring entries.</p>;
  }

  const templates = templatesQ.data?.templates ?? [];

  return (
    <div className="space-y-6 max-w-4xl">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>Recurring Journal Entries</CardTitle>
            <p className="text-sm text-slate mt-1">
              Automatic periodic entries — fixed amounts, formula-based, or fed from the fixed-asset schedule.
              All templates ship <strong>off by default</strong>; toggle each one on explicitly.
            </p>
          </div>
          {isAdmin && (
            <Button variant="outline" size="sm" onClick={handleSeed}>
              Seed standard templates
            </Button>
          )}
        </CardHeader>
        <CardContent className="space-y-3">
          {templatesQ.isLoading && (
            <div className="space-y-3">
              {[1, 2, 3].map((n) => <Skeleton key={n} className="h-24 w-full" />)}
            </div>
          )}
          {!templatesQ.isLoading && templates.length === 0 && (
            <div className="text-center py-8 text-slate">
              <p>No recurring entries configured.</p>
              {isAdmin && (
                <p className="text-sm mt-1">
                  Click <strong>Seed standard templates</strong> to add DGIP forgiveness,
                  percentage rent, interest accrual, and monthly depreciation templates.
                </p>
              )}
            </div>
          )}
          {templates.map((t) => (
            <TemplateRow
              key={t.id}
              template={t}
              entityCode={entityCode}
              actorEmail={actorEmail}
              isAdmin={isAdmin}
              onToggle={() => handleToggle(t)}
            />
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">How auto-posting works</CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-slate space-y-2">
          <p>
            <strong className="text-ink">Fixed entries</strong> (e.g. DGIP forgiveness) post directly
            to <em>posted</em> status when triggered — the amount never changes so no approval is needed.
          </p>
          <p>
            <strong className="text-ink">Formula / Schedule entries</strong> (e.g. percentage rent,
            monthly depreciation) create a <em>draft</em> batch for one-click approval in the
            Journal Entries section. The amount is computed fresh each period.
          </p>
          <p>
            Every entry is guarded against locked periods (closed_locked) and can never post
            to account 3900. A balance guard rejects any entry where debits ≠ credits.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
