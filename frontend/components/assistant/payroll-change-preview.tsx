'use client';

import { Button } from '@/components/ui/button';
import type { PayrollChangePreview } from '@/lib/api/assistant';
import { formatMoney } from '@/lib/utils';
import { ClipboardCheck } from 'lucide-react';

interface Props {
  preview: PayrollChangePreview;
  onConfirm?: () => void;
  onCancel?: () => void;
  busy?: boolean;
  resolved?: boolean;
}

/**
 * BEFORE / AFTER card rendered for any assistant proposed_action of
 * type update_employee_rate / update_employee_salary /
 * one_time_pay_override / add_bonus_line.
 *
 * The render shape is intentionally similar to JournalPreview so the
 * confirm/cancel ergonomics inside assistant-message.tsx feel
 * consistent across action types.
 */
export function PayrollChangePreview({
  preview,
  onConfirm,
  onCancel,
  busy,
  resolved,
}: Props) {
  const isOneTime =
    preview.change_type === 'one_time_override' ||
    preview.change_type === 'bonus_line';
  const headerLabel =
    {
      hourly_rate: 'Hourly rate change',
      biweekly_salary: 'Salary change',
      one_time_override: 'One-time pay override',
      bonus_line: 'Bonus',
    }[preview.change_type] ?? 'Payroll change';

  const rows: Array<{ label: string; before: string; after: string }> = [];
  if (preview.before.hourly_rate != null || preview.after.hourly_rate != null) {
    rows.push({
      label: 'Hourly rate',
      before: fmt(preview.before.hourly_rate),
      after: fmt(preview.after.hourly_rate),
    });
  }
  if (
    preview.before.biweekly_salary != null ||
    preview.after.biweekly_salary != null
  ) {
    rows.push({
      label: 'Bi-weekly salary',
      before: fmt(preview.before.biweekly_salary),
      after: fmt(preview.after.biweekly_salary),
    });
  }
  rows.push({
    label: 'Est. gross',
    before: fmt(preview.before.sample_biweekly_gross),
    after: fmt(preview.after.sample_biweekly_gross),
  });
  rows.push({
    label: 'Est. net',
    before: fmt(preview.before.net_est),
    after: fmt(preview.after.net_est),
  });

  return (
    <div className="rounded-xl border border-border bg-white text-ink overflow-hidden">
      <header className="px-3 py-1.5 bg-cloud border-b border-border flex items-center gap-1.5">
        <ClipboardCheck className="h-3.5 w-3.5 text-deep-navy" strokeWidth={1.5} />
        <div className="text-xs font-semibold text-deep-navy">
          {headerLabel}
          {preview.employee_name && (
            <span className="text-slate font-normal"> — {preview.employee_name}</span>
          )}
          {isOneTime && preview.run_period_label && (
            <span className="text-slate font-normal"> · {preview.run_period_label}</span>
          )}
        </div>
      </header>

      <table className="w-full text-xs tabular-nums">
        <thead className="bg-cloud/60">
          <tr>
            <th className="text-left px-3 py-1 font-semibold text-slate"></th>
            <th className="text-right px-3 py-1 font-semibold text-slate">Before</th>
            <th className="text-right px-3 py-1 font-semibold text-slate">After</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.map((r) => (
            <tr key={r.label}>
              <td className="px-3 py-1 text-ink">{r.label}</td>
              <td className="px-3 py-1 text-right text-slate">{r.before}</td>
              <td className="px-3 py-1 text-right text-deep-navy font-semibold">
                {r.after}
              </td>
            </tr>
          ))}
          {preview.annual_impact != null && !isOneTime && (
            <tr className="bg-amber-50">
              <td className="px-3 py-1 text-amber-900 font-semibold">
                Annual impact
              </td>
              <td className="px-3 py-1 text-right text-slate"></td>
              <td className="px-3 py-1 text-right text-amber-900 font-semibold">
                {formatMoney(preview.annual_impact, { signed: true })}
              </td>
            </tr>
          )}
          {preview.annual_impact != null && preview.change_type === 'bonus_line' && (
            <tr className="bg-amber-50">
              <td className="px-3 py-1 text-amber-900 font-semibold">
                Take-home (after tax)
              </td>
              <td className="px-3 py-1 text-right text-slate"></td>
              <td className="px-3 py-1 text-right text-amber-900 font-semibold">
                {formatMoney(preview.annual_impact)}
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {preview.note && (
        <p className="px-3 py-1.5 text-[11px] text-slate border-t border-border bg-cloud/40">
          {preview.note}
        </p>
      )}

      {!resolved && (
        <div className="px-3 py-2 flex gap-2 border-t border-border">
          <Button
            size="sm"
            variant="primary"
            onClick={() => onConfirm?.()}
            disabled={busy}
          >
            Apply change
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => onCancel?.()}
            disabled={busy}
          >
            Cancel
          </Button>
        </div>
      )}
      {resolved && (
        <div className="px-3 py-1.5 text-[11px] text-bw-teal border-t border-border bg-cloud/40">
          ✓ Applied
        </div>
      )}
    </div>
  );
}

function fmt(value: number | null | undefined): string {
  if (value == null) return '—';
  return formatMoney(value);
}
