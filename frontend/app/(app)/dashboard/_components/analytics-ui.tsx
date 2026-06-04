'use client';

import { TrendingUp, TrendingDown } from 'lucide-react';
import { formatMoney } from '@/lib/utils';

export type MetricSource = 'gl_net' | 'pos_gross';

export function SourcePill({ source }: { source: MetricSource }) {
  const isGl = source === 'gl_net';
  return (
    <span
      className={
        'rounded-full px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide ' +
        (isGl ? 'bg-ledger-blue/10 text-ledger-blue' : 'bg-bw-teal/10 text-bw-teal')
      }
      title={
        isGl
          ? 'From the general ledger (reconciles to the financial statements)'
          : 'From daily cash balancing (POS gross sales)'
      }
    >
      {isGl ? 'GL net' : 'POS gross'}
    </span>
  );
}

export function GrowthChip({ pct }: { pct: number | null }) {
  if (pct === null || pct === undefined) return <span className="text-slate">—</span>;
  const up = pct >= 0;
  return (
    <span className={'inline-flex items-center gap-1 text-sm font-medium ' + (up ? 'text-green-700' : 'text-red-700')}>
      {up ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
      {up ? '+' : ''}{pct.toFixed(1)}%
    </span>
  );
}

export const AXIS = '#64748B';
export const THIS_YR = '#1454C8';
export const LAST_YR = '#0B2E72';
export const ACCENT = '#13B8B4';
export const moneyTick = (v: number) => `$${((v ?? 0) / 1000).toFixed(0)}k`;
export const moneyTip = (value: unknown) => {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? formatMoney(n) : '—';
};
