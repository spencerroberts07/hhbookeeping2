import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { format, parseISO } from 'date-fns';

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

/**
 * Format a number as CAD currency. Returns "—" for null/undefined so empty
 * states never show "$0.00" by accident.
 */
export function formatMoney(
  value: number | string | null | undefined,
  options: { signed?: boolean } = {},
): string {
  if (value === null || value === undefined || value === '') return '—';
  const n = typeof value === 'string' ? Number(value) : value;
  if (Number.isNaN(n)) return '—';
  const formatted = new Intl.NumberFormat('en-CA', {
    style: 'currency',
    currency: 'CAD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Math.abs(n));
  if (options.signed && n < 0) return `(${formatted})`;
  if (options.signed && n > 0) return `+${formatted}`;
  return n < 0 ? `(${formatted})` : formatted;
}

/**
 * Format an ISO date or Date as "MMM DD, YYYY". Use this everywhere — never
 * `new Date(...).toLocaleString()` directly.
 */
export function formatDate(
  value: string | Date | null | undefined,
  pattern = 'MMM dd, yyyy',
): string {
  if (!value) return '—';
  const date = typeof value === 'string' ? parseISO(value) : value;
  if (Number.isNaN(date.getTime())) return '—';
  return format(date, pattern);
}

/**
 * "Jan 2026" style label for month/year selectors.
 */
export function formatMonthLabel(value: string | Date): string {
  return formatDate(value, 'MMM yyyy');
}

/**
 * Convert "2026-02-28" → JS Date in local timezone (avoids UTC shift).
 */
export function parseLocalDate(iso: string): Date {
  const [y, m, d] = iso.split('-').map(Number);
  return new Date(y ?? 0, (m ?? 1) - 1, d ?? 1);
}

/**
 * Percent formatter — `null` returns "—", otherwise "12.3%".
 */
export function formatPercent(
  value: number | null | undefined,
  digits = 1,
): string {
  if (value === null || value === undefined) return '—';
  if (Number.isNaN(value)) return '—';
  return `${value.toFixed(digits)}%`;
}
