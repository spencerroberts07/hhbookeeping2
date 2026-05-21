'use client';

import { useQuery } from '@tanstack/react-query';
import {
  AlertCircle,
  AlertTriangle,
  Clock,
  FileWarning,
  Receipt,
  TrendingUp,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import Link from 'next/link';
import { useEntityStore } from '@/lib/store/entity';
import { getDashboardAlerts } from '@/lib/api/dashboard';
import { listSuggestions } from '@/lib/api/vendor_classification';
import { Skeleton } from '@/components/ui/skeleton';

// Alert icon + accent colour by type. Falls back to a neutral chip
// when the backend invents a new alert type we don't have an icon for.
const ICONS: Record<string, LucideIcon> = {
  period_late: Clock,
  draft_journals: FileWarning,
  missing_hh_ap_statement: FileWarning,
  unmatched_invoices: Receipt,
  unclassified_transactions: TrendingUp,
};

function severityClass(severity: string): string {
  if (severity === 'error') return 'text-red-600';
  if (severity === 'warning') return 'text-amber-600';
  return 'text-ledger-blue';
}

export function AlertsFeed() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  // Real backend alerts — replaces the static "Missing HH AP statement"
  // / "Month-end reminder" placeholders.
  const alerts = useQuery({
    queryKey: ['dashboard-alerts', entityCode],
    enabled: !!entityCode,
    queryFn: () => getDashboardAlerts(entityCode!),
  });

  // Vendor-classification suggestions are still served by their own
  // endpoint and shown inline at the top of the feed.
  const suggestions = useQuery({
    queryKey: ['classification-suggestions', entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      listSuggestions({ entity_code: entityCode!, status: 'pending', limit: 5 }),
  });

  if (alerts.isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-12 w-full" />
        <Skeleton className="h-12 w-full" />
        <Skeleton className="h-12 w-full" />
      </div>
    );
  }

  const backendAlerts = alerts.data?.alerts ?? [];
  const suggestionAlerts =
    (suggestions.data?.suggestions ?? []).map((s) => ({
      type: 'classification_suggestion',
      severity: 'info' as const,
      label: `Classification suggestion · ${s.vendor_key || 'Bank transaction'}`,
      detail: `${s.suggested_account_code ?? 'Unclassified'} (${(s.confidence ?? 0).toFixed(0)}%)`,
      href: undefined,
    }));

  const items = [...backendAlerts, ...suggestionAlerts];

  if (items.length === 0) {
    return <p className="text-sm text-slate">No alerts. Nice work.</p>;
  }

  return (
    <ul className="space-y-2">
      {items.map((item, idx) => {
        const Icon = ICONS[item.type] ?? AlertCircle;
        const ColorIcon = item.severity === 'warning' ? AlertTriangle : Icon;
        const body = (
          <>
            <ColorIcon
              className={`h-5 w-5 mt-0.5 shrink-0 ${severityClass(item.severity)}`}
              strokeWidth={1.5}
            />
            <div className="min-w-0">
              <div className="text-sm font-semibold text-deep-navy truncate">
                {item.label}
              </div>
              <div className="text-xs text-slate truncate">{item.detail}</div>
            </div>
          </>
        );
        return (
          <li
            key={idx}
            className="flex items-start gap-3 rounded-lg border border-border bg-white p-3"
          >
            {item.href ? (
              <Link
                href={item.href}
                className="flex items-start gap-3 flex-1 hover:bg-cloud rounded-md -m-1 p-1"
              >
                {body}
              </Link>
            ) : (
              body
            )}
          </li>
        );
      })}
    </ul>
  );
}
