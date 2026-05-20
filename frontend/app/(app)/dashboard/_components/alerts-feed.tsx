'use client';

import { useQuery } from '@tanstack/react-query';
import { AlertCircle, FileWarning, Clock } from 'lucide-react';
import { useEntityStore } from '@/lib/store/entity';
import { listSuggestions } from '@/lib/api/vendor_classification';
import { Skeleton } from '@/components/ui/skeleton';

export function AlertsFeed() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  const pending = useQuery({
    queryKey: ['classification-suggestions', entityCode],
    enabled: !!entityCode,
    queryFn: () =>
      listSuggestions({ entity_code: entityCode!, status: 'pending', limit: 5 }),
  });

  if (pending.isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-12 w-full" />
        <Skeleton className="h-12 w-full" />
        <Skeleton className="h-12 w-full" />
      </div>
    );
  }

  const items = [
    ...(pending.data?.suggestions ?? []).map((s) => ({
      icon: AlertCircle,
      label: `Classification suggestion · ${s.vendor_key}`,
      detail: `${s.suggested_account_code ?? 'UNCLASSIFIED'} (${(s.confidence ?? 0).toFixed(0)}%)`,
    })),
    // TODO: backend endpoint not built — unified alerts feed (period-locks,
    // unmatched bank, missing documents)
    {
      icon: FileWarning,
      label: 'Missing HH AP statement',
      detail: 'No statement uploaded for current month yet',
    },
    {
      icon: Clock,
      label: 'Month-end reminder',
      detail: 'Close window opens on the 1st of next month',
    },
  ];

  if (items.length === 0) {
    return <p className="text-sm text-slate">No alerts. Nice work.</p>;
  }

  return (
    <ul className="space-y-2">
      {items.map((item, idx) => {
        const Icon = item.icon;
        return (
          <li
            key={idx}
            className="flex items-start gap-3 rounded-lg border border-border bg-white p-3"
          >
            <Icon
              className="h-5 w-5 text-amber-600 mt-0.5 shrink-0"
              strokeWidth={1.5}
            />
            <div className="min-w-0">
              <div className="text-sm font-semibold text-deep-navy truncate">
                {item.label}
              </div>
              <div className="text-xs text-slate truncate">{item.detail}</div>
            </div>
          </li>
        );
      })}
    </ul>
  );
}
