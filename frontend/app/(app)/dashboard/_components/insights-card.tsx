'use client';

import { useState } from 'react';
import Link from 'next/link';
import { useQuery } from '@tanstack/react-query';
import { Sparkles, ChevronDown, ChevronUp, X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import { getAssistantInsights } from '@/lib/api/assistant';

// Top-3 actionable insights from the assistant. Each is dismissible
// for the duration of the session (state lives in component memory —
// dismissing doesn't persist across reloads on purpose, so the dealer
// gets a fresh nudge after sleeping).
export function InsightsCard() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const [collapsed, setCollapsed] = useState(false);
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());

  const q = useQuery({
    queryKey: ['assistant-insights', entityCode],
    enabled: !!entityCode,
    queryFn: () => getAssistantInsights(entityCode!),
    staleTime: 60_000,
  });

  const visible = (q.data?.insights ?? [])
    .filter((i) => !dismissed.has(i.type + i.description))
    .slice(0, 3);

  // Hide the section entirely if there's nothing to show — keeps the
  // dashboard tight when the dealer is caught up.
  if (!q.isLoading && visible.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-sm">
          <span className="flex items-center gap-2 text-deep-navy">
            <Sparkles className="h-4 w-4 text-bw-teal" />
            Assistant Insights
            {visible.length > 0 && (
              <Badge variant="info" className="text-[10px]">
                {visible.length}
              </Badge>
            )}
          </span>
          <button
            onClick={() => setCollapsed((c) => !c)}
            aria-label={collapsed ? 'Expand' : 'Collapse'}
            className="text-slate hover:text-deep-navy"
          >
            {collapsed ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronUp className="h-4 w-4" />
            )}
          </button>
        </CardTitle>
      </CardHeader>
      {!collapsed && (
        <CardContent className="space-y-2">
          {q.isLoading ? (
            <>
              <Skeleton className="h-14 w-full" />
              <Skeleton className="h-14 w-full" />
            </>
          ) : (
            visible.map((i, idx) => (
              <div
                key={`${i.type}-${idx}`}
                className="flex items-start gap-3 rounded-lg border border-border bg-white p-3"
              >
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-deep-navy">{i.description}</div>
                  {i.action && i.action_url && (
                    <Link
                      href={i.action_url}
                      className="text-xs text-ledger-blue hover:underline mt-1 inline-block"
                    >
                      {i.action} →
                    </Link>
                  )}
                </div>
                <button
                  onClick={() =>
                    setDismissed(
                      (d) => new Set([...Array.from(d), i.type + i.description]),
                    )
                  }
                  aria-label="Dismiss"
                  className="text-slate hover:text-deep-navy"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            ))
          )}
        </CardContent>
      )}
    </Card>
  );
}
