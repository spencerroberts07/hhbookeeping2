'use client';

import { useQuery } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { listSuggestions, acceptSuggestion } from '@/lib/api/vendor_classification';
import { useUser } from '@clerk/nextjs';
import { toast } from 'sonner';
import { useQueryClient } from '@tanstack/react-query';

export function StepReviewQueue({ entityCode }: { entityCode: string }) {
  const { user } = useUser();
  const actorEmail = user?.primaryEmailAddress?.emailAddress ?? '';
  const qc = useQueryClient();

  const suggestions = useQuery({
    queryKey: ['classification-suggestions', entityCode, 'pending'],
    queryFn: () =>
      listSuggestions({ entity_code: entityCode, status: 'pending', limit: 100 }),
  });

  const handleAccept = async (id: string) => {
    try {
      await acceptSuggestion(id, {
        entity_code: entityCode,
        actor_email: actorEmail,
      });
      toast.success('Accepted');
      qc.invalidateQueries({ queryKey: ['classification-suggestions'] });
    } catch {
      // Toast handled by interceptor.
    }
  };

  if (suggestions.isLoading) {
    return <Skeleton className="h-32" />;
  }

  const items = suggestions.data?.suggestions ?? [];
  if (items.length === 0) {
    return (
      <p className="text-sm text-slate">
        No pending suggestions. Re-run auto-classify if you&apos;ve uploaded new
        statements.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto border border-border rounded-xl">
      <table className="min-w-full text-sm">
        <thead className="bg-cloud">
          <tr>
            <th className="text-left font-semibold text-deep-navy px-4 py-2">
              Vendor
            </th>
            <th className="text-left font-semibold text-deep-navy px-4 py-2">
              Suggested account
            </th>
            <th className="text-left font-semibold text-deep-navy px-4 py-2">
              Source
            </th>
            <th className="text-left font-semibold text-deep-navy px-4 py-2">
              Confidence
            </th>
            <th className="px-4 py-2" />
          </tr>
        </thead>
        <tbody>
          {items.map((s) => (
            <tr key={s.id} className="border-t border-border hover:bg-cloud">
              <td className="px-4 py-2 font-mono text-xs text-ink">
                {s.vendor_key}
              </td>
              <td className="px-4 py-2 text-ink">
                {s.suggested_account_code}{' '}
                <span className="text-slate uppercase text-xs">
                  ({s.suggested_debit_or_credit})
                </span>
              </td>
              <td className="px-4 py-2">
                <Badge variant="info">{s.source}</Badge>
              </td>
              <td className="px-4 py-2 tabular-nums text-slate">
                {s.confidence.toFixed(0)}%
              </td>
              <td className="px-4 py-2 text-right">
                <Button
                  size="sm"
                  variant="accent"
                  onClick={() => handleAccept(s.id)}
                  disabled={!actorEmail}
                >
                  Accept
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
