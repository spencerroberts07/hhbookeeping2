'use client';

import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { useEntityStore } from '@/lib/store/entity';
import { listAccounts } from '@/lib/api/accounts';

export default function AccountsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const q = useQuery({
    queryKey: ['accounts', entityCode],
    enabled: !!entityCode,
    queryFn: () => listAccounts(entityCode!),
  });

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>Chart of accounts</span>
          {q.data && (
            <Badge variant="info">
              {q.data.count} {q.data.count === 1 ? 'account' : 'accounts'}
              {q.data.seeded_from === 'journal_lines' && ' · seeded from history'}
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {q.isLoading ? (
          <div className="p-6 space-y-2">
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
            <Skeleton className="h-6 w-full" />
          </div>
        ) : !q.data || q.data.count === 0 ? (
          <p className="p-6 text-sm text-slate">
            No accounts yet. Import a chart from QuickBooks or upload a CSV
            during onboarding.
          </p>
        ) : (
          <table className="min-w-full text-sm">
            <thead className="bg-cloud">
              <tr>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Code</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Name</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Type</th>
                <th className="text-left font-semibold text-deep-navy px-4 py-2">Normal balance</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {q.data.accounts.map((a) => (
                <tr key={a.code} className="hover:bg-cloud">
                  <td className="px-4 py-2 font-mono text-xs text-slate">{a.code}</td>
                  <td className="px-4 py-2 text-ink">{a.name}</td>
                  <td className="px-4 py-2 text-slate">{a.type}</td>
                  <td className="px-4 py-2 text-slate capitalize">{a.normal_balance}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}
