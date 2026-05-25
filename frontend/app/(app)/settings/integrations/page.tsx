'use client';

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import {
  AlertTriangle,
  CheckCircle2,
  Database,
  Loader2,
} from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  disconnectQuickbooks,
  getQuickbooksStatus,
  startQuickbooksConnect,
} from '@/lib/api/dashboard';
import { formatDate } from '@/lib/utils';

// Force dynamic so Next.js doesn't try to prerender this page at
// build time — the Zustand store + React Query state only resolve
// client-side and a prerender failure under static generation would
// cascade to a route-not-found at runtime.
export const dynamic = 'force-dynamic';

const DEMO_REALM_ID = '9341456852590440';

export default function IntegrationsSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ['qbo-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => {
      if (!entityCode) throw new Error('No active entity');
      return getQuickbooksStatus(entityCode);
    },
  });

  const connect = useMutation({
    mutationFn: () => {
      if (!entityCode) throw new Error('No active entity');
      return startQuickbooksConnect(entityCode);
    },
    onSuccess: (res) => {
      window.location.href = res.authorization_url;
    },
    onError: () => toast.error('Could not start QuickBooks connection'),
  });

  const disconnect = useMutation({
    mutationFn: () => {
      if (!entityCode) throw new Error('No active entity');
      return disconnectQuickbooks(entityCode);
    },
    onSuccess: (res) => {
      toast.success(
        `QuickBooks disconnected. Cleared ${res.account_mappings_cleared} account mapping(s).`,
      );
      qc.invalidateQueries({ queryKey: ['qbo-status'] });
    },
    onError: () => toast.error('Disconnect failed'),
  });

  const isDemoRealm = q.data?.realm_id === DEMO_REALM_ID;

  // Pre-hydration: Zustand persist hasn't restored localStorage yet.
  // Render a stable skeleton instead of churning through render branches
  // that depend on the active entity.
  if (!entityCode) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-ledger-blue" />
            QuickBooks Online
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-24 w-full" />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-ledger-blue" />
            QuickBooks Online
          </CardTitle>
        </CardHeader>
        <CardContent>
          {q.isLoading ? (
            <Skeleton className="h-24 w-full" />
          ) : q.data?.is_connected ? (
            <div className="space-y-4">
              {isDemoRealm && (
                <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 flex items-start gap-2">
                  <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                  <div>
                    <p className="font-semibold">
                      Connected to demo company
                    </p>
                    <p className="mt-1">
                      This QuickBooks connection points at Intuit's
                      Craig's Design and Landscaping sample-company file —
                      not your real books. Disconnect and reconnect using
                      your actual QuickBooks company.
                    </p>
                  </div>
                </div>
              )}

              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-5 w-5 text-bw-teal" />
                <span className="font-semibold text-deep-navy">Connected</span>
                <Badge variant="complete">QBO</Badge>
              </div>
              <dl className="text-sm grid grid-cols-[120px_1fr] gap-x-3 gap-y-1 text-slate">
                {q.data.company_name && (
                  <>
                    <dt>Company</dt>
                    <dd className="text-ink">{q.data.company_name}</dd>
                  </>
                )}
                <dt>Realm ID</dt>
                <dd className="font-mono text-ink">{q.data.realm_id ?? '—'}</dd>
                {q.data.last_synced_at && (
                  <>
                    <dt>Connected</dt>
                    <dd className="text-ink">
                      {formatDate(q.data.last_synced_at, 'MMM dd, yyyy HH:mm')}
                    </dd>
                  </>
                )}
              </dl>

              <div className="flex gap-2 pt-2">
                {isDemoRealm ? (
                  <Button
                    onClick={() => connect.mutate()}
                    disabled={!isAdmin || connect.isPending}
                  >
                    {connect.isPending && (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    )}
                    Reconnect to correct company
                  </Button>
                ) : (
                  <Button
                    variant="outline"
                    onClick={() => connect.mutate()}
                    disabled={!isAdmin || connect.isPending}
                  >
                    {connect.isPending && (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    )}
                    Reconnect
                  </Button>
                )}
                <Button
                  variant="ghost"
                  onClick={() => disconnect.mutate()}
                  disabled={!isAdmin || disconnect.isPending}
                  className="text-red-700 hover:text-red-800 hover:bg-red-50"
                >
                  {disconnect.isPending && (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  )}
                  Disconnect
                </Button>
              </div>
              {!isAdmin && (
                <p className="text-xs text-slate">
                  Admin role required to change this connection.
                </p>
              )}
            </div>
          ) : (
            <div className="space-y-4">
              <p className="text-sm text-slate">
                QuickBooks isn't connected for this store. Connect to import
                your chart of accounts, GL history, and stay in sync.
              </p>
              <Button
                onClick={() => connect.mutate()}
                disabled={!isAdmin || connect.isPending}
              >
                {connect.isPending && (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                )}
                Connect QuickBooks
              </Button>
              {!isAdmin && (
                <p className="text-xs text-slate">
                  Admin role required to connect.
                </p>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
