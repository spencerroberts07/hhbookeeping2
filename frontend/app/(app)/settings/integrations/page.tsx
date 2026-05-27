'use client';

import Link from 'next/link';
import { useUser } from '@clerk/nextjs';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Database,
  Loader2,
  RefreshCw,
} from 'lucide-react';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';
import { useIsAdmin } from '@/lib/store/user';
import {
  disconnectQuickbooks,
  getQuickbooksStatus,
  startQuickbooksConnect,
} from '@/lib/api/dashboard';
import { pullChartFromQbo } from '@/lib/api/onboarding';
import { getChartSyncStatus } from '@/lib/api/data_import';
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

              {/* Chart of Accounts sync — pulls from the connected QBO
                  company and upserts into the local accounts table.
                  Reuses the onboarding chart-of-accounts/qbo endpoint
                  (entity-scoped, no wizard state). */}
              <ChartOfAccountsSection isAdmin={isAdmin} />
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

      <Card>
        <CardContent className="py-5">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="font-semibold text-deep-navy">Bulk data import</div>
              <p className="text-sm text-slate mt-0.5">
                Upload trial balances or GL CSV exports outside the onboarding
                wizard.
              </p>
            </div>
            <Link
              href="/settings/data-import"
              className="inline-flex items-center gap-1 text-sm font-semibold text-ledger-blue hover:underline"
            >
              Open data import
              <ArrowRight className="h-4 w-4" />
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function ChartOfAccountsSection({ isAdmin }: { isAdmin: boolean }) {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  const actor = user?.primaryEmailAddress?.emailAddress ?? '';
  const qc = useQueryClient();

  // Persisted across browser sessions via quickbooks_sync_runs.
  // Replaces the per-session "Never (this session)" placeholder the
  // earlier version had.
  const status = useQuery({
    queryKey: ['chart-sync-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getChartSyncStatus(entityCode!),
  });

  const sync = useMutation({
    mutationFn: () => {
      if (!entityCode) throw new Error('No active entity');
      return pullChartFromQbo({ entity_code: entityCode, actor_email: actor });
    },
    onSuccess: (res) => {
      toast.success(
        `Synced ${res.account_count} account${res.account_count === 1 ? '' : 's'} from QuickBooks`,
      );
      qc.invalidateQueries({ queryKey: ['chart-sync-status', entityCode] });
      qc.invalidateQueries({ queryKey: ['onboarding-status', entityCode] });
    },
    onError: (err: Error) =>
      toast.error(err.message || 'Could not sync chart of accounts'),
  });

  const currentCount = status.data?.accounts_count ?? 0;
  const qboMapped = status.data?.qbo_mapped_count ?? 0;
  const lastSyncedAt = status.data?.last_synced_at ?? null;

  return (
    <div className="border-t border-border pt-4 mt-4 space-y-3">
      <div className="flex items-center gap-2">
        <Database className="h-4 w-4 text-ledger-blue" />
        <span className="font-semibold text-deep-navy">Chart of accounts</span>
      </div>
      <p className="text-sm text-slate">
        Sync your QBO chart of accounts to BookWize. Each sync upserts every
        account from QuickBooks and writes the QBO account id mapping that
        later GL imports rely on.
      </p>
      <dl className="text-sm grid grid-cols-[120px_1fr] gap-x-3 gap-y-1 text-slate">
        <dt>Accounts loaded</dt>
        <dd className="text-ink">
          {currentCount}
          {qboMapped > 0 && (
            <span className="text-slate"> · {qboMapped} mapped to QBO</span>
          )}
        </dd>
        <dt>Last synced</dt>
        <dd className="text-ink">
          {lastSyncedAt
            ? formatDate(lastSyncedAt, 'MMM dd, yyyy HH:mm')
            : 'Never'}
        </dd>
      </dl>
      <div className="flex flex-wrap items-center gap-2 pt-1">
        <Button
          variant="outline"
          onClick={() => sync.mutate()}
          disabled={!isAdmin || sync.isPending}
        >
          {sync.isPending ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <RefreshCw className="h-4 w-4 mr-2" />
          )}
          Sync chart of accounts
        </Button>
        {sync.isError && (
          <span className="text-xs text-red-700 inline-flex items-center gap-1">
            <AlertTriangle className="h-3.5 w-3.5" />
            Sync failed — try again
          </span>
        )}
      </div>
      {sync.data && (
        <div className="rounded-md border border-bw-teal/30 bg-bw-teal/5 p-3 text-sm">
          <div className="flex items-center gap-2 font-semibold text-deep-navy">
            <CheckCircle2 className="h-4 w-4 text-bw-teal" />
            {sync.data.account_count} accounts synced
          </div>
          <p className="text-xs text-slate mt-1 ml-6">
            {sync.data.bank_account_count} bank-type accounts found.
          </p>
        </div>
      )}
      {!isAdmin && (
        <p className="text-xs text-slate">
          Admin role required to sync the chart.
        </p>
      )}
    </div>
  );
}
