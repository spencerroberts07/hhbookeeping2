'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import { getSubscription, createPortalSession } from '@/lib/api/billing';
import { formatDate } from '@/lib/utils';
import { useIsAdmin } from '@/lib/store/user';
import { toast } from 'sonner';

export default function BillingSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const isAdmin = useIsAdmin();
  const [opening, setOpening] = useState(false);

  const sub = useQuery({
    queryKey: ['billing-subscription', entityCode],
    enabled: !!entityCode,
    queryFn: () => getSubscription(entityCode!),
  });

  const openPortal = async () => {
    if (!entityCode) return;
    setOpening(true);
    try {
      const origin =
        typeof window !== 'undefined' ? window.location.origin : '';
      const { url } = await createPortalSession({
        entity_code: entityCode,
        return_url: `${origin}/settings/billing`,
      });
      window.location.href = url;
    } catch {
      toast.error('Could not open the billing portal.');
      setOpening(false);
    }
  };

  // Owner / demo accounts — hide every Stripe surface.
  // TODO: Replace with real Stripe subscription when this internal
  // account is ready to be billed. Delete the billing_subscriptions
  // row with plan_tier='internal' and run through this same checkout
  // flow.
  if (sub.data?.plan_tier === 'internal') {
    return (
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle>Subscription</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 mb-3">
              <Badge
                variant="locked"
                className="bg-deep-navy text-white border-white/20 uppercase"
              >
                Owner
              </Badge>
              <Badge variant="complete">active</Badge>
            </div>
            <p className="text-sm text-slate">
              This is an owner account. Billing is managed internally.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Subscription</CardTitle>
        </CardHeader>
        <CardContent>
          {sub.isLoading ? (
            <Skeleton className="h-32" />
          ) : !sub.data || !sub.data.status ? (
            <p className="text-slate">No subscription on file for this store.</p>
          ) : (
            <div className="space-y-3">
              <div className="flex items-center gap-2">
                <Badge variant="complete" className="uppercase">
                  {sub.data.plan_tier ?? '—'}
                </Badge>
                <Badge
                  variant={
                    sub.data.status === 'active' || sub.data.status === 'trialing'
                      ? 'complete'
                      : sub.data.status === 'past_due'
                        ? 'warning'
                        : 'error'
                  }
                >
                  {sub.data.status}
                </Badge>
                {sub.data.cancel_at_period_end && (
                  <Badge variant="warning">Cancels at period end</Badge>
                )}
              </div>
              <dl className="grid grid-cols-2 gap-3 text-sm max-w-md">
                <dt className="text-slate">Trial ends</dt>
                <dd className="text-ink">
                  {sub.data.trial_end ? formatDate(sub.data.trial_end) : '—'}
                </dd>
                <dt className="text-slate">Next renewal</dt>
                <dd className="text-ink">
                  {sub.data.current_period_end
                    ? formatDate(sub.data.current_period_end)
                    : '—'}
                </dd>
                <dt className="text-slate">Stores billed</dt>
                <dd className="text-ink">{sub.data.store_count}</dd>
              </dl>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Manage</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-slate mb-3">
            Update payment method, change plan, view invoices, or cancel from
            the Stripe Customer Portal.
          </p>
          {isAdmin ? (
            <Button onClick={openPortal} disabled={opening}>
              {opening ? 'Opening…' : 'Open billing portal'}
            </Button>
          ) : (
            <p className="text-xs text-slate">
              Only admins can change billing.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
