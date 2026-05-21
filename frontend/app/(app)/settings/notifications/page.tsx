'use client';

import { useEffect, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { useEntityStore } from '@/lib/store/entity';
import {
  getNotificationPreferences,
  updateNotificationPreferences,
  type NotificationPreferences,
} from '@/lib/api/notifications';
import { toast } from 'sonner';

const ROWS: Array<{ key: keyof NotificationPreferences; label: string; desc: string }> = [
  { key: 'month_end_reminders', label: 'Month-end reminders', desc: 'Around the 1st of each month' },
  { key: 'variance_alerts', label: 'Variance alerts', desc: 'When trial balance variance exceeds threshold' },
  { key: 'approval_requests', label: 'Approval requests', desc: 'When a batch needs your sign-off' },
  { key: 'payment_receipts', label: 'Payment receipts', desc: 'Stripe billing receipts and invoice updates' },
];

export default function NotificationsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();
  const q = useQuery({
    queryKey: ['notification-prefs', entityCode],
    enabled: !!entityCode,
    queryFn: () => getNotificationPreferences(entityCode!),
  });

  const [local, setLocal] = useState<NotificationPreferences | null>(null);
  useEffect(() => {
    if (q.data) setLocal(q.data.notification_preferences);
  }, [q.data]);

  const mutation = useMutation({
    mutationFn: (patch: Partial<NotificationPreferences>) =>
      updateNotificationPreferences(entityCode!, patch),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['notification-prefs', entityCode] });
    },
    onError: () => toast.error('Could not save preference'),
  });

  const toggle = (key: keyof NotificationPreferences) => {
    if (!local) return;
    const next = !local[key];
    setLocal({ ...local, [key]: next });
    mutation.mutate({ [key]: next });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Notification preferences</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {q.isLoading || !local ? (
          <>
            <Skeleton className="h-14 w-full" />
            <Skeleton className="h-14 w-full" />
            <Skeleton className="h-14 w-full" />
            <Skeleton className="h-14 w-full" />
          </>
        ) : (
          ROWS.map((row) => (
            <div
              key={row.key}
              className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white p-3"
            >
              <div className="min-w-0">
                <div className="text-sm font-semibold text-deep-navy">{row.label}</div>
                <div className="text-xs text-slate">{row.desc}</div>
              </div>
              <label className="inline-flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={local[row.key]}
                  onChange={() => toggle(row.key)}
                  className="h-5 w-5 rounded border-input text-ledger-blue focus:ring-ledger-blue"
                />
                <span className="text-xs text-slate">
                  {local[row.key] ? 'Enabled' : 'Disabled'}
                </span>
              </label>
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}
