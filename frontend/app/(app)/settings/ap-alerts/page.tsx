'use client';

import { useEffect, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';
import { useEntityStore } from '@/lib/store/entity';
import { api } from '@/lib/api/client';
import { toast } from 'sonner';

interface ApAlertSettings {
  email_enabled: boolean;
  remittance_advice_enabled: boolean;
  thresholds: number[];
}

const DEFAULT_SETTINGS: ApAlertSettings = {
  email_enabled: true,
  remittance_advice_enabled: true,
  thresholds: [7, 3],
};

async function getApAlertSettings(entityCode: string): Promise<ApAlertSettings> {
  const res = await api.get<{ notification_preferences: Record<string, unknown> }>(
    `/api/entities/${entityCode}/notifications`,
  );
  const ap = res.data.notification_preferences?.ap_alerts as Partial<ApAlertSettings> | undefined;
  return { ...DEFAULT_SETTINGS, ...(ap ?? {}) };
}

async function updateApAlertSettings(
  entityCode: string,
  settings: ApAlertSettings,
): Promise<void> {
  await api.patch(`/api/entities/${entityCode}/notifications`, {
    ap_alerts: settings,
  });
}

export default function ApAlertsSettingsPage() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const qc = useQueryClient();

  const q = useQuery({
    queryKey: ['ap-alert-settings', entityCode],
    enabled: !!entityCode,
    queryFn: () => getApAlertSettings(entityCode!),
  });

  const [local, setLocal] = useState<ApAlertSettings>(DEFAULT_SETTINGS);
  useEffect(() => {
    if (q.data) setLocal(q.data);
  }, [q.data]);

  const mutation = useMutation({
    mutationFn: (s: ApAlertSettings) => updateApAlertSettings(entityCode!, s),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['ap-alert-settings', entityCode] });
      toast.success('AP alert preferences saved');
    },
    onError: () => toast.error('Could not save preferences'),
  });

  const handleToggle = (key: 'email_enabled' | 'remittance_advice_enabled') => {
    const next = { ...local, [key]: !local[key] };
    setLocal(next);
    mutation.mutate(next);
  };

  const handleThresholdToggle = (days: number) => {
    const next = {
      ...local,
      thresholds: local.thresholds.includes(days)
        ? local.thresholds.filter((t) => t !== days)
        : [...local.thresholds, days].sort((a, b) => b - a),
    };
    setLocal(next);
    mutation.mutate(next);
  };

  if (!entityCode) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle>AP due-date alerts</CardTitle>
        <p className="text-sm text-slate">
          Controls email reminders and in-app alerts for outside-vendor invoices
          approaching or past their due date. HH AP invoices are not included.
        </p>
      </CardHeader>
      <CardContent className="space-y-5">
        {q.isLoading ? (
          <>
            <Skeleton className="h-14 w-full" />
            <Skeleton className="h-14 w-full" />
          </>
        ) : (
          <>
            {/* Alert thresholds */}
            <div className="space-y-2">
              <p className="text-sm font-semibold text-ink">Alert thresholds</p>
              <p className="text-xs text-slate">Fire alerts this many days before the due date.</p>
              <div className="flex gap-3">
                {[7, 3, 1].map((days) => {
                  const active = local.thresholds.includes(days);
                  return (
                    <button
                      key={days}
                      onClick={() => handleThresholdToggle(days)}
                      className={`rounded-lg border px-4 py-2 text-sm font-semibold transition-colors ${
                        active
                          ? 'bg-deep-navy text-white border-deep-navy'
                          : 'bg-white text-ink border-border hover:bg-cloud'
                      }`}
                    >
                      {days}-day
                    </button>
                  );
                })}
              </div>
              <p className="text-xs text-slate">
                Overdue invoices always trigger an alert regardless of threshold settings.
              </p>
            </div>

            <hr />

            {/* Toggle rows */}
            <ToggleRow
              label="Email notifications"
              description="Send email reminders when invoices are due (requires email service configuration)"
              checked={local.email_enabled}
              onChange={() => handleToggle('email_enabled')}
              saving={mutation.isPending}
            />

            <ToggleRow
              label="Remittance advice emails"
              description="When an EFT payment file is generated, automatically send a remittance advice email to each vendor with a known email address"
              checked={local.remittance_advice_enabled}
              onChange={() => handleToggle('remittance_advice_enabled')}
              saving={mutation.isPending}
            />

            <hr />

            <div className="rounded-lg bg-cloud p-3 text-xs text-slate space-y-1">
              <p><strong>How AP alerts work:</strong></p>
              <p>The daily alert cron runs at 08:00 UTC and fires in-app alerts for each open invoice whose due date matches a configured threshold (e.g. exactly 7 days away). Each alert fires at most once per invoice per threshold — no duplicate reminders.</p>
              <p>Alerts appear in the Dashboard alert feed and on the <a href="/ap/payments" className="underline text-deep-navy">Vendor Payments</a> page.</p>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function ToggleRow({
  label,
  description,
  checked,
  onChange,
  saving,
}: {
  label: string;
  description: string;
  checked: boolean;
  onChange: () => void;
  saving: boolean;
}) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <p className="text-sm font-medium text-ink">{label}</p>
        <p className="text-xs text-slate">{description}</p>
      </div>
      <button
        role="switch"
        aria-checked={checked}
        onClick={onChange}
        disabled={saving}
        className={`relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 focus:outline-none ${
          checked ? 'bg-deep-navy' : 'bg-gray-200'
        }`}
      >
        <span
          className={`inline-block h-5 w-5 transform rounded-full bg-white shadow transition duration-200 ${
            checked ? 'translate-x-5' : 'translate-x-0'
          }`}
        />
      </button>
    </div>
  );
}
