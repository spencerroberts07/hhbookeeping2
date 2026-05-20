'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';

// TODO: backend endpoint not built — notification preferences storage.
// Persist these via /api/me/notifications when available.
const NOTIFICATIONS = [
  { key: 'month_end', label: 'Month-end reminders', desc: 'Around the 1st of each month' },
  { key: 'variance_alerts', label: 'Variance alerts', desc: 'When trial balance variance exceeds threshold' },
  { key: 'approval_requests', label: 'Approval requests', desc: 'When a batch needs your sign-off' },
  { key: 'payment_receipts', label: 'Payment receipts', desc: 'Stripe billing receipts and invoice updates' },
];

export default function NotificationsPage() {
  const [prefs, setPrefs] = useState<Record<string, boolean>>(
    Object.fromEntries(NOTIFICATIONS.map((n) => [n.key, true])),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Notification preferences</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {NOTIFICATIONS.map((n) => (
          <div
            key={n.key}
            className="flex items-center justify-between gap-4 rounded-lg border border-border bg-white p-3"
          >
            <div className="min-w-0">
              <div className="text-sm font-semibold text-deep-navy">{n.label}</div>
              <div className="text-xs text-slate">{n.desc}</div>
            </div>
            <label className="inline-flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={prefs[n.key]}
                onChange={(e) => setPrefs({ ...prefs, [n.key]: e.target.checked })}
                className="h-5 w-5 rounded border-input text-ledger-blue focus:ring-ledger-blue"
              />
              <span className="text-xs text-slate">
                {prefs[n.key] ? 'Enabled' : 'Disabled'}
              </span>
            </label>
          </div>
        ))}
        <p className="text-xs text-slate">
          Preferences are stored locally for now and will sync once the backend
          notification preferences endpoint lands.
        </p>
      </CardContent>
    </Card>
  );
}
