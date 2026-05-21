'use client';

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { getOnboardingStatus, startQboConnect } from '@/lib/api/onboarding';
import { Database, Upload, CheckCircle2 } from 'lucide-react';
import { toast } from 'sonner';

export function StepConnect() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const setField = useOnboardingStore((s) => s.setField);
  const goTo = useOnboardingStore((s) => s.goTo);
  const prev = useOnboardingStore((s) => s.prev);

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getOnboardingStatus(entityCode!),
  });

  const [busy, setBusy] = useState(false);

  if (status.data?.has_qbo_connection) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className="text-h2 text-deep-navy">QuickBooks connected</h2>
          <p className="text-slate mt-1">
            You're already linked to QuickBooks. We'll use it for the rest of
            setup.
          </p>
        </div>
        <div className="rounded-xl border border-bw-teal/30 bg-bw-teal/5 p-5 space-y-2">
          <div className="flex items-center gap-2">
            <CheckCircle2 className="h-5 w-5 text-bw-teal" />
            <span className="font-semibold text-deep-navy">Connected</span>
            <Badge variant="complete">QBO</Badge>
          </div>
          <div className="text-xs text-slate ml-7">
            Realm ID: <span className="font-mono">{status.data.qbo_realm_id}</span>
          </div>
          {status.data.qbo_connected_at && (
            <div className="text-xs text-slate ml-7">
              Connected {new Date(status.data.qbo_connected_at).toLocaleString()}
            </div>
          )}
        </div>
        <div className="flex justify-between pt-2">
          <Button variant="ghost" onClick={prev}>
            ← Back
          </Button>
          <Button
            variant="accent"
            size="lg"
            onClick={() => {
              setField('connect_path', 'qbo');
              goTo('chart');
            }}
          >
            Continue →
          </Button>
        </div>
      </div>
    );
  }

  const connectQbo = async () => {
    if (!entityCode) return;
    setBusy(true);
    try {
      const res = await startQboConnect(entityCode);
      setField('connect_path', 'qbo');
      // Redirect to Intuit's consent page in the same tab. Intuit's
      // callback will land back on the backend's /api/auth/quickbooks/
      // callback, which redirects to the frontend (handled elsewhere).
      window.location.href = res.authorization_url;
    } catch (err) {
      toast.error('Could not start QuickBooks connection');
      setBusy(false);
    }
  };

  const pickFileUpload = () => {
    setField('connect_path', 'file');
    goTo('chart');
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-h2 text-deep-navy">How should we load your data?</h2>
        <p className="text-slate mt-1">
          Pick the easiest path. You can switch later.
        </p>
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        {/* QBO card */}
        <div className="rounded-xl border-2 border-border hover:border-ledger-blue transition-colors p-6 flex flex-col">
          <div className="flex items-center gap-2 mb-3">
            <Database className="h-6 w-6 text-ledger-blue" />
            <h3 className="text-lg font-bold text-deep-navy">QuickBooks Online</h3>
          </div>
          <p className="text-sm text-slate mb-4">
            We'll import your accounts, history, and opening balances
            automatically.
          </p>
          <ul className="text-xs text-slate space-y-1 mb-6 flex-1">
            <li>✓ Chart of accounts imported instantly</li>
            <li>✓ Up to 10 years of GL history</li>
            <li>✓ Opening balances pulled automatically</li>
            <li>✓ Stays in sync going forward</li>
          </ul>
          <Button onClick={connectQbo} disabled={busy} className="w-full">
            {busy ? 'Redirecting…' : 'Connect QuickBooks'}
          </Button>
        </div>

        {/* File upload card */}
        <div className="rounded-xl border-2 border-border hover:border-deep-navy transition-colors p-6 flex flex-col">
          <div className="flex items-center gap-2 mb-3">
            <Upload className="h-6 w-6 text-deep-navy" />
            <h3 className="text-lg font-bold text-deep-navy">Upload files</h3>
          </div>
          <p className="text-sm text-slate mb-4">
            We accept exports from QuickBooks, Sage, Excel, or any format.
          </p>
          <ul className="text-xs text-slate space-y-1 mb-6 flex-1">
            <li>✓ Any file format accepted</li>
            <li>✓ AI-powered parsing</li>
            <li>✓ Preview before importing</li>
            <li>✓ No software connection required</li>
          </ul>
          <Button onClick={pickFileUpload} variant="outline" className="w-full">
            Upload files instead
          </Button>
        </div>
      </div>

      <div className="flex justify-between pt-2">
        <Button variant="ghost" onClick={prev}>
          ← Back
        </Button>
      </div>
    </div>
  );
}
