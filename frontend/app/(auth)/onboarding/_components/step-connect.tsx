'use client';

import { useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { useQuery } from '@tanstack/react-query';
import { useEntityStore } from '@/lib/store/entity';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { getOnboardingStatus, startQboConnect } from '@/lib/api/onboarding';
import {
  AlertTriangle,
  CheckCircle2,
  Database,
  Upload,
} from 'lucide-react';
import { toast } from 'sonner';

// localStorage flag we set right before the OAuth redirect leaves the
// tab, and check on return. Used to detect "user came back without
// the ?qbo=connected query param" — meaning the OAuth flow didn't
// complete successfully.
const OAUTH_PENDING_KEY = 'bookwize.qbo_oauth_pending_until';
// Window during which "missing ?qbo=connected" is treated as a failed
// or cancelled OAuth attempt. Matches the spec's 60s timeout.
const OAUTH_FOLLOWUP_WINDOW_MS = 60_000;

export function StepConnect() {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const setField = useOnboardingStore((s) => s.setField);
  const goTo = useOnboardingStore((s) => s.goTo);
  const prev = useOnboardingStore((s) => s.prev);
  const searchParams = useSearchParams();

  const status = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getOnboardingStatus(entityCode!),
  });

  const [busy, setBusy] = useState(false);
  const [oauthFailed, setOauthFailed] = useState(false);

  // Detect a failed or cancelled OAuth attempt. If we previously set
  // OAUTH_PENDING_KEY before redirecting to Intuit but the user is now
  // back on this step without ?qbo=connected and without an active
  // QBO connection, the flow didn't complete.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const raw = window.localStorage.getItem(OAUTH_PENDING_KEY);
    if (!raw) return;
    const pendingUntil = Number(raw);
    if (!pendingUntil || Date.now() > pendingUntil) {
      window.localStorage.removeItem(OAUTH_PENDING_KEY);
      return;
    }
    const qboParam = searchParams.get('qbo');
    if (qboParam === 'connected') {
      window.localStorage.removeItem(OAUTH_PENDING_KEY);
      return;
    }
    if (qboParam === 'failed') {
      window.localStorage.removeItem(OAUTH_PENDING_KEY);
      setOauthFailed(true);
      return;
    }
    // We came back to this page within the 60s window but with no
    // success/failure marker — treat as cancelled / errored.
    if (status.data && !status.data.has_qbo_connection) {
      window.localStorage.removeItem(OAUTH_PENDING_KEY);
      setOauthFailed(true);
    }
  }, [searchParams, status.data]);

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
    setOauthFailed(false);
    try {
      // Surface a slow /connect call instead of letting the spinner sit
      // forever. 15s is generous for a token-mint round-trip.
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 15_000);
      try {
        const res = await startQboConnect(entityCode);
        clearTimeout(timer);
        if (typeof window !== 'undefined') {
          window.localStorage.setItem(
            OAUTH_PENDING_KEY,
            String(Date.now() + OAUTH_FOLLOWUP_WINDOW_MS),
          );
        }
        setField('connect_path', 'qbo');
        window.location.href = res.authorization_url;
      } finally {
        clearTimeout(timer);
      }
    } catch (err) {
      const msg =
        err instanceof DOMException && err.name === 'AbortError'
          ? "QuickBooks didn't respond in time. Try again, or upload files instead."
          : 'Could not start QuickBooks connection';
      toast.error(msg);
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

      {oauthFailed && (
        <div className="rounded-xl border-2 border-amber-300 bg-amber-50 p-4 flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-700 shrink-0 mt-0.5" />
          <div className="flex-1">
            <div className="font-semibold text-amber-900">
              QuickBooks connection didn't complete
            </div>
            <div className="text-sm text-amber-900/80 mt-1">
              It looks like authorization was cancelled or timed out. Try
              again, or load your data from files instead.
            </div>
          </div>
        </div>
      )}

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
            {busy
              ? 'Redirecting…'
              : oauthFailed
                ? 'Try connecting again'
                : 'Connect QuickBooks'}
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
            {oauthFailed ? 'Skip — upload files instead' : 'Upload files instead'}
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
