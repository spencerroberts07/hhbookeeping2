'use client';

import Link from 'next/link';
import { ArrowLeft } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { BalanceTrendCard } from '../_components/balance-trend-card';
import { AnalyticsNav } from '../_components/analytics-nav';

export default function CashAnalyticsPage() {
  return (
    <div>
      <Topbar title="Cash analytics" />
      <div className="p-6 space-y-6">
        <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-ledger-blue hover:underline">
          <ArrowLeft className="h-4 w-4" /> Back to dashboard
        </Link>
        <AnalyticsNav />
        <BalanceTrendCard title="Cash" accountCode="1020" />
        <p className="text-xs text-slate">
          Month-end cash balance (GL account 1020), cutover-aware so each point reconciles to the
          balance sheet, compared to the same month-end last year.
        </p>
      </div>
    </div>
  );
}
