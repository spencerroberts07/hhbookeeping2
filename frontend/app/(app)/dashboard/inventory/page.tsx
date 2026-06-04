'use client';

import Link from 'next/link';
import { ArrowLeft } from 'lucide-react';
import { Topbar } from '@/components/layout/topbar';
import { BalanceTrendCard } from '../_components/balance-trend-card';
import { AnalyticsNav } from '../_components/analytics-nav';

export default function InventoryAnalyticsPage() {
  return (
    <div>
      <Topbar title="Inventory analytics" />
      <div className="p-6 space-y-6">
        <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-ledger-blue hover:underline">
          <ArrowLeft className="h-4 w-4" /> Back to dashboard
        </Link>
        <AnalyticsNav />
        <BalanceTrendCard title="Inventory" accountCode="1120" />
        <p className="text-xs text-slate">
          Month-end inventory balance (GL account 1120), cutover-aware and reconciling to the
          balance sheet, vs the same month-end last year.
        </p>
      </div>
    </div>
  );
}
