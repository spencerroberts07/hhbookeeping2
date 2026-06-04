'use client';

import { DrillDownProvider } from '@/components/reports/drill-down/use-drill-down';
import { DrillDownPanel } from '@/components/reports/drill-down/drill-down-panel';

/**
 * Wraps every report page in the drill-down provider and mounts the
 * slide-over panel once. Report rows call `useDrillDown().openAt(...)`.
 */
export default function ReportsLayout({ children }: { children: React.ReactNode }) {
  return (
    <DrillDownProvider>
      {children}
      <DrillDownPanel />
    </DrillDownProvider>
  );
}
