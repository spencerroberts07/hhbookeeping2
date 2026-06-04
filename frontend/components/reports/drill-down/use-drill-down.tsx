'use client';

import * as React from 'react';
import type { AccountActivityMode } from '@/lib/api/reports';

/**
 * Drill-down navigation state. The panel is a stack of levels; the
 * breadcrumb is the stack, and "back" pops it. Hoisted in the reports
 * layout so any report page can open the panel without prop-drilling.
 */
export type DrillLevel =
  | {
      kind: 'account';
      account_code: string;
      account_name: string;
      mode: AccountActivityMode;
      period_start: string | null;
      period_end: string;
      /** The clicked report figure, for the reconcile chip. */
      line_amount: number;
    }
  | { kind: 'entry'; journal_batch_id: string }
  | { kind: 'document'; journal_batch_id: string; journal_line_id?: string }
  | {
      // HH AP invoice root — resolves to a journal entry if linked, else
      // the source document. Breadcrumb root reads "Invoice …".
      kind: 'invoice';
      hh_ap_invoice_id: string;
      title: string;
    };

interface DrillDownContextValue {
  open: boolean;
  stack: DrillLevel[];
  /** Open the panel at a fresh root level (replaces any existing stack). */
  openAt: (level: DrillLevel) => void;
  /** Push a deeper level onto the stack. */
  push: (level: DrillLevel) => void;
  /** Pop back to a given depth (breadcrumb click). */
  popTo: (index: number) => void;
  close: () => void;
}

const DrillDownContext = React.createContext<DrillDownContextValue | null>(null);

export function DrillDownProvider({ children }: { children: React.ReactNode }) {
  const [stack, setStack] = React.useState<DrillLevel[]>([]);
  const [open, setOpen] = React.useState(false);

  const openAt = React.useCallback((level: DrillLevel) => {
    setStack([level]);
    setOpen(true);
  }, []);

  const push = React.useCallback((level: DrillLevel) => {
    setStack((s) => [...s, level]);
  }, []);

  const popTo = React.useCallback((index: number) => {
    setStack((s) => s.slice(0, index + 1));
  }, []);

  const close = React.useCallback(() => setOpen(false), []);

  const value = React.useMemo(
    () => ({ open, stack, openAt, push, popTo, close }),
    [open, stack, openAt, push, popTo, close],
  );

  return (
    <DrillDownContext.Provider value={value}>
      {children}
    </DrillDownContext.Provider>
  );
}

export function useDrillDown(): DrillDownContextValue {
  const ctx = React.useContext(DrillDownContext);
  if (!ctx) {
    throw new Error('useDrillDown must be used within a DrillDownProvider');
  }
  return ctx;
}
