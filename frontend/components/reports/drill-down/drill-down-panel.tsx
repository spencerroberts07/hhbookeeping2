'use client';

import * as React from 'react';
import { ChevronRight } from 'lucide-react';
import { Sheet, SheetContent, SheetTitle } from '@/components/ui/sheet';
import { cn } from '@/lib/utils';
import { useDrillDown, type DrillLevel } from './use-drill-down';
import { AccountView } from './account-view';
import { EntryView } from './entry-view';
import { DocumentView } from './document-view';

function crumbLabel(level: DrillLevel): string {
  switch (level.kind) {
    case 'account':
      return `${level.account_code} ${level.account_name}`;
    case 'entry':
      return 'Journal entry';
    case 'document':
      return 'Document';
  }
}

/**
 * Slide-over drill-down. Renders the top of the navigation stack with a
 * breadcrumb that pops back up the chain. Mounted once per reports layout;
 * report pages open it through `useDrillDown`.
 */
export function DrillDownPanel() {
  const { open, stack, popTo, close } = useDrillDown();
  const active = stack[stack.length - 1];

  return (
    <Sheet open={open} onOpenChange={(o) => !o && close()}>
      <SheetContent side="right" size="xl" variant="light" className="flex flex-col p-0">
        <div className="border-b border-border px-5 py-3 pr-12">
          <SheetTitle className="text-base font-semibold text-deep-navy">
            Drill-down
          </SheetTitle>
          {stack.length > 0 && (
            <nav className="mt-1 flex flex-wrap items-center gap-1 text-xs text-slate">
              {stack.map((level, i) => {
                const isLast = i === stack.length - 1;
                return (
                  <React.Fragment key={`${level.kind}-${i}`}>
                    {i > 0 && <ChevronRight className="h-3 w-3 shrink-0" strokeWidth={1.5} />}
                    <button
                      type="button"
                      disabled={isLast}
                      onClick={() => popTo(i)}
                      className={cn(
                        'max-w-[16rem] truncate rounded px-1',
                        isLast
                          ? 'font-medium text-deep-navy'
                          : 'text-ledger-blue hover:underline',
                      )}
                    >
                      {crumbLabel(level)}
                    </button>
                  </React.Fragment>
                );
              })}
            </nav>
          )}
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {active?.kind === 'account' && <AccountView level={active} />}
          {active?.kind === 'entry' && (
            <EntryView journalBatchId={active.journal_batch_id} />
          )}
          {active?.kind === 'document' && (
            <DocumentView
              journalBatchId={active.journal_batch_id}
              journalLineId={active.journal_line_id}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
