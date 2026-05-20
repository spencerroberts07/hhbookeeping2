'use client';

import { Bell, HelpCircle } from 'lucide-react';
import { MobileSidebar } from './mobile-sidebar';

interface TopbarProps {
  title: string;
  periodLabel?: string;
}

export function Topbar({ title, periodLabel }: TopbarProps) {
  return (
    <header className="app-topbar sticky top-0 z-30 flex h-14 items-center justify-between gap-4 border-b border-border bg-white px-4 md:px-6 no-print">
      <div className="flex min-w-0 items-center gap-3">
        <MobileSidebar />
        <div className="min-w-0">
          <h1 className="truncate text-base font-semibold text-deep-navy">
            {title}
          </h1>
          {periodLabel && (
            <div className="truncate text-xs text-slate">{periodLabel}</div>
          )}
        </div>
      </div>
      <div className="flex items-center gap-1">
        <button
          aria-label="Notifications"
          className="grid h-9 w-9 place-items-center rounded-md text-slate hover:bg-cloud hover:text-deep-navy"
        >
          <Bell className="h-5 w-5" strokeWidth={1.5} />
        </button>
        <a
          href="https://help.bookwize.ca"
          target="_blank"
          rel="noreferrer"
          aria-label="Help"
          className="grid h-9 w-9 place-items-center rounded-md text-slate hover:bg-cloud hover:text-deep-navy"
        >
          <HelpCircle className="h-5 w-5" strokeWidth={1.5} />
        </a>
      </div>
    </header>
  );
}
