'use client';

import Link from 'next/link';
import { Upload, Wand2, ClipboardCheck, Calendar } from 'lucide-react';

const ACTIONS = [
  {
    href: '/bank',
    icon: Upload,
    label: 'Upload statement',
    desc: 'Bank PDF, CSV, or HH AP',
  },
  {
    href: '/bank?run=true',
    icon: Wand2,
    label: 'Run auto-journal',
    desc: 'Match bank to GL',
  },
  {
    href: '/month-end?tab=review',
    icon: ClipboardCheck,
    label: 'Pending approvals',
    desc: 'Review batches',
  },
  {
    href: '/month-end',
    icon: Calendar,
    label: 'Start month-end',
    desc: 'Begin close workflow',
  },
];

export function QuickActions() {
  return (
    <ul className="space-y-2">
      {ACTIONS.map((a) => {
        const Icon = a.icon;
        return (
          <li key={a.href}>
            <Link
              href={a.href}
              className="flex items-center gap-3 rounded-lg border border-border bg-white p-3 hover:bg-cloud transition"
            >
              <div className="grid h-9 w-9 place-items-center rounded-md bg-cloud text-ledger-blue">
                <Icon className="h-5 w-5" strokeWidth={1.5} />
              </div>
              <div className="min-w-0">
                <div className="text-sm font-semibold text-deep-navy truncate">
                  {a.label}
                </div>
                <div className="text-xs text-slate truncate">{a.desc}</div>
              </div>
            </Link>
          </li>
        );
      })}
    </ul>
  );
}
