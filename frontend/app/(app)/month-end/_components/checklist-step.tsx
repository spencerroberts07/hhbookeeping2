'use client';

import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

interface Props {
  number: number;
  title: string;
  description: string;
  status?: 'complete' | 'in_progress' | 'blocked' | 'not_started';
  children: React.ReactNode;
}

const STATUS_LABEL: Record<NonNullable<Props['status']>, string> = {
  complete: 'Complete',
  in_progress: 'In progress',
  blocked: 'Blocked',
  not_started: 'Not started',
};

const STATUS_VARIANT: Record<
  NonNullable<Props['status']>,
  'complete' | 'warning' | 'locked' | 'info'
> = {
  complete: 'complete',
  in_progress: 'warning',
  blocked: 'locked',
  not_started: 'info',
};

const STATUS_GLYPH: Record<NonNullable<Props['status']>, string> = {
  complete: '✅',
  in_progress: '⚠️',
  blocked: '🔒',
  not_started: '⬜',
};

export function ChecklistStep({
  number,
  title,
  description,
  status = 'not_started',
  children,
}: Props) {
  const [open, setOpen] = useState(true);

  return (
    <Card>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center justify-between gap-4 p-5 text-left"
        aria-expanded={open}
      >
        <div className="flex items-center gap-4 min-w-0">
          <span
            className={cn(
              'grid h-9 w-9 place-items-center rounded-full text-sm font-bold shrink-0',
              status === 'complete'
                ? 'bg-bw-teal text-white'
                : 'bg-cloud text-deep-navy',
            )}
            aria-hidden
          >
            {number}
          </span>
          <div className="min-w-0">
            <div className="font-semibold text-deep-navy flex items-center gap-2">
              <span>{title}</span>
              <span className="text-sm" aria-hidden>
                {STATUS_GLYPH[status]}
              </span>
            </div>
            <div className="text-sm text-slate truncate">{description}</div>
          </div>
        </div>
        <div className="flex items-center gap-3 shrink-0">
          <Badge variant={STATUS_VARIANT[status]}>{STATUS_LABEL[status]}</Badge>
          {open ? (
            <ChevronDown className="h-5 w-5 text-slate" strokeWidth={1.5} />
          ) : (
            <ChevronRight className="h-5 w-5 text-slate" strokeWidth={1.5} />
          )}
        </div>
      </button>
      {open && (
        <div className="border-t border-border px-5 py-4">{children}</div>
      )}
    </Card>
  );
}
