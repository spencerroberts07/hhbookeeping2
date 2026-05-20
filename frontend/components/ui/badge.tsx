import * as React from 'react';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '@/lib/utils';

const badgeVariants = cva(
  'inline-flex items-center gap-1 rounded-full border border-transparent px-2.5 py-0.5 text-xs font-semibold transition-colors',
  {
    variants: {
      variant: {
        // Status colours per design system
        complete: 'bg-bw-teal text-white',
        warning: 'bg-amber-100 text-amber-800',
        error: 'bg-red-100 text-red-800',
        pending: 'bg-ledger-blue text-white',
        locked: 'bg-slate text-white',
        info: 'bg-cloud text-deep-navy border-border',
        outline: 'border-input text-ink',
        secondary: 'bg-white text-deep-navy border-border',
      },
    },
    defaultVariants: {
      variant: 'info',
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}
