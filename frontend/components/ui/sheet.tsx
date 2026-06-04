'use client';

import * as React from 'react';
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import { cn } from '@/lib/utils';

export const Sheet = DialogPrimitive.Root;
export const SheetTrigger = DialogPrimitive.Trigger;
export const SheetClose = DialogPrimitive.Close;
export const SheetPortal = DialogPrimitive.Portal;

const SheetOverlay = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay
    ref={ref}
    className={cn('fixed inset-0 z-50 bg-black/40 backdrop-blur-sm', className)}
    {...props}
  />
));
SheetOverlay.displayName = DialogPrimitive.Overlay.displayName;

const SHEET_SIZES = {
  sm: 'w-72',
  md: 'w-[480px]',
  lg: 'w-[640px]',
  xl: 'w-[820px] max-w-[95vw]',
} as const;

export const SheetContent = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Content> & {
    side?: 'left' | 'right' | 'top' | 'bottom';
    /** Panel width. Defaults to 'sm' so existing nav usages are unchanged. */
    size?: keyof typeof SHEET_SIZES;
    /** Colour scheme. 'dark' (default) keeps the navy nav drawer look;
     *  'light' is for data panels on a white background. */
    variant?: 'dark' | 'light';
  }
>(({ className, children, side = 'left', size = 'sm', variant = 'dark', ...props }, ref) => (
  <SheetPortal>
    <SheetOverlay />
    <DialogPrimitive.Content
      ref={ref}
      className={cn(
        'fixed z-50 shadow-lg',
        variant === 'dark' ? 'bg-deep-navy text-white' : 'bg-white text-ink',
        SHEET_SIZES[size],
        side === 'left' &&
          cn('inset-y-0 left-0 h-full border-r', variant === 'dark' ? 'border-white/10' : 'border-border'),
        side === 'right' &&
          cn('inset-y-0 right-0 h-full border-l', variant === 'dark' ? 'border-white/10' : 'border-border'),
        className,
      )}
      {...props}
    >
      {children}
      <DialogPrimitive.Close
        className={cn(
          'absolute right-4 top-4 rounded-md p-1 focus:outline-none focus:ring-2 focus:ring-bw-teal',
          variant === 'dark'
            ? 'text-white/70 hover:bg-white/10 hover:text-white'
            : 'text-slate hover:bg-cloud hover:text-deep-navy',
        )}
        aria-label="Close"
      >
        <X className="h-4 w-4" strokeWidth={1.5} />
      </DialogPrimitive.Close>
    </DialogPrimitive.Content>
  </SheetPortal>
));
SheetContent.displayName = DialogPrimitive.Content.displayName;

export const SheetTitle = DialogPrimitive.Title;
export const SheetDescription = DialogPrimitive.Description;
