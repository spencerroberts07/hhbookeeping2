'use client';

import Image from 'next/image';
import { Menu } from 'lucide-react';
import { useState } from 'react';
import {
  Sheet,
  SheetContent,
  SheetTitle,
  SheetTrigger,
} from '@/components/ui/sheet';
import { SidebarNav } from './sidebar-nav';
import { EntitySwitcher } from './entity-switcher';
import { UserProfile } from './user-profile';

export function MobileSidebar() {
  const [open, setOpen] = useState(false);
  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger
        className="md:hidden inline-flex h-9 w-9 items-center justify-center rounded-md text-ink hover:bg-cloud focus:outline-none focus:ring-2 focus:ring-ledger-blue"
        aria-label="Open menu"
      >
        <Menu className="h-5 w-5" strokeWidth={1.5} />
      </SheetTrigger>
      <SheetContent side="left" className="flex flex-col p-0">
        <SheetTitle className="sr-only">Menu</SheetTitle>
        <div className="px-4 py-4">
          <Image
            src="/brand/bookwize-logo-reversed.svg"
            alt="BookWize"
            width={160}
            height={40}
          />
        </div>
        <div className="px-3 pb-3">
          <EntitySwitcher />
        </div>
        <div onClick={() => setOpen(false)} className="flex-1 overflow-y-auto">
          <SidebarNav />
        </div>
        <UserProfile />
      </SheetContent>
    </Sheet>
  );
}
