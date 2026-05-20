'use client';

import Image from 'next/image';
import Link from 'next/link';
import { SidebarNav } from './sidebar-nav';
import { EntitySwitcher } from './entity-switcher';
import { UserProfile } from './user-profile';

/**
 * Persistent left sidebar (desktop). On mobile a Sheet wraps the same nav
 * content — see MobileSidebarTrigger below.
 */
export function Sidebar() {
  return (
    <aside className="hidden md:flex md:flex-col w-64 shrink-0 bg-deep-navy h-screen sticky top-0">
      <div className="px-4 py-4">
        <Link href="/dashboard" className="block" aria-label="BookWize home">
          <Image
            src="/brand/bookwize-logo-reversed.svg"
            alt="BookWize"
            width={160}
            height={40}
            priority
          />
        </Link>
      </div>
      <div className="px-3 pb-3">
        <EntitySwitcher />
      </div>
      <SidebarNav />
      <UserProfile />
    </aside>
  );
}
