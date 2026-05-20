'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  LayoutDashboard,
  CalendarCheck2,
  FileBarChart,
  Receipt,
  Files,
  Users,
  Banknote,
  CircleDollarSign,
  Settings,
  ShieldCheck,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useHasRole, useIsBookwizeAdmin, useIsProfessional } from '@/lib/store/user';

interface NavItem {
  label: string;
  href: string;
  icon: LucideIcon;
  /** If set, only show when the user is at least this role. */
  minRole?: 'viewer' | 'bookkeeper' | 'approver' | 'admin';
  /** If true, the link is gated behind the Professional plan tier. */
  professionalOnly?: boolean;
  /** If true, only visible to BookWize internal staff. */
  bookwizeAdminOnly?: boolean;
}

const ITEMS: NavItem[] = [
  { label: 'Dashboard', href: '/dashboard', icon: LayoutDashboard },
  { label: 'Month-end', href: '/month-end', icon: CalendarCheck2 },
  { label: 'Reports', href: '/reports', icon: FileBarChart },
  { label: 'Transactions', href: '/transactions', icon: Receipt },
  { label: 'Documents', href: '/documents', icon: Files },
  {
    label: 'Payroll',
    href: '/payroll',
    icon: Users,
    professionalOnly: true,
  },
  { label: 'AP', href: '/ap', icon: CircleDollarSign },
  { label: 'Bank', href: '/bank', icon: Banknote },
  { label: 'Settings', href: '/settings', icon: Settings },
  {
    label: 'BookWize Admin',
    href: '/admin',
    icon: ShieldCheck,
    bookwizeAdminOnly: true,
  },
];

export function SidebarNav() {
  const pathname = usePathname();
  const isProfessional = useIsProfessional();
  const isBookwizeAdmin = useIsBookwizeAdmin();
  const isBookkeeperOrAbove = useHasRole('bookkeeper');

  const visible = ITEMS.filter((item) => {
    if (item.bookwizeAdminOnly && !isBookwizeAdmin) return false;
    if (item.professionalOnly && !isProfessional) return false;
    // Settings is admin-only; the page itself further role-gates its tabs.
    if (item.href === '/settings' && !isBookkeeperOrAbove) return false;
    return true;
  });

  return (
    <nav aria-label="Primary" className="flex-1 px-2 py-3 space-y-1">
      {visible.map((item) => {
        const Icon = item.icon;
        const active =
          pathname === item.href || pathname.startsWith(`${item.href}/`);
        return (
          <Link
            key={item.href}
            href={item.href}
            className={cn(
              'group flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
              active
                ? 'bg-white/10 text-white border-l-2 border-ledger-blue'
                : 'text-white/70 hover:bg-white/5 hover:text-white',
            )}
          >
            <Icon className="h-5 w-5 shrink-0" strokeWidth={1.5} />
            <span className="truncate">{item.label}</span>
          </Link>
        );
      })}
    </nav>
  );
}
