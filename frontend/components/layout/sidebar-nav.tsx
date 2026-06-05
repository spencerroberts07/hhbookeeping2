'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useQuery } from '@tanstack/react-query';
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
  ListChecks,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useHasRole, useIsBookwizeAdmin, useIsProfessional } from '@/lib/store/user';
import { useEntityStore } from '@/lib/store/entity';
import {
  getOnboardingStatus,
  type OnboardingStatus,
} from '@/lib/api/onboarding';

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
  { label: 'Year-end', href: '/year-end', icon: ListChecks, minRole: 'approver' },
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

// Setup wizard items the dealer needs to complete. Used to compute the
// "{N} remaining" badge on the Setup sidebar entry. These map to the
// flag fields on /api/onboarding/status — keep the keys aligned with
// the OnboardingStatus type.
const SETUP_TASKS: Array<{ key: keyof OnboardingStatus; label: string }> = [
  { key: 'has_chart_of_accounts', label: 'Chart of accounts' },
  { key: 'has_opening_balances', label: 'Opening balances' },
  { key: 'has_gl_history', label: 'GL history' },
  { key: 'has_hh_ap_history', label: 'HH AP statements' },
];

function remainingSetupSteps(status: OnboardingStatus | undefined): number {
  if (!status) return 0;
  return SETUP_TASKS.reduce(
    (n, t) => n + (status[t.key] ? 0 : 1),
    0,
  );
}

export function SidebarNav() {
  const pathname = usePathname();
  const isProfessional = useIsProfessional();
  const isBookwizeAdmin = useIsBookwizeAdmin();
  const isBookkeeperOrAbove = useHasRole('bookkeeper');
  const entityCode = useEntityStore((s) => s.activeEntityCode);

  // Onboarding-status query for the Setup nav item. Stale-time keeps
  // this from refetching on every sidebar render — onboarding state
  // changes are rare, and the wizard itself invalidates this query
  // whenever it writes.
  const onboarding = useQuery({
    queryKey: ['onboarding-status', entityCode],
    enabled: !!entityCode,
    queryFn: () => getOnboardingStatus(entityCode!),
    staleTime: 60_000,
  });

  const showSetup =
    !!entityCode &&
    onboarding.data !== undefined &&
    !onboarding.data.onboarding_complete;
  const setupRemaining = showSetup ? remainingSetupSteps(onboarding.data) : 0;

  const visible = ITEMS.filter((item) => {
    if (item.bookwizeAdminOnly && !isBookwizeAdmin) return false;
    if (item.professionalOnly && !isProfessional) return false;
    if (item.href === '/settings' && !isBookkeeperOrAbove) return false;
    return true;
  });

  const setupActive =
    pathname === '/onboarding' || pathname.startsWith('/onboarding/');

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

      {/* Setup wizard — only shown while onboarding is incomplete.
          Hidden once the entity's onboarding_complete flag flips true,
          so it doesn't clutter the nav for live dealers. */}
      {showSetup && (
        <Link
          href="/onboarding"
          className={cn(
            'group flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
            setupActive
              ? 'bg-white/10 text-white border-l-2 border-ledger-blue'
              : 'text-white/70 hover:bg-white/5 hover:text-white',
          )}
        >
          <ListChecks className="h-5 w-5 shrink-0" strokeWidth={1.5} />
          <span className="truncate flex-1">Setup</span>
          {setupRemaining > 0 && (
            <span className="ml-auto rounded-full bg-bw-teal/20 text-bw-teal px-2 py-0.5 text-[10px] font-semibold leading-none">
              {setupRemaining} left
            </span>
          )}
        </Link>
      )}
    </nav>
  );
}
