'use client';

import { useClerk, useUser } from '@clerk/nextjs';
import { LogOut } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { useUserStore, useIsProfessional } from '@/lib/store/user';

/**
 * Bottom of the sidebar — current user identity, plan tier badge, sign-out.
 */
export function UserProfile() {
  const { user } = useUser();
  const { signOut } = useClerk();
  const role = useUserStore((s) => s.role);
  const isProfessional = useIsProfessional();

  if (!user) return null;

  const initials = (
    user.fullName ||
    user.primaryEmailAddress?.emailAddress ||
    '?'
  )
    .split(/[\s@.]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((s) => s[0]?.toUpperCase() ?? '')
    .join('');

  return (
    <div className="border-t border-white/10 px-3 py-3">
      <div className="flex items-center gap-3">
        <div className="grid h-9 w-9 place-items-center rounded-full bg-bw-teal/20 text-bw-teal font-semibold">
          {initials || '?'}
        </div>
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-semibold text-white">
            {user.fullName || user.primaryEmailAddress?.emailAddress}
          </div>
          <div className="flex items-center gap-1.5 text-xs text-white/60">
            <span className="truncate capitalize">{role ?? '—'}</span>
            <span>·</span>
            <Badge
              variant={isProfessional ? 'complete' : 'secondary'}
              className="text-[10px] uppercase tracking-wide"
            >
              {isProfessional ? 'Pro' : 'Starter'}
            </Badge>
          </div>
        </div>
        <button
          onClick={() => signOut({ redirectUrl: '/' })}
          aria-label="Sign out"
          className="rounded-md p-2 text-white/60 hover:bg-white/10 hover:text-white focus:outline-none focus:ring-2 focus:ring-bw-teal"
        >
          <LogOut className="h-4 w-4" strokeWidth={1.5} />
        </button>
      </div>
    </div>
  );
}
