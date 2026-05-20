'use client';

import { useOrganizationList, useOrganization } from '@clerk/nextjs';
import { Check, ChevronsUpDown, Building2 } from 'lucide-react';
import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useEntityStore } from '@/lib/store/entity';

/**
 * Entity switcher — driven by Clerk's organization list.
 *
 * Clerk org id → entity_code mapping comes from useEntityStore (hydrated by
 * ClerkTokenBridge from /api/me/entities). The visible label is the entity
 * name; the switcher action calls Clerk's setActive({organization: ...})
 * which mints a new session token carrying the new org_id — the Zustand
 * store reads the new active org via the bridge.
 *
 * After switching, we reset the React Query cache so no entity-A data
 * lingers in entity-B's context (Phase 5 rule 5).
 */
export function EntitySwitcher() {
  const [open, setOpen] = useState(false);
  const { userMemberships, setActive, isLoaded } = useOrganizationList({
    userMemberships: { infinite: true },
  });
  const { organization: currentOrg } = useOrganization();
  const memberships = useEntityStore((s) => s.memberships);
  const activeEntityCode = useEntityStore((s) => s.activeEntityCode);
  const activeEntityName = useEntityStore((s) => s.activeEntityName);
  const queryClient = useQueryClient();

  if (!isLoaded) {
    return (
      <div className="rounded-xl border border-white/15 bg-white/5 px-3 py-2 text-sm text-white/60">
        Loading entities…
      </div>
    );
  }

  const onPick = async (clerkOrgId: string) => {
    if (!setActive) return;
    await setActive({ organization: clerkOrgId });
    // Clear all cached data so the new entity's data is fetched fresh.
    queryClient.clear();
    setOpen(false);
  };

  const hasMemberships = (userMemberships.data?.length ?? 0) > 0;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          aria-label="Switch entity"
          className={cn(
            'flex w-full items-center justify-between gap-2 rounded-xl border border-white/15 bg-white/5 px-3 py-2 text-left text-sm text-white',
            'hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-bw-teal',
          )}
        >
          <div className="flex min-w-0 items-center gap-2">
            <Building2 className="h-4 w-4 text-bw-teal" strokeWidth={1.5} />
            <div className="min-w-0">
              <div className="truncate font-semibold">
                {activeEntityName ?? currentOrg?.name ?? 'Choose an entity'}
              </div>
              <div className="truncate text-xs text-white/60">
                {activeEntityCode ?? '—'}
              </div>
            </div>
          </div>
          <ChevronsUpDown className="h-4 w-4 text-white/60" strokeWidth={1.5} />
        </button>
      </PopoverTrigger>
      <PopoverContent
        className="w-72 p-1"
        align="start"
        sideOffset={4}
      >
        {!hasMemberships && (
          <div className="px-3 py-2 text-sm text-slate">
            You aren&apos;t a member of any organization yet.
          </div>
        )}
        {userMemberships.data?.map((m) => {
          const orgId = m.organization.id;
          const matchedMembership = memberships.find(
            (em) => em.clerk_org_id === orgId,
          );
          const isActive = currentOrg?.id === orgId;
          return (
            <button
              key={orgId}
              onClick={() => onPick(orgId)}
              className={cn(
                'flex w-full items-center justify-between gap-2 rounded-md px-3 py-2 text-sm text-ink',
                'hover:bg-cloud focus:outline-none focus:bg-cloud',
              )}
            >
              <div className="min-w-0 text-left">
                <div className="truncate font-semibold text-deep-navy">
                  {matchedMembership?.entity_name ?? m.organization.name}
                </div>
                {matchedMembership ? (
                  <div className="truncate text-xs text-slate">
                    {matchedMembership.entity_code}
                  </div>
                ) : (
                  <div className="truncate text-xs text-amber-700">
                    Not linked to an entity yet
                  </div>
                )}
              </div>
              {isActive && (
                <Check
                  className="h-4 w-4 text-bw-teal shrink-0"
                  strokeWidth={1.5}
                />
              )}
            </button>
          );
        })}
      </PopoverContent>
    </Popover>
  );
}
