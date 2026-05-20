'use client';

import { useAuth, useOrganizationList, useUser } from '@clerk/nextjs';
import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { setTokenResolver } from '@/lib/api/client';
import { useEntityStore } from '@/lib/store/entity';
import { useUserStore } from '@/lib/store/user';
import { getMyEntities } from '@/lib/api/me';

/**
 * Wires the Clerk session into the Axios client and Zustand stores. Mount
 * this once inside the authenticated (app) layout so every protected page
 * benefits.
 *
 * What this does:
 *   - Registers a token resolver so the Axios interceptor can attach a
 *     fresh Clerk session JWT on every request.
 *   - Fetches the caller's entity memberships from the backend
 *     (GET /api/me/entities) and hydrates useEntityStore.
 *   - Mirrors the Clerk user identity into useUserStore so role/plan
 *     selectors work from any component without a Clerk hook.
 *   - When the user switches active org in Clerk, that flows through here
 *     and refreshes memberships.
 */
export function ClerkTokenBridge({ children }: { children: React.ReactNode }) {
  const { isLoaded: authLoaded, isSignedIn, getToken, orgId, orgRole } =
    useAuth();
  const { user } = useUser();
  const { isLoaded: orgListLoaded } = useOrganizationList({
    userMemberships: { infinite: true },
  });

  useEffect(() => {
    setTokenResolver(async () => {
      if (!authLoaded || !isSignedIn) return null;
      return getToken();
    });
  }, [authLoaded, isSignedIn, getToken]);

  useEffect(() => {
    if (!user) {
      useUserStore.getState().clear();
      return;
    }
    const role = mapClerkRoleToAppRole(orgRole ?? null);
    useUserStore.getState().setUser({
      clerkUserId: user.id,
      email: user.primaryEmailAddress?.emailAddress ?? '',
      fullName: user.fullName ?? null,
      role,
      isBookwizeAdmin: Boolean(
        (user.publicMetadata as { is_bookwize_admin?: boolean })
          ?.is_bookwize_admin,
      ),
    });
  }, [user, orgRole]);

  // Fetch /api/me/entities whenever the signed-in user or active org changes.
  useQuery({
    queryKey: ['me', 'entities', user?.id, orgId],
    enabled: Boolean(authLoaded && isSignedIn && orgListLoaded),
    queryFn: async () => {
      const memberships = await getMyEntities();
      useEntityStore.getState().setMemberships(memberships);
      return memberships;
    },
  });

  return <>{children}</>;
}

function mapClerkRoleToAppRole(
  clerkRole: string | null,
): 'viewer' | 'bookkeeper' | 'approver' | 'admin' | null {
  if (!clerkRole) return null;
  // Clerk's session-token `org_role` claim ships the short form ('admin')
  // by default; only a customized session-token template emits the full
  // 'org:admin' key. Strip any 'org:' prefix before matching.
  const normalized = clerkRole.startsWith('org:')
    ? clerkRole.slice(4)
    : clerkRole;
  switch (normalized) {
    case 'viewer':
      return 'viewer';
    case 'bookkeeper':
      return 'bookkeeper';
    case 'approver':
      return 'approver';
    case 'admin':
    case 'owner':
      return 'admin';
    default:
      return null;
  }
}
