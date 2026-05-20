import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

export interface EntityMembership {
  entity_code: string;
  entity_name: string;
  clerk_org_id: string;
  role: 'viewer' | 'bookkeeper' | 'approver' | 'admin';
}

interface EntityState {
  activeEntityCode: string | null;
  activeEntityName: string | null;
  memberships: EntityMembership[];

  setMemberships: (memberships: EntityMembership[]) => void;
  switchEntity: (entityCode: string) => void;
  clear: () => void;
}

/**
 * Active entity is derived from Clerk's session — but we cache it here so
 * every API call can read it synchronously through the Axios interceptor.
 * On entity switch we MUST also call Clerk's setActive({organization: ...})
 * upstream so the next session token carries the new org_id; otherwise the
 * backend will 403 on the org-match check.
 */
export const useEntityStore = create<EntityState>()(
  persist(
    (set, get) => ({
      activeEntityCode: null,
      activeEntityName: null,
      memberships: [],

      setMemberships: (memberships) => {
        const current = get().activeEntityCode;
        const stillValid = memberships.find((m) => m.entity_code === current);
        const fallback = memberships[0];
        const next = stillValid ?? fallback ?? null;
        set({
          memberships,
          activeEntityCode: next?.entity_code ?? null,
          activeEntityName: next?.entity_name ?? null,
        });
      },

      switchEntity: (entityCode) => {
        const found = get().memberships.find(
          (m) => m.entity_code === entityCode,
        );
        if (!found) return;
        set({
          activeEntityCode: found.entity_code,
          activeEntityName: found.entity_name,
        });
      },

      clear: () =>
        set({
          activeEntityCode: null,
          activeEntityName: null,
          memberships: [],
        }),
    }),
    {
      name: 'bookwize.entity',
      storage: createJSONStorage(() => {
        if (typeof window === 'undefined') {
          // No-op storage for SSR.
          return {
            getItem: () => null,
            setItem: () => undefined,
            removeItem: () => undefined,
          };
        }
        return window.localStorage;
      }),
      // Only persist the choice — memberships are re-fetched on mount so
      // a removed entity can't linger.
      partialize: (state) => ({
        activeEntityCode: state.activeEntityCode,
        activeEntityName: state.activeEntityName,
      }),
    },
  ),
);

export function useActiveEntityRole():
  | EntityMembership['role']
  | null {
  const code = useEntityStore((s) => s.activeEntityCode);
  const memberships = useEntityStore((s) => s.memberships);
  return memberships.find((m) => m.entity_code === code)?.role ?? null;
}
