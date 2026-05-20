import { create } from 'zustand';

export type AppRole = 'viewer' | 'bookkeeper' | 'approver' | 'admin';
export type PlanTier = 'starter' | 'professional' | null;

interface UserState {
  clerkUserId: string | null;
  email: string | null;
  fullName: string | null;
  role: AppRole | null;
  planTier: PlanTier;
  isBookwizeAdmin: boolean;

  setUser: (input: {
    clerkUserId: string;
    email: string;
    fullName: string | null;
    role: AppRole | null;
    isBookwizeAdmin: boolean;
  }) => void;
  setPlanTier: (planTier: PlanTier) => void;
  setRole: (role: AppRole | null) => void;
  clear: () => void;
}

const ROLE_RANK: Record<AppRole, number> = {
  viewer: 10,
  bookkeeper: 20,
  approver: 30,
  admin: 40,
};

export const useUserStore = create<UserState>()((set) => ({
  clerkUserId: null,
  email: null,
  fullName: null,
  role: null,
  planTier: null,
  isBookwizeAdmin: false,

  setUser: ({ clerkUserId, email, fullName, role, isBookwizeAdmin }) =>
    set({ clerkUserId, email, fullName, role, isBookwizeAdmin }),

  setPlanTier: (planTier) => set({ planTier }),
  setRole: (role) => set({ role }),

  clear: () =>
    set({
      clerkUserId: null,
      email: null,
      fullName: null,
      role: null,
      planTier: null,
      isBookwizeAdmin: false,
    }),
}));

/** Convenience selectors. Always use these instead of comparing role strings inline. */
export function useHasRole(min: AppRole): boolean {
  const role = useUserStore((s) => s.role);
  if (!role) return false;
  return ROLE_RANK[role] >= ROLE_RANK[min];
}

export function useIsProfessional(): boolean {
  return useUserStore((s) => s.planTier) === 'professional';
}

export function useIsAdmin(): boolean {
  return useHasRole('admin');
}

export function useIsApprover(): boolean {
  return useHasRole('approver');
}

export function useIsBookkeeper(): boolean {
  return useHasRole('bookkeeper');
}

export function useIsBookwizeAdmin(): boolean {
  return useUserStore((s) => s.isBookwizeAdmin);
}
