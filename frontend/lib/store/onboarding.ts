import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

export type OnboardingStep =
  | 'welcome'
  | 'bank'
  | 'hh-ap'
  | 'chart'
  | 'payroll'
  | 'invite'
  | 'billing'
  | 'complete';

export const STEP_ORDER: OnboardingStep[] = [
  'welcome',
  'bank',
  'hh-ap',
  'chart',
  'payroll',
  'invite',
  'billing',
  'complete',
];

export interface OnboardingState {
  currentStep: OnboardingStep;
  // Step 1
  store_number: string;
  store_name: string;
  province: string;
  fiscal_year_end_month: number;
  fiscal_year_end_day: number;
  entity_code: string | null; // populated after POST /api/entities

  // Step 2
  bank_type: string | null;
  bank_sample_uploaded: boolean;

  // Step 3
  hh_ap_sample_uploaded: boolean;

  // Step 4
  chart_choice: 'standard' | 'custom' | null;

  // Step 5
  uses_enetemployer: boolean | null;
  payroll_sample_uploaded: boolean;

  // Step 6
  invited_team: Array<{ email: string; role: string }>;

  // Step 7
  plan_tier: 'starter' | 'professional' | null;

  setField: <K extends keyof Omit<OnboardingState, 'setField' | 'goTo' | 'reset' | 'markComplete'>>(
    key: K,
    value: OnboardingState[K],
  ) => void;
  goTo: (step: OnboardingStep) => void;
  reset: () => void;
  markComplete: () => void;
}

const INITIAL: Omit<
  OnboardingState,
  'setField' | 'goTo' | 'reset' | 'markComplete'
> = {
  currentStep: 'welcome',
  store_number: '',
  store_name: '',
  province: 'ON',
  fiscal_year_end_month: 12,
  fiscal_year_end_day: 31,
  entity_code: null,
  bank_type: null,
  bank_sample_uploaded: false,
  hh_ap_sample_uploaded: false,
  chart_choice: null,
  uses_enetemployer: null,
  payroll_sample_uploaded: false,
  invited_team: [],
  plan_tier: null,
};

export const useOnboardingStore = create<OnboardingState>()(
  persist(
    (set) => ({
      ...INITIAL,
      setField: (key, value) => set({ [key]: value } as Partial<OnboardingState>),
      goTo: (step) => set({ currentStep: step }),
      reset: () => set(INITIAL),
      markComplete: () => set({ currentStep: 'complete' }),
    }),
    {
      name: 'bookwize.onboarding',
      storage: createJSONStorage(() => {
        if (typeof window === 'undefined') {
          return {
            getItem: () => null,
            setItem: () => undefined,
            removeItem: () => undefined,
          };
        }
        return window.localStorage;
      }),
    },
  ),
);

export function getNextStep(current: OnboardingStep): OnboardingStep | null {
  const idx = STEP_ORDER.indexOf(current);
  if (idx < 0 || idx >= STEP_ORDER.length - 1) return null;
  return STEP_ORDER[idx + 1] ?? null;
}

export function getPrevStep(current: OnboardingStep): OnboardingStep | null {
  const idx = STEP_ORDER.indexOf(current);
  if (idx <= 0) return null;
  return STEP_ORDER[idx - 1] ?? null;
}
