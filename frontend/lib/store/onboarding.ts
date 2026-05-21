import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';

// Onboarding wizard step keys. Order matters — the shell renders the
// progress indicator off STEP_ORDER and goTo() doesn't constrain to
// monotonic forward motion.
export type OnboardingStep =
  | 'welcome'
  | 'connect'
  | 'chart'
  | 'cutover'
  | 'opening'
  | 'gl-history'
  | 'hh-ap'
  | 'complete';

export const STEP_ORDER: OnboardingStep[] = [
  'welcome',
  'connect',
  'chart',
  'cutover',
  'opening',
  'gl-history',
  'hh-ap',
  'complete',
];

export interface OnboardingState {
  currentStep: OnboardingStep;

  // Step 2 / connect path
  connect_path: 'qbo' | 'file' | null;

  // Step 4 / cutover
  cutover_date: string; // ISO YYYY-MM-DD

  // Step 6 / GL history
  gl_date_from: string;
  gl_date_to: string;

  setField: <K extends keyof Omit<OnboardingState, 'setField' | 'goTo' | 'reset' | 'next' | 'prev'>>(
    key: K,
    value: OnboardingState[K],
  ) => void;
  goTo: (step: OnboardingStep) => void;
  next: () => void;
  prev: () => void;
  reset: () => void;
}

function defaultCutoverISO(): string {
  // Default: start of current fiscal year (Oct 1 for HH dealers).
  const today = new Date();
  const year = today.getMonth() >= 9 ? today.getFullYear() : today.getFullYear() - 1;
  return `${year}-10-01`;
}

const INITIAL: Omit<
  OnboardingState,
  'setField' | 'goTo' | 'next' | 'prev' | 'reset'
> = {
  currentStep: 'welcome',
  connect_path: null,
  cutover_date: defaultCutoverISO(),
  gl_date_from: '',
  gl_date_to: '',
};

export const useOnboardingStore = create<OnboardingState>()(
  persist(
    (set, get) => ({
      ...INITIAL,
      setField: (key, value) => set({ [key]: value } as Partial<OnboardingState>),
      goTo: (step) => set({ currentStep: step }),
      next: () => {
        const i = STEP_ORDER.indexOf(get().currentStep);
        if (i >= 0 && i < STEP_ORDER.length - 1) {
          set({ currentStep: STEP_ORDER[i + 1]! });
        }
      },
      prev: () => {
        const i = STEP_ORDER.indexOf(get().currentStep);
        if (i > 0) set({ currentStep: STEP_ORDER[i - 1]! });
      },
      reset: () => set(INITIAL),
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
