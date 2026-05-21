import type { ReactNode } from 'react';
import { OnboardingShell } from './_components/shell';

// Pre-app shell: midnight background, BookWize logo top-left, progress
// indicator along the top, single centered card. Differs from the app
// shell — no sidebar, no entity switcher, just the wizard.
export default function OnboardingLayout({ children }: { children: ReactNode }) {
  return <OnboardingShell>{children}</OnboardingShell>;
}
