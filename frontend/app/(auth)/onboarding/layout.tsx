import type { ReactNode } from 'react';
import { ClerkTokenBridge } from '@/components/providers/clerk-token-bridge';
import { OnboardingShell } from './_components/shell';

// Pre-app shell: midnight background, BookWize logo top-left, progress
// indicator along the top, single centered card. Differs from the app
// shell — no sidebar, no entity switcher, just the wizard.
//
// ClerkTokenBridge is mounted here (not at the (auth) route-group level)
// because sign-in / sign-up pages must NOT have a token resolver
// installed — they run pre-auth. Onboarding is the only (auth) child
// that needs authenticated API calls, so the bridge lives at this
// nested layout. Without it, /api/onboarding/* GETs go out with no
// Authorization header and the backend require_role(...) deps 401.
export default function OnboardingLayout({ children }: { children: ReactNode }) {
  return (
    <ClerkTokenBridge>
      <OnboardingShell>{children}</OnboardingShell>
    </ClerkTokenBridge>
  );
}
