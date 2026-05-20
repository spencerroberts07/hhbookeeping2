import { SignIn } from '@clerk/nextjs';

export default function SignInPage() {
  return (
    <div className="flex flex-col items-center">
      <SignIn
        // afterSignInUrl is the deprecated alias retained for older Clerk
        // builds; fallbackRedirectUrl is the v6 source of truth. Both are
        // set so a v5 → v6 rev doesn't reintroduce the redirect loop.
        afterSignInUrl="/dashboard"
        fallbackRedirectUrl="/dashboard"
        forceRedirectUrl="/dashboard"
      />
    </div>
  );
}
