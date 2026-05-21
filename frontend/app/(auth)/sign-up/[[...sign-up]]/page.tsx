import { SignUp } from '@clerk/nextjs';

export default function SignUpPage() {
  return (
    <div className="flex flex-col items-center">
      <SignUp
        // v6-native redirect props only. New signups land on the
        // onboarding wizard, not the dashboard.
        fallbackRedirectUrl="/onboarding"
        forceRedirectUrl="/onboarding"
      />
    </div>
  );
}
