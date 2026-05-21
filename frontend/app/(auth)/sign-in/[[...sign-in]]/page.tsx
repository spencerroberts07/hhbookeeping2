import { SignIn } from '@clerk/nextjs';

export default function SignInPage() {
  return (
    <div className="flex flex-col items-center">
      <SignIn
        // v6-native redirect props only. fallbackRedirectUrl is used
        // when no explicit redirect_url is present; forceRedirectUrl
        // always wins. Both target /dashboard.
        fallbackRedirectUrl="/dashboard"
        forceRedirectUrl="/dashboard"
      />
    </div>
  );
}
