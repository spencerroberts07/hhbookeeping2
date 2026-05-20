import { clerkMiddleware, createRouteMatcher } from '@clerk/nextjs/server';

// Public routes: the marketing site, sign-in, sign-up, Stripe webhook,
// and the API health/diagnostic paths. Everything else requires auth.
const isPublicRoute = createRouteMatcher([
  '/',
  '/pricing',
  '/about(.*)',
  '/contact(.*)',
  '/legal/(.*)',
  '/sign-in(.*)',
  '/sign-up(.*)',
  '/api/webhooks/(.*)', // Clerk + Stripe webhooks must be unauthenticated
]);

export default clerkMiddleware(async (auth, req) => {
  if (isPublicRoute(req)) {
    return;
  }
  // protect() returns a redirect to sign-in when no session is present.
  await auth.protect();
});

export const config = {
  matcher: [
    // Skip Next.js internals and all static files unless an actual route.
    '/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)',
    // Always run middleware for API routes.
    '/(api|trpc)(.*)',
  ],
};
