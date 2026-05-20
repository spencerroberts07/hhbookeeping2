import Link from 'next/link';
import { Check } from 'lucide-react';
import { PLAN_FEATURES } from '@/lib/api/reports';

export default function PricingPage() {
  return (
    <section className="bg-cloud py-16 px-4 min-h-screen">
      <div className="container mx-auto">
        <h1 className="text-h1 text-deep-navy text-center mb-2">
          Simple, dealer-friendly pricing.
        </h1>
        <p className="text-slate text-center mb-12 max-w-xl mx-auto">
          Start with a 30-day free trial. No credit card needed up front.
          Cancel any time from your billing portal.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 max-w-3xl mx-auto">
          <PricingCard
            tier="starter"
            name="Starter"
            price="$49"
            tagline="Everything you need to close the books each month."
            features={PLAN_FEATURES.starter}
          />
          <PricingCard
            tier="professional"
            name="Professional"
            price="$149"
            tagline="For dealers who want AI, multi-store, and team workflows."
            features={PLAN_FEATURES.professional}
            recommended
          />
        </div>
        <p className="text-xs text-slate text-center mt-8 max-w-2xl mx-auto">
          Multi-store dealers: each additional store is billed as an add-on to
          your base subscription. One Stripe customer, multiple subscriptions.
        </p>
      </div>
    </section>
  );
}

function PricingCard({
  tier,
  name,
  price,
  tagline,
  features,
  recommended,
}: {
  tier: 'starter' | 'professional';
  name: string;
  price: string;
  tagline: string;
  features: readonly string[];
  recommended?: boolean;
}) {
  const isPro = tier === 'professional';
  return (
    <div
      className={`relative rounded-2xl p-8 ${isPro ? 'bg-deep-navy text-white border-deep-navy' : 'bg-white text-ink border border-border shadow-sm'}`}
    >
      {recommended && (
        <span className="absolute -top-3 left-6 inline-flex items-center rounded-full bg-bw-teal text-white text-xs font-bold px-3 py-1">
          Most popular
        </span>
      )}
      <div className={isPro ? 'text-white' : 'text-deep-navy'}>
        <h3 className="text-2xl font-bold">{name}</h3>
        <p className={`text-sm mt-1 ${isPro ? 'text-white/70' : 'text-slate'}`}>
          {tagline}
        </p>
        <div className="text-4xl font-extrabold mt-4">
          {price}
          <span className={`text-base font-normal ml-1 ${isPro ? 'text-white/70' : 'text-slate'}`}>
            /month
          </span>
        </div>
        <ul className="space-y-2 mt-6">
          {features.map((f) => (
            <li key={f} className="flex items-start gap-2 text-sm">
              <Check
                className="h-4 w-4 mt-0.5 text-bw-teal shrink-0"
                strokeWidth={2}
              />
              <span className={isPro ? 'text-white/90' : 'text-ink'}>{f}</span>
            </li>
          ))}
        </ul>
        <Link
          href="/sign-up"
          className={`mt-8 inline-block w-full text-center rounded-xl py-3 font-semibold transition ${
            isPro
              ? 'bg-bw-teal text-white hover:bg-aqua'
              : 'border border-deep-navy text-deep-navy hover:bg-cloud'
          }`}
        >
          Start free trial
        </Link>
      </div>
    </div>
  );
}
