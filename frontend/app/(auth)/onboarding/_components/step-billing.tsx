'use client';

import { useState } from 'react';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { createCheckoutSession } from '@/lib/api/billing';
import { PLAN_FEATURES } from '@/lib/api/reports';
import { toast } from 'sonner';
import { Check } from 'lucide-react';

export function StepBilling() {
  const store = useOnboardingStore();
  const [submitting, setSubmitting] = useState(false);

  const startCheckout = async () => {
    if (!store.plan_tier || !store.entity_code) return;
    setSubmitting(true);
    try {
      const origin =
        typeof window !== 'undefined'
          ? window.location.origin
          : process.env.NEXT_PUBLIC_APP_URL ?? '';
      const { url } = await createCheckoutSession({
        entity_code: store.entity_code,
        plan_tier: store.plan_tier,
        success_url: `${origin}/onboarding?step=complete`,
        cancel_url: `${origin}/onboarding`,
      });
      window.location.href = url;
    } catch {
      toast.error('Could not start checkout.');
      setSubmitting(false);
    }
  };

  const PlanCard = ({
    tier,
    name,
    price,
    recommended,
  }: {
    tier: 'starter' | 'professional';
    name: string;
    price: string;
    recommended?: boolean;
  }) => {
    const isProfessional = tier === 'professional';
    const active = store.plan_tier === tier;
    return (
      <button
        type="button"
        onClick={() => store.setField('plan_tier', tier)}
        className={cn(
          'w-full text-left rounded-2xl border p-6 transition relative',
          isProfessional
            ? 'bg-deep-navy text-white border-deep-navy'
            : 'bg-white border-border',
          active && 'ring-4 ring-bw-teal',
        )}
      >
        {recommended && (
          <Badge variant="complete" className="absolute -top-3 left-4">
            Most popular
          </Badge>
        )}
        <div className={cn('text-lg font-semibold', isProfessional ? 'text-white' : 'text-deep-navy')}>
          {name}
        </div>
        <div className={cn('text-3xl font-extrabold my-3', isProfessional ? 'text-white' : 'text-deep-navy')}>
          {price}
          <span className={cn('text-sm font-normal ml-1', isProfessional ? 'text-white/70' : 'text-slate')}>
            /month
          </span>
        </div>
        <ul className="space-y-2 mt-4">
          {PLAN_FEATURES[tier].map((feat) => (
            <li
              key={feat}
              className={cn(
                'flex items-start gap-2 text-sm',
                isProfessional ? 'text-white/90' : 'text-ink',
              )}
            >
              <Check
                className={cn('h-4 w-4 mt-0.5 shrink-0', isProfessional ? 'text-bw-teal' : 'text-bw-teal')}
                strokeWidth={2}
              />
              <span>{feat}</span>
            </li>
          ))}
        </ul>
      </button>
    );
  };

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-h2 text-deep-navy mb-2">Choose your plan</h2>
        <p className="text-slate">
          30-day free trial — no charge today. You can change plans or cancel
          any time from Settings → Billing.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <PlanCard tier="starter" name="Starter" price="$49" />
        <PlanCard tier="professional" name="Professional" price="$149" recommended />
      </div>
      <div className="flex justify-between pt-4">
        <Button type="button" variant="ghost" onClick={() => store.goTo('invite')}>
          Back
        </Button>
        <Button
          type="button"
          variant="accent"
          onClick={startCheckout}
          disabled={!store.plan_tier || submitting}
        >
          {submitting ? 'Opening checkout…' : 'Start free trial'}
        </Button>
      </div>
    </div>
  );
}
