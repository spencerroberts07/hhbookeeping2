'use client';

import { useUser } from '@clerk/nextjs';
import { Button } from '@/components/ui/button';
import { useOnboardingStore } from '@/lib/store/onboarding';
import { useEntityStore } from '@/lib/store/entity';

// Step 1: welcome. The entity is created by the Clerk webhook when the
// org gets provisioned, so by the time the user lands here, the
// entity_code is already populated in useEntityStore. We just show a
// confirmation card and kick the user into the connect/upload step.
export function StepWelcome() {
  const { user } = useUser();
  const next = useOnboardingStore((s) => s.next);
  const entities = useEntityStore((s) => s.entities);
  const activeCode = useEntityStore((s) => s.activeEntityCode);
  const entity = entities.find((e) => e.entity_code === activeCode);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-h1 text-deep-navy">
          Welcome to BookWize{user?.firstName ? `, ${user.firstName}` : ''}
        </h1>
        <p className="text-slate mt-1">
          {entity?.entity_name
            ? `Let's set up ${entity.entity_name}.`
            : "Let's get your books wired up."}
        </p>
      </div>

      <div className="rounded-xl bg-cloud p-5 space-y-3">
        <div className="flex justify-between text-sm">
          <span className="text-slate">Store name</span>
          <span className="font-semibold text-deep-navy">
            {entity?.entity_name ?? '—'}
          </span>
        </div>
        <div className="flex justify-between text-sm">
          <span className="text-slate">Store number</span>
          <span className="font-mono text-deep-navy">{activeCode ?? '—'}</span>
        </div>
      </div>

      <div className="text-sm text-slate space-y-2">
        <p>This setup takes about 5–10 minutes. We'll:</p>
        <ul className="list-disc list-inside space-y-1 ml-2">
          <li>Pull or upload your chart of accounts</li>
          <li>Pick a cut-over date</li>
          <li>Import your opening balances</li>
          <li>Load historical transactions so the AI assistant learns your business</li>
        </ul>
      </div>

      <div className="flex justify-end pt-2">
        <Button variant="accent" size="lg" onClick={next}>
          Get started →
        </Button>
      </div>
    </div>
  );
}
