'use client';

import Link from 'next/link';
import { Button } from '@/components/ui/button';
import { useDemoMode } from '@/lib/hooks/use-demo-mode';

/**
 * Top-of-app banner shown only when the active entity is a demo
 * account. Renders nothing for normal entities.
 */
export function DemoBanner() {
  const isDemo = useDemoMode();
  if (!isDemo) return null;
  return (
    <div className="bg-bw-teal text-white px-4 py-2 text-sm flex items-center gap-3 flex-wrap">
      <span>
        📋 Demo account — explore freely. Sign up for a free trial to get
        started.
      </span>
      <div className="flex-1" />
      <Link href="/sign-up">
        <Button size="sm" variant="primary" className="bg-deep-navy">
          Start free trial →
        </Button>
      </Link>
    </div>
  );
}
