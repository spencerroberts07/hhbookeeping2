'use client';

import { useUser } from '@clerk/nextjs';
import { useEntityStore } from '@/lib/store/entity';

/**
 * Returns the two fields every backend upload endpoint expects in its
 * FormData: entity_code (from Zustand, kept in sync with the active Clerk
 * org) and actor_email (from Clerk's identity).
 *
 * Call sites spread this into the MultiFileUpload `extraFields` prop:
 *
 *   const defaults = useUploadDefaults();
 *   <MultiFileUpload extraFields={{ ...defaults, document_type: 'monthly_statement' }} ... />
 *
 * Either field can be `undefined` for a split second on first render
 * (memberships still resolving, Clerk user still loading) — the component
 * detects missing required fields and disables the upload button, so the
 * dealer can pick files but can't fire an upload until the context is
 * ready.
 */
export function useUploadDefaults(): {
  entity_code: string | undefined;
  actor_email: string | undefined;
} {
  const entityCode = useEntityStore((s) => s.activeEntityCode);
  const { user } = useUser();
  return {
    entity_code: entityCode ?? undefined,
    actor_email: user?.primaryEmailAddress?.emailAddress,
  };
}
