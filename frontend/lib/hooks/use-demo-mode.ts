import { useEntityStore } from '@/lib/store/entity';

/**
 * Returns true when the active entity is a demo account.
 * Demo accounts: entity_code starts with "DEMO-" (case-insensitive).
 *
 * Every write button in the app should check this and either disable
 * or show a "Sign up to use" toast on click.
 */
export function useDemoMode(): boolean {
  const code = useEntityStore((s) => s.activeEntityCode);
  return !!code && code.toUpperCase().startsWith('DEMO-');
}

/** Helper for non-hook contexts (toasts inside event handlers). */
export function isDemoEntity(code: string | null | undefined): boolean {
  return !!code && code.toUpperCase().startsWith('DEMO-');
}
