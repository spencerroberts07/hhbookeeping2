import { api } from './client';
import type { EntityMembership } from '@/lib/store/entity';

/** GET /api/me/entities — returns entities the caller is mapped to via Clerk org. */
export async function getMyEntities(): Promise<EntityMembership[]> {
  const res = await api.get<{ entities: EntityMembership[] }>(
    '/api/me/entities',
  );
  return res.data.entities;
}
