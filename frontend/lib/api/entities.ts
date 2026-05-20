import { api } from './client';

export interface Entity {
  id: string;
  entity_code: string;
  entity_name: string;
  fiscal_year_end_month: number;
  fiscal_year_end_day: number;
  base_currency: string;
  clerk_org_id: string | null;
  organization_id: string;
}

export interface CreateEntityInput {
  entity_code: string;
  entity_name: string;
  fiscal_year_end_month: number;
  fiscal_year_end_day: number;
  province: string;
  base_currency?: string;
  clerk_org_id?: string;
}

/** POST /api/entities — create a new entity (admin only). Used by onboarding step 1. */
export async function createEntity(input: CreateEntityInput): Promise<Entity> {
  const res = await api.post<Entity>('/api/entities', input);
  return res.data;
}

/** PATCH /api/entities/{entity_code} — update entity details. */
export async function updateEntity(
  entityCode: string,
  patch: Partial<Omit<CreateEntityInput, 'entity_code'>>,
): Promise<Entity> {
  const res = await api.patch<Entity>(
    `/api/entities/${encodeURIComponent(entityCode)}`,
    patch,
  );
  return res.data;
}
