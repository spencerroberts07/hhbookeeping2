import axios, { AxiosError, AxiosInstance, InternalAxiosRequestConfig } from 'axios';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';

const BASE_URL =
  process.env.NEXT_PUBLIC_API_URL ?? 'https://hhbookeeping2.onrender.com';

/**
 * The token resolver is set once at app boot by ClerkTokenBridge (see
 * components/providers/clerk-token-bridge.tsx). We avoid importing Clerk
 * hooks here so this file stays usable from server components too.
 */
type TokenResolver = () => Promise<string | null>;
let tokenResolver: TokenResolver = async () => null;

export function setTokenResolver(resolver: TokenResolver): void {
  tokenResolver = resolver;
}

function buildClient(): AxiosInstance {
  const instance = axios.create({
    baseURL: BASE_URL,
    timeout: 30_000,
  });

  instance.interceptors.request.use(
    async (config: InternalAxiosRequestConfig) => {
      // 1) Clerk session token
      const token = await tokenResolver();
      if (token) {
        config.headers.set('Authorization', `Bearer ${token}`);
      }

      // 2) entity_code — auto-inject for GET requests that don't already
      // have one. For POST bodies, the call site is responsible for setting
      // entity_code in the payload (backend uses both query and body shapes).
      const entityCode = useEntityStore.getState().activeEntityCode;
      if (
        entityCode &&
        !config.params?.entity_code &&
        config.method?.toLowerCase() === 'get'
      ) {
        config.params = { ...(config.params ?? {}), entity_code: entityCode };
      }

      return config;
    },
  );

  instance.interceptors.response.use(
    (res) => res,
    (error: AxiosError<{ detail?: string }>) => {
      const status = error.response?.status;
      const detail =
        error.response?.data?.detail ?? error.message ?? 'Request failed';

      if (status === 401) {
        // Clerk's middleware handles the redirect on the server; surface a
        // toast so client-side fetches that fail surface clearly.
        toast.error('Your session expired. Please sign in again.');
        if (typeof window !== 'undefined') {
          window.location.href = '/sign-in';
        }
      } else if (status === 403) {
        toast.error(`Permission denied: ${detail}`);
      } else if (status === 404) {
        // Don't toast 404s — call sites typically render their own empty state.
      } else if (status === 409) {
        toast.warning(detail);
      } else if (status && status >= 500) {
        toast.error('Server error. Please try again in a moment.');
      } else if (!error.response) {
        toast.error('Network error. Check your connection.');
      } else {
        toast.error(detail);
      }
      return Promise.reject(error);
    },
  );

  return instance;
}

export const api = buildClient();

/** Shape returned by FastAPI HTTPException — kept here so call sites can narrow. */
export interface ApiError {
  response?: {
    status?: number;
    data?: { detail?: string };
  };
  message: string;
}
