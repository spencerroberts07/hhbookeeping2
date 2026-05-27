import axios, { AxiosError, AxiosInstance, InternalAxiosRequestConfig } from 'axios';
import { toast } from 'sonner';
import { useEntityStore } from '@/lib/store/entity';

const BASE_URL =
  process.env.NEXT_PUBLIC_API_URL ?? 'https://hhbookeeping2.onrender.com';

/**
 * The token resolver is set once at app boot by ClerkTokenBridge (see
 * components/providers/clerk-token-bridge.tsx). We avoid importing Clerk
 * hooks here so this file stays usable from server components too.
 *
 * Race-condition guard: useQuery hooks on protected pages can fire
 * before ClerkTokenBridge's useEffect runs setTokenResolver(). Without
 * a gate, those requests go out with no Authorization header → backend
 * returns 401 → the response interceptor below bounces to /sign-in →
 * Clerk sees a signed-in user on /sign-in → loop. The
 * resolverReadyPromise below holds the first interceptor call until
 * the bridge has installed the real resolver (or 5 s elapses — a hard
 * cap so we don't hang an unauthed marketing page indefinitely).
 */
type TokenResolver = () => Promise<string | null>;
let tokenResolver: TokenResolver = async () => null;

let resolverReady = false;
let resolverReadyResolve: () => void = () => {};
const resolverReadyPromise = new Promise<void>((resolve) => {
  resolverReadyResolve = resolve;
});

export function setTokenResolver(resolver: TokenResolver): void {
  tokenResolver = resolver;
  if (!resolverReady) {
    resolverReady = true;
    resolverReadyResolve();
  }
}

/** True once ClerkTokenBridge has installed the real token resolver. */
export function isTokenResolverReady(): boolean {
  return resolverReady;
}

function buildClient(): AxiosInstance {
  const instance = axios.create({
    baseURL: BASE_URL,
    timeout: 30_000,
  });

  instance.interceptors.request.use(
    async (config: InternalAxiosRequestConfig) => {
      // 1) Clerk session token. Wait (up to 5 s) for ClerkTokenBridge
      // to install the resolver — without this gate the first batch
      // of useQuery requests on any protected page can race the
      // bridge's useEffect and go out unauthenticated.
      if (!resolverReady) {
        await Promise.race([
          resolverReadyPromise,
          new Promise<void>((r) => setTimeout(r, 5000)),
        ]);
      }
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
        // Belt-and-suspenders: only redirect to /sign-in if the Clerk
        // resolver has actually been installed (i.e. we expected the
        // request to be authenticated). A 401 received before the
        // resolver attaches is almost always the token-loading race —
        // surfacing the redirect would just bounce a signed-in user
        // through /sign-in → dashboard and lose their page. The
        // request gate above already prevents this in practice; this
        // guard catches any future regression.
        if (resolverReady && typeof window !== 'undefined') {
          toast.error('Your session expired. Please sign in again.');
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
