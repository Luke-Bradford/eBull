/**
 * Thin fetch wrapper for the eBull backend.
 *
 * - All requests go to /api/* and Vite's dev proxy strips the prefix before
 *   forwarding to FastAPI (see vite.config.ts).
 * - Auth is cookie-based: the backend sets an HttpOnly session cookie on
 *   /auth/login (issue #98). We pass `credentials: "include"` so the cookie
 *   travels with every request, but JS never reads or writes the cookie
 *   itself -- HttpOnly + same-origin (via the Vite proxy) is the security
 *   boundary.
 * - On 401 we invoke the registered onUnauthorized handler exactly once per
 *   401, which the SessionProvider uses to clear in-memory session state and
 *   redirect to /login.
 */

let onUnauthorized: (() => void) | null = null;

/**
 * Register a callback that fires whenever any apiFetch call returns 401.
 * Called by SessionProvider on mount; replaces any prior handler.
 */
export function setUnauthorizedHandler(handler: (() => void) | null): void {
  onUnauthorized = handler;
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  // Contract: callers pass the backend-relative path (e.g. "/instruments"),
  // never the proxied path. apiFetch prepends "/api" exactly once; passing
  // "/api/..." would resolve to "/api/api/..." after the Vite proxy strip
  // and 404 on the backend.
  if (path.startsWith("/api")) {
    throw new Error(
      `apiFetch path must not start with "/api"; got "${path}". Pass the backend-relative path only.`,
    );
  }

  const headers = new Headers(init?.headers);
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetch(`/api${path}`, {
    ...init,
    headers,
    credentials: "include",
  });

  if (res.status === 401) {
    // Surface to the session provider so it can clear state + redirect.
    // We still throw so the caller's promise rejects -- the redirect is
    // a side effect, not a substitute for error handling.
    if (onUnauthorized) onUnauthorized();
  }

  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { detail?: unknown };
      if (typeof body.detail === "string") detail = body.detail;
    } catch {
      // non-JSON error body — keep statusText
    }
    throw new ApiError(res.status, detail);
  }
  // 204 No Content (e.g. /auth/logout) has no body to parse.
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
