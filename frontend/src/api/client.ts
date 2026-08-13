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
  /**
   * Raw `detail` body from the backend, when the response was JSON. The
   * shape is endpoint-specific — most legacy endpoints return a string,
   * but `/system/processes/*` returns `{reason, advice?}` dicts so the
   * FE can render structured 409 tooltips. Callers narrow with their
   * own type guard before reading; never assume a shape blindly.
   */
  public readonly detail: unknown;

  constructor(
    public readonly status: number,
    message: string,
    detail: unknown = undefined,
  ) {
    super(message);
    this.name = "ApiError";
    this.detail = detail;
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
    let message = res.statusText;
    let detailRaw: unknown = undefined;
    try {
      const body = (await res.json()) as { detail?: unknown };
      detailRaw = body.detail;
      if (typeof body.detail === "string") message = body.detail;
    } catch {
      // non-JSON error body — keep statusText
    }
    throw new ApiError(res.status, message, detailRaw);
  }
  // 204 No Content always has an empty body — auth logout, cancel
  // dispatcher rows, etc. Calling res.json() on it throws "Unexpected
  // end of JSON input".
  if (res.status === 204) return undefined as T;
  // 202 Accepted is split: PR1b-2 #1064 made POST /jobs/{name}/run
  // return JobRunQueuedResponse ({"request_id": N}) on 202 so the FE
  // Advanced disclosure (PR2) can pivot the operator to the queue
  // row. Other 202 callers may still send an empty body. Read the
  // response as text once, parse JSON only when non-empty, and fall
  // back to undefined on either an empty body or invalid JSON. This
  // preserves the pre-PR2 contract for callers that expected
  // undefined while unlocking the body for new endpoints.
  if (res.status === 202) {
    const text = await res.text();
    if (!text) return undefined as T;
    try {
      return JSON.parse(text) as T;
    } catch {
      return undefined as T;
    }
  }
  return (await res.json()) as T;
}
