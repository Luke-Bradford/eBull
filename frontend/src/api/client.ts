/**
 * Thin fetch wrapper for the eBull backend.
 *
 * - All requests go to /api/* and Vite's dev proxy strips the prefix before
 *   forwarding to FastAPI (see vite.config.ts).
 * - The bearer token slot is in-memory only. Wire-up to the real auth flow
 *   lands with #58 — for now nothing calls setAuthToken(), so requests go
 *   out unauthenticated and Header reports "disconnected".
 * - This file deliberately exposes no real fetchers yet (#59 is scaffold-only).
 *   Pages should import the mock layer in @/api/mocks instead.
 */

let authToken: string | null = null;

export function setAuthToken(token: string | null): void {
  authToken = token;
}

export function hasAuthToken(): boolean {
  return authToken !== null;
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
  const headers = new Headers(init?.headers);
  if (authToken !== null) {
    headers.set("Authorization", `Bearer ${authToken}`);
  }
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetch(`/api${path}`, { ...init, headers });
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
  return (await res.json()) as T;
}
