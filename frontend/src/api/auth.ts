/**
 * Auth API client (issue #98).
 *
 * Wraps the three browser-session endpoints. Each helper hands the response
 * straight back to the caller -- session storage is the SessionProvider's
 * job, not this module's.
 */

import { apiFetch } from "@/api/client";

export interface Operator {
  id: string;
  username: string;
}

export interface LoginResponse {
  operator: Operator;
}

export function login(username: string, password: string): Promise<LoginResponse> {
  return apiFetch<LoginResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export function logout(): Promise<void> {
  return apiFetch<void>("/auth/logout", { method: "POST" });
}

export function getMe(): Promise<Operator> {
  return apiFetch<Operator>("/auth/me");
}

// ---------------------------------------------------------------------------
// First-run setup (issue #106 / Ticket G)
// ---------------------------------------------------------------------------

export interface SetupResponse {
  operator: Operator;
}

// ---------------------------------------------------------------------------
// Bootstrap state + recovery (#116 / ADR-0003 Tickets 1 + 3)
// ---------------------------------------------------------------------------

/**
 * Result of GET /auth/bootstrap-state. The two booleans drive the
 * frontend boot routing precedence per ADR-0003 §6:
 *
 *     recovery_required → setup → normal
 *
 * Always fetched fresh: the backend sets `Cache-Control: no-store`
 * and the helper below passes `cache: "no-store"` so the browser
 * cannot serve a stale routing decision after a recovery flow.
 */
export interface BootstrapStateResponse {
  needs_setup: boolean;
  recovery_required: boolean;
}

export function getBootstrapState(): Promise<BootstrapStateResponse> {
  return apiFetch<BootstrapStateResponse>("/auth/bootstrap-state", {
    cache: "no-store",
  });
}

export interface RecoverResponse {
  boot_state: string;
  recovery_required: boolean;
}

/**
 * POST /auth/recover with a 24-word phrase. The backend collapses
 * every validation failure (bad checksum, wrong phrase for this
 * installation, malformed input) into a flat 400; the frontend
 * uses its own client-side checksum gate to surface the precise
 * "word N is not recognised" message before the request ever
 * leaves, and treats any 400 from the server as the generic
 * "phrase doesn't match this installation" case.
 *
 * The phrase is sent as a single space-joined string to mirror
 * the backend `RecoverRequest` shape and to keep the wire payload
 * out of any structured logging that might pretty-print arrays.
 */
export function postRecover(phrase: string): Promise<RecoverResponse> {
  return apiFetch<RecoverResponse>("/auth/recover", {
    method: "POST",
    body: JSON.stringify({ phrase }),
  });
}

export function postSetup(
  username: string,
  password: string,
  setupToken: string | null,
): Promise<SetupResponse> {
  return apiFetch<SetupResponse>("/auth/setup", {
    method: "POST",
    body: JSON.stringify({
      username,
      password,
      ...(setupToken ? { setup_token: setupToken } : {}),
    }),
  });
}
