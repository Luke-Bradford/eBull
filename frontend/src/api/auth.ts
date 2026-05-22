/**
 * Auth API client (issue #98).
 *
 * Wraps the browser-session endpoints. Each helper hands the response
 * straight back to the caller — session storage is the SessionProvider's
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
// Bootstrap state (#116 / ADR-0003, amended 2026-05-07)
// ---------------------------------------------------------------------------

/**
 * Result of GET /auth/bootstrap-state. Drives the frontend boot
 * routing precedence:
 *
 *     needs_setup                     → /setup
 *     needs_broker_credentials        → /settings (force, until creds added)
 *     otherwise                       → normal
 *
 * `needs_broker_credentials` reflects whether at least one non-revoked
 * `broker_credentials` row exists. eBull is fundamentally eToro-binding
 * (CLAUDE.md non-negotiable I12 — eToro is the sole execution boundary);
 * a logged-in operator with no active credentials cannot reach the
 * main app shell.
 *
 * Always fetched fresh: the backend sets `Cache-Control: no-store`
 * and the helper below passes `cache: "no-store"` so the browser
 * cannot serve a stale routing decision.
 */
export interface BootstrapStateResponse {
  boot_state: string;
  needs_setup: boolean;
  needs_broker_credentials: boolean;
}

export function getBootstrapState(): Promise<BootstrapStateResponse> {
  return apiFetch<BootstrapStateResponse>("/auth/bootstrap-state", {
    cache: "no-store",
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
