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

export interface SetupStatusResponse {
  needs_setup: boolean;
}

export interface SetupResponse {
  operator: Operator;
}

export function getSetupStatus(): Promise<SetupStatusResponse> {
  return apiFetch<SetupStatusResponse>("/auth/setup-status");
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
