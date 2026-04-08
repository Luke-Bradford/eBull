/**
 * Broker credentials API client (issue #99 / Ticket B / ADR 0001).
 *
 * Wraps GET / POST / DELETE on /broker-credentials. The backend is
 * session-only -- the cookie carries auth, no header is added here.
 *
 * Security note:
 *   The read shape (`BrokerCredentialView`) intentionally has NO
 *   secret-bearing field. The plaintext only flows in one direction: in
 *   the POST request body. Adding a `secret` or `ciphertext` field to
 *   this type would be a regression and is covered by the backend API
 *   test that asserts the response schema field-by-field.
 */

import { apiFetch } from "@/api/client";

export type BrokerProvider = "etoro";

export interface BrokerCredentialView {
  id: string;
  provider: BrokerProvider;
  label: string;
  last_four: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

export function listBrokerCredentials(): Promise<BrokerCredentialView[]> {
  return apiFetch<BrokerCredentialView[]>("/broker-credentials");
}

export function createBrokerCredential(input: {
  provider: BrokerProvider;
  label: string;
  secret: string;
}): Promise<BrokerCredentialView> {
  return apiFetch<BrokerCredentialView>("/broker-credentials", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function revokeBrokerCredential(id: string): Promise<void> {
  return apiFetch<void>(`/broker-credentials/${id}`, { method: "DELETE" });
}
