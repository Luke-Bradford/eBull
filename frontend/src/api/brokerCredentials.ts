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
  environment: string;
  last_four: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

export function listBrokerCredentials(): Promise<BrokerCredentialView[]> {
  return apiFetch<BrokerCredentialView[]>("/broker-credentials");
}

/**
 * Response shape for POST /broker-credentials.
 *
 * Mirrors the backend ``CreateCredentialResponse`` (see
 * app/api/broker_credentials.py:101). The ``recovery_phrase`` field is
 * populated exactly once in the lifetime of an installation -- on the
 * first credential save in clean_install mode, when lazy generation of
 * the root secret occurs (ADR-0003 §4 / #114). On every subsequent save
 * it is absent / null and the UI must NOT show the recovery phrase
 * modal.
 *
 * Security note: this is the only place in the frontend where a 24-word
 * phrase ever transits the API boundary. It is held in component state
 * by the caller and discarded -- never persisted to localStorage,
 * sessionStorage, or any cache. See RecoveryPhraseConfirm.tsx for the
 * display contract.
 */
export interface CreateBrokerCredentialResponse {
  credential: BrokerCredentialView;
  recovery_phrase: readonly string[] | null;
}

export function createBrokerCredential(input: {
  provider: BrokerProvider;
  label: string;
  environment: string;
  secret: string;
}): Promise<CreateBrokerCredentialResponse> {
  return apiFetch<CreateBrokerCredentialResponse>("/broker-credentials", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function revokeBrokerCredential(id: string): Promise<void> {
  return apiFetch<void>(`/broker-credentials/${id}`, { method: "DELETE" });
}

// ---------------------------------------------------------------------------
// Validate (transient probe — nothing is persisted)
// ---------------------------------------------------------------------------

export interface ValidateIdentity {
  gcid: number | null;
  demo_cid: number | null;
  real_cid: number | null;
}

export interface ValidateCredentialResponse {
  auth_valid: boolean;
  identity: ValidateIdentity | null;
  environment: string;
  env_valid: boolean;
  env_check: string;
  note: string;
}

export function validateBrokerCredential(input: {
  api_key: string;
  user_key: string;
  environment: string;
}): Promise<ValidateCredentialResponse> {
  return apiFetch<ValidateCredentialResponse>(
    "/broker-credentials/validate",
    {
      method: "POST",
      body: JSON.stringify(input),
    },
  );
}

/**
 * Validate already-stored credentials by loading them from the DB
 * server-side and probing eToro. Returns the same response shape as
 * the transient validate endpoint.
 *
 * Returns 404 if either api_key or user_key is not stored.
 */
export function validateStoredCredentials(): Promise<ValidateCredentialResponse> {
  return apiFetch<ValidateCredentialResponse>(
    "/broker-credentials/validate-stored",
    { method: "POST" },
  );
}
