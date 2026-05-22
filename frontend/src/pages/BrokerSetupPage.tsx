/**
 * /setup/broker — focused eToro credentials setup wizard.
 *
 * Displayed instead of the main app shell whenever the logged-in
 * operator has no active broker credential set. eBull is fundamentally
 * eToro-binding (CLAUDE.md non-negotiable I12 — eToro is the sole
 * execution boundary); without keys the main app is inert, so we route
 * the operator to this chrome-free single-form page instead of the
 * Settings page (which buries the credential form mid-page next to
 * other settings the operator doesn't care about right now).
 *
 * Contract: the Save button runs validation BEFORE saving — keys are
 * never persisted unless the eToro `/api/v1/me` probe succeeds. This
 * matches the operator ask "enter and validate keys before getting
 * into the site" (2026-05-22).
 *
 * Flow:
 *   1. Operator enters api_key + user_key.
 *   2. Click Save → frontend calls validateBrokerCredential (no save).
 *   3. On validate success: createBrokerCredential × 2 (api_key + user_key).
 *   4. Refresh bootstrap state → RequireAuth gate releases → main app.
 *   5. On validate failure: surface eToro's note + keep keys in form;
 *      do NOT save.
 *
 * The Test connection button is available as an optional dry-run check
 * before commit. Save also validates, so Test is redundant for the
 * happy path; kept for parity with the SettingsPage flow.
 */

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import {
  type ValidateCredentialResponse,
  createBrokerCredential,
  revokeBrokerCredential,
  validateBrokerCredential,
} from "@/api/brokerCredentials";
import { ValidationResultDisplay } from "@/components/broker/ValidationResultDisplay";
import { ENVIRONMENT } from "@/lib/credentialSetMode";
import { useSession } from "@/lib/session";

const MIN_SECRET_LEN = 4;

export function BrokerSetupPage(): JSX.Element {
  const { status, bootstrapState, refreshBootstrapState, logout } = useSession();
  const navigate = useNavigate();

  const [apiKey, setApiKey] = useState("");
  const [userKey, setUserKey] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] =
    useState<ValidateCredentialResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Auth gates — page is only meaningful when logged in + still missing creds.
  useEffect(() => {
    if (status === "loading") return;
    if (status === "unauthenticated") {
      navigate("/login", { replace: true });
      return;
    }
    if (status === "needs_setup") {
      navigate("/setup", { replace: true });
      return;
    }
    if (bootstrapState && !bootstrapState.needs_broker_credentials) {
      // Creds already present (or just landed) — release to the main app.
      navigate("/", { replace: true });
    }
  }, [status, bootstrapState, navigate]);

  const keysReady =
    apiKey.length >= MIN_SECRET_LEN && userKey.length >= MIN_SECRET_LEN;

  async function handleTestConnection(): Promise<void> {
    setError(null);
    setValidationResult(null);
    setValidating(true);
    try {
      const result = await validateBrokerCredential({
        api_key: apiKey,
        user_key: userKey,
        environment: ENVIRONMENT,
      });
      setValidationResult(result);
    } catch {
      setError("Could not reach the validator. Try again.");
    } finally {
      setValidating(false);
    }
  }

  async function handleSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      // Validate before saving — operator ask: never persist keys that
      // don't authenticate against eToro.
      const validation = await validateBrokerCredential({
        api_key: apiKey,
        user_key: userKey,
        environment: ENVIRONMENT,
      });
      setValidationResult(validation);

      if (!validation.auth_valid) {
        setError(
          validation.note ||
            "eToro rejected these credentials. Check the keys and try again.",
        );
        return;
      }

      // Save both rows. The partial-UNIQUE (operator_id, provider,
      // label, environment) WHERE NOT revoked index means a duplicate
      // active row throws 409 — so the second create cannot land if
      // the first one is left behind. Bot iter 1 caught the deadlock:
      // if api_key save succeeds but user_key save fails (network
      // blip, race, etc.), the operator is stuck on /setup/broker
      // because every retry 409s on api_key + /settings is gated out.
      //
      // Recovery: capture the first save's id and revoke it on the
      // second failure so a retry can land cleanly. Best-effort —
      // a revoke that itself fails still leaves the operator with an
      // actionable error message naming the stuck label.
      let firstSaveId: string | null = null;
      try {
        const firstSave = await createBrokerCredential({
          provider: "etoro",
          label: "api_key",
          environment: ENVIRONMENT,
          secret: apiKey,
        });
        firstSaveId = firstSave.credential.id;

        await createBrokerCredential({
          provider: "etoro",
          label: "user_key",
          environment: ENVIRONMENT,
          secret: userKey,
        });
      } catch (saveErr: unknown) {
        if (firstSaveId !== null) {
          // The api_key save succeeded; user_key save threw. Revoke
          // the orphan so the next retry doesn't 409.
          try {
            await revokeBrokerCredential(firstSaveId);
          } catch {
            setError(
              "Partial save: api_key was stored but user_key failed, and " +
                "the recovery revoke ALSO failed. Sign out and contact " +
                "operator support — the api_key row needs manual revoke " +
                "before the next attempt can land.",
            );
            return;
          }
        }
        throw saveErr;
      }

      // Flip the RequireAuth gate.
      await refreshBootstrapState();
      navigate("/", { replace: true });
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setError(
          "A credential row already exists for this label. " +
            "Sign out and back in to retry, or revoke the existing row " +
            "via operator support.",
        );
      } else {
        setError("Could not save credentials. Try again.");
      }
    } finally {
      setSubmitting(false);
    }
  }

  async function handleSignOut(): Promise<void> {
    try {
      await logout();
    } catch {
      // logout itself navigates to /login; swallow surface error.
    }
  }

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center bg-slate-50 dark:bg-slate-900/40 text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50 dark:bg-slate-900/40">
      <form
        onSubmit={handleSubmit}
        className="w-full max-w-md rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 shadow-sm"
      >
        <h1 className="mb-1 text-lg font-semibold text-slate-800 dark:text-slate-100">
          Add eToro credentials
        </h1>
        <p className="mb-4 text-xs text-slate-500 dark:text-slate-400">
          eBull needs your eToro API key + user key to connect. Keys are
          validated against eToro before being saved. Stored encrypted at
          rest under your master key; never displayed back.
        </p>

        <label className="mb-3 block text-sm">
          <span className="mb-1 block text-slate-600 dark:text-slate-300">
            Public key (API key)
          </span>
          <input
            type="password"
            autoComplete="off"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            required
            minLength={MIN_SECRET_LEN}
            className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm dark:bg-slate-800 dark:text-slate-100"
          />
        </label>
        <label className="mb-4 block text-sm">
          <span className="mb-1 block text-slate-600 dark:text-slate-300">
            Private key (user key)
          </span>
          <input
            type="password"
            autoComplete="off"
            value={userKey}
            onChange={(e) => setUserKey(e.target.value)}
            required
            minLength={MIN_SECRET_LEN}
            className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm dark:bg-slate-800 dark:text-slate-100"
          />
        </label>

        {validationResult !== null && (
          <div className="mb-3">
            <ValidationResultDisplay result={validationResult} error={null} />
          </div>
        )}

        {error !== null && (
          <div
            role="alert"
            className="mb-3 rounded bg-rose-50 dark:bg-rose-900/30 px-2 py-1.5 text-xs text-rose-700 dark:text-rose-300"
          >
            {error}
          </div>
        )}

        <div className="flex gap-2">
          <button
            type="button"
            onClick={handleTestConnection}
            disabled={!keysReady || validating || submitting}
            className="flex-1 rounded border border-slate-300 dark:border-slate-700 py-2 text-sm font-medium text-slate-700 dark:text-slate-200 disabled:opacity-50"
          >
            {validating ? "Testing…" : "Test connection"}
          </button>
          <button
            type="submit"
            disabled={!keysReady || submitting || validating}
            className="flex-1 rounded bg-slate-800 dark:bg-slate-700 py-2 text-sm font-medium text-white disabled:bg-slate-400 dark:disabled:bg-slate-600"
          >
            {submitting ? "Saving…" : "Save & continue"}
          </button>
        </div>

        <button
          type="button"
          onClick={handleSignOut}
          className="mt-4 block w-full text-center text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
        >
          Sign out
        </button>
      </form>
    </div>
  );
}
