/**
 * /settings page.
 *
 * Hosts the broker credentials section (issue #99 / Ticket B, updated
 * in #139 PR D for two-key credential model).
 *
 * Credential-set mode detection:
 *   The eToro two-key model requires exactly two active credential rows
 *   (label="api_key" and label="user_key") for provider="etoro",
 *   environment="demo". The form inspects the loaded credential list
 *   to derive one of three modes:
 *     - Create: neither key exists — show both fields.
 *     - Repair: one key exists, one missing — show only the missing field.
 *     - Complete: both keys exist — hide the create form.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";

import { ApiError } from "@/api/client";
import {
  type BrokerCredentialView,
  type ValidateCredentialResponse,
  createBrokerCredential,
  listBrokerCredentials,
  revokeBrokerCredential,
  validateBrokerCredential,
} from "@/api/brokerCredentials";
import { ValidationResultDisplay } from "@/components/broker/ValidationResultDisplay";
import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";
import { deriveCredentialSetMode, ENVIRONMENT } from "@/lib/credentialSetMode";

const MIN_SECRET_LEN = 4;

export function SettingsPage(): JSX.Element {
  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold">Settings</h1>
      <BrokerCredentialsSection />
    </div>
  );
}

function BrokerCredentialsSection(): JSX.Element {
  const [rows, setRows] = useState<BrokerCredentialView[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  // Two-key form state. Keys are stored only in component state, never
  // in any context or query cache, and are cleared on successful submit.
  const [apiKey, setApiKey] = useState("");
  const [userKey, setUserKey] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  // Validation state (transient probe — nothing is persisted).
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] =
    useState<ValidateCredentialResponse | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);

  const [busyId, setBusyId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const { mode, missingLabel } = useMemo(() => deriveCredentialSetMode(rows), [rows]);

  // Clear stale validation result when inputs change or mode transitions.
  useEffect(() => {
    setValidationResult(null);
    setValidationError(null);
  }, [apiKey, userKey, mode]);

  const phraseModal = useRecoveryPhraseModal({
    onClose: () => {
      setCreateError(null);
      void refresh();
    },
  });

  const refresh = useCallback(async () => {
    setLoadError(null);
    try {
      const data = await listBrokerCredentials();
      setRows(data);
    } catch {
      setRows([]);
      setLoadError("Could not load broker credentials.");
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  async function handleTestConnection(): Promise<void> {
    setValidationResult(null);
    setValidationError(null);
    setValidating(true);
    try {
      const result = await validateBrokerCredential({
        api_key: apiKey,
        user_key: userKey,
        environment: ENVIRONMENT,
      });
      setValidationResult(result);
    } catch {
      setValidationError("Could not reach the validation endpoint.");
    } finally {
      setValidating(false);
    }
  }

  async function handleCreate(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setCreateError(null);
    setCreating(true);
    try {
      let phrase: readonly string[] | null = null;

      // Save api_key if needed (Create mode or Repair with api_key missing).
      if (mode === "create" || (mode === "repair" && missingLabel === "api_key")) {
        const response = await createBrokerCredential({
          provider: "etoro",
          label: "api_key",
          environment: ENVIRONMENT,
          secret: apiKey,
        });
        if (response.recovery_phrase != null && response.recovery_phrase.length > 0) {
          phrase = response.recovery_phrase;
        }
      }

      // Save user_key if needed (Create mode or Repair with user_key missing).
      if (mode === "create" || (mode === "repair" && missingLabel === "user_key")) {
        await createBrokerCredential({
          provider: "etoro",
          label: "user_key",
          environment: ENVIRONMENT,
          secret: userKey,
        });
      }

      // If the first save triggered a recovery phrase, show the modal
      // now that both credentials are durable.
      if (phrase !== null) {
        setApiKey("");
        setUserKey("");
        phraseModal.open(phrase);
        return;
      }

      setApiKey("");
      setUserKey("");
      await refresh();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setCreateError("A credential with that label already exists. Revoke it first to replace.");
      } else if (err instanceof ApiError && err.status === 400) {
        setCreateError("Invalid API key or user key value.");
      } else {
        setCreateError("Could not save credential.");
      }
      // Re-derive mode from the refreshed list in case the first call
      // succeeded but the second failed (partial state).
      await refresh();
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(row: BrokerCredentialView): Promise<void> {
    setActionError(null);
    if (
      !window.confirm(
        `Revoke "${row.label}" (${row.provider} · ${row.environment})? This cannot be undone.`,
      )
    ) {
      return;
    }
    setBusyId(row.id);
    try {
      await revokeBrokerCredential(row.id);
      await refresh();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 404) {
        setActionError("That credential no longer exists.");
        await refresh();
      } else {
        setActionError("Could not revoke credential.");
      }
    } finally {
      setBusyId(null);
    }
  }

  const showApiKeyField = mode === "create" || (mode === "repair" && missingLabel === "api_key");
  const showUserKeyField = mode === "create" || (mode === "repair" && missingLabel === "user_key");
  const canTestConnection = mode === "create" && apiKey.length >= MIN_SECRET_LEN && userKey.length >= MIN_SECRET_LEN;
  const canSave =
    !creating &&
    (showApiKeyField ? apiKey.length >= MIN_SECRET_LEN : true) &&
    (showUserKeyField ? userKey.length >= MIN_SECRET_LEN : true);

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-sm font-medium text-slate-700">Broker credentials</h2>
        <p className="text-xs text-slate-500">
          Encrypted broker secrets stored against your operator account. eBull uses
          these to place orders — the plaintext value is never returned to this UI.
        </p>
      </div>

      {loadError !== null && (
        <div role="alert" className="rounded bg-rose-50 px-3 py-2 text-xs text-rose-700">
          {loadError}
        </div>
      )}

      {rows === null ? (
        <p className="text-xs text-slate-400">Loading…</p>
      ) : rows.length === 0 ? (
        <p className="text-xs text-slate-400">No broker credentials saved yet.</p>
      ) : (
        <ul className="divide-y divide-slate-200 rounded border border-slate-200 bg-white">
          {rows.map((row) => {
            const revoked = row.revoked_at !== null;
            return (
              <li
                key={row.id}
                className="flex items-center justify-between px-3 py-2 text-sm"
              >
                <div>
                  <span className="font-medium text-slate-800">{row.label}</span>
                  <span className="ml-2 text-xs text-slate-500">
                    {row.provider} · {row.environment} · ••••{row.last_four}
                  </span>
                  {revoked && (
                    <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-slate-500">
                      revoked
                    </span>
                  )}
                </div>
                {!revoked && (
                  <button
                    type="button"
                    onClick={() => void handleRevoke(row)}
                    disabled={busyId === row.id}
                    className="rounded border border-rose-300 px-2 py-1 text-xs text-rose-700 hover:bg-rose-50 disabled:opacity-50"
                  >
                    {busyId === row.id ? "Revoking…" : "Revoke"}
                  </button>
                )}
              </li>
            );
          })}
        </ul>
      )}
      {actionError !== null && (
        <p role="alert" className="text-xs text-rose-700">
          {actionError}
        </p>
      )}

      {mode === "complete" ? (
        <div className="max-w-sm rounded border border-slate-200 bg-white p-4">
          <p className="text-sm text-slate-700">
            Credentials configured. Revoke existing credentials to replace them.
          </p>
        </div>
      ) : (
        <form
          onSubmit={handleCreate}
          className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4"
        >
          <h3 className="text-sm font-medium text-slate-700">
            {mode === "repair" ? "Complete credential setup" : "Add eToro credentials"}
          </h3>
          {mode === "repair" && (
            <p className="text-xs text-slate-500">
              One key was already saved. Enter the missing {missingLabel === "api_key" ? "API key" : "user key"} to
              complete the credential pair.
            </p>
          )}

          {showApiKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">API key</span>
              <input
                type="password"
                name="broker-credential-api-key"
                autoComplete="new-password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
          )}

          {showUserKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">User key</span>
              <input
                type="password"
                name="broker-credential-user-key"
                autoComplete="new-password"
                value={userKey}
                onChange={(e) => setUserKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
              />
            </label>
          )}

          {/* Test connection — only available in Create mode (both keys present). */}
          <div className="space-y-1">
            <button
              type="button"
              onClick={() => void handleTestConnection()}
              disabled={!canTestConnection || validating}
              className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50"
            >
              {validating ? "Testing…" : "Test connection"}
            </button>
            {mode === "repair" && (
              <p className="text-xs text-slate-400">
                Connection testing requires both keys. The already-saved key cannot be read back.
              </p>
            )}
          </div>

          <ValidationResultDisplay
            result={validationResult}
            error={validationError}
          />

          {createError !== null && (
            <div
              role="alert"
              className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
            >
              {createError}
            </div>
          )}
          <button
            type="submit"
            disabled={!canSave}
            className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
          >
            {creating ? "Saving…" : "Save credential"}
          </button>
        </form>
      )}

      {phraseModal.element}
    </section>
  );
}

