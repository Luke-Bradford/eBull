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
import { useRecoveryPhraseModal } from "@/components/security/RecoveryPhraseModal";

const MIN_SECRET_LEN = 4;
const ENVIRONMENT = "demo";

type CredentialSetMode = "create" | "repair" | "complete";

function deriveMode(
  rows: BrokerCredentialView[] | null,
): { mode: CredentialSetMode; missingLabel: "api_key" | "user_key" | null } {
  if (rows === null) return { mode: "create", missingLabel: null };

  const active = rows.filter(
    (r) =>
      r.provider === "etoro" &&
      r.environment === ENVIRONMENT &&
      r.revoked_at === null,
  );
  const hasApiKey = active.some((r) => r.label === "api_key");
  const hasUserKey = active.some((r) => r.label === "user_key");

  if (hasApiKey && hasUserKey) return { mode: "complete", missingLabel: null };
  if (hasApiKey) return { mode: "repair", missingLabel: "user_key" };
  if (hasUserKey) return { mode: "repair", missingLabel: "api_key" };
  return { mode: "create", missingLabel: null };
}

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

  const { mode, missingLabel } = useMemo(() => deriveMode(rows), [rows]);

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
      if (mode === "create" || (mode === "repair" && missingLabel === "api_key")) {
        const keyValue = mode === "repair" ? apiKey : apiKey;
        const response = await createBrokerCredential({
          provider: "etoro",
          label: "api_key",
          environment: ENVIRONMENT,
          secret: keyValue,
        });
        if (response.recovery_phrase != null && response.recovery_phrase.length > 0) {
          // Recovery phrase shown — defer refresh + second save until
          // modal closes. But we need to save user_key too in Create mode.
          // Store a flag so the modal onClose path can continue.
          if (mode === "create") {
            // Save user_key immediately — the phrase modal is about the
            // root secret, not the individual credential. The second
            // credential should be committed before the modal opens so
            // both rows are durable.
            await createBrokerCredential({
              provider: "etoro",
              label: "user_key",
              environment: ENVIRONMENT,
              secret: userKey,
            });
          }
          setApiKey("");
          setUserKey("");
          phraseModal.open(response.recovery_phrase);
          return;
        }
      }

      // Save user_key if we're in Create mode and haven't saved it yet,
      // or if we're in Repair mode and user_key is the missing one.
      if (mode === "create" || (mode === "repair" && missingLabel === "user_key")) {
        const keyValue = mode === "repair" ? userKey : userKey;
        await createBrokerCredential({
          provider: "etoro",
          label: "user_key",
          environment: ENVIRONMENT,
          secret: keyValue,
        });
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

function ValidationResultDisplay({
  result,
  error,
}: {
  result: ValidateCredentialResponse | null;
  error: string | null;
}): JSX.Element | null {
  if (error !== null) {
    return (
      <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
        {error}
      </div>
    );
  }
  if (result === null) return null;

  if (!result.auth_valid) {
    return (
      <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
        Authentication failed — check your API key and user key.
      </div>
    );
  }

  if (!result.env_valid) {
    return (
      <div className="space-y-1">
        <div className="rounded bg-amber-50 px-2 py-1.5 text-xs text-amber-700">
          Authenticated, but environment check failed: {result.env_check}
        </div>
        {result.note && (
          <p className="text-xs text-slate-400">{result.note}</p>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <div className="rounded bg-emerald-50 px-2 py-1.5 text-xs text-emerald-700">
        Connection verified
        {result.identity?.gcid != null && (
          <span className="ml-1 text-emerald-600">
            (account {result.identity.gcid})
          </span>
        )}
      </div>
      {result.note && (
        <p className="text-xs text-slate-400">{result.note}</p>
      )}
    </div>
  );
}
