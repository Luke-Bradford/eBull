/**
 * /settings page.
 *
 * Hosts the broker credentials section (issue #99 / Ticket B, updated
 * in #139 PR D for two-key credential model, #144 for edit/replace UX).
 *
 * Credential-set mode detection:
 *   The eToro two-key model requires exactly two active credential rows
 *   (label="api_key" and label="user_key") for provider="etoro",
 *   environment="demo". The form inspects the loaded credential list
 *   to derive one of three modes:
 *     - Create: neither key exists — show both fields.
 *     - Repair: one key exists, one missing — show only the missing field.
 *     - Complete: both keys exist — show management actions.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";

import { ApiError } from "@/api/client";
import {
  type BrokerCredentialView,
  type ValidateCredentialResponse,
  createBrokerCredential,
  replaceBrokerCredential,
  listBrokerCredentials,
  revokeBrokerCredential,
  validateBrokerCredential,
  validateStoredCredentials,
} from "@/api/brokerCredentials";
import { runJob } from "@/api/jobs";
import { ValidationResultDisplay } from "@/components/broker/ValidationResultDisplay";
import { BudgetConfigSection } from "@/components/settings/BudgetConfigSection";
import { DisplayCurrencySection } from "@/components/settings/DisplayCurrencySection";
import { deriveCredentialSetMode, ENVIRONMENT } from "@/lib/credentialSetMode";
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";

const MIN_SECRET_LEN = 4;

/** Which action is active in the "complete" mode management panel. */
type ManageAction = "idle" | "edit-api_key" | "edit-user_key" | "replace";

export function SettingsPage(): JSX.Element {
  const displayCurrency = useDisplayCurrency();
  return (
    <div className="space-y-6 pt-6">
      <h1 className="text-xl font-semibold">Settings</h1>
      <DisplayCurrencySection
        currentCurrency={displayCurrency}
        onChanged={() => window.location.reload()}
      />
      <BudgetConfigSection />
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

  // Management panel state (complete mode only).
  const [manageAction, setManageAction] = useState<ManageAction>("idle");
  const [editSecret, setEditSecret] = useState("");
  const [editError, setEditError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);

  const { mode, missingLabel } = useMemo(() => deriveCredentialSetMode(rows), [rows]);

  // Clear stale validation result when inputs change or mode transitions.
  useEffect(() => {
    setValidationResult(null);
    setValidationError(null);
  }, [apiKey, userKey, mode]);

  // Reset management action when mode changes away from complete.
  useEffect(() => {
    if (mode !== "complete") {
      setManageAction("idle");
      setEditSecret("");
      setEditError(null);
    }
  }, [mode]);

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

  // -- Test connection (transient candidate keys) -------------------------

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

  // -- Test connection (stored keys) --------------------------------------

  async function handleTestStored(): Promise<void> {
    setValidationResult(null);
    setValidationError(null);
    setValidating(true);
    try {
      const result = await validateStoredCredentials();
      setValidationResult(result);
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 404) {
        setValidationError("Both credentials must be stored before testing.");
      } else if (err instanceof ApiError && err.status === 503) {
        setValidationError("Credential decryption failed. Check server key material.");
      } else {
        setValidationError("Could not reach the validation endpoint.");
      }
    } finally {
      setValidating(false);
    }
  }

  // -- Create flow (initial setup or repair) ------------------------------

  async function handleCreate(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setCreateError(null);
    setCreating(true);
    // Capture mode before the save so we can detect first-time creation.
    const wasCreate = mode === "create";
    try {
      // Save api_key if needed (Create mode or Repair with api_key missing).
      if (mode === "create" || (mode === "repair" && missingLabel === "api_key")) {
        await createBrokerCredential({
          provider: "etoro",
          label: "api_key",
          environment: ENVIRONMENT,
          secret: apiKey,
        });
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

      // First-run bootstrap: kick off the universe sync when both keys
      // are saved for the first time. Fire-and-forget — errors are
      // swallowed because the operator can always trigger manually.
      if (wasCreate) {
        runJob("nightly_universe_sync").catch(() => {});
      }

      setApiKey("");
      setUserKey("");
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setCreateError("A credential with that label already exists. Revoke it first to replace.");
      } else if (err instanceof ApiError && err.status === 400) {
        setCreateError("Invalid public key or private key value.");
      } else {
        setCreateError("Could not save credential.");
      }
    } finally {
      setCreating(false);
      // Always re-derive mode from server state, whether success or
      // partial failure (e.g. first key saved, second failed → Repair).
      await refresh();
    }
  }

  // -- Revoke flow --------------------------------------------------------

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
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 404) {
        setActionError("That credential no longer exists.");
      } else {
        setActionError("Could not revoke credential.");
      }
    } finally {
      setBusyId(null);
      await refresh();
    }
  }

  // -- Edit single key (complete mode) ------------------------------------

  function startEdit(label: "api_key" | "user_key"): void {
    setManageAction(label === "api_key" ? "edit-api_key" : "edit-user_key");
    setEditSecret("");
    setEditError(null);
    setValidationResult(null);
    setValidationError(null);
  }

  function startReplace(): void {
    setManageAction("replace");
    setApiKey("");
    setUserKey("");
    setEditError(null);
    setValidationResult(null);
    setValidationError(null);
  }

  function cancelManage(): void {
    setManageAction("idle");
    setEditSecret("");
    setApiKey("");
    setUserKey("");
    setEditError(null);
    setValidationResult(null);
    setValidationError(null);
  }

  async function handleEditSave(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    const label = manageAction === "edit-api_key" ? "api_key" : "user_key";
    setEditError(null);
    setEditing(true);
    try {
      // #980 / #974/F: atomic revoke + insert in one server-side
      // transaction. Pre-#980 this path issued DELETE + POST sequentially,
      // letting subscribers (orchestrator gate, WS subscriber, admin
      // banner) observe a transient MISSING state between the two calls.
      // The PUT /replace endpoint does both inside one tx so no
      // intermediate state is ever visible.
      await replaceBrokerCredential({
        provider: "etoro",
        label,
        environment: ENVIRONMENT,
        secret: editSecret,
      });
      setEditSecret("");
      setManageAction("idle");
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 404) {
        // Replace requires an existing active row. Falling here means
        // the row was revoked between mount and submit; create instead.
        try {
          await createBrokerCredential({
            provider: "etoro",
            label,
            environment: ENVIRONMENT,
            secret: editSecret,
          });
          setEditSecret("");
          setManageAction("idle");
        } catch (createErr: unknown) {
          if (createErr instanceof ApiError && createErr.status === 400) {
            setEditError("Invalid key value.");
          } else {
            setEditError("Could not update credential.");
          }
        }
      } else if (err instanceof ApiError && err.status === 400) {
        setEditError("Invalid key value.");
      } else {
        setEditError("Could not update credential.");
      }
    } finally {
      setEditing(false);
      await refresh();
    }
  }

  async function handleReplaceSave(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setEditError(null);
    setEditing(true);
    // #980 / #974/F: each label is replaced via PUT /broker-credentials/replace
    // (atomic revoke + insert in one server-side tx). Pre-#980 this
    // path issued DELETE-DELETE-POST-POST sequentially and observers
    // saw a transient MISSING + half-saved state; now each label is
    // its own atomic replace, so worst case one label is replaced
    // and the other isn't (operator-visible).
    let savedApiKey = false;
    try {
      await replaceBrokerCredential({
        provider: "etoro",
        label: "api_key",
        environment: ENVIRONMENT,
        secret: apiKey,
      });
      savedApiKey = true;
      await replaceBrokerCredential({
        provider: "etoro",
        label: "user_key",
        environment: ENVIRONMENT,
        secret: userKey,
      });

      setApiKey("");
      setUserKey("");
      setManageAction("idle");
    } catch (err: unknown) {
      let message: string;
      if (err instanceof ApiError && err.status === 404) {
        // Replace endpoint requires an existing active row. If we
        // hit this, it means the credential pair isn't actually
        // present despite the UI being in 'replace' mode — race
        // with another tab. Refresh will recover into the right mode.
        message = "Credentials no longer exist — refreshing.";
      } else if (err instanceof ApiError && err.status === 400) {
        message = "Invalid key value.";
      } else {
        message = "Could not replace credentials.";
      }

      if (savedApiKey) {
        setActionError(`api_key was saved but user_key failed — re-enter user_key below. ${message}`);
      } else {
        setEditError(message);
      }
    } finally {
      setEditing(false);
      await refresh();
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
        <ul className="divide-y divide-slate-200 rounded border border-slate-200 bg-white dark:divide-slate-800 dark:border-slate-800 dark:bg-slate-900">
          {rows.map((row) => {
            const revoked = row.revoked_at !== null;
            const isActiveEtoro =
              row.provider === "etoro" &&
              row.environment === ENVIRONMENT &&
              !revoked;
            const editLabel =
              row.label === "api_key" || row.label === "user_key"
                ? (row.label as "api_key" | "user_key")
                : null;
            return (
              <li
                key={row.id}
                className="flex items-center justify-between px-3 py-2 text-sm"
              >
                <div>
                  <span className="font-medium text-slate-800 dark:text-slate-100">
                    {row.label === "api_key"
                      ? "Public key"
                      : row.label === "user_key"
                        ? "Private key"
                        : row.label}
                  </span>
                  <span className="ml-2 text-xs text-slate-500">
                    {row.provider} · {row.environment} · ••••{row.last_four}
                  </span>
                  {revoked && (
                    <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                      revoked
                    </span>
                  )}
                </div>
                <div className="flex gap-2">
                  {mode === "complete" && isActiveEtoro && editLabel !== null && (
                    <button
                      type="button"
                      onClick={() => startEdit(editLabel)}
                      disabled={manageAction !== "idle"}
 className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
                    >
                      Edit
                    </button>
                  )}
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
                </div>
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

      {/* Complete mode — management panel */}
      {mode === "complete" && manageAction === "idle" && (
        <div className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
          <p className="text-sm text-slate-700">Credentials configured.</p>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => void handleTestStored()}
              disabled={validating}
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
            >
              {validating ? "Testing…" : "Test connection"}
            </button>
            <button
              type="button"
              onClick={startReplace}
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
            >
              Replace both
            </button>
          </div>
          <ValidationResultDisplay
            result={validationResult}
            error={validationError}
          />
        </div>
      )}

      {/* Complete mode — edit single key */}
      {mode === "complete" && (manageAction === "edit-api_key" || manageAction === "edit-user_key") && (
        <form
          onSubmit={handleEditSave}
          className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900"
        >
          <h3 className="text-sm font-medium text-slate-700">
            Edit {manageAction === "edit-api_key" ? "public key" : "private key"}
          </h3>
          <p className="text-xs text-slate-500">
            Enter the new value. The existing key will be revoked and replaced.
          </p>
          <label className="block text-sm">
            <span className="mb-1 block text-slate-600">
              {manageAction === "edit-api_key" ? "New public key" : "New private key"}
            </span>
            <input
              type="password"
              autoComplete="new-password"
              value={editSecret}
              onChange={(e) => setEditSecret(e.target.value)}
              minLength={MIN_SECRET_LEN}
              required
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
            />
          </label>
          {editError !== null && (
            <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
              {editError}
            </div>
          )}
          <div className="flex gap-2">
            <button
              type="submit"
              disabled={editing || editSecret.length < MIN_SECRET_LEN}
              className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
            >
              {editing ? "Saving…" : "Save"}
            </button>
            <button
              type="button"
              onClick={cancelManage}
              disabled={editing}
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {/* Complete mode — replace both keys */}
      {mode === "complete" && manageAction === "replace" && (
        <form
          onSubmit={handleReplaceSave}
          className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900"
        >
          <h3 className="text-sm font-medium text-slate-700">Replace both keys</h3>
          <p className="text-xs text-slate-500">
            Enter new values for both keys. The existing pair will be revoked and replaced.
            You can test the new credentials before saving.
          </p>
          <label className="block text-sm">
            <span className="mb-1 block text-slate-600">New public key</span>
            <input
              type="password"
              name="broker-credential-api-key"
              autoComplete="new-password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              minLength={MIN_SECRET_LEN}
              required
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
            />
          </label>
          <label className="block text-sm">
            <span className="mb-1 block text-slate-600">New private key</span>
            <input
              type="password"
              name="broker-credential-user-key"
              autoComplete="new-password"
              value={userKey}
              onChange={(e) => setUserKey(e.target.value)}
              minLength={MIN_SECRET_LEN}
              required
              className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
            />
          </label>

          <div className="space-y-1">
            <button
              type="button"
              onClick={() => void handleTestConnection()}
              disabled={
                apiKey.length < MIN_SECRET_LEN ||
                userKey.length < MIN_SECRET_LEN ||
                validating
              }
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
            >
              {validating ? "Testing…" : "Test connection"}
            </button>
          </div>

          <ValidationResultDisplay
            result={validationResult}
            error={validationError}
          />

          {editError !== null && (
            <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
              {editError}
            </div>
          )}
          <div className="flex gap-2">
            <button
              type="submit"
              disabled={
                editing ||
                apiKey.length < MIN_SECRET_LEN ||
                userKey.length < MIN_SECRET_LEN
              }
              className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
            >
              {editing ? "Replacing…" : "Replace both"}
            </button>
            <button
              type="button"
              onClick={cancelManage}
              disabled={editing}
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {/* Create / Repair mode — initial setup form */}
      {mode !== "complete" && (
        <form
          onSubmit={handleCreate}
          className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900"
        >
          <h3 className="text-sm font-medium text-slate-700">
            {mode === "repair" ? "Complete credential setup" : "Add eToro credentials"}
          </h3>
          {mode === "repair" && (
            <p className="text-xs text-slate-500">
              One key was already saved. Enter the missing {missingLabel === "api_key" ? "public key" : "private key"} to
              complete the credential pair.
            </p>
          )}

          {showApiKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">Public key</span>
              <input
                type="password"
                name="broker-credential-api-key"
                autoComplete="new-password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
              />
            </label>
          )}

          {showUserKeyField && (
            <label className="block text-sm">
              <span className="mb-1 block text-slate-600">Private key</span>
              <input
                type="password"
                name="broker-credential-user-key"
                autoComplete="new-password"
                value={userKey}
                onChange={(e) => setUserKey(e.target.value)}
                minLength={MIN_SECRET_LEN}
                required
                className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:placeholder:text-slate-500"
              />
            </label>
          )}

          {/* Test connection — only available in Create mode (both keys present). */}
          <div className="space-y-1">
            <button
              type="button"
              onClick={() => void handleTestConnection()}
              disabled={!canTestConnection || validating}
 className="rounded border border-slate-300 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:text-slate-100 dark:hover:bg-slate-800"
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
    </section>
  );
}
