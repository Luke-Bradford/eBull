/**
 * /settings page.
 *
 * Currently hosts the broker credentials section (issue #99 / Ticket B).
 * Runtime config + kill switch (#65) will land alongside this section
 * once that ticket ships.
 */

import { useCallback, useEffect, useState } from "react";
import type { FormEvent } from "react";

import { ApiError } from "@/api/client";
import {
  type BrokerCredentialView,
  createBrokerCredential,
  listBrokerCredentials,
  revokeBrokerCredential,
} from "@/api/brokerCredentials";

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

  // Add-form local state. Secret is stored only in this state, not in
  // any context or query cache, and is cleared on successful submit.
  const [provider, setProvider] = useState<"etoro">("etoro");
  const [label, setLabel] = useState("");
  const [secret, setSecret] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  const [busyId, setBusyId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

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

  async function handleCreate(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setCreateError(null);
    setCreating(true);
    try {
      await createBrokerCredential({ provider, label, secret });
      // Clear the secret immediately and re-fetch to show the row.
      // The form is intentionally NOT pre-populated from the response.
      setLabel("");
      setSecret("");
      await refresh();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setCreateError("A credential with that label already exists for this provider.");
      } else if (err instanceof ApiError && err.status === 400) {
        setCreateError("Provider, label, or secret is invalid.");
      } else {
        setCreateError("Could not save credential.");
      }
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(row: BrokerCredentialView): Promise<void> {
    setActionError(null);
    if (
      !window.confirm(
        `Revoke "${row.label}" (${row.provider})? This cannot be undone.`,
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

  return (
    <section className="space-y-4">
      <div>
        <h2 className="text-sm font-medium text-slate-700">Broker credentials</h2>
        <p className="text-xs text-slate-500">
          Encrypted broker secrets stored against your operator account. eBull uses
          these to place orders -- the plaintext value is never returned to this UI.
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
                    {row.provider} · ••••{row.last_four}
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

      <form
        onSubmit={handleCreate}
        className="max-w-sm space-y-3 rounded border border-slate-200 bg-white p-4"
      >
        <h3 className="text-sm font-medium text-slate-700">Add credential</h3>
        <label className="block text-sm">
          <span className="mb-1 block text-slate-600">Provider</span>
          <select
            value={provider}
            onChange={(e) => setProvider(e.target.value as "etoro")}
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          >
            <option value="etoro">eToro</option>
          </select>
        </label>
        <label className="block text-sm">
          <span className="mb-1 block text-slate-600">Label</span>
          <input
            type="text"
            autoComplete="off"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            required
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          />
        </label>
        <label className="block text-sm">
          <span className="mb-1 block text-slate-600">Secret</span>
          <input
            type="password"
            autoComplete="off"
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
            minLength={MIN_SECRET_LEN}
            required
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          />
        </label>
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
          disabled={
            creating ||
            label.trim() === "" ||
            secret.length < MIN_SECRET_LEN
          }
          className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
        >
          {creating ? "Saving…" : "Save credential"}
        </button>
      </form>
    </section>
  );
}
