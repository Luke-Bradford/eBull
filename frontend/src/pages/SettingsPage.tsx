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
import { RecoveryPhraseConfirm } from "@/components/security/RecoveryPhraseConfirm";
import { Modal } from "@/components/ui/Modal";

const MIN_SECRET_LEN = 4;

/**
 * Recovery-phrase modal sub-state (#121 / ADR-0003 §5).
 *
 * The modal has two inner views and we model them explicitly rather
 * than threading two booleans:
 *
 *   - "confirm"        — RecoveryPhraseConfirm display + challenge
 *   - "confirm-cancel" — fail-closed gate the operator must pass through
 *                        when they try to dismiss the phrase via Cancel,
 *                        Escape, or the close button. Single misclicks
 *                        must not destroy the only copy of the phrase.
 *
 * On Confirm of the challenge OR on the operator confirming dismissal,
 * we drop the phrase from component state (it lives nowhere else) and
 * close the modal.
 */
type PhraseModalView = "confirm" | "confirm-cancel";

const CANCEL_WARNING_TEXT =
  "This recovery phrase will not be shown again. If you close this now, " +
  "you may lose recovery ability for this installation's broker " +
  "credentials unless you have backed up the app data directory.";

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

  // Recovery-phrase modal state. The phrase lives ONLY in this state
  // for the lifetime of the modal -- never written to localStorage,
  // sessionStorage, IndexedDB, console, or any cache. Cleared on
  // unmount-equivalent transitions (confirm, confirmed-cancel).
  const [phrase, setPhrase] = useState<readonly string[] | null>(null);
  const [phraseModalView, setPhraseModalView] =
    useState<PhraseModalView>("confirm");

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
      const response = await createBrokerCredential({ provider, label, secret });
      // Clear the secret immediately and re-fetch to show the row.
      // The form is intentionally NOT pre-populated from the response.
      setLabel("");
      setSecret("");
      // First-save-in-clean_install path: backend returns the 24-word
      // recovery phrase exactly once (#114 / ADR-0003 §4). Show the
      // confirmation modal. Note: the credential row is ALREADY
      // committed by the time we get here -- the modal cannot block
      // commit, only operator acknowledgement of the phrase. The
      // confirm-cancel gate inside the modal protects against a
      // misclick destroying the only copy of the phrase.
      //
      // We DEFER the list refresh until the modal closes. Otherwise
      // any error from `refresh()` would be set on `loadError` while
      // the modal covers the page, hiding it from the operator
      // (review feedback PR #125 round 1). When no modal opens we
      // refresh inline as before.
      if (response.recovery_phrase != null && response.recovery_phrase.length > 0) {
        setPhrase(response.recovery_phrase);
        setPhraseModalView("confirm");
      } else {
        await refresh();
      }
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

  function closePhraseModal(): void {
    // Single shared close path for both the challenge-confirm success
    // and the "Close anyway" branch (review feedback PR #125 round 2).
    // Responsibilities:
    //   - drop the phrase from state (it lives nowhere else)
    //   - reset the inner view back to "confirm" so the next time the
    //     modal opens it starts on the phrase, not on the warning
    //   - clear any stale createError from a prior failed attempt --
    //     otherwise after the modal closes the operator could see a
    //     leftover create error AND a fresh load error side by side
    //   - run the deferred list refresh now that the modal no longer
    //     covers the page, so any refresh failure (loadError) is
    //     visible to the operator (PR #125 round 1)
    // No revoke -- the credential row is already committed and the
    // phrase is root-secret scoped, not row scoped.
    setPhrase(null);
    setPhraseModalView("confirm");
    setCreateError(null);
    void refresh();
  }

  function requestPhraseDismiss(): void {
    // Routed from RecoveryPhraseConfirm.onCancel AND from the Modal's
    // Escape handler. Both go through the confirm-cancel gate -- a
    // single misclick or stray Escape must not destroy the only copy
    // of the phrase.
    setPhraseModalView("confirm-cancel");
  }

  function handleConfirmCancelKeep(): void {
    // "Go back" -- operator changed their mind, return to the phrase.
    setPhraseModalView("confirm");
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

      <Modal
        isOpen={phrase !== null}
        onRequestClose={requestPhraseDismiss}
        // aria-label (not aria-labelledby) on purpose: the dialog has
        // two inner views ("confirm" and "confirm-cancel") with
        // different visible headings, but the dialog as a whole has a
        // single stable accessible name. Trying to point
        // aria-labelledby at "whichever heading is currently mounted"
        // is fragile -- see PR #125 round 2 review.
        label="Recovery phrase confirmation"
      >
        {/*
          The Modal above is already gated on `isOpen={phrase !== null}`,
          so when these branches render `phrase` is guaranteed non-null.
          We still need the null check on the `RecoveryPhraseConfirm`
          phrase prop for TypeScript narrowing -- so it stays inline
          on the confirm branch only.
        */}
        {phrase !== null && phraseModalView === "confirm" ? (
          <RecoveryPhraseConfirm
            phrase={phrase}
            onConfirmed={closePhraseModal}
            onCancel={requestPhraseDismiss}
          />
        ) : null}
        {phraseModalView === "confirm-cancel" ? (
          <div className="flex flex-col gap-4">
            <h2 className="text-sm font-semibold text-slate-700">
              Close without confirming?
            </h2>
            <p className="text-xs text-slate-600">{CANCEL_WARNING_TEXT}</p>
            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={handleConfirmCancelKeep}
                className="rounded bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700"
              >
                Go back
              </button>
              <button
                type="button"
                onClick={closePhraseModal}
                className="rounded border border-rose-300 bg-white px-3 py-1.5 text-xs font-medium text-rose-700 hover:bg-rose-50"
              >
                Close anyway
              </button>
            </div>
          </div>
        ) : null}
      </Modal>
    </section>
  );
}
