/**
 * /operators page (issue #106 / Ticket G).
 *
 * Operator management surface for an authenticated session:
 *   - list every operator with an "is self" marker
 *   - create another operator (does NOT log them in)
 *   - delete any operator, including self (subject to last-operator block)
 *
 * On a successful self-delete the backend has already cleared the
 * session cookie via Set-Cookie; we drive in-memory state by navigating
 * to /login (the next request will 401 and the session provider will
 * settle into unauthenticated cleanly).
 */

import { useCallback, useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import {
  type OperatorView,
  createOperator,
  deleteOperator,
  listOperators,
} from "@/api/operators";
import { useSession } from "@/lib/session";

const MIN_PASSWORD_LEN = 12;

export function OperatorsPage(): JSX.Element {
  const { logout } = useSession();
  const navigate = useNavigate();

  const [rows, setRows] = useState<OperatorView[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  const [busyId, setBusyId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoadError(null);
    try {
      const data = await listOperators();
      setRows(data);
    } catch {
      setRows([]);
      setLoadError("Could not load operators.");
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
      await createOperator(newUsername, newPassword);
      setNewUsername("");
      setNewPassword("");
      await refresh();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setCreateError("That username is already taken.");
      } else if (err instanceof ApiError && err.status === 400) {
        setCreateError("Username or password is invalid.");
      } else {
        setCreateError("Could not create operator.");
      }
    } finally {
      setCreating(false);
    }
  }

  async function handleDelete(row: OperatorView): Promise<void> {
    setActionError(null);
    const label = row.is_self ? "yourself" : `operator "${row.username}"`;
    if (!window.confirm(`Delete ${label}? This cannot be undone.`)) return;
    setBusyId(row.id);
    try {
      await deleteOperator(row.id);
      if (row.is_self) {
        // Backend cleared the cookie + session row in the same tx.
        // Tear down the in-memory session and bounce to /login.
        await logout().catch(() => undefined);
        navigate("/login", { replace: true });
        return;
      }
      await refresh();
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 409) {
        setActionError("Cannot delete the only remaining operator.");
      } else if (err instanceof ApiError && err.status === 404) {
        setActionError("That operator no longer exists.");
        await refresh();
      } else {
        setActionError("Could not delete operator.");
      }
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Operators</h1>
        <p className="text-xs text-slate-500">
          Manage browser logins for this eBull instance. New operators created here
          must sign in via /login -- they are not auto-logged-in.
        </p>
      </div>

      {loadError !== null && (
        <div role="alert" className="rounded bg-rose-50 px-3 py-2 text-xs text-rose-700">
          {loadError}
        </div>
      )}

      <section>
        <h2 className="mb-2 text-sm font-medium text-slate-700">Existing operators</h2>
        {rows === null ? (
          <p className="text-xs text-slate-400">Loading…</p>
        ) : rows.length === 0 ? (
          <p className="text-xs text-slate-400">No operators.</p>
        ) : (
          <ul className="divide-y divide-slate-200 rounded border border-slate-200 bg-white dark:divide-slate-800 dark:border-slate-800 dark:bg-slate-900">
            {rows.map((row) => (
              <li
                key={row.id}
                className="flex items-center justify-between px-3 py-2 text-sm"
              >
                <div>
                  <span className="font-medium text-slate-800 dark:text-slate-100">{row.username}</span>
                  {row.is_self && (
                    <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-slate-500 dark:bg-slate-800 dark:text-slate-300">
                      you
                    </span>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => void handleDelete(row)}
                  disabled={busyId === row.id}
                  className="rounded border border-rose-300 px-2 py-1 text-xs text-rose-700 hover:bg-rose-50 disabled:opacity-50"
                >
                  {busyId === row.id ? "Deleting…" : "Delete"}
                </button>
              </li>
            ))}
          </ul>
        )}
        {actionError !== null && (
          <p role="alert" className="mt-2 text-xs text-rose-700">
            {actionError}
          </p>
        )}
      </section>

      <section>
        <h2 className="mb-2 text-sm font-medium text-slate-700">Add operator</h2>
        <form
          onSubmit={handleCreate}
          className="max-w-sm space-y-3 rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4"
        >
          <label className="block text-sm">
            <span className="mb-1 block text-slate-600">Username</span>
            <input
              type="text"
              autoComplete="off"
              value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)}
              required
              className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
            />
          </label>
          <label className="block text-sm">
            <span className="mb-1 block text-slate-600">
              Password <span className="text-slate-400">(min {MIN_PASSWORD_LEN} chars)</span>
            </span>
            <input
              type="password"
              autoComplete="new-password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              minLength={MIN_PASSWORD_LEN}
              required
              className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
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
              newUsername === "" ||
              newPassword.length < MIN_PASSWORD_LEN
            }
            className="rounded bg-slate-800 px-3 py-1.5 text-sm font-medium text-white disabled:bg-slate-400"
          >
            {creating ? "Creating…" : "Create operator"}
          </button>
        </form>
      </section>
    </div>
  );
}
