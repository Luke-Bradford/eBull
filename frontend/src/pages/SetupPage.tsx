/**
 * /setup page (issue #106 / Ticket G).
 *
 * Shown when SessionProvider's bootstrap call to /auth/setup-status
 * returns needs_setup=true. Captures the first operator's username,
 * password, and (if running in non-loopback bootstrap-token mode) the
 * one-shot setup token printed to the server log.
 *
 * Generic-404 mapping: every non-2xx from POST /auth/setup is rendered
 * as the same fixed string. The backend deliberately collapses
 * already-setup, bad token, missing token, bad password, and bad
 * username into one 404 -- we honour that here so the page never leaks
 * which input was wrong.
 */

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { postSetup } from "@/api/auth";
import { useSession } from "@/lib/session";

const GENERIC_ERROR = "Setup unavailable or invalid token.";
const MIN_PASSWORD_LEN = 12;

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // If the bootstrap discovered we are already set up, the user does
  // not belong on /setup -- bounce to the appropriate place.
  useEffect(() => {
    if (status === "authenticated") {
      navigate("/", { replace: true });
    } else if (status === "unauthenticated") {
      navigate("/login", { replace: true });
    }
  }, [status, navigate]);

  async function handleSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const { operator } = await postSetup(
        username,
        password,
        setupToken.trim() === "" ? null : setupToken.trim(),
      );
      // Cookie is already on the response; flip in-memory state and go.
      markAuthenticated(operator);
      navigate("/", { replace: true });
    } catch {
      // Single fixed phrase for every failure mode -- matches the
      // backend's generic-404 discipline.
      setError(GENERIC_ERROR);
    } finally {
      setSubmitting(false);
    }
  }

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50">
      <form
        onSubmit={handleSubmit}
        className="w-full max-w-sm rounded border border-slate-200 bg-white p-6 shadow-sm"
      >
        <h1 className="mb-1 text-lg font-semibold text-slate-800">First-run setup</h1>
        <p className="mb-4 text-xs text-slate-500">
          Create the first operator for this eBull instance. If the server printed a
          setup token at startup, paste it below.
        </p>
        <label className="mb-3 block text-sm">
          <span className="mb-1 block text-slate-600">Username</span>
          <input
            type="text"
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          />
        </label>
        <label className="mb-3 block text-sm">
          <span className="mb-1 block text-slate-600">
            Password <span className="text-slate-400">(min {MIN_PASSWORD_LEN} chars)</span>
          </span>
          <input
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            minLength={MIN_PASSWORD_LEN}
            required
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          />
        </label>
        <label className="mb-4 block text-sm">
          <span className="mb-1 block text-slate-600">
            Setup token <span className="text-slate-400">(optional on loopback)</span>
          </span>
          <input
            type="text"
            autoComplete="off"
            value={setupToken}
            onChange={(e) => setSetupToken(e.target.value)}
            className="w-full rounded border border-slate-300 px-2 py-1.5 font-mono text-xs"
          />
        </label>
        {error !== null && (
          <div role="alert" className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
            {error}
          </div>
        )}
        <button
          type="submit"
          disabled={
            submitting || username === "" || password.length < MIN_PASSWORD_LEN
          }
          className="w-full rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
        >
          {submitting ? "Creating…" : "Create operator"}
        </button>
      </form>
    </div>
  );
}
