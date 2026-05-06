/**
 * /setup page — first-run wizard (single-step, post-amendment 2026-05-07).
 *
 * Captures the first operator's username, password, and (if running in
 * non-loopback bootstrap-token mode) the one-shot setup token printed
 * to the server log. POST /auth/setup. Generic-error mapping is
 * preserved — every non-2xx surfaces the same fixed string so the page
 * never leaks which input was wrong (#98).
 *
 * Broker credentials are no longer captured in the wizard (#971). The
 * operator adds eToro keys in Settings after setup completes; the
 * dashboard surfaces a "add eToro credentials" banner when no creds
 * exist (BootstrapProgress).
 */

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { useSession } from "@/lib/session";
import { GENERIC_ERROR } from "@/pages/setupErrorMessages";
import { useSetupWizard } from "@/pages/useSetupWizard";

const MIN_PASSWORD_LEN = 12;

// Re-export so tests that import GENERIC_ERROR via this module keep working.
export { GENERIC_ERROR };

export function SetupPage(): JSX.Element {
  const { status, markAuthenticated } = useSession();
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [setupToken, setSetupToken] = useState("");

  const wizard = useSetupWizard({
    onComplete: () => {
      const op = wizard.state.pendingOperator;
      if (op !== null) {
        markAuthenticated(op);
      }
      navigate("/", { replace: true });
    },
  });

  useEffect(() => {
    if (status === "authenticated") {
      navigate("/", { replace: true });
    } else if (status === "unauthenticated") {
      navigate("/login", { replace: true });
    }
  }, [status, navigate]);

  async function handleOperatorSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    await wizard.submitOperator({ username, password, setupToken });
  }

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  const submitting = wizard.state.operatorSubmitting;
  const error = wizard.state.operatorError;

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50 dark:bg-slate-900/40">
      <form
        onSubmit={handleOperatorSubmit}
        className="w-full max-w-sm rounded border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 shadow-sm"
      >
        <h1 className="mb-1 text-lg font-semibold text-slate-800 dark:text-slate-100">
          First-run setup
        </h1>
        <p className="mb-4 text-xs text-slate-500">
          Create the first operator for this eBull instance. If the server
          printed a setup token at startup, paste it below. eToro
          credentials are configured in Settings after sign-in.
        </p>
        <label className="mb-3 block text-sm">
          <span className="mb-1 block text-slate-600">Username</span>
          <input
            type="text"
            autoComplete="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
            className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
          />
        </label>
        <label className="mb-3 block text-sm">
          <span className="mb-1 block text-slate-600">
            Password{" "}
            <span className="text-slate-400">(min {MIN_PASSWORD_LEN} chars)</span>
          </span>
          <input
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            minLength={MIN_PASSWORD_LEN}
            required
            className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 text-sm"
          />
        </label>
        <label className="mb-4 block text-sm">
          <span className="mb-1 block text-slate-600">
            Setup token{" "}
            <span className="text-slate-400">(optional on loopback)</span>
          </span>
          <input
            type="text"
            autoComplete="off"
            value={setupToken}
            onChange={(e) => setSetupToken(e.target.value)}
            className="w-full rounded border border-slate-300 dark:border-slate-700 px-2 py-1.5 font-mono text-xs"
          />
        </label>
        {error !== null && (
          <div
            role="alert"
            className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700 dark:bg-rose-950/40 dark:text-rose-300"
          >
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
