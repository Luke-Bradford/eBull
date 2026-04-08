/**
 * /login page (issue #98).
 *
 * Username + password form. On success the SessionProvider stores the
 * operator and we navigate to ?next=<path> if it is a safe same-origin
 * path, else to "/".
 *
 * Failure handling: any error (wrong password, rate limit, network)
 * renders a fixed phrase. We never echo backend error text -- the
 * backend itself uses a generic 401 phrase, but we still defend in
 * depth by not surfacing ApiError.message verbatim.
 */

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import { useSession } from "@/lib/session";

function safeNextPath(raw: string | null): string {
  if (!raw) return "/";
  // Only allow same-origin paths starting with "/" and not protocol-relative
  // ("//evil.example"). This blocks open-redirect attempts via crafted
  // ?next= values.
  if (!raw.startsWith("/") || raw.startsWith("//")) return "/";
  return raw;
}

export function LoginPage(): JSX.Element {
  const { status, login, bootstrapState } = useSession();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const next = safeNextPath(searchParams.get("next"));

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Already logged in? Skip the form and bounce to next.
  useEffect(() => {
    if (status === "authenticated") {
      navigate(next, { replace: true });
    }
  }, [status, next, navigate]);

  async function handleSubmit(e: FormEvent<HTMLFormElement>): Promise<void> {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await login(username, password);
      navigate(next, { replace: true });
    } catch (err: unknown) {
      if (err instanceof ApiError && err.status === 429) {
        setError("Too many login attempts. Please wait and try again.");
      } else {
        setError("Login failed. Check your username and password.");
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50">
      <form
        onSubmit={handleSubmit}
        className="w-full max-w-sm rounded border border-slate-200 bg-white p-6 shadow-sm"
      >
        <h1 className="mb-4 text-lg font-semibold text-slate-800">eBull operator</h1>
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
        <label className="mb-4 block text-sm">
          <span className="mb-1 block text-slate-600">Password</span>
          <input
            type="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            className="w-full rounded border border-slate-300 px-2 py-1.5 text-sm"
          />
        </label>
        {error !== null && (
          <div role="alert" className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
            {error}
          </div>
        )}
        <button
          type="submit"
          disabled={submitting || username === "" || password === ""}
          className="w-full rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
        >
          {submitting ? "Signing in…" : "Sign in"}
        </button>
        {/*
          ADR-0003 §6: the "Recover existing eBull data" link is
          shown ONLY when the most recent /auth/bootstrap-state
          response said recovery_required: true. In the normal
          case (recovery not required) this link must be hidden
          entirely — the recovery flow is not a routine option.
        */}
        {bootstrapState?.recovery_required === true && (
          <div className="mt-3 text-center text-xs">
            <a
              href="/recover"
              className="text-slate-600 underline hover:text-slate-800"
            >
              Recover existing eBull data
            </a>
          </div>
        )}
      </form>
    </div>
  );
}
