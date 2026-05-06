/**
 * SessionProvider + useSession hook (issue #98).
 *
 * Owns:
 *   - in-memory copy of the current operator (or null when logged out)
 *   - bootstrap call to /auth/me on mount
 *   - the 401 interceptor wired into apiFetch via setUnauthorizedHandler
 *   - login / logout flows that update local state and navigate
 *
 * The cookie itself is HttpOnly and not visible to JS. This module never
 * tries to read it; it only tracks the *result* of /auth/me, which is the
 * authoritative "do I have a valid session" probe.
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useNavigate } from "react-router-dom";

import { setUnauthorizedHandler } from "@/api/client";
import * as authApi from "@/api/auth";
import type { Operator } from "@/api/auth";

type Status =
  | "loading"
  | "authenticated"
  | "unauthenticated"
  | "needs_setup";

interface SessionContextValue {
  status: Status;
  operator: Operator | null;
  bootstrapState: authApi.BootstrapStateResponse | null;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  // Called by SetupPage on a successful POST /auth/setup. Mirrors the
  // login flow — the cookie is already set by the response; this just
  // updates in-memory state so RequireAuth lets us through.
  markAuthenticated: (op: Operator) => void;
  /**
   * Re-fetch /auth/bootstrap-state and re-apply the routing rule
   * (post-amendment 2026-05-07: needs_setup → /setup, otherwise →
   * normal). Runs at app load (handled internally) and after first-run
   * setup completion.
   */
  refreshBootstrapState: () => Promise<void>;
}

const SessionContext = createContext<SessionContextValue | null>(null);

export function useSession(): SessionContextValue {
  const ctx = useContext(SessionContext);
  if (!ctx) throw new Error("useSession must be used within <SessionProvider>");
  return ctx;
}

export function SessionProvider({ children }: { children: ReactNode }): JSX.Element {
  const [status, setStatus] = useState<Status>("loading");
  const [operator, setOperator] = useState<Operator | null>(null);
  const [bootstrapState, setBootstrapState] =
    useState<authApi.BootstrapStateResponse | null>(null);
  const navigate = useNavigate();

  // Stash the latest navigate / setStatus in refs so the 401 handler does
  // not need to be re-registered on every render. The handler runs on a
  // module singleton -- the freshest closure must read the freshest state.
  const stateRef = useRef({ status, operator });
  stateRef.current = { status, operator };

  const handleUnauthorized = useCallback(() => {
    setOperator(null);
    setStatus("unauthenticated");
    // Preserve the current path so post-login lands the user back where
    // they were (validated as same-origin in LoginPage).
    const next = window.location.pathname + window.location.search;
    if (window.location.pathname !== "/login") {
      navigate(`/login?next=${encodeURIComponent(next)}`, { replace: true });
    }
  }, [navigate]);

  // Register the 401 interceptor exactly once. The handler closes over a
  // stable callback (handleUnauthorized) which itself uses navigate.
  useEffect(() => {
    setUnauthorizedHandler(handleUnauthorized);
    return () => setUnauthorizedHandler(null);
  }, [handleUnauthorized]);

  // Bootstrap (post-amendment 2026-05-07 ADR-0003):
  //
  //   1. Probe /auth/bootstrap-state. The response is fetched with
  //      `cache: "no-store"` (see api/auth.ts) and the backend sets
  //      `Cache-Control: no-store`, so the routing decision is always
  //      derived from a live network response.
  //
  //   2. Apply the routing rule:
  //        needs_setup: true → status "needs_setup"
  //        otherwise         → fall through to /auth/me
  //
  //   3. Probe /auth/me only when needs_setup is false. 401 → normal
  //      "unauthenticated" path for a fresh visitor with no cookie.
  //
  // The "fetch + apply" half of step 1+2 is factored into
  // `applyBootstrapState` so the post-setup caller can re-run exactly
  // the same logic without copy-pasting it.
  const applyBootstrapState = useCallback(
    async (cancelled?: { current: boolean }): Promise<void> => {
      let probe: authApi.BootstrapStateResponse | null = null;
      try {
        probe = await authApi.getBootstrapState();
      } catch {
        // bootstrap-state is public; failure here means the backend is
        // unreachable. Fall through to the getMe path which will
        // surface the same problem via the unauthenticated state.
      }
      if (cancelled?.current) return;
      setBootstrapState(probe);

      if (probe?.needs_setup) {
        setOperator(null);
        setStatus("needs_setup");
        return;
      }

      try {
        const op = await authApi.getMe();
        if (cancelled?.current) return;
        setOperator(op);
        setStatus("authenticated");
      } catch {
        if (cancelled?.current) return;
        setOperator(null);
        setStatus("unauthenticated");
      }
    },
    [],
  );

  useEffect(() => {
    const cancelled = { current: false };
    void applyBootstrapState(cancelled);
    return () => {
      cancelled.current = true;
    };
  }, [applyBootstrapState]);

  const refreshBootstrapState = useCallback(async (): Promise<void> => {
    await applyBootstrapState();
  }, [applyBootstrapState]);

  const doLogin = useCallback(
    async (username: string, password: string) => {
      const { operator: op } = await authApi.login(username, password);
      setOperator(op);
      setStatus("authenticated");
    },
    [],
  );

  const doLogout = useCallback(async () => {
    try {
      await authApi.logout();
    } finally {
      setOperator(null);
      setStatus("unauthenticated");
      navigate("/login", { replace: true });
    }
  }, [navigate]);

  const markAuthenticated = useCallback((op: Operator) => {
    setOperator(op);
    setStatus("authenticated");
  }, []);

  const value = useMemo<SessionContextValue>(
    () => ({
      status,
      operator,
      bootstrapState,
      login: doLogin,
      logout: doLogout,
      markAuthenticated,
      refreshBootstrapState,
    }),
    [
      status,
      operator,
      bootstrapState,
      doLogin,
      doLogout,
      markAuthenticated,
      refreshBootstrapState,
    ],
  );

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
