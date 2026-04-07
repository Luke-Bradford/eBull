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

import { ApiError, setUnauthorizedHandler } from "@/api/client";
import * as authApi from "@/api/auth";
import type { Operator } from "@/api/auth";

type Status = "loading" | "authenticated" | "unauthenticated";

interface SessionContextValue {
  status: Status;
  operator: Operator | null;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
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

  // Bootstrap: probe /auth/me on first mount. A 401 here is the normal
  // path for a fresh visitor with no cookie -- we drop into the
  // unauthenticated state and rely on RequireAuth to redirect.
  useEffect(() => {
    let cancelled = false;
    authApi
      .getMe()
      .then((op) => {
        if (cancelled) return;
        setOperator(op);
        setStatus("authenticated");
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 401) {
          // handleUnauthorized was already invoked by the interceptor;
          // it set state to unauthenticated. Nothing else to do.
          return;
        }
        // Any non-401 error during bootstrap is treated as unauthenticated
        // for safety -- we never render the protected app on an unknown
        // session state.
        setOperator(null);
        setStatus("unauthenticated");
      });
    return () => {
      cancelled = true;
    };
  }, []);

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

  const value = useMemo<SessionContextValue>(
    () => ({ status, operator, login: doLogin, logout: doLogout }),
    [status, operator, doLogin, doLogout],
  );

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
