/**
 * <RequireAuth> wraps protected routes (issue #98).
 *
 * Behaviour:
 *   - status === "loading":      render a minimal placeholder so the app
 *                                does not flash logged-out content for the
 *                                fraction of a second between mount and
 *                                /auth/me resolving.
 *   - status === "unauthenticated": redirect to /login?next=<current path>
 *   - status === "authenticated":   render children.
 *
 * No fancy spinner -- the loading window is intentionally tiny and any
 * heavy chrome here would flicker on every cold load.
 */

import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { useSession } from "@/lib/session";

export function RequireAuth({ children }: { children: ReactNode }): JSX.Element {
  const { status } = useSession();
  const location = useLocation();

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  if (status === "unauthenticated") {
    const next = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`/login?next=${next}`} replace />;
  }

  return <>{children}</>;
}
