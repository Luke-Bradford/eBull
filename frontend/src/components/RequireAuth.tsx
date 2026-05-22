/**
 * <RequireAuth> wraps protected routes (issue #98).
 *
 * Behaviour:
 *   - status === "loading":      render a minimal placeholder so the app
 *                                does not flash logged-out content for the
 *                                fraction of a second between mount and
 *                                /auth/me resolving.
 *   - status === "needs_setup":  redirect to /setup
 *   - status === "unauthenticated": redirect to /login?next=<current path>
 *   - status === "authenticated" + no active broker_credentials:
 *                                redirect to /settings UNLESS already there
 *                                (eBull is eToro-binding; main app inert
 *                                without credentials — CLAUDE.md I12).
 *   - status === "authenticated":   render children.
 *
 * No fancy spinner -- the loading window is intentionally tiny and any
 * heavy chrome here would flicker on every cold load.
 */

import type { ReactNode } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { useSession } from "@/lib/session";

// Routes that an authenticated operator can reach with no active
// broker_credentials. /settings is where the add-credentials form lives;
// /logout must always be reachable. Keep this list intentionally tiny.
const BROKER_FREE_PATHS: readonly string[] = ["/settings", "/logout"];

export function RequireAuth({ children }: { children: ReactNode }): JSX.Element {
  const { status, bootstrapState } = useSession();
  const location = useLocation();

  if (status === "loading") {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  if (status === "needs_setup") {
    return <Navigate to="/setup" replace />;
  }

  if (status === "unauthenticated") {
    const next = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`/login?next=${next}`} replace />;
  }

  // Authenticated but no active broker credentials → force /settings.
  // The check is strict path-prefix so sub-routes of /settings (e.g.
  // /settings/something) stay reachable; everything else redirects.
  if (
    bootstrapState?.needs_broker_credentials &&
    !BROKER_FREE_PATHS.some((p) => location.pathname === p || location.pathname.startsWith(p + "/"))
  ) {
    return <Navigate to="/settings" replace />;
  }

  return <>{children}</>;
}
