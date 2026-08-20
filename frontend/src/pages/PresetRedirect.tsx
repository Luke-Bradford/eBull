import { Navigate, useLocation } from "react-router-dom";

/**
 * Route shim for the four pages the Research hub (#1917) subsumed
 * (`/instruments`, `/rankings`, `/theses`, `/recommendations`). Redirects to
 * `/research?view=…` while FORWARDING the incoming query string, so existing
 * deep links + bookmarks survive — e.g. the AlertsStrip's
 * `/theses?held=true&stale=true` lands on `/research?held=true&stale=true&view=theses`
 * and the Theses lens reads those params unchanged.
 */
export function PresetRedirect({ view }: { view: string }): JSX.Element {
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  params.set("view", view);
  return <Navigate to={`/research?${params.toString()}`} replace />;
}
