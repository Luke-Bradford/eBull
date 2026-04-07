/**
 * Minimal async-data hook for the dashboard (#60).
 *
 * This is intentionally tiny — no caching, no deduping, no background
 * refetch. Each call site owns its own request lifecycle so that one
 * failing endpoint cannot blank an unrelated section of the page.
 *
 * If a second page needs caching/refetch/staleness, revisit and consider
 * adopting @tanstack/react-query (already in package.json) deliberately
 * rather than growing this hook into a half-baked client.
 *
 * Contract:
 *   - `fn` is invoked once per change in `deps` (passed straight to useEffect).
 *   - Concurrent invocations are guarded with a `cancelled` flag so a stale
 *     resolution cannot overwrite a newer one.
 *   - `refetch()` re-runs the latest `fn` without changing `deps`.
 *   - Errors are surfaced as the raw `unknown` thrown — callers render a
 *     fixed phrase, never the message text (mirrors ErrorBoundary policy).
 */

import { useCallback, useEffect, useRef, useState } from "react";

export interface AsyncState<T> {
  data: T | null;
  error: unknown;
  loading: boolean;
  refetch: () => void;
}

export function useAsync<T>(fn: () => Promise<T>, deps: ReadonlyArray<unknown>): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [tick, setTick] = useState(0);

  // Capture the latest `fn` in a ref so refetch() always runs the freshest
  // closure without forcing callers to memoise their fetcher.
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fnRef
      .current()
      .then((result) => {
        if (cancelled) return;
        setData(result);
        setLoading(false);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setError(err);
        setLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);

  const refetch = useCallback(() => {
    setTick((t) => t + 1);
  }, []);

  return { data, error, loading, refetch };
}
