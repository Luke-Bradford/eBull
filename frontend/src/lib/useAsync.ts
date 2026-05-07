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
 *
 * Stale-while-revalidate (#1016 obs 1+2):
 *
 *   On a poll-driven refetch (cadence-triggered, not user-triggered), pass
 *   `{ preserveOnRefetch: true }` to keep the prior `data` visible while
 *   the new request is in flight. Without this, every poll tick clears
 *   `data` to `null`, the consumer re-mounts its skeleton (different DOM
 *   shape than the rendered table), the browser loses scroll position,
 *   and the operator sees a flicker every 5s.
 *
 *   The default behaviour is unchanged (clear-on-refetch) — opt-in only,
 *   for callers that genuinely poll. Filter-driven refetches (operator
 *   clicks "filter by X") should NOT preserve stale data, because the
 *   prior payload is now semantically wrong, not just stale-by-time.
 *
 *   Even with `preserveOnRefetch: true`, a refetch that LANDS in error
 *   still clears `data` — a stale-but-correct payload is preferable to
 *   blank, but a stale-while-erroring payload is misleading. The
 *   `loading` flag stays `false` during preserved-data refetches; a
 *   separate `isRevalidating` flag exposes the in-flight refetch state
 *   for callers that want to surface a subtle indicator.
 */

import { useCallback, useEffect, useRef, useState } from "react";

export interface AsyncState<T> {
  data: T | null;
  error: unknown;
  loading: boolean;
  isRevalidating: boolean;
  refetch: () => void;
}

export interface UseAsyncOptions {
  /**
   * Keep `data` visible during refetch (#1016). Defaults to `false` for
   * backward compat. Set to `true` for poll-driven refetches where the
   * prior payload is stale-by-time, not stale-by-intent.
   */
  preserveOnRefetch?: boolean;
}

export function useAsync<T>(
  fn: () => Promise<T>,
  deps: ReadonlyArray<unknown>,
  options: UseAsyncOptions = {},
): AsyncState<T> {
  const { preserveOnRefetch = false } = options;
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [isRevalidating, setIsRevalidating] = useState<boolean>(false);
  const [tick, setTick] = useState(0);

  // Capture the latest `fn` in a ref so refetch() always runs the freshest
  // closure without forcing callers to memoise their fetcher.
  const fnRef = useRef(fn);
  fnRef.current = fn;

  // Track whether we've ever resolved successfully — `preserveOnRefetch`
  // only kicks in after the first successful load. Pre-load (initial
  // mount) still shows the skeleton, otherwise the consumer sees an
  // empty table on first paint instead of a clear loading state.
  const hasLoadedRef = useRef(false);

  useEffect(() => {
    let cancelled = false;
    const isRefetch = tick > 0;
    const preserve = preserveOnRefetch && isRefetch && hasLoadedRef.current;

    if (preserve) {
      // Stale-while-revalidate: keep `data` visible, surface revalidation
      // via the dedicated flag instead. `loading` stays false so the
      // consumer's `loading ? <Skeleton/> : <Body/>` branch keeps showing
      // the body — no flicker, no scroll-jump.
      setIsRevalidating(true);
    } else {
      setLoading(true);
      setError(null);
      setData(null);
    }

    fnRef
      .current()
      .then((result) => {
        if (cancelled) return;
        setData(result);
        setLoading(false);
        setIsRevalidating(false);
        hasLoadedRef.current = true;
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setError(err);
        setLoading(false);
        setIsRevalidating(false);
        if (preserve) {
          // Stale-while-erroring is misleading; clear data on the failed
          // refetch even when preservation was requested. Operator sees
          // the error rather than a stale payload alongside an error
          // banner.
          setData(null);
        }
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);

  const refetch = useCallback(() => {
    setTick((t) => t + 1);
  }, []);

  return { data, error, loading, isRevalidating, refetch };
}
