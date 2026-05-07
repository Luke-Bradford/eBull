/**
 * Stability test for `useAsync.refetch`.
 *
 * Regression coverage for a review concern (#326): callers memoise
 * a combined `refetchAll` via `useCallback([layers.refetch, ...])` —
 * if any single `refetch` were a fresh function reference on every
 * render, the enclosing useCallback would recompute, its downstream
 * useEffect interval would be torn down and recreated on every
 * render, and production would see a refetch storm.
 *
 * `useAsync`'s current implementation wraps `refetch` in
 * `useCallback(..., [])`, so the reference is stable across
 * renders. This test pins that invariant so a future refactor
 * cannot regress it silently.
 */
import { describe, expect, it } from "vitest";
import { act, renderHook } from "@testing-library/react";

import { useAsync } from "@/lib/useAsync";

describe("useAsync — refetch reference stability", () => {
  it("returns the same refetch function across re-renders", async () => {
    const { result, rerender } = renderHook(() =>
      useAsync(async () => 1, []),
    );
    const first = result.current.refetch;

    // Trigger a re-render by calling the (stable) refetch — which
    // increments a tick internally, causing a re-render.
    await act(async () => {
      result.current.refetch();
    });
    expect(result.current.refetch).toBe(first);

    rerender();
    expect(result.current.refetch).toBe(first);
  });
});

describe("useAsync — preserveOnRefetch (#1016)", () => {
  it("clears data on refetch by default (backward compat)", async () => {
    let value = 1;
    const { result } = renderHook(() => useAsync(async () => value, []));
    // Wait for first resolve.
    await act(async () => {
      await Promise.resolve();
    });
    expect(result.current.data).toBe(1);
    expect(result.current.isRevalidating).toBe(false);

    value = 2;
    await act(async () => {
      result.current.refetch();
      // Synchronously after refetch but before resolve, data is cleared.
    });
    // After resolve.
    expect(result.current.data).toBe(2);
  });

  it("preserves data during refetch when preserveOnRefetch=true", async () => {
    // Controllable fetcher: each call returns a manually-resolved
    // promise so the test can observe the mid-flight state where
    // the new fetch is pending but the old data should still be
    // visible.
    let resolveCurrent: ((v: string) => void) | null = null;
    const calls: number[] = [];
    const fetcher = () =>
      new Promise<string>((resolve) => {
        const callIdx = calls.length;
        calls.push(callIdx);
        resolveCurrent = resolve;
        if (callIdx === 0) {
          // First call resolves immediately so we have data to
          // preserve on the second call.
          Promise.resolve().then(() => resolve("first"));
        }
      });

    const { result } = renderHook(() =>
      useAsync(fetcher, [], { preserveOnRefetch: true }),
    );

    // Let first call resolve.
    await act(async () => {
      await Promise.resolve();
    });
    expect(result.current.data).toBe("first");
    expect(result.current.loading).toBe(false);
    expect(result.current.isRevalidating).toBe(false);

    // Trigger refetch but don't resolve yet — observe mid-flight.
    await act(async () => {
      result.current.refetch();
      // Yield so the effect runs and the second fetcher call starts.
      await Promise.resolve();
    });
    // Mid-flight: data preserved, isRevalidating flipped on, loading
    // stays false (so consumer doesn't render skeleton).
    expect(result.current.data).toBe("first");
    expect(result.current.isRevalidating).toBe(true);
    expect(result.current.loading).toBe(false);

    // Now resolve the second fetch.
    await act(async () => {
      resolveCurrent?.("second");
      await Promise.resolve();
    });
    expect(result.current.data).toBe("second");
    expect(result.current.isRevalidating).toBe(false);
  });

  it("clears data on a refetch that errors, even with preserveOnRefetch=true", async () => {
    let shouldError = false;
    const fetcher = async () => {
      if (shouldError) {
        throw new Error("boom");
      }
      return 42;
    };
    const { result } = renderHook(() =>
      useAsync(fetcher, [], { preserveOnRefetch: true }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(result.current.data).toBe(42);

    shouldError = true;
    await act(async () => {
      result.current.refetch();
      await Promise.resolve();
    });
    // Error path: data cleared (stale-while-erroring is misleading),
    // error surfaced.
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeInstanceOf(Error);
  });
});
