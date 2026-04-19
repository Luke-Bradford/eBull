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
