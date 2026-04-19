import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";

import { ApiError } from "@/api/client";
import { useSyncTrigger } from "@/lib/useSyncTrigger";

vi.mock("@/api/sync", () => ({ triggerSync: vi.fn() }));

import { triggerSync } from "@/api/sync";

const mockedTrigger = vi.mocked(triggerSync);

beforeEach(() => {
  mockedTrigger.mockReset();
});
afterEach(() => vi.useRealTimers());

function setup() {
  const onTriggered = vi.fn();
  const hook = renderHook(() => useSyncTrigger(onTriggered));
  return { hook, onTriggered };
}

describe("useSyncTrigger", () => {
  it("starts idle", () => {
    const { hook } = setup();
    expect(hook.result.current.kind).toBe("idle");
    expect(hook.result.current.queuedRunId).toBeNull();
  });

  it("transitions to queued on success and invokes onTriggered", async () => {
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 42,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    const { hook, onTriggered } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });

    expect(hook.result.current.kind).toBe("queued");
    expect(hook.result.current.queuedRunId).toBe(42);
    expect(onTriggered).toHaveBeenCalledTimes(1);
  });

  it("surfaces 409 as 'Sync already running'", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, "conflict"));
    const { hook, onTriggered } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });

    expect(hook.result.current.kind).toBe("error");
    expect(hook.result.current.message).toBe("Sync already running");
    expect(onTriggered).not.toHaveBeenCalled();
  });

  it("surfaces 503 as 'Sync orchestrator disabled'", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(503, "disabled"));
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.message).toBe("Sync orchestrator disabled");
  });

  it("guards against a second click while already running", async () => {
    // First call returns a promise we never resolve so state stays in
    // "running" — representative of the in-flight window.
    let resolveFirst: (v: { sync_run_id: number; plan: { layers_to_refresh: []; layers_skipped: [] } }) => void = () => undefined;
    mockedTrigger.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveFirst = resolve;
        }),
    );
    const { hook } = setup();
    await act(async () => {
      void hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("running");

    // Second click while `running` — no extra POST, no state change.
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(mockedTrigger).toHaveBeenCalledTimes(1);

    // Let the first finish to avoid an unresolved promise warning.
    await act(async () => {
      resolveFirst({
        sync_run_id: 1,
        plan: { layers_to_refresh: [], layers_skipped: [] },
      });
    });
  });

  it("clearQueued(true) drops the queued badge once server confirms running", async () => {
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 7,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("queued");

    act(() => hook.result.current.clearQueued(true));
    expect(hook.result.current.kind).toBe("idle");
  });

  it("clearQueued(false) is a no-op while queued (still waiting)", async () => {
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 7,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    act(() => hook.result.current.clearQueued(false));
    expect(hook.result.current.kind).toBe("queued");
  });

  it("clearQueued does NOT auto-reset an error state", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, ""));
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    act(() => hook.result.current.clearQueued(true));
    expect(hook.result.current.kind).toBe("error");
  });

  it("re-triggers after a successful cycle (trigger → queued → idle → trigger)", async () => {
    // Regression: inFlightRef was previously reset manually in
    // multiple paths; a missed reset (e.g. clearQueued(false) in
    // the fast-run case) could leave the hook permanently refusing
    // to trigger. Now driven by a useEffect on state.kind, so
    // any transition back to idle releases the guard.
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 1,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    const { hook } = setup();

    // First successful cycle.
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("queued");
    act(() => hook.result.current.clearQueued(true));
    expect(hook.result.current.kind).toBe("idle");

    // Second trigger must go through.
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 2,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(mockedTrigger).toHaveBeenCalledTimes(2);
    expect(hook.result.current.queuedRunId).toBe(2);
  });

  it("re-triggers after an error (retry flow)", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, "conflict"));
    const { hook } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("error");

    // An error transition drops the inFlight guard via the same
    // useEffect — operator's retry click should dispatch a second
    // POST.
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 9,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(mockedTrigger).toHaveBeenCalledTimes(2);
  });

  it("POSTs with scope=behind, not full", async () => {
    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 1,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(mockedTrigger).toHaveBeenCalledWith({ scope: "behind" });
  });
});
