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

  it("surfaces 409 as conflict (not error) and calls onTriggered for immediate reconcile", async () => {
    // A 409 means the orchestrator is already running a sync. The UI
    // should render an amber informational pill (not a red error), AND
    // the caller's status poll must fire right away so the "conflict"
    // resolves into the grey "Running" disabled state within one poll
    // cycle instead of waiting up to 60s for the idle-cadence tick.
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, "conflict"));
    const { hook, onTriggered } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });

    expect(hook.result.current.kind).toBe("conflict");
    expect(hook.result.current.message).toBe("Another sync is already running");
    expect(onTriggered).toHaveBeenCalledTimes(1);
  });

  it("surfaces 503 as error (orchestrator disabled) — not a conflict", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(503, "disabled"));
    const { hook, onTriggered } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("error");
    expect(hook.result.current.message).toBe("Sync orchestrator disabled");
    expect(onTriggered).not.toHaveBeenCalled();
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

  it("clearQueued(true) collapses a conflict to idle once the server confirms a sync is running", async () => {
    // 409 means "someone else is already running a sync" — the amber
    // pill has done its job. As soon as /sync/status reports
    // is_running=true, the pill must collapse so the caller's normal
    // "Running" grey state (driven by `isRunning`) takes over. Without
    // this, the pill would persist past the end of the server-side
    // sync and mislead the operator.
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, ""));
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("conflict");

    act(() => hook.result.current.clearQueued(true));
    expect(hook.result.current.kind).toBe("idle");
  });

  it("clearQueued(false) keeps a conflict state (server not yet confirmed running)", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, ""));
    const { hook } = setup();
    await act(async () => {
      await hook.result.current.trigger();
    });
    act(() => hook.result.current.clearQueued(false));
    expect(hook.result.current.kind).toBe("conflict");
  });

  it("clearQueued does NOT auto-reset a 503 error state", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(503, "disabled"));
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

  it("re-triggers after a conflict (retry flow)", async () => {
    // 409 lands in `conflict`. The inFlight guard must still release
    // so the operator's retry click dispatches a second POST once the
    // orchestrator sync finishes.
    mockedTrigger.mockRejectedValueOnce(new ApiError(409, "conflict"));
    const { hook } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("conflict");

    mockedTrigger.mockResolvedValueOnce({
      sync_run_id: 9,
      plan: { layers_to_refresh: [], layers_skipped: [] },
    });
    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(mockedTrigger).toHaveBeenCalledTimes(2);
  });

  it("re-triggers after a 503 error (retry flow)", async () => {
    mockedTrigger.mockRejectedValueOnce(new ApiError(503, "disabled"));
    const { hook } = setup();

    await act(async () => {
      await hook.result.current.trigger();
    });
    expect(hook.result.current.kind).toBe("error");

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
