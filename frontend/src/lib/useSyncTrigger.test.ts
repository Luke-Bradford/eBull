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
});
