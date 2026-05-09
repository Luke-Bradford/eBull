import { renderHook, waitFor } from "@testing-library/react";
import type { MockInstance } from "vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { fetchProcesses } from "@/api/processes";
import {
  POLL_INTERVAL_IDLE_MS,
  POLL_INTERVAL_RUNNING_MS,
  useProcesses,
} from "@/lib/useProcesses";
import {
  makeProcessList,
  makeProcessRow,
} from "@/components/admin/__fixtures__/processes";

vi.mock("@/api/processes", async () => {
  const actual =
    await vi.importActual<typeof import("@/api/processes")>("@/api/processes");
  return {
    ...actual,
    fetchProcesses: vi.fn(),
  };
});

const mockedFetch = vi.mocked(fetchProcesses);

let setIntervalSpy: MockInstance;

beforeEach(() => {
  mockedFetch.mockReset();
  setIntervalSpy = vi.spyOn(window, "setInterval");
});

afterEach(() => {
  setIntervalSpy.mockRestore();
});

function setIntervalCalledWith(delay: number): boolean {
  // testing-library's `waitFor` also calls setInterval at 50ms; filter
  // by exact delay match so we only see the hook's own subscriptions.
  return setIntervalSpy.mock.calls.some((c) => c[1] === delay);
}

describe("useProcesses cadence", () => {
  it("subscribes at the IDLE interval when no row is running", async () => {
    mockedFetch.mockResolvedValue(
      makeProcessList([makeProcessRow({ status: "ok" })]),
    );
    const { result } = renderHook(() => useProcesses());
    await waitFor(() => expect(result.current.data).not.toBeNull());
    expect(setIntervalCalledWith(POLL_INTERVAL_IDLE_MS)).toBe(true);
    expect(setIntervalCalledWith(POLL_INTERVAL_RUNNING_MS)).toBe(false);
  });

  it("flips to the RUNNING interval when a row transitions to running", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeProcessList([makeProcessRow({ status: "ok" })]),
    );
    mockedFetch.mockResolvedValue(
      makeProcessList([makeProcessRow({ status: "running" })]),
    );
    const { result } = renderHook(() => useProcesses());
    await waitFor(() => expect(result.current.data).not.toBeNull());
    result.current.refetch();
    await waitFor(() =>
      expect(result.current.data?.rows[0]?.status).toBe("running"),
    );
    await waitFor(() =>
      expect(setIntervalCalledWith(POLL_INTERVAL_RUNNING_MS)).toBe(true),
    );
  });

  it("returns to the IDLE interval once running rows clear", async () => {
    mockedFetch.mockResolvedValueOnce(
      makeProcessList([makeProcessRow({ status: "running" })]),
    );
    mockedFetch.mockResolvedValue(
      makeProcessList([makeProcessRow({ status: "ok" })]),
    );
    const { result } = renderHook(() => useProcesses());
    await waitFor(() =>
      expect(result.current.data?.rows[0]?.status).toBe("running"),
    );
    await waitFor(() =>
      expect(setIntervalCalledWith(POLL_INTERVAL_RUNNING_MS)).toBe(true),
    );
    setIntervalSpy.mockClear();
    result.current.refetch();
    await waitFor(() =>
      expect(result.current.data?.rows[0]?.status).toBe("ok"),
    );
    await waitFor(() =>
      expect(setIntervalCalledWith(POLL_INTERVAL_IDLE_MS)).toBe(true),
    );
  });
});
