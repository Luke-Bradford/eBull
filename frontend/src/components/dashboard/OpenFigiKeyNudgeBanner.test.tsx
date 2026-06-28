import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { OpenFigiKeyNudgeBanner } from "@/components/dashboard/OpenFigiKeyNudgeBanner";
import type {
  BootstrapStageResponse,
  BootstrapStageStatus,
  BootstrapStatusResponse,
} from "@/api/bootstrap";

vi.mock("@/api/bootstrap", () => ({
  fetchBootstrapStatus: vi.fn(),
}));

import * as bootstrapApi from "@/api/bootstrap";

const mockedFetch = vi.mocked(bootstrapApi.fetchBootstrapStatus);

function status(
  overrides: Partial<BootstrapStatusResponse>,
): BootstrapStatusResponse {
  return {
    status: "pending",
    current_run_id: null,
    last_completed_at: null,
    stages: [],
    bulk_manifest: null,
    openfigi_key_present: false,
    ...overrides,
  };
}

function s13Stage(status: BootstrapStageStatus): BootstrapStageResponse {
  return {
    stage_key: "cusip_resolver_post_bulk_sweep",
    stage_order: 13,
    lane: "openfigi",
    job_name: "cusip_resolver_post_bulk_sweep",
    status,
    started_at: null,
    completed_at: null,
    rows_processed: null,
    expected_units: null,
    units_done: null,
    last_error: null,
    attempt_count: 1,
    archive_results: [],
  };
}

const NUDGE = /OPENFIGI_API_KEY/;

function runningS13(startedAt: string | null): BootstrapStatusResponse {
  return status({
    status: "running",
    current_run_id: 42,
    openfigi_key_present: false,
    stages: [{ ...s13Stage("running"), started_at: startedAt }],
  });
}

const DRIFTHEAL = /running over 2 minutes/;

beforeEach(() => {
  mockedFetch.mockReset();
  window.localStorage.clear();
  window.sessionStorage.clear();
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("OpenFigiKeyNudgeBanner", () => {
  it("shows when bootstrap pending and no key", async () => {
    mockedFetch.mockResolvedValue(status({ status: "pending", openfigi_key_present: false }));
    render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();
  });

  it("shows on partial_error when S13 failed and no key (retry may rerun it)", async () => {
    mockedFetch.mockResolvedValue(
      status({
        status: "partial_error",
        openfigi_key_present: false,
        stages: [s13Stage("error")],
      }),
    );
    render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();
  });

  it("hidden on partial_error when S13 already succeeded (retry won't rerun it)", async () => {
    mockedFetch.mockResolvedValue(
      status({
        status: "partial_error",
        openfigi_key_present: false,
        stages: [s13Stage("success")],
      }),
    );
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when key already present", async () => {
    mockedFetch.mockResolvedValue(status({ status: "pending", openfigi_key_present: true }));
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden mid-run (status running) and when complete", async () => {
    mockedFetch.mockResolvedValue(status({ status: "running", openfigi_key_present: false }));
    const { unmount } = render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
    unmount();

    mockedFetch.mockResolvedValue(status({ status: "complete", openfigi_key_present: false }));
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(2));
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("dismiss persists to localStorage and stays hidden on remount", async () => {
    mockedFetch.mockResolvedValue(status({ status: "pending", openfigi_key_present: false }));
    const user = userEvent.setup();
    const { unmount } = render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /dismiss/i }));
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
    expect(window.localStorage.getItem("openfigiKeyNudgeDismissed")).toBe("1");

    // Remount — persistent dismiss keeps it hidden.
    unmount();
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  // --- drift-heal mode (#1791) ---

  it("drift-heal shows when S13 has run > 2 min with no key", async () => {
    const startedAt = new Date(Date.now() - 3 * 60_000).toISOString();
    mockedFetch.mockResolvedValue(runningS13(startedAt));
    render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(DRIFTHEAL)).toBeInTheDocument();
  });

  it("drift-heal hidden when S13 under the 2-min threshold", async () => {
    const startedAt = new Date(Date.now() - 60_000).toISOString();
    mockedFetch.mockResolvedValue(runningS13(startedAt));
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();
  });

  it("drift-heal hidden when running but S13 stage is not running", async () => {
    mockedFetch.mockResolvedValue(
      status({
        status: "running",
        current_run_id: 42,
        openfigi_key_present: false,
        stages: [s13Stage("success")],
      }),
    );
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();
  });

  it("drift-heal hidden when started_at is null (no parseable clock)", async () => {
    mockedFetch.mockResolvedValue(runningS13(null));
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();
  });

  it("drift-heal hidden when key already present", async () => {
    const startedAt = new Date(Date.now() - 3 * 60_000).toISOString();
    mockedFetch.mockResolvedValue({
      ...runningS13(startedAt),
      openfigi_key_present: true,
    });
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();
  });

  it("drift-heal dismiss is run-scoped (sessionStorage) and survives remount, but a new run re-surfaces it", async () => {
    const startedAt = new Date(Date.now() - 3 * 60_000).toISOString();
    mockedFetch.mockResolvedValue(runningS13(startedAt));
    const user = userEvent.setup();
    const { unmount } = render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(DRIFTHEAL)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /dismiss/i }));
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();
    expect(
      window.sessionStorage.getItem("openfigiKeyDriftHealDismissed:42"),
    ).toBe("1");
    // Pre-flight localStorage key untouched — independent dismiss state.
    expect(window.localStorage.getItem("openfigiKeyNudgeDismissed")).toBeNull();

    // Same run, remount — run-scoped dismiss keeps it hidden.
    unmount();
    render(<OpenFigiKeyNudgeBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(2));
    expect(screen.queryByText(DRIFTHEAL)).not.toBeInTheDocument();

    // A new bootstrap run (different current_run_id) re-surfaces the nudge.
    mockedFetch.mockResolvedValue({
      ...runningS13(startedAt),
      current_run_id: 99,
    });
    const next = render(<OpenFigiKeyNudgeBanner />);
    expect(await next.findByText(DRIFTHEAL)).toBeInTheDocument();
  });

  it("pre-flight dismiss does NOT suppress a later drift-heal nudge", async () => {
    // Operator dismisses the pre-flight nudge…
    mockedFetch.mockResolvedValue(status({ status: "pending" }));
    const user = userEvent.setup();
    const { unmount } = render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /dismiss/i }));
    unmount();

    // …then S13 crawls mid-run: the drift-heal nudge still shows.
    const startedAt = new Date(Date.now() - 3 * 60_000).toISOString();
    mockedFetch.mockResolvedValue(runningS13(startedAt));
    render(<OpenFigiKeyNudgeBanner />);
    expect(await screen.findByText(DRIFTHEAL)).toBeInTheDocument();
  });
});
