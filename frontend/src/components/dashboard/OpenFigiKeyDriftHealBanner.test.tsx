import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { OpenFigiKeyDriftHealBanner } from "@/components/dashboard/OpenFigiKeyDriftHealBanner";
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

// Frozen clock — fake ONLY Date so async/promises + the 60s poll interval
// still run on real timers, but Date.now()/argless new Date() are pinned.
// new Date(isoString) parsing is unaffected (only argless construction is
// faked), so the component's elapsed calc is deterministic.
const NOW = Date.UTC(2026, 5, 28, 12, 0, 0);

function minutesAgo(mins: number): string {
  return new Date(NOW - mins * 60_000).toISOString();
}

function status(
  overrides: Partial<BootstrapStatusResponse>,
): BootstrapStatusResponse {
  return {
    status: "running",
    current_run_id: 42,
    last_completed_at: null,
    stages: [],
    bulk_manifest: null,
    openfigi_key_present: false,
    ...overrides,
  };
}

function s13Stage(
  stageStatus: BootstrapStageStatus,
  startedAt: string | null,
): BootstrapStageResponse {
  return {
    stage_key: "cusip_resolver_post_bulk_sweep",
    stage_order: 13,
    lane: "openfigi",
    job_name: "cusip_resolver_post_bulk_sweep",
    status: stageStatus,
    started_at: startedAt,
    completed_at: null,
    rows_processed: null,
    expected_units: null,
    units_done: null,
    last_error: null,
    attempt_count: 1,
    archive_results: [],
  };
}

const DISMISS_RUN_KEY = "openfigiKeyDriftHealDismissedRunId";
const NUDGE = /has been running several minutes/;

beforeEach(() => {
  mockedFetch.mockReset();
  window.sessionStorage.clear();
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("OpenFigiKeyDriftHealBanner", () => {
  it("shows when S13 running >5 min, mid-run, no key, not dismissed", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("running", minutesAgo(6))] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();
  });

  it("hidden when key already present", async () => {
    mockedFetch.mockResolvedValue(
      status({
        openfigi_key_present: true,
        stages: [s13Stage("running", minutesAgo(6))],
      }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when top-level status is not running (stale S13 row)", async () => {
    mockedFetch.mockResolvedValue(
      status({
        status: "partial_error",
        stages: [s13Stage("running", minutesAgo(6))],
      }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when S13 running but only 4 min in (below threshold)", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("running", minutesAgo(4))] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden at exactly 5 min (condition is strict >)", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("running", minutesAgo(5))] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when started_at is in the future", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("running", minutesAgo(-1))] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when started_at is unparseable (NaN)", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("running", "not-a-date")] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when S13 not running (pending / success)", async () => {
    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("pending", minutesAgo(6))] }),
    );
    const { unmount } = render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
    unmount();

    mockedFetch.mockResolvedValue(
      status({ stages: [s13Stage("success", minutesAgo(6))] }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(2));
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("hidden when S13 stage absent", async () => {
    mockedFetch.mockResolvedValue(status({ stages: [] }));
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("dismiss writes current_run_id to sessionStorage and hides", async () => {
    mockedFetch.mockResolvedValue(
      status({
        current_run_id: 42,
        stages: [s13Stage("running", minutesAgo(6))],
      }),
    );
    const user = userEvent.setup();
    render(<OpenFigiKeyDriftHealBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /dismiss/i }));
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
    expect(window.sessionStorage.getItem(DISMISS_RUN_KEY)).toBe("42");
  });

  it("pre-seeded dismiss for the SAME run id → hidden on mount", async () => {
    window.sessionStorage.setItem(DISMISS_RUN_KEY, "42");
    mockedFetch.mockResolvedValue(
      status({
        current_run_id: 42,
        stages: [s13Stage("running", minutesAgo(6))],
      }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    expect(screen.queryByText(NUDGE)).not.toBeInTheDocument();
  });

  it("pre-seeded dismiss for a DIFFERENT run id → shown (re-armed)", async () => {
    window.sessionStorage.setItem(DISMISS_RUN_KEY, "41");
    mockedFetch.mockResolvedValue(
      status({
        current_run_id: 42,
        stages: [s13Stage("running", minutesAgo(6))],
      }),
    );
    render(<OpenFigiKeyDriftHealBanner />);
    expect(await screen.findByText(NUDGE)).toBeInTheDocument();
  });
});
