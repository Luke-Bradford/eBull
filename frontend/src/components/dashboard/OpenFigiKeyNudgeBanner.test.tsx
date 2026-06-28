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

beforeEach(() => {
  mockedFetch.mockReset();
  window.localStorage.clear();
});

afterEach(() => {
  vi.clearAllTimers();
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
});
