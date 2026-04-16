/**
 * Tests for AdminPage (#260 Phase 5).
 *
 * After Phase 5, AdminPage is composed of two sections:
 *   1. SyncDashboard — the orchestrator's 15-layer freshness view. It
 *      hits /sync/layers, /sync/status, /sync/runs on mount, so the
 *      sync API is mocked here to return empty responses.
 *   2. Background tasks — the 5 scheduled jobs outside the orchestrator
 *      DAG. Orchestrator-owned job names are filtered out at the
 *      component boundary.
 *
 * The legacy "Recent runs" table and the full scheduled-jobs list are
 * gone, so those assertions were removed. The API client is mocked at
 * the module boundary; this test exercises the page's state machine,
 * not the network layer.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AdminPage } from "@/pages/AdminPage";
import { fetchJobsOverview, runJob } from "@/api/jobs";
import {
  fetchSyncLayers,
  fetchSyncRuns,
  fetchSyncStatus,
} from "@/api/sync";
import { ApiError } from "@/api/client";
import type { JobsListResponse } from "@/api/types";

vi.mock("@/api/jobs", () => ({
  fetchJobsOverview: vi.fn(),
  runJob: vi.fn(),
}));

vi.mock("@/api/sync", () => ({
  fetchSyncLayers: vi.fn(),
  fetchSyncStatus: vi.fn(),
  fetchSyncRuns: vi.fn(),
  triggerSync: vi.fn(),
}));

const mockedJobs = vi.mocked(fetchJobsOverview);
const mockedRun = vi.mocked(runJob);
const mockedLayers = vi.mocked(fetchSyncLayers);
const mockedStatus = vi.mocked(fetchSyncStatus);
const mockedSyncRuns = vi.mocked(fetchSyncRuns);

function jobsResponse(): JobsListResponse {
  // Mix of orchestrator-owned + background tasks. The two orchestrator
  // entries must be filtered out by the component.
  return {
    checked_at: "2026-04-16T01:00:00Z",
    jobs: [
      {
        name: "orchestrator_full_sync",
        description: "Nightly orchestrator sweep.",
        cadence: "daily at 02:00 UTC",
        cadence_kind: "daily",
        next_run_time: "2026-04-17T02:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-16T02:00:00Z",
        last_finished_at: "2026-04-16T02:05:00Z",
        detail: "",
      },
      {
        name: "orchestrator_high_frequency_sync",
        description: "5-min orchestrator tick.",
        cadence: "every 5 minutes",
        cadence_kind: "every_n_minutes",
        next_run_time: "2026-04-16T01:05:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-16T01:00:00Z",
        last_finished_at: "2026-04-16T01:00:03Z",
        detail: "",
      },
      {
        name: "execute_approved_orders",
        description: "Execute operator-approved orders.",
        cadence: "every 1 minutes",
        cadence_kind: "every_n_minutes",
        next_run_time: "2026-04-16T01:01:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-16T01:00:00Z",
        last_finished_at: "2026-04-16T01:00:01Z",
        detail: "",
      },
      {
        name: "monitor_positions",
        description: "Monitor open positions.",
        cadence: "every 5 minutes",
        cadence_kind: "every_n_minutes",
        next_run_time: "2026-04-16T01:05:00Z",
        next_run_time_source: "declared",
        last_status: null,
        last_started_at: null,
        last_finished_at: null,
        detail: "no runs recorded",
      },
      {
        name: "retry_deferred_recommendations",
        description: "Retry deferred recommendations.",
        cadence: "hourly",
        cadence_kind: "hourly",
        next_run_time: "2026-04-16T02:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-16T01:00:00Z",
        last_finished_at: "2026-04-16T01:00:05Z",
        detail: "",
      },
      {
        name: "weekly_coverage_review",
        description: "Weekly coverage review.",
        cadence: "weekly",
        cadence_kind: "weekly",
        next_run_time: "2026-04-20T08:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-13T08:00:00Z",
        last_finished_at: "2026-04-13T08:00:20Z",
        detail: "",
      },
      {
        name: "attribution_summary",
        description: "Portfolio attribution summary.",
        cadence: "daily at 07:00 UTC",
        cadence_kind: "daily",
        next_run_time: "2026-04-17T07:00:00Z",
        next_run_time_source: "declared",
        last_status: "failure",
        last_started_at: "2026-04-16T07:00:00Z",
        last_finished_at: "2026-04-16T07:00:02Z",
        detail: "provider timeout",
      },
    ],
  };
}

beforeEach(() => {
  mockedJobs.mockReset();
  mockedRun.mockReset();
  mockedLayers.mockReset();
  mockedStatus.mockReset();
  mockedSyncRuns.mockReset();
  mockedJobs.mockResolvedValue(jobsResponse());
  mockedLayers.mockResolvedValue({ layers: [] });
  mockedStatus.mockResolvedValue({
    is_running: false,
    current_run: null,
    active_layer: null,
  });
  mockedSyncRuns.mockResolvedValue({ runs: [] });
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("AdminPage — background tasks table", () => {
  it("filters out orchestrator-owned jobs and lists background tasks", async () => {
    render(<AdminPage />);
    // Background tasks present.
    expect(
      await screen.findByRole("button", {
        name: "Run execute_approved_orders now",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Run monitor_positions now" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "Run retry_deferred_recommendations now",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "Run weekly_coverage_review now",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Run attribution_summary now" }),
    ).toBeInTheDocument();
    // Orchestrator-owned entries filtered out.
    expect(
      screen.queryByRole("button", { name: "Run orchestrator_full_sync now" }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", {
        name: "Run orchestrator_high_frequency_sync now",
      }),
    ).toBeNull();
    // Never-run state rendered for monitor_positions.
    expect(screen.getByText("never run")).toBeInTheDocument();
  });
});

describe("AdminPage — Run now button", () => {
  it("POSTs to runJob and refetches jobs on success", async () => {
    const user = userEvent.setup();
    mockedRun.mockResolvedValueOnce(undefined);
    render(<AdminPage />);
    await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });

    expect(mockedJobs).toHaveBeenCalledTimes(1);

    await user.click(
      screen.getByRole("button", { name: "Run execute_approved_orders now" }),
    );

    await waitFor(() => {
      expect(mockedRun).toHaveBeenCalledWith("execute_approved_orders");
    });
    await waitFor(() => {
      expect(mockedJobs).toHaveBeenCalledTimes(2);
    });
    expect(
      await screen.findByRole("button", {
        name: "Run execute_approved_orders now",
      }),
    ).toHaveTextContent("Queued");
  });

  it("renders 'Already running' on 409 without throwing", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(409, "job already running"));
    render(<AdminPage />);
    await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });

    await user.click(
      screen.getByRole("button", { name: "Run execute_approved_orders now" }),
    );

    expect(
      await screen.findByRole("button", {
        name: "Run execute_approved_orders now",
      }),
    ).toHaveTextContent("Already running");
    expect(mockedJobs).toHaveBeenCalledTimes(1);
  });

  it("renders 'Unknown job' on 404", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(404, "unknown job"));
    render(<AdminPage />);
    await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });

    await user.click(
      screen.getByRole("button", { name: "Run execute_approved_orders now" }),
    );

    expect(
      await screen.findByRole("button", {
        name: "Run execute_approved_orders now",
      }),
    ).toHaveTextContent("Unknown job");
  });
});
