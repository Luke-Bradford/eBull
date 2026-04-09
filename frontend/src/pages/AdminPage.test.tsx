/**
 * Tests for AdminPage scheduled-jobs surface (#13 PR B).
 *
 * Scope:
 *   - jobs table renders the overview shape from /system/jobs
 *   - "Run now" button POSTs /jobs/{name}/run and surfaces success
 *   - 409 from runJob renders "Already running" without throwing
 *   - successful trigger refetches both panels (so the operator's
 *     action is reflected without a manual refresh)
 *   - recent runs table renders the row shape from /jobs/runs
 *
 * The API client is mocked at the module boundary; this test exercises
 * the page's state machine, not the network layer.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { AdminPage } from "@/pages/AdminPage";
import { fetchJobRuns, fetchJobsOverview, runJob } from "@/api/jobs";
import { ApiError } from "@/api/client";
import type { JobsListResponse, JobRunsListResponse } from "@/api/types";

vi.mock("@/api/jobs", () => ({
  fetchJobsOverview: vi.fn(),
  fetchJobRuns: vi.fn(),
  runJob: vi.fn(),
}));

const mockedJobs = vi.mocked(fetchJobsOverview);
const mockedRuns = vi.mocked(fetchJobRuns);
const mockedRun = vi.mocked(runJob);

function jobsResponse(): JobsListResponse {
  return {
    checked_at: "2026-04-09T01:00:00Z",
    jobs: [
      {
        name: "nightly_universe_sync",
        description: "Sync the eToro tradable instrument universe.",
        cadence: "daily at 02:00 UTC",
        cadence_kind: "daily",
        next_run_time: "2026-04-10T02:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: "2026-04-09T02:00:00Z",
        last_finished_at: "2026-04-09T02:00:12Z",
        detail: "",
      },
      {
        name: "daily_news_refresh",
        description: "Fetch and score news.",
        cadence: "daily at 04:00 UTC",
        cadence_kind: "daily",
        next_run_time: "2026-04-10T04:00:00Z",
        next_run_time_source: "declared",
        last_status: null,
        last_started_at: null,
        last_finished_at: null,
        detail: "no runs recorded",
      },
    ],
  };
}

function runsResponse(): JobRunsListResponse {
  return {
    items: [
      {
        run_id: 42,
        job_name: "nightly_universe_sync",
        started_at: "2026-04-09T02:00:00Z",
        finished_at: "2026-04-09T02:00:12Z",
        status: "success",
        row_count: 1234,
        error_msg: null,
      },
    ],
    count: 1,
    limit: 50,
    job_name: null,
  };
}

beforeEach(() => {
  mockedJobs.mockReset();
  mockedRuns.mockReset();
  mockedRun.mockReset();
  mockedJobs.mockResolvedValue(jobsResponse());
  mockedRuns.mockResolvedValue(runsResponse());
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("AdminPage — jobs table", () => {
  it("renders one row per declared job with status + cadence", async () => {
    render(<AdminPage />);
    expect(
      await screen.findByRole("button", { name: "Run nightly_universe_sync now" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Run daily_news_refresh now" }),
    ).toBeInTheDocument();
    expect(screen.getAllByText(/daily at \d\d:00 UTC/)).toHaveLength(2);
    expect(screen.getByText("never run")).toBeInTheDocument();
  });

  it("renders recent runs from /jobs/runs", async () => {
    render(<AdminPage />);
    await screen.findAllByText("nightly_universe_sync");
    // Both the jobs row and the recent-runs row include the job name.
    // 1234 is the row_count cell, which only appears in the runs table.
    expect(await screen.findByText("1234")).toBeInTheDocument();
  });
});

describe("AdminPage — Run now button", () => {
  it("POSTs to runJob and refetches both panels on success", async () => {
    const user = userEvent.setup();
    mockedRun.mockResolvedValueOnce(undefined);
    render(<AdminPage />);
    await screen.findAllByText("nightly_universe_sync");

    expect(mockedJobs).toHaveBeenCalledTimes(1);
    expect(mockedRuns).toHaveBeenCalledTimes(1);

    await user.click(
      screen.getByRole("button", { name: "Run nightly_universe_sync now" }),
    );

    await waitFor(() => {
      expect(mockedRun).toHaveBeenCalledWith("nightly_universe_sync");
    });
    // Both panels refetched after a successful trigger.
    await waitFor(() => {
      expect(mockedJobs).toHaveBeenCalledTimes(2);
      expect(mockedRuns).toHaveBeenCalledTimes(2);
    });
    // Button shows the queued state.
    expect(
      await screen.findByRole("button", { name: "Run nightly_universe_sync now" }),
    ).toHaveTextContent("Queued");
  });

  it("renders 'Already running' on 409 without throwing", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(409, "job already running"));
    render(<AdminPage />);
    await screen.findAllByText("nightly_universe_sync");

    await user.click(
      screen.getByRole("button", { name: "Run nightly_universe_sync now" }),
    );

    expect(
      await screen.findByRole("button", {
        name: "Run nightly_universe_sync now",
      }),
    ).toHaveTextContent("Already running");
    // No refetch on failure -- the error path leaves the panels alone.
    expect(mockedJobs).toHaveBeenCalledTimes(1);
    expect(mockedRuns).toHaveBeenCalledTimes(1);
  });

  it("renders 'Unknown job' on 404", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(404, "unknown job"));
    render(<AdminPage />);
    await screen.findAllByText("nightly_universe_sync");

    await user.click(
      screen.getByRole("button", { name: "Run nightly_universe_sync now" }),
    );

    expect(
      await screen.findByRole("button", {
        name: "Run nightly_universe_sync now",
      }),
    ).toHaveTextContent("Unknown job");
  });
});
