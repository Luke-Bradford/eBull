/**
 * Tests for AdminPage after the #323 triage rewrite.
 *
 * AdminPage now has:
 *   1. Problems panel (collapsed/hidden semantics per per-source
 *      cached snapshots — see ProblemsPanel).
 *   2. Fund data row (5 live-or-pending cells plus 3 pending-only).
 *   3. Collapsible "Orchestrator details" (SyncDashboard), "Background
 *      tasks" (JobsTable), "Filings coverage" (CoverageSummaryCard).
 *
 * The legacy Run-Now tests are preserved but updated to expand the
 * Background tasks section first (the existing always-visible layout
 * is gone).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { AdminPage } from "@/pages/AdminPage";
import { ApiError } from "@/api/client";
import type {
  CoverageSummaryResponse,
  JobsListResponse,
  RecommendationsListResponse,
  ConfigResponse,
} from "@/api/types";

vi.mock("@/api/jobs", () => ({ fetchJobsOverview: vi.fn(), runJob: vi.fn() }));
vi.mock("@/api/sync", () => ({
  fetchSyncLayers: vi.fn(),
  fetchSyncStatus: vi.fn(),
  fetchSyncRuns: vi.fn(),
  triggerSync: vi.fn(),
}));
vi.mock("@/api/coverage", () => ({ fetchCoverageSummary: vi.fn() }));
vi.mock("@/api/recommendations", () => ({ fetchRecommendations: vi.fn() }));
vi.mock("@/api/config", () => ({ fetchConfig: vi.fn() }));

import { fetchJobsOverview, runJob } from "@/api/jobs";
import {
  fetchSyncLayers,
  fetchSyncRuns,
  fetchSyncStatus,
} from "@/api/sync";
import { fetchCoverageSummary } from "@/api/coverage";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchConfig } from "@/api/config";

const mockedJobs = vi.mocked(fetchJobsOverview);
const mockedRun = vi.mocked(runJob);
const mockedLayers = vi.mocked(fetchSyncLayers);
const mockedStatus = vi.mocked(fetchSyncStatus);
const mockedSyncRuns = vi.mocked(fetchSyncRuns);
const mockedCoverage = vi.mocked(fetchCoverageSummary);
const mockedRecs = vi.mocked(fetchRecommendations);
const mockedConfig = vi.mocked(fetchConfig);

function demoConfig(): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      updated_at: "2026-04-18T00:00:00Z",
      updated_by: "system",
      reason: "",
    },
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}

function coverageHealthy(): CoverageSummaryResponse {
  return {
    checked_at: "2026-04-19T00:00:00Z",
    analysable: 100,
    insufficient: 0,
    structurally_young: 0,
    fpi: 0,
    no_primary_sec_cik: 0,
    unknown: 0,
    null_rows: 0,
    total_tradable: 100,
  };
}

function recsEmpty(): RecommendationsListResponse {
  return { items: [], total: 0, limit: 1, offset: 0 };
}

function jobsResponse(): JobsListResponse {
  return {
    checked_at: "2026-04-16T01:00:00Z",
    jobs: [
      {
        name: "orchestrator_full_sync",
        description: "orchestrator sweep",
        cadence: "daily",
        cadence_kind: "daily",
        next_run_time: "2026-04-20T00:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: null,
        last_finished_at: null,
        detail: "",
      },
      {
        name: "execute_approved_orders",
        description: "execute orders",
        cadence: "every 1 minutes",
        cadence_kind: "every_n_minutes",
        next_run_time: "2026-04-20T00:00:00Z",
        next_run_time_source: "declared",
        last_status: "success",
        last_started_at: null,
        last_finished_at: null,
        detail: "",
      },
      {
        name: "attribution_summary",
        description: "attribution",
        cadence: "daily",
        cadence_kind: "daily",
        next_run_time: "2026-04-20T00:00:00Z",
        next_run_time_source: "declared",
        last_status: "failure",
        last_started_at: null,
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
  mockedCoverage.mockReset();
  mockedRecs.mockReset();
  mockedConfig.mockReset();

  mockedConfig.mockResolvedValue(demoConfig());
  mockedJobs.mockResolvedValue(jobsResponse());
  mockedLayers.mockResolvedValue({ layers: [] });
  mockedStatus.mockResolvedValue({
    is_running: false,
    current_run: null,
    active_layer: null,
  });
  mockedSyncRuns.mockResolvedValue({ runs: [] });
  mockedCoverage.mockResolvedValue(coverageHealthy());
  mockedRecs.mockResolvedValue(recsEmpty());
});

afterEach(() => vi.clearAllMocks());

function renderPage() {
  return render(
    <MemoryRouter>
      <AdminPage />
    </MemoryRouter>,
  );
}

describe("AdminPage — top-level composition", () => {
  it("renders a top-level Sync-now button", async () => {
    renderPage();
    // Multiple buttons (top-level + inner once expanded) will exist;
    // here we just assert the top one is present.
    expect(
      await screen.findByRole("button", { name: /Sync now/ }),
    ).toBeInTheDocument();
  });

  it("renders the fund-data row with universe + analysable cells", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Tradable universe")).toBeInTheDocument();
    });
    expect(screen.getByText("Analysable")).toBeInTheDocument();
    // Tier/score/thesis pending placeholders.
    expect(screen.getByText("Tier 1/2/3")).toBeInTheDocument();
  });

  it("hides the problems panel when no sources surface any problem", async () => {
    // Override the default fixture: no failing jobs, no layer
    // failures, no null coverage rows → hidden state.
    mockedJobs.mockReset();
    mockedJobs.mockResolvedValue({
      checked_at: "2026-04-16T01:00:00Z",
      jobs: [
        {
          name: "execute_approved_orders",
          description: "execute orders",
          cadence: "every 1 minutes",
          cadence_kind: "every_n_minutes",
          next_run_time: "2026-04-20T00:00:00Z",
          next_run_time_source: "declared",
          last_status: "success",
          last_started_at: null,
          last_finished_at: null,
          detail: "",
        },
      ],
    });
    renderPage();
    // Wait for all three sources to resolve.
    await waitFor(() => {
      expect(mockedLayers).toHaveBeenCalled();
      expect(mockedJobs).toHaveBeenCalled();
      expect(mockedCoverage).toHaveBeenCalled();
    });
    // Once resolved, the panel should be hidden (no "problems" text).
    await waitFor(() => {
      expect(
        screen.queryByText(/need.*attention/i),
      ).not.toBeInTheDocument();
    });
  });

  it("shows problems panel when a job has failed", async () => {
    renderPage();
    expect(
      await screen.findByText(/attribution_summary/),
    ).toBeInTheDocument();
    expect(screen.getByText(/last run failed/)).toBeInTheDocument();
  });

  it("shows problems panel when a layer has consecutive failures", async () => {
    mockedLayers.mockReset();
    mockedLayers.mockResolvedValue({
      layers: [
        {
          name: "cik_mapping",
          display_name: "CIK mapping",
          tier: 1,
          is_fresh: false,
          freshness_detail: "failed",
          last_success_at: null,
          last_duration_seconds: null,
          last_error_category: "db_constraint",
          consecutive_failures: 3,
          dependencies: [],
          is_blocking: true,
        },
      ],
    });
    renderPage();
    expect(
      await screen.findByText(/CIK mapping — 3 consecutive failures/),
    ).toBeInTheDocument();
  });
});

describe("AdminPage — collapsible sections", () => {
  it("keeps Orchestrator details collapsed on mount", async () => {
    renderPage();
    await waitFor(() => screen.getByText("Orchestrator details"));
    // SyncDashboard's recent-runs-table heading should NOT be in DOM
    // while the section is collapsed.
    expect(screen.queryByText("Recent sync runs")).toBeNull();
  });

  it("expands Orchestrator details on chevron click", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(
      await screen.findByRole("button", { name: /Orchestrator details/ }),
    );
    await waitFor(() => {
      expect(screen.getByText("Recent sync runs")).toBeInTheDocument();
    });
  });

  it("expands Orchestrator details when a layer problem's action fires", async () => {
    mockedLayers.mockReset();
    mockedLayers.mockResolvedValue({
      layers: [
        {
          name: "cik_mapping",
          display_name: "CIK mapping",
          tier: 1,
          is_fresh: false,
          freshness_detail: "failed",
          last_success_at: null,
          last_duration_seconds: null,
          last_error_category: "db_constraint",
          consecutive_failures: 3,
          dependencies: [],
          is_blocking: true,
        },
      ],
    });
    const user = userEvent.setup();
    renderPage();
    await user.click(
      await screen.findByRole("button", {
        name: /Open orchestrator details/,
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Recent sync runs")).toBeInTheDocument();
    });
  });
});

describe("AdminPage — Background tasks collapsible", () => {
  it("filters orchestrator-owned jobs once expanded", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(
      await screen.findByRole("button", { name: /Background tasks/ }),
    );

    await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });
    expect(
      screen.queryByRole("button", { name: "Run orchestrator_full_sync now" }),
    ).toBeNull();
  });

  it("Run-now happy path: POST + refetch + Queued badge", async () => {
    const user = userEvent.setup();
    mockedRun.mockResolvedValueOnce(undefined);
    renderPage();
    await user.click(
      await screen.findByRole("button", { name: /Background tasks/ }),
    );
    const btn = await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });

    const callsBefore = mockedJobs.mock.calls.length;
    await user.click(btn);
    await waitFor(() => {
      expect(mockedRun).toHaveBeenCalledWith("execute_approved_orders");
    });
    await waitFor(() => {
      expect(mockedJobs.mock.calls.length).toBeGreaterThan(callsBefore);
    });
    expect(btn).toHaveTextContent("Queued");
  });

  it("Run-now 409 shows 'Already running'", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(409, "conflict"));
    renderPage();
    await user.click(
      await screen.findByRole("button", { name: /Background tasks/ }),
    );
    const btn = await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });
    await user.click(btn);
    expect(btn).toHaveTextContent("Already running");
  });

  it("Run-now 404 shows 'Unknown job'", async () => {
    const user = userEvent.setup();
    mockedRun.mockRejectedValueOnce(new ApiError(404, "unknown"));
    renderPage();
    await user.click(
      await screen.findByRole("button", { name: /Background tasks/ }),
    );
    const btn = await screen.findByRole("button", {
      name: "Run execute_approved_orders now",
    });
    await user.click(btn);
    expect(btn).toHaveTextContent("Unknown job");
  });
});
