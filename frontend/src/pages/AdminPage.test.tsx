/**
 * Tests for AdminPage after the #1064 admin control hub rewrite.
 *
 * Post-PR7 (#1080) AdminPage shape:
 *   1. ProblemsPanel — failing layers + failing jobs + coverage anomalies.
 *   2. Processes table — unified mechanism rows; bootstrap row + DAG drill-in
 *      + Timeline drill-in routes live under /admin/processes/:id.
 *   3. FundDataRow + SeedProgressPanel + Background tasks + Filings coverage.
 *
 * Decommissioned in PR6 (no longer covered here):
 *   - Sync-now button (top-level)
 *   - Orchestrator details collapsible (SyncDashboard)
 *   - Layer health collapsible (LayerHealthList)
 *   - Layer toggle wire (setLayerEnabled)
 *
 * Decommissioned in PR7:
 *   - BootstrapPanel mount on /admin (data lives on the bootstrap drill-in).
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
  SyncLayersV2Response,
} from "@/api/types";

vi.mock("@/api/jobs", () => ({ fetchJobsOverview: vi.fn(), runJob: vi.fn() }));
vi.mock("@/api/sync", () => ({
  fetchSyncLayersV2: vi.fn(),
  fetchSyncStatus: vi.fn(),
  // SeedProgressPanel still mounts on AdminPage (decommission in PR9).
  fetchSeedProgress: vi.fn().mockResolvedValue({
    sources: [],
    latest_run: null,
    ingest_paused: false,
  }),
  fetchCikTimingLatest: vi.fn().mockResolvedValue({
    ingestion_run_id: null,
    run_source: null,
    run_started_at: null,
    run_finished_at: null,
    run_status: null,
    modes: [],
    slowest: [],
  }),
  setIngestEnabled: vi.fn(),
}));
vi.mock("@/api/coverage", () => ({ fetchCoverageSummary: vi.fn() }));
vi.mock("@/api/recommendations", () => ({ fetchRecommendations: vi.fn() }));
vi.mock("@/api/config", () => ({ fetchConfig: vi.fn() }));
vi.mock("@/api/system", () => ({ fetchSystemStatus: vi.fn() }));
vi.mock("@/api/processes", () => ({ fetchProcesses: vi.fn() }));

import { fetchJobsOverview, runJob } from "@/api/jobs";
import { fetchSyncLayersV2, fetchSyncStatus } from "@/api/sync";
import { fetchCoverageSummary } from "@/api/coverage";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchConfig } from "@/api/config";
import { fetchSystemStatus } from "@/api/system";
import { fetchProcesses } from "@/api/processes";

const mockedJobs = vi.mocked(fetchJobsOverview);
const mockedRun = vi.mocked(runJob);
const mockedV2 = vi.mocked(fetchSyncLayersV2);
const mockedStatus = vi.mocked(fetchSyncStatus);
const mockedCoverage = vi.mocked(fetchCoverageSummary);
const mockedRecs = vi.mocked(fetchRecommendations);
const mockedConfig = vi.mocked(fetchConfig);
const mockedSystem = vi.mocked(fetchSystemStatus);
const mockedProcesses = vi.mocked(fetchProcesses);

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

function emptyV2(): SyncLayersV2Response {
  return {
    generated_at: new Date().toISOString(),
    system_state: "ok",
    system_summary: "All layers healthy",
    action_needed: [],
    degraded: [],
    secret_missing: [],
    healthy: [],
    disabled: [],
    cascade_groups: [],
    layers: [],
  };
}

beforeEach(() => {
  mockedJobs.mockReset();
  mockedRun.mockReset();
  mockedV2.mockReset();
  mockedStatus.mockReset();
  mockedCoverage.mockReset();
  mockedRecs.mockReset();
  mockedConfig.mockReset();
  mockedSystem.mockReset();
  mockedProcesses.mockReset();

  mockedConfig.mockResolvedValue(demoConfig());
  mockedJobs.mockResolvedValue(jobsResponse());
  mockedV2.mockResolvedValue(emptyV2());
  mockedStatus.mockResolvedValue({
    is_running: false,
    current_run: null,
    active_layer: null,
  });
  mockedCoverage.mockResolvedValue(coverageHealthy());
  mockedRecs.mockResolvedValue(recsEmpty());
  mockedSystem.mockResolvedValue({
    checked_at: "2026-04-19T00:00:00Z",
    overall_status: "ok",
    layers: [],
    jobs: [],
    kill_switch: { active: false, activated_at: null, activated_by: null, reason: null },
    credential_health: {
      state: "valid",
      last_recovered_at: null,
      last_error: null,
    },
  });
  mockedProcesses.mockResolvedValue({ rows: [], partial: false });
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
    await waitFor(() => {
      expect(mockedV2).toHaveBeenCalled();
      expect(mockedJobs).toHaveBeenCalled();
      expect(mockedCoverage).toHaveBeenCalled();
    });
    await waitFor(() => {
      expect(
        screen.queryByText(/need.*attention/i),
      ).not.toBeInTheDocument();
    });
  });

  it("shows problems panel when a job has failed", async () => {
    renderPage();
    expect(
      await screen.findByText(/attribution_summary — last run failed/),
    ).toBeInTheDocument();
  });

  it("shows problems panel when a layer has consecutive failures", async () => {
    const v2WithProblem = emptyV2();
    v2WithProblem.system_state = "needs_attention";
    v2WithProblem.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "CIK mapping",
        category: "db_constraint",
        operator_message: "3 consecutive failures",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 3,
        affected_downstream: [],
      },
    ];
    mockedV2.mockReset();
    mockedV2.mockResolvedValue(v2WithProblem);
    renderPage();
    expect(
      await screen.findByText(/CIK mapping — 3 consecutive failures/),
    ).toBeInTheDocument();
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

describe("AdminPage — PR6 decommission", () => {
  it("does not render the legacy Sync-now button", async () => {
    renderPage();
    await waitFor(() => screen.getByText("Admin"));
    expect(screen.queryByRole("button", { name: /^Sync now$/i })).toBeNull();
  });

  it("does not render the Orchestrator details collapsible", async () => {
    renderPage();
    await waitFor(() => screen.getByText("Admin"));
    expect(screen.queryByText("Orchestrator details")).toBeNull();
  });

  it("does not render the Layer health collapsible", async () => {
    renderPage();
    await waitFor(() => screen.getByText("Admin"));
    expect(screen.queryByText("Layer health")).toBeNull();
  });
});

describe("AdminPage — PR7 decommission", () => {
  it("does not render the legacy BootstrapPanel 'Run bootstrap' control", async () => {
    // BootstrapPanel surfaced the run/retry/mark-complete buttons keyed
    // off /system/bootstrap/status. PR7 deletes the panel; the bootstrap
    // row + Timeline drill-in own the surface now.
    renderPage();
    await waitFor(() => screen.getByText("Admin"));
    expect(
      screen.queryByRole("button", { name: /^Run bootstrap$/i }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", { name: /^Retry failed/i }),
    ).toBeNull();
  });
});
