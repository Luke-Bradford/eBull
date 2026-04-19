import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import type {
  CoverageSummaryResponse,
  JobsListResponse,
  SyncLayersV2Response,
} from "@/api/types";

import { ProblemsPanel } from "./ProblemsPanel";


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


function emptyJobs(): JobsListResponse {
  return { checked_at: new Date().toISOString(), jobs: [] };
}


function emptyCoverage(): CoverageSummaryResponse {
  return {
    checked_at: new Date().toISOString(),
    total_tradable: 0,
    analysable: 0,
    insufficient: 0,
    structurally_young: 0,
    fpi: 0,
    no_primary_sec_cik: 0,
    unknown: 0,
    null_rows: 0,
  };
}


function renderPanel(
  props: Partial<React.ComponentProps<typeof ProblemsPanel>>,
): ReturnType<typeof render> {
  const defaults: React.ComponentProps<typeof ProblemsPanel> = {
    v2: emptyV2(),
    jobs: emptyJobs(),
    coverage: emptyCoverage(),
    v2Error: false,
    jobsError: false,
    coverageError: false,
    onOpenOrchestrator: () => {},
  };
  return render(
    <MemoryRouter>
      <ProblemsPanel {...defaults} {...props} />
    </MemoryRouter>,
  );
}


describe("ProblemsPanel", () => {
  it("renders nothing when all sources are clean", () => {
    const { container } = renderPanel({});
    expect(container).toBeEmptyDOMElement();
  });

  it("renders a red row per action_needed entry", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.system_summary = "SEC CIK Mapping needs attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "Database constraint violated",
        operator_fix: "Open orchestrator details and inspect the offending row",
        self_heal: false,
        consecutive_failures: 3,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    // header contains "SEC CIK Mapping needs attention"; row also contains "SEC CIK Mapping"
    expect(screen.getAllByText(/SEC CIK Mapping/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/Database constraint violated/)).toBeInTheDocument();
    expect(screen.getByText(/3 consecutive failures/)).toBeInTheDocument();
  });

  it("renders secret_missing row with a Link to /settings#providers", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.secret_missing = [
      {
        layer: "news",
        display_name: "News & Sentiment",
        missing_secret: "ANTHROPIC_API_KEY",
        operator_fix: "Set ANTHROPIC_API_KEY in Settings → Providers",
      },
    ];
    renderPanel({ v2 });
    const link = screen.getByRole("link", { name: /Set ANTHROPIC_API_KEY/i });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "/settings#providers");
  });

  it("renders action_needed operator_fix as Settings link when it mentions Settings", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "auth_expired",
        operator_message: "Credential expired",
        operator_fix: "Update the API key in Settings → Providers",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    const link = screen.getByRole("link", { name: /Update the API key/i });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "/settings#providers");
  });

  it("renders plain text for action_needed operator_fix when no Settings mention", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "db_constraint",
        operator_message: "DB error",
        operator_fix: "Open orchestrator details and inspect the offending row",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    expect(screen.queryByRole("link", { name: /Open orchestrator details and inspect/i })).toBeNull();
    expect(screen.getByText(/Open orchestrator details and inspect the offending row/)).toBeInTheDocument();
  });

  it("expands cascade waiters when +N layers is clicked", async () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "DB error",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 3,
        affected_downstream: ["financial_facts", "thesis", "scoring"],
      },
    ];
    renderPanel({ v2 });
    screen.getByText(/\+3 layers waiting/).click();
    expect(await screen.findByText("financial_facts")).toBeInTheDocument();
    expect(screen.getByText("thesis")).toBeInTheDocument();
    expect(screen.getByText("scoring")).toBeInTheDocument();
  });

  it("renders Checking skeleton when v2 is null and has no cached snapshot", () => {
    renderPanel({ v2: null, jobs: null, coverage: null });
    expect(screen.getByText(/Checking for problems/i)).toBeInTheDocument();
  });

  it("keeps last-good snapshot rendered when v2 briefly goes null", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "source_down",
        operator_message: "down",
        operator_fix: null,
        self_heal: true,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    const { rerender } = render(
      <MemoryRouter>
        <ProblemsPanel
          v2={v2}
          jobs={emptyJobs()}
          coverage={emptyCoverage()}
          v2Error={false}
          jobsError={false}
          coverageError={false}
          onOpenOrchestrator={() => {}}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText(/Layer X/)).toBeInTheDocument();
    rerender(
      <MemoryRouter>
        <ProblemsPanel
          v2={null}
          jobs={emptyJobs()}
          coverage={emptyCoverage()}
          v2Error={false}
          jobsError={false}
          coverageError={false}
          onOpenOrchestrator={() => {}}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText(/Layer X/)).toBeInTheDocument();
  });

  it("calls onOpenOrchestrator with the layer name when drill-through clicked", () => {
    const onOpen = vi.fn();
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "err",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2, onOpenOrchestrator: onOpen });
    screen.getByRole("button", { name: /Open orchestrator details for cik_mapping/ }).click();
    expect(onOpen).toHaveBeenCalledWith("cik_mapping");
  });

  it("carries over failing jobs from v1 behaviour", () => {
    const jobs: JobsListResponse = {
      checked_at: new Date().toISOString(),
      jobs: [
        {
          name: "test_job",
          description: "test job",
          cadence: "daily",
          cadence_kind: "daily",
          next_run_time: new Date().toISOString(),
          next_run_time_source: "declared",
          last_status: "failure",
          last_started_at: null,
          last_finished_at: new Date().toISOString(),
          detail: "",
        },
      ],
    };
    renderPanel({ jobs });
    expect(screen.getByText(/test_job/)).toBeInTheDocument();
    expect(screen.getByText(/last run failed/i)).toBeInTheDocument();
  });

  it("carries over coverage null_rows from v1 behaviour", () => {
    const coverage: CoverageSummaryResponse = {
      ...emptyCoverage(),
      null_rows: 12,
    };
    renderPanel({ coverage });
    expect(screen.getByText(/12 instrument/)).toBeInTheDocument();
  });

  it("renders plain text when operator_fix mentions Settings only as context, not as a destination", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "db_constraint",
        operator_message: "DB error",
        operator_fix: "Nothing to do with Settings or Providers — inspect the offending row manually",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    expect(
      screen.queryByRole("link", { name: /Nothing to do with Settings/i }),
    ).toBeNull();
    expect(
      screen.getByText(/Nothing to do with Settings or Providers/),
    ).toBeInTheDocument();
  });

  it("renders a Settings link when operator_fix says 'Update the API key in Settings'", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "auth_expired",
        operator_message: "Credential expired",
        operator_fix: "Update the API key in Settings",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    const link = screen.getByRole("link", { name: /Update the API key in Settings/i });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "/settings#providers");
  });

  it("keeps last-good coverage visible when coverage errors while v2 updates", () => {
    // Initial render: both sources succeed, coverage has 5 null rows.
    const initialV2 = emptyV2();
    initialV2.system_state = "ok";
    const initialCoverage: CoverageSummaryResponse = {
      ...emptyCoverage(),
      null_rows: 5,
    };
    const { rerender } = render(
      <MemoryRouter>
        <ProblemsPanel
          v2={initialV2}
          jobs={emptyJobs()}
          coverage={initialCoverage}
          v2Error={false}
          jobsError={false}
          coverageError={false}
          onOpenOrchestrator={() => {}}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText(/5 instrument/)).toBeInTheDocument();

    // Simultaneous transition: v2 flips to needs_attention with a new
    // action_needed row; coverage refetch errors (data stays as the
    // last-good value). Assert (a) v2 update visible, (b) amber banner
    // lists coverage only, (c) the 5 null_rows still render from cache.
    const updatedV2 = emptyV2();
    updatedV2.system_state = "needs_attention";
    updatedV2.action_needed = [
      {
        root_layer: "cik_mapping",
        display_name: "SEC CIK Mapping",
        category: "db_constraint",
        operator_message: "Database error",
        operator_fix: null,
        self_heal: false,
        consecutive_failures: 2,
        affected_downstream: [],
      },
    ];
    rerender(
      <MemoryRouter>
        <ProblemsPanel
          v2={updatedV2}
          jobs={emptyJobs()}
          coverage={initialCoverage}
          v2Error={false}
          jobsError={false}
          coverageError={true}
          onOpenOrchestrator={() => {}}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/SEC CIK Mapping/)).toBeInTheDocument();
    expect(screen.getByText(/5 instrument/)).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(/coverage/i);
    expect(screen.getByRole("status")).not.toHaveTextContent(/layers/i);
    expect(screen.getByRole("status")).not.toHaveTextContent(/jobs/i);
  });

  it("uses a combined-count header when carry-over rows contribute problems but v2 is clean", () => {
    // Regression guard: previously the header text came from
    // v2.system_summary ("All layers healthy") even when a failed
    // job was rendered underneath. Must say "N problem(s) need
    // attention" whenever jobs/coverage contribute.
    const jobs: JobsListResponse = {
      jobs: [
        {
          name: "test_job",
          description: "test job",
          cadence: "daily",
          next_run_time: null,
          last_status: "failure",
          last_finished_at: new Date().toISOString(),
        } as unknown as JobsListResponse["jobs"][number],
      ],
    };
    renderPanel({ v2: emptyV2(), jobs });
    // v2 is clean; v2.system_summary = "All layers healthy".
    // Header MUST reflect the one failed job, not parrot the v2 summary.
    expect(screen.queryByText(/All layers healthy/)).toBeNull();
    expect(screen.getByText(/1 problem.*need.*attention/i)).toBeInTheDocument();
  });

  it("renders plain text for a SecretMissingItem whose operator_fix lacks Settings phrasing", () => {
    // Regression guard: the backend has a defensive fallback
    // operator_fix="Check layer secret configuration" for layers
    // without declared secret_refs. That must render as plain text,
    // not a misleading /settings#providers link.
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.secret_missing = [
      {
        layer: "some_layer",
        display_name: "Some Layer",
        missing_secret: "(unknown)",
        operator_fix: "Check layer secret configuration",
      },
    ];
    renderPanel({ v2 });
    expect(screen.getByText(/Check layer secret configuration/)).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: /Check layer secret configuration/i }),
    ).toBeNull();
  });
});
