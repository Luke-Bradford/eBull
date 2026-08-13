import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import type {
  CoverageSummaryResponse,
  ProcessListResponse,
  SyncLayersV2Response,
} from "@/api/types";

import { makeProcessList, makeProcessRow } from "./__fixtures__/processes";
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


function emptyProcesses(): ProcessListResponse {
  return makeProcessList([]);
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
    processes: emptyProcesses(),
    coverage: emptyCoverage(),
    v2Error: false,
    processesError: false,
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

  it("injects credential-rejected banner when credential_health.state === 'rejected'", () => {
    // #979 / #974/E: when the operator's aggregate health is REJECTED,
    // the orchestrator gate (#977) PREREQ_SKIPs all credential-using
    // layers — so v2.action_needed would be empty even though the
    // system is fully gated. The banner item compensates so the
    // operator still sees the actionable "fix Settings" surface.
    renderPanel({
      credentialHealth: {
        state: "rejected",
        last_recovered_at: null,
        last_error: null,
      },
    });
    expect(screen.getByText(/Credentials rejected by provider/i)).toBeInTheDocument();
  });

  it("does NOT inject the banner when credential_health.state === 'valid'", () => {
    const { container } = renderPanel({
      credentialHealth: {
        state: "valid",
        last_recovered_at: null,
        last_error: null,
      },
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("does NOT inject the banner when credential_health is null (legacy / pre-bootstrap)", () => {
    const { container } = renderPanel({ credentialHealth: null });
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

  it("renders error_excerpt under the message when the API returns one (#645 forensics)", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "fundamentals",
        display_name: "Fundamentals",
        category: "internal_error",
        operator_message: "Unclassified error — retrying with backoff",
        operator_fix: null,
        self_heal: true,
        consecutive_failures: 7,
        affected_downstream: [],
        error_excerpt: "KeyError: 'cik'",
      },
    ];
    renderPanel({ v2 });
    expect(screen.getByTestId("problems-error-excerpt")).toHaveTextContent("KeyError: 'cik'");
  });

  it("omits the error_excerpt block when the API does not return one (legacy pre-#645 row)", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "fundamentals",
        display_name: "Fundamentals",
        category: "internal_error",
        operator_message: "Unclassified error — retrying with backoff",
        operator_fix: null,
        self_heal: true,
        consecutive_failures: 1,
        affected_downstream: [],
        // error_excerpt omitted entirely (undefined) — same as a
        // legacy row with no message column populated.
      },
    ];
    renderPanel({ v2 });
    expect(screen.queryByTestId("problems-error-excerpt")).not.toBeInTheDocument();
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
        operator_fix: "Update the public key in Settings → Providers",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    const link = screen.getByRole("link", { name: /Update the public key/i });
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
    renderPanel({ v2: null, processes: null, coverage: null });
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
          processes={emptyProcesses()}
          coverage={emptyCoverage()}
          v2Error={false}
          processesError={false}
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
          processes={emptyProcesses()}
          coverage={emptyCoverage()}
          v2Error={false}
          processesError={false}
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

  it("surfaces a failing steady-state process (attention verdict)", () => {
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "test_job",
        display_name: "Test job",
        status: "failed", // derives health_verdict "attention", reason "last run failed"
      }),
    ]);
    renderPanel({ processes });
    expect(screen.getByText(/Test job — last run failed/i)).toBeInTheDocument();
  });

  it("counts an ingest_sweep process the legacy jobs list omitted (#1959)", () => {
    // The whole point of #1959: nport_sweep is a `mechanism=ingest_sweep`
    // steady-state process that /system/jobs never carried, so the top
    // banner under-counted. It must now surface here.
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "nport_sweep",
        display_name: "N-PORT (fund holdings) sweep",
        mechanism: "ingest_sweep",
        lane: "ownership",
        role: "steady_state",
        status: "failed",
      }),
    ]);
    renderPanel({ processes });
    expect(
      screen.getByText(/N-PORT \(fund holdings\) sweep — last run failed/i),
    ).toBeInTheDocument();
    const link = screen.getByRole("link", {
      name: /View runs for N-PORT \(fund holdings\) sweep/i,
    });
    expect(link).toHaveAttribute("href", "/admin/processes/nport_sweep");
  });

  it("does NOT count bootstrap / backfill attention rows (matches control-hub scope)", () => {
    // #1530 C7 — the control-hub "N need attention" count operates on
    // steady-state rows only; bootstrap/backfill one-shots fold into a
    // separate section. The top banner now matches that scope so the two
    // counts agree (#1959). A freshly-failed backfill must NOT raise the
    // top red banner.
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "ownership_observations_backfill",
        display_name: "Ownership observations backfill",
        role: "backfill",
        status: "failed",
      }),
      makeProcessRow({
        process_id: "bootstrap",
        display_name: "Bootstrap",
        role: "bootstrap",
        mechanism: "bootstrap",
        status: "failed",
      }),
    ]);
    const { container } = renderPanel({ processes });
    expect(container).toBeEmptyDOMElement();
  });

  it("does NOT count a non-attention steady-state process (self-healing / paused)", () => {
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "retrying_job",
        display_name: "Retrying job",
        status: "pending_retry", // → self_healing, not attention
      }),
      makeProcessRow({
        process_id: "paused_job",
        display_name: "Paused job",
        status: "disabled", // → paused (kill switch), not attention
      }),
    ]);
    const { container } = renderPanel({ processes });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders a drill-through link to /admin/processes/<id> for a failing process", () => {
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "fundamentals_sync",
        display_name: "Fundamentals research refresh",
        lane: "fundamentals",
        status: "failed",
      }),
    ]);
    renderPanel({ processes });
    const link = screen.getByRole("link", {
      name: /View runs for Fundamentals research refresh/i,
    });
    expect(link).toHaveAttribute("href", "/admin/processes/fundamentals_sync");
  });

  it("renders a 'Clears when' hint on each alert type", () => {
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
        consecutive_failures: 2,
        affected_downstream: [],
      },
    ];
    v2.secret_missing = [
      {
        layer: "news",
        display_name: "News & Sentiment",
        missing_secret: "ANTHROPIC_API_KEY",
        operator_fix: "Set ANTHROPIC_API_KEY in Settings → Providers",
      },
    ];
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "fundamentals_sync",
        display_name: "fundamentals_sync",
        lane: "fundamentals",
        status: "failed",
      }),
    ]);
    const coverage: CoverageSummaryResponse = {
      ...emptyCoverage(),
      null_rows: 7,
    };
    renderPanel({ v2, processes, coverage });
    expect(
      screen.getByText(/Clears when the next run of cik_mapping succeeds/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Clears when the credential is supplied/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        /Clears when the next run of fundamentals_sync succeeds/i,
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Clears after the fundamentals\/coverage audit/i),
    ).toBeInTheDocument();
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

  it("renders a Settings link when operator_fix says 'Update the public key in Settings'", () => {
    const v2 = emptyV2();
    v2.system_state = "needs_attention";
    v2.action_needed = [
      {
        root_layer: "x",
        display_name: "Layer X",
        category: "auth_expired",
        operator_message: "Credential expired",
        operator_fix: "Update the public key in Settings",
        self_heal: false,
        consecutive_failures: 1,
        affected_downstream: [],
      },
    ];
    renderPanel({ v2 });
    const link = screen.getByRole("link", { name: /Update the public key in Settings/i });
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
          processes={emptyProcesses()}
          coverage={initialCoverage}
          v2Error={false}
          processesError={false}
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
          processes={emptyProcesses()}
          coverage={initialCoverage}
          v2Error={false}
          processesError={false}
          coverageError={true}
          onOpenOrchestrator={() => {}}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/SEC CIK Mapping/)).toBeInTheDocument();
    expect(screen.getByText(/5 instrument/)).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(/coverage/i);
    expect(screen.getByRole("status")).not.toHaveTextContent(/layers/i);
    expect(screen.getByRole("status")).not.toHaveTextContent(/processes/i);
  });

  it("uses a combined-count header when carry-over rows contribute problems but v2 is clean", () => {
    // Regression guard: previously the header text came from
    // v2.system_summary ("All layers healthy") even when a failed
    // process was rendered underneath. Must say "N problem(s) need
    // attention" whenever processes/coverage contribute.
    const processes = makeProcessList([
      makeProcessRow({
        process_id: "test_job",
        display_name: "test_job",
        status: "failed",
      }),
    ]);
    renderPanel({ v2: emptyV2(), processes });
    // v2 is clean; v2.system_summary = "All layers healthy".
    // Header MUST reflect the one failed process, not parrot the v2 summary.
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
