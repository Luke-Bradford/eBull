import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import { ProblemsPanel } from "@/components/admin/ProblemsPanel";
import type {
  CoverageSummaryResponse,
  JobsListResponse,
} from "@/api/types";
import type { SyncLayer, SyncLayersResponse } from "@/api/sync";

function coverageClean(): CoverageSummaryResponse {
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

function layer(overrides: Partial<SyncLayer> = {}): SyncLayer {
  return {
    name: "x",
    display_name: "X",
    tier: 1,
    is_fresh: true,
    freshness_detail: "ok",
    last_success_at: "2026-04-19T00:00:00Z",
    last_duration_seconds: 1,
    last_error_category: null,
    consecutive_failures: 0,
    dependencies: [],
    is_blocking: true,
    ...overrides,
  };
}

function jobs(): JobsListResponse {
  return { checked_at: "2026-04-19T00:00:00Z", jobs: [] };
}

function render_(
  props: Partial<React.ComponentProps<typeof ProblemsPanel>> = {},
) {
  const onOpenOrchestrator = vi.fn();
  const defaults: React.ComponentProps<typeof ProblemsPanel> = {
    layers: { layers: [] } as SyncLayersResponse,
    jobs: jobs(),
    coverage: coverageClean(),
    layersError: false,
    jobsError: false,
    coverageError: false,
    onOpenOrchestrator,
  };
  const result = render(<ProblemsPanel {...defaults} {...props} />);
  return { onOpenOrchestrator, ...result };
}

describe("ProblemsPanel — rendering contract", () => {
  it("renders nothing when all sources resolved clean", async () => {
    const { container } = render_();
    await waitFor(() => {
      expect(container.querySelector("section")).toBeNull();
    });
  });

  it("shows 'Checking for problems…' when all sources are null", async () => {
    render_({ layers: null, jobs: null, coverage: null });
    expect(
      await screen.findByText(/Checking for problems/i),
    ).toBeInTheDocument();
  });

  it("renders resolved-source problems even while other sources are still null", async () => {
    render_({
      layers: {
        layers: [
          layer({
            name: "cik_mapping",
            display_name: "CIK mapping",
            is_fresh: false,
            consecutive_failures: 3,
            last_error_category: "db_constraint",
          }),
        ],
      },
      jobs: null, // still pending
      coverage: null, // still pending
    });
    expect(
      await screen.findByText(/CIK mapping — 3 consecutive failures/),
    ).toBeInTheDocument();
    expect(screen.getByText(/Checking 2 more sources/i)).toBeInTheDocument();
  });

  it("surfaces stale non-blocking layers as amber rows", async () => {
    render_({
      layers: {
        layers: [
          layer({
            name: "news",
            display_name: "News",
            is_fresh: false,
            is_blocking: false,
          }),
        ],
      },
    });
    expect(
      await screen.findByText(/News — stale \(non-blocking\)/),
    ).toBeInTheDocument();
  });

  it("surfaces an amber 'could not re-check' line on source refetch error", async () => {
    // Cached snapshot present → still renders last-good problems +
    // amber notice.
    const { rerender } = render(
      <ProblemsPanel
        layers={{
          layers: [
            layer({
              name: "cik",
              display_name: "CIK",
              is_fresh: false,
              consecutive_failures: 2,
              last_error_category: "db_constraint",
            }),
          ],
        }}
        jobs={jobs()}
        coverage={coverageClean()}
        layersError={false}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => undefined}
      />,
    );
    await screen.findByText(/CIK — 2 consecutive failures/);

    // Refetch fails for layers — component re-renders with
    // layers=null + layersError=true. Cache keeps the problem; panel
    // adds the "could not re-check layers" amber line.
    rerender(
      <ProblemsPanel
        layers={null}
        jobs={jobs()}
        coverage={coverageClean()}
        layersError={true}
        jobsError={false}
        coverageError={false}
        onOpenOrchestrator={() => undefined}
      />,
    );
    expect(screen.getByText(/CIK — 2 consecutive failures/)).toBeInTheDocument();
    expect(
      screen.getByText(/Could not re-check layers/i),
    ).toBeInTheDocument();
  });

  it("click on 'Open orchestrator details' action fires the callback", async () => {
    const { onOpenOrchestrator } = render_({
      layers: {
        layers: [
          layer({
            is_fresh: false,
            consecutive_failures: 1,
            last_error_category: "network",
          }),
        ],
      },
    });
    const btn = await screen.findByRole("button", {
      name: /Open orchestrator details/,
    });
    btn.click();
    expect(onOpenOrchestrator).toHaveBeenCalled();
  });
});
