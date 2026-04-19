/**
 * Unit tests for SyncDashboard helpers + LayerProgressBar rendering.
 *
 * Focus:
 *   - parseUtc — the Safari-strict ISO-8601 parse used when
 *     computing sync-run durations. Without timezone normalisation,
 *     Safari parses offset-less strings as local time, which would
 *     produce wrong durations for operators outside UTC.
 *   - LayerProgressBar — the three render shapes (starting / counter
 *     / proportional bar) driven by the active_layer progress payload
 *     produced by the Phase 2 ticks. We render the full SyncDashboard
 *     with a mocked sync API so the in-card progress rendering is
 *     exercised end-to-end.
 */

import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";

import { SyncDashboard, parseUtc } from "./SyncDashboard";
import {
  fetchSyncLayers,
  fetchSyncRuns,
  fetchSyncStatus,
} from "@/api/sync";
import type { SyncTriggerState } from "@/lib/useSyncTrigger";

function fakeTrigger(): SyncTriggerState {
  return {
    kind: "idle",
    queuedRunId: null,
    message: null,
    trigger: vi.fn(),
    clearQueued: vi.fn(),
  };
}

vi.mock("@/api/sync", () => ({
  fetchSyncLayers: vi.fn(),
  fetchSyncStatus: vi.fn(),
  fetchSyncRuns: vi.fn(),
  triggerSync: vi.fn(),
}));

const mockedLayers = vi.mocked(fetchSyncLayers);
const mockedStatus = vi.mocked(fetchSyncStatus);
const mockedRuns = vi.mocked(fetchSyncRuns);

describe("parseUtc", () => {
  it("parses string with explicit +00:00 offset as UTC", () => {
    const d = parseUtc("2026-04-16T12:30:00+00:00");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("parses string with Z suffix as UTC", () => {
    const d = parseUtc("2026-04-16T12:30:00Z");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("appends Z when offset is missing (Safari-safe)", () => {
    const d = parseUtc("2026-04-16T12:30:00");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("preserves explicit non-UTC offset", () => {
    const d = parseUtc("2026-04-16T12:30:00+02:00");
    // 12:30 at +02:00 = 10:30 UTC
    expect(d.toISOString()).toBe("2026-04-16T10:30:00.000Z");
  });
});

function baseLayersResponse() {
  return {
    layers: [
      {
        name: "candles",
        display_name: "Candles",
        tier: 1 as const,
        is_fresh: false,
        freshness_detail: "stale: 3h since last sync",
        last_success_at: "2026-04-16T09:00:00Z",
        last_duration_seconds: 120,
        last_error_category: null,
        consecutive_failures: 0,
        dependencies: ["universe"],
        is_blocking: true,
      },
    ],
  };
}

function runningStatus(opts: {
  itemsDone: number | null;
  itemsTotal: number | null;
}) {
  return {
    is_running: true,
    current_run: {
      sync_run_id: 42,
      scope: "full",
      trigger: "manual",
      started_at: "2026-04-16T12:00:00Z",
      layers_planned: 15,
      layers_done: 3,
      layers_failed: 0,
      layers_skipped: 0,
    },
    active_layer: {
      name: "candles",
      started_at: "2026-04-16T12:05:00Z",
      items_total: opts.itemsTotal,
      items_done: opts.itemsDone,
    },
  };
}

describe("LayerProgressBar", () => {
  beforeEach(() => {
    mockedLayers.mockReset();
    mockedStatus.mockReset();
    mockedRuns.mockReset();
    mockedLayers.mockResolvedValue(baseLayersResponse());
    mockedRuns.mockResolvedValue({ runs: [] });
  });

  it("renders 'starting…' before the first items tick lands", async () => {
    mockedStatus.mockResolvedValue(
      runningStatus({ itemsDone: null, itemsTotal: null }),
    );
    render(<SyncDashboard syncTrigger={fakeTrigger()} />);
    expect(await screen.findByText("starting…")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("renders plain counter when items_total is unknown", async () => {
    mockedStatus.mockResolvedValue(
      runningStatus({ itemsDone: 42, itemsTotal: null }),
    );
    render(<SyncDashboard syncTrigger={fakeTrigger()} />);
    expect(await screen.findByText("42 items processed")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("renders proportional progressbar when both sides are known", async () => {
    mockedStatus.mockResolvedValue(
      runningStatus({ itemsDone: 25, itemsTotal: 100 }),
    );
    render(<SyncDashboard syncTrigger={fakeTrigger()} />);
    const bar = await screen.findByRole("progressbar", {
      name: "candles progress",
    });
    expect(bar).toHaveAttribute("aria-valuenow", "25");
    expect(bar).toHaveAttribute("aria-valuemin", "0");
    expect(bar).toHaveAttribute("aria-valuemax", "100");
    expect(screen.getByText("25 / 100")).toBeInTheDocument();
    expect(screen.getByText("25%")).toBeInTheDocument();
  });

  it("caps visible progress at 100 if items_done overshoots items_total", async () => {
    // Defensive: an adapter that misreports (e.g. counts both a skip
    // and a retry against the same item) must not blow past the track.
    mockedStatus.mockResolvedValue(
      runningStatus({ itemsDone: 150, itemsTotal: 100 }),
    );
    render(<SyncDashboard syncTrigger={fakeTrigger()} />);
    const bar = await screen.findByRole("progressbar", {
      name: "candles progress",
    });
    expect(bar).toHaveAttribute("aria-valuenow", "100");
    expect(screen.getByText("100%")).toBeInTheDocument();
  });
});
