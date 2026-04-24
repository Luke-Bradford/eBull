import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  CikTimingSummaryResponse,
  SeedProgressResponse,
} from "@/api/sync";

import { SeedProgressPanel } from "./SeedProgressPanel";

// Mock the API module — panel pulls all data via these functions.
vi.mock("@/api/sync", () => ({
  fetchSeedProgress: vi.fn(),
  fetchCikTimingLatest: vi.fn(),
  setIngestEnabled: vi.fn(),
}));

import {
  fetchCikTimingLatest,
  fetchSeedProgress,
  setIngestEnabled,
} from "@/api/sync";

const mockFetchSeedProgress = vi.mocked(fetchSeedProgress);
const mockFetchTiming = vi.mocked(fetchCikTimingLatest);
const mockSetEnabled = vi.mocked(setIngestEnabled);


function buildSeedProgress(
  overrides: Partial<SeedProgressResponse> = {},
): SeedProgressResponse {
  return {
    sources: [
      {
        source: "sec.submissions",
        key_description: "SEC submissions.json (per-CIK top accession)",
        seeded: 3_691,
        total: 5_134,
      },
    ],
    latest_run: {
      ingestion_run_id: 42,
      source: "sec_edgar",
      started_at: "2026-04-24T02:30:00+00:00",
      finished_at: "2026-04-24T02:33:10+00:00",
      status: "success",
      rows_upserted: 150_000,
      rows_skipped: 0,
    },
    ingest_paused: false,
    ...overrides,
  };
}


function buildTiming(
  overrides: Partial<CikTimingSummaryResponse> = {},
): CikTimingSummaryResponse {
  return {
    ingestion_run_id: 42,
    run_source: "sec_edgar",
    run_started_at: "2026-04-24T02:30:00+00:00",
    run_finished_at: "2026-04-24T02:33:10+00:00",
    run_status: "success",
    modes: [
      {
        mode: "seed",
        count: 120,
        p50_seconds: 0.45,
        p95_seconds: 1.8,
        max_seconds: 4.8,
        facts_upserted_total: 120_000,
      },
    ],
    slowest: [
      {
        cik: "0000320193",
        mode: "seed",
        seconds: 4.8,
        facts_upserted: 12_500,
        outcome: "success",
        finished_at: "2026-04-24T02:33:10+00:00",
      },
    ],
    ...overrides,
  };
}


afterEach(() => {
  vi.clearAllMocks();
});


describe("SeedProgressPanel", () => {
  it("renders the seed progress bar + latest run + timing percentiles", async () => {
    mockFetchSeedProgress.mockResolvedValue(buildSeedProgress());
    mockFetchTiming.mockResolvedValue(buildTiming());

    render(<SeedProgressPanel />);

    await waitFor(() => {
      expect(
        screen.getByText(/SEC submissions.json/i),
      ).toBeInTheDocument();
    });

    expect(screen.getByText(/3,691 \/ 5,134/)).toBeInTheDocument();
    expect(screen.getByText(/71.9%/)).toBeInTheDocument();
    expect(screen.getByText(/Latest run #42/)).toBeInTheDocument();
    // "success" appears twice (run status + slowest-CIK outcome column);
    // assert presence via count rather than a unique getByText.
    expect(screen.getAllByText("success").length).toBeGreaterThanOrEqual(1);
    // p50 0.45s rendered as "450 ms"
    expect(screen.getByText(/450 ms/)).toBeInTheDocument();
  });

  it("surfaces the paused banner + Resume label when ingest_paused is true", async () => {
    mockFetchSeedProgress.mockResolvedValue(
      buildSeedProgress({ ingest_paused: true }),
    );
    mockFetchTiming.mockResolvedValue(buildTiming());

    render(<SeedProgressPanel />);

    await waitFor(() => {
      expect(screen.getByText(/Ingest is paused/i)).toBeInTheDocument();
    });
    expect(
      screen.getByRole("button", { name: /Resume ingest/i }),
    ).toBeInTheDocument();
  });

  it("calls setIngestEnabled(true) when the Resume button is clicked", async () => {
    mockFetchSeedProgress.mockResolvedValue(
      buildSeedProgress({ ingest_paused: true }),
    );
    mockFetchTiming.mockResolvedValue(buildTiming());
    mockSetEnabled.mockResolvedValue({
      key: "fundamentals_ingest",
      display_name: "Fundamentals ingest",
      is_enabled: true,
    });

    render(<SeedProgressPanel />);

    const button = await screen.findByRole("button", { name: /Resume ingest/i });

    // Baseline fetch counts — both APIs fire once during initial mount.
    const seedBefore = mockFetchSeedProgress.mock.calls.length;
    const timingBefore = mockFetchTiming.mock.calls.length;

    await userEvent.click(button);

    await waitFor(() => {
      expect(mockSetEnabled).toHaveBeenCalledWith("fundamentals_ingest", true);
    });
    // Toggle must refetch BOTH sibling states (review #424 WARNING:
    // timing table would otherwise stay stale for up to 60 s of idle
    // polling after the operator toggles ingest).
    await waitFor(() => {
      expect(mockFetchSeedProgress.mock.calls.length).toBeGreaterThan(seedBefore);
      expect(mockFetchTiming.mock.calls.length).toBeGreaterThan(timingBefore);
    });
  });

  it("calls setIngestEnabled(false) when Pause is clicked while enabled", async () => {
    mockFetchSeedProgress.mockResolvedValue(
      buildSeedProgress({ ingest_paused: false }),
    );
    mockFetchTiming.mockResolvedValue(buildTiming());
    mockSetEnabled.mockResolvedValue({
      key: "fundamentals_ingest",
      display_name: "Fundamentals ingest",
      is_enabled: false,
    });

    render(<SeedProgressPanel />);

    const button = await screen.findByRole("button", { name: /Pause ingest/i });
    await userEvent.click(button);

    await waitFor(() => {
      expect(mockSetEnabled).toHaveBeenCalledWith("fundamentals_ingest", false);
    });
  });

  it("renders an empty-state message when no ingest run exists yet", async () => {
    mockFetchSeedProgress.mockResolvedValue(buildSeedProgress({ latest_run: null }));
    mockFetchTiming.mockResolvedValue(
      buildTiming({
        ingestion_run_id: null,
        run_source: null,
        run_started_at: null,
        run_finished_at: null,
        run_status: null,
        modes: [],
        slowest: [],
      }),
    );

    render(<SeedProgressPanel />);

    await waitFor(() => {
      expect(
        screen.getByText(/Per-CIK timing will appear here/i),
      ).toBeInTheDocument();
    });
    expect(screen.queryByText(/Latest run #/)).not.toBeInTheDocument();
  });
});
