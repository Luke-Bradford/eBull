/**
 * Render-state tests for the segments pane (#554): loading / error /
 * empty / table / geographic-bars / axis-toggle-refetch, with the
 * fetcher module mocked — the state contract per
 * loading-error-empty-states.md.
 */

import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { InstrumentSegments, SegmentRow } from "@/api/instruments";

import { SegmentsPane } from "./SegmentsPane";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentSegments: vi.fn(),
}));

import { fetchInstrumentSegments } from "@/api/instruments";

const fetchMock = vi.mocked(fetchInstrumentSegments);

afterEach(() => {
  vi.restoreAllMocks();
  fetchMock.mockReset();
});

function row(over: Partial<SegmentRow> = {}): SegmentRow {
  return {
    member_qname: "aapl:AmericasSegmentMember",
    member_label: "Americas Segment",
    revenue: 178_353_000_000,
    operating_income: 72_480_000_000,
    assets: null,
    pct_of_total: 0.4286,
    ...over,
  };
}

function response(rows: SegmentRow[], axis: InstrumentSegments["axis"] = "business"): InstrumentSegments {
  return {
    symbol: "AAPL",
    axis,
    period_end: "2025-09-27",
    filed_at: "2025-10-31T00:00:00Z",
    sources: { revenue: "0000320193-25-000079" },
    total_revenue: 416_161_000_000,
    rows,
  };
}

describe("SegmentsPane states", () => {
  it("shows the skeleton while loading", () => {
    fetchMock.mockReturnValue(new Promise(() => {}));
    render(<SegmentsPane symbol="AAPL" />);
    expect(screen.getByRole("status")).toBeInTheDocument();
  });

  it("shows the error surface with retry on failure", async () => {
    fetchMock.mockRejectedValue(new Error("boom"));
    render(<SegmentsPane symbol="AAPL" />);
    await waitFor(() => expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument());
  });

  it("shows the empty state on 404 (fetcher null)", async () => {
    fetchMock.mockResolvedValue(null);
    render(<SegmentsPane symbol="AAPL" />);
    await waitFor(() => expect(screen.getByText("No breakdown on file")).toBeInTheDocument());
  });

  it("renders the segments table with revenue, pct and op income", async () => {
    fetchMock.mockResolvedValue(response([row()]));
    render(<SegmentsPane symbol="AAPL" />);
    await waitFor(() => expect(screen.getByText("Americas Segment")).toBeInTheDocument());
    expect(screen.getByText("178.35B")).toBeInTheDocument();
    expect(screen.getByText("42.9%")).toBeInTheDocument();
    expect(screen.getByText("72.48B")).toBeInTheDocument();
    // No assets anywhere in the rows → the column collapses entirely.
    expect(screen.queryByText("Assets")).not.toBeInTheDocument();
    expect(screen.getByText(/FY ending 2025-09-27/)).toBeInTheDocument();
  });

  it("switches to geographic bars and refetches with the new axis", async () => {
    fetchMock.mockResolvedValue(response([row()]));
    const user = userEvent.setup();
    render(<SegmentsPane symbol="AAPL" />);
    await waitFor(() => expect(screen.getByText("Americas Segment")).toBeInTheDocument());

    fetchMock.mockResolvedValue(
      response(
        [
          row({ member_qname: "country:US", member_label: "UNITED STATES", revenue: 151_790_000_000, operating_income: null, pct_of_total: 0.3648 }),
          row({ member_qname: "aapl:OtherCountriesMember", member_label: "Other Countries", revenue: 199_994_000_000, operating_income: null, pct_of_total: 0.4806 }),
        ],
        "geographic",
      ),
    );
    await user.click(screen.getByRole("button", { name: "Geography" }));

    await waitFor(() => expect(screen.getByText("UNITED STATES")).toBeInTheDocument());
    expect(fetchMock).toHaveBeenLastCalledWith("AAPL", "geographic");
    // Bars, not a table.
    expect(screen.queryByText("% of total")).not.toBeInTheDocument();
    expect(screen.getByText("(36.5%)")).toBeInTheDocument();
  });
});
