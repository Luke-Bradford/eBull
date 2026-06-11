/**
 * Render-state tests for the ownership history pane (#922):
 * loading / error / empty / data / partial-failure / unsupported
 * modes, with the fetcher module mocked. Chart internals (recharts
 * lines) are covered by the pure-series tests; here we assert the
 * state contract per loading-error-empty-states.md.
 */

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { OwnershipHistoryResponse } from "@/api/ownershipHistory";

import { OwnershipHistoryChart } from "./OwnershipHistoryChart";

vi.mock("@/api/ownershipHistory", () => ({
  fetchOwnershipHistory: vi.fn(),
}));

import { fetchOwnershipHistory } from "@/api/ownershipHistory";

const fetchMock = vi.mocked(fetchOwnershipHistory);

afterEach(() => {
  vi.restoreAllMocks();
  fetchMock.mockReset();
});

function response(
  category: string,
  points: OwnershipHistoryResponse["points"],
): OwnershipHistoryResponse {
  return { symbol: "AAPL", instrument_id: 1, category, holder_id: null, points };
}

function aggPoint(period_end: string, shares: string) {
  return {
    period_end,
    ownership_nature: "economic",
    shares,
    source: "13f",
    source_accession: null,
    filed_at: null,
    holder_count: 6011,
  };
}

function renderChart(over: Partial<Parameters<typeof OwnershipHistoryChart>[0]> = {}) {
  return render(
    <OwnershipHistoryChart
      symbol="AAPL"
      categoryFilter={null}
      filerFilter={null}
      filerLabel={null}
      outstanding={14_900_000_000}
      {...over}
    />,
  );
}

describe("OwnershipHistoryChart states", () => {
  it("renders both aggregate lines' legend when fetches succeed", async () => {
    fetchMock.mockImplementation((_s, p) =>
      Promise.resolve(
        p.category === "institutions"
          ? response("institutions", [aggPoint("2025-12-31", "4645585728"), aggPoint("2026-03-31", "4818120397")])
          : response("treasury", []),
      ),
    );
    renderChart();
    await waitFor(() => {
      expect(screen.getByText("Institutions (13F)")).toBeInTheDocument();
    });
    // Treasury series empty → no legend row for it.
    expect(screen.queryByText("Treasury")).toBeNull();
  });

  it("degrades to the surviving line + note when one aggregate fetch fails", async () => {
    fetchMock.mockImplementation((_s, p) =>
      p.category === "institutions"
        ? Promise.resolve(response("institutions", [aggPoint("2026-03-31", "100")]))
        : Promise.reject(new Error("boom")),
    );
    renderChart();
    await waitFor(() => {
      expect(screen.getByText(/Treasury failed to load/)).toBeInTheDocument();
    });
    expect(screen.getByText("Institutions (13F)")).toBeInTheDocument();
  });

  it("shows the error state when every fetch fails", async () => {
    fetchMock.mockRejectedValue(new Error("boom"));
    renderChart();
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
    });
  });

  it("shows the no-history empty state for an empty aggregate", async () => {
    fetchMock.mockImplementation((_s, p) =>
      Promise.resolve(response(p.category, [])),
    );
    renderChart();
    await waitFor(() => {
      expect(screen.getByText("No history yet")).toBeInTheDocument();
    });
  });

  it("explains per-holder-only categories without fetching", () => {
    renderChart({ categoryFilter: "insiders" });
    expect(screen.getByText("Per-holder view only")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("explains the no-CIK case without fetching", () => {
    renderChart({ categoryFilter: "insiders", filerFilter: "name:Cohen Ryan" });
    expect(screen.getByText("No per-holder history")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fetches the per-holder series with the mapped holder id", async () => {
    fetchMock.mockResolvedValue(
      response("blockholders", [
        {
          period_end: "2026-03-31",
          ownership_nature: "beneficial",
          shares: "38347842",
          source: "13d",
          source_accession: "0001234500-26-000001",
          filed_at: null,
          holder_count: null,
        },
      ]),
    );
    renderChart({
      categoryFilter: "blockholders",
      filerFilter: "block:0001767470",
      filerLabel: "Cohen Ryan",
    });
    await waitFor(() => {
      expect(screen.getByText("Cohen Ryan")).toBeInTheDocument();
    });
    expect(fetchMock).toHaveBeenCalledExactlyOnceWith("AAPL", {
      category: "blockholders",
      holderId: "0001767470",
      fromDate: expect.any(String) as string,
    });
  });
});
