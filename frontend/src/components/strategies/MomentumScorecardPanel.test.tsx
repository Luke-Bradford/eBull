import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { MomentumScorecardResponse } from "@/api/types";
import { MomentumScorecardPanel } from "@/components/strategies/MomentumScorecardPanel";

const SCORED: MomentumScorecardResponse = {
  periods: [
    {
      formation_month: "2026-09-01",
      start: "2026-10-14",
      end: "2026-11-02",
      universe_source: "reconstructed",
      names_priced: 29,
      names_stale: 1,
      book_return: -0.0312,
      spy_return: 0.0073,
      control_median: -0.005,
      control_p05: -0.038,
      control_p95: 0.0242,
      book_percentile: 0.085,
    },
  ],
  book_cumulative: -0.0312,
  spy_cumulative: 0.0073,
  control_cumulative_median: -0.005,
  book_cumulative_percentile: 0.085,
  control_draws: 200,
  pending: [],
};

describe("MomentumScorecardPanel", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("states the book against SPY and the random baskets", async () => {
    vi.spyOn(strategiesApi, "fetchMomentumScorecard").mockResolvedValue(SCORED);
    render(<MomentumScorecardPanel />);
    expect(await screen.findByText("Behind SPY")).toBeInTheDocument();
    expect(screen.getByText(/beat 9% of the baskets/)).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "Momentum book by formation" })).toBeInTheDocument();
    expect(screen.getByText(/29 priced · 1 stale · control universe rebuilt/)).toBeInTheDocument();
  });

  it("explains a formed book that has no close yet", async () => {
    vi.spyOn(strategiesApi, "fetchMomentumScorecard").mockResolvedValue({
      ...SCORED,
      periods: [],
      book_cumulative: null,
      spy_cumulative: null,
      control_cumulative_median: null,
      book_cumulative_percentile: null,
      pending: ["2026-09-01"],
    });
    render(<MomentumScorecardPanel />);
    expect(await screen.findByText(/Sept? 2026 book is formed; its first score appears after the next close/)).toBeInTheDocument();
    expect(screen.queryByText("Behind SPY")).not.toBeInTheDocument();
  });
});
