import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { VerdictScore } from "@/api/types";
import { RankSummary } from "@/components/instrument/RankSummary";

function makeScore(overrides: Partial<VerdictScore> = {}): VerdictScore {
  return {
    scored_at: "2026-09-25T22:00:00Z",
    model_version: "v1.5-balanced",
    rank: 12,
    rank_delta: 3,
    ranked: true,
    not_ranked_reason: null,
    filings_status: "analysable",
    contributions: [
      { family: "quality", weight: 0.25, score: 0.9, contribution: 0.225 },
      { family: "value", weight: 0.25, score: 0.72, contribution: 0.18 },
      { family: "momentum", weight: 0.1, score: 0.5, contribution: 0.05 },
    ],
    total_score: 0.5,
    raw_total: 0.55,
    quality_score: 0.9,
    value_score: 0.7,
    turnaround_score: 0.4,
    momentum_score: 0.5,
    sentiment_score: 0.5,
    confidence_score: 0.5,
    data_completeness: 0.9,
    completeness_tier: "high",
    penalties_json: [
      { name: "wide_spread", reason: "spread", kind: "penalty", deduction: 0.05 },
      { name: "deep_drawdown", reason: "dd", kind: "penalty", deduction: 0.1 },
      { name: "stale_thesis", reason: "old", kind: "penalty", deduction: 0.02 },
      { name: "strong_calmar", reason: "calmar", kind: "reward", addition: 0.04 },
    ],
    explanation: null,
    analytics_json: null,
    ...overrides,
  };
}

describe("RankSummary", () => {
  it("shows the current rank, its delta, the two largest contributions and deductions", () => {
    render(<RankSummary score={makeScore()} errored={false} />);
    const line = screen.getByTestId("rank-summary");
    expect(line).toHaveTextContent("Rank #12");
    expect(screen.getByText("improved by 3")).toBeInTheDocument();
    expect(line).toHaveTextContent("largest: Quality +0.23 · Value +0.18");
    expect(line).not.toHaveTextContent("Momentum");
    expect(line).toHaveTextContent("Deep drawdown −0.10");
    expect(line).toHaveTextContent("Wide spread −0.05");
    expect(line).not.toHaveTextContent("Stale thesis");
    expect(line).not.toHaveTextContent("Strong Calmar");
  });

  it("never shows a stale stored rank as current", () => {
    render(
      <RankSummary
        score={makeScore({ ranked: false, not_ranked_reason: "not_in_latest_run", rank: 12 })}
        errored={false}
      />,
    );
    const line = screen.getByTestId("rank-summary");
    expect(line).not.toHaveTextContent("#12");
    expect(line).toHaveTextContent("Not ranked · absent from the latest run (last scored 2026-09-25)");
  });

  it("names a coverage reason for an unanalysable instrument", () => {
    render(
      <RankSummary
        score={makeScore({
          ranked: false,
          not_ranked_reason: "not_analysable",
          filings_status: "insufficient",
        })}
        errored={false}
      />,
    );
    expect(screen.getByTestId("rank-summary")).toHaveTextContent(
      "Not ranked · SEC 10-K/10-Q history too thin to rank",
    );
  });

  it("renders nothing while loading, and says so when never scored or errored", () => {
    const { container, rerender } = render(<RankSummary score={undefined} errored={false} />);
    expect(container).toBeEmptyDOMElement();
    rerender(<RankSummary score={null} errored={false} />);
    expect(screen.getByTestId("rank-summary")).toHaveTextContent("Not scored");
    rerender(<RankSummary score={undefined} errored />);
    expect(screen.getByTestId("rank-summary")).toHaveTextContent("Rank unavailable");
  });
});
