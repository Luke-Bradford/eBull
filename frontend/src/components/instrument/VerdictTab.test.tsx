import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import { VerdictTab } from "@/components/instrument/VerdictTab";
import * as verdictApi from "@/api/verdict";
import * as historyApi from "@/api/scoreHistory";
import type {
  IarAnalytics,
  ThesisDetail,
  VerdictResponse,
} from "@/api/types";

function makeVerdict(
  overrides: Partial<VerdictResponse["score"]> = {},
  analytics: IarAnalytics | null = null,
): VerdictResponse {
  return {
    instrument_id: 1,
    score: {
      scored_at: "2026-06-29T09:00:00Z",
      model_version: "v1.2-balanced",
      rank: 5,
      rank_delta: -2,
      total_score: 0.82,
      raw_total: 0.87,
      quality_score: 0.9,
      value_score: 0.75,
      turnaround_score: 0.6,
      momentum_score: 0.7,
      sentiment_score: 0.5,
      confidence_score: 0.85,
      data_completeness: 0.91,
      completeness_tier: "high",
      penalties_json: null,
      explanation: "Strong quality + value",
      analytics_json: analytics,
      ...overrides,
    },
  };
}

const FULL_IAR: IarAnalytics = {
  schema: "iar_v1",
  piotroski: { score: 7, components_available: 9, band: "strong", suppressed: false },
  altman_z: { z: 5.81, band: "safe", suppressed: false },
  positioning: {
    insider_net_90d: { signal: 0.62, net_shares: 120000, source: "insider_transactions" },
    inst_13f_qoq: { signal: 0.55, delta_shares_pct: 0.04, caveat: "<=135d stale" },
    short_interest: {
      signal: 0.8,
      short_pct: 0.03,
      days_to_cover: 1.2,
      falling: true,
      caveat: "% shares outstanding (public float not ingested); bi-monthly",
    },
  },
  peer_grade: {
    peer_key: "4",
    peer_n: 412,
    basis: "run_eligible_sector",
    families: {
      quality: { absolute: 0.9, percentile: 0.88, hybrid: 0.76 },
    },
  },
};

const THESIS: ThesisDetail = {
  thesis_id: 1,
  instrument_id: 1,
  thesis_version: 1,
  thesis_type: "full",
  stance: "buy",
  confidence_score: 0.8,
  buy_zone_low: 100,
  buy_zone_high: 120,
  base_value: 150,
  bull_value: 200,
  bear_value: 90,
  break_conditions_json: ["margin compression"],
  memo_markdown: "The bull case rests on services.",
  critic_json: null,
  created_at: "2026-06-29T09:00:00Z",
};

function mockHistoryEmpty() {
  vi.spyOn(historyApi, "fetchScoreHistory").mockResolvedValue({
    instrument_id: 1,
    items: [],
  });
}

describe("VerdictTab", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockHistoryEmpty();
  });

  it("renders never-scored empty state when score is null", async () => {
    vi.spyOn(verdictApi, "fetchScoreVerdict").mockResolvedValue({
      instrument_id: 1,
      score: null,
    });
    render(
      <MemoryRouter>
        <VerdictTab instrumentId={1} thesis={null} />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/not yet scored/i)).toBeInTheDocument();
  });

  it("renders error state on fetch failure", async () => {
    vi.spyOn(verdictApi, "fetchScoreVerdict").mockRejectedValue(
      new Error("boom"),
    );
    render(
      <MemoryRouter>
        <VerdictTab instrumentId={1} thesis={null} />
      </MemoryRouter>,
    );
    expect(await screen.findByText(/boom/i)).toBeInTheDocument();
  });

  it("renders headline + populated IAR signals", async () => {
    vi.spyOn(verdictApi, "fetchScoreVerdict").mockResolvedValue(
      makeVerdict({}, FULL_IAR),
    );
    render(
      <MemoryRouter>
        <VerdictTab instrumentId={1} thesis={THESIS} />
      </MemoryRouter>,
    );
    // headline
    expect(await screen.findByText("0.82")).toBeInTheDocument();
    // Exact match: the ThesisPane now also renders a "Buy zone" label
    // (#1902), so a loose /buy/i regex would double-match.
    expect(screen.getByText("buy")).toBeInTheDocument();
    expect(screen.getByText(/rank #5/)).toBeInTheDocument();
    // Piotroski 7/9 strong + Altman safe
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByText("strong")).toBeInTheDocument();
    expect(screen.getByText("safe")).toBeInTheDocument();
    // peer percentile labelled evidence-only (appears in the family header)
    expect(screen.getAllByText(/evidence-only/i).length).toBeGreaterThan(0);
    expect(screen.getByText("88%")).toBeInTheDocument();
    // thesis narrative reused
    expect(screen.getByText(/bull case rests on services/i)).toBeInTheDocument();
  });

  it("renders scored-but-no-IAR honestly (pre-#1823 row)", async () => {
    vi.spyOn(verdictApi, "fetchScoreVerdict").mockResolvedValue(
      makeVerdict({}, null),
    );
    render(
      <MemoryRouter>
        <VerdictTab instrumentId={1} thesis={null} />
      </MemoryRouter>,
    );
    // headline still renders
    expect(await screen.findByText("0.82")).toBeInTheDocument();
    // signal sections honest about missing evidence
    expect(
      screen.getAllByText(/evidence not yet computed/i).length,
    ).toBeGreaterThan(0);
  });

  it("renders sparse IAR honestly: suppressed F/Z + unavailable positioning", async () => {
    const sparse: IarAnalytics = {
      schema: "iar_v1",
      piotroski: { score: null, suppressed: true, reason: "quality_signal_na_financials" },
      altman_z: { z: null, suppressed: true, reason: "quality_signal_na_financials" },
      positioning: {
        insider_net_90d: { signal: null, reason: "no_insider_or_shares" },
        inst_13f_qoq: { signal: null, reason: "insufficient_periods" },
        short_interest: { signal: null, reason: "no_short_interest_or_shares" },
      },
      peer_grade: { basis: "absolute_only", reason: "no_run_context", families: {} },
    };
    vi.spyOn(verdictApi, "fetchScoreVerdict").mockResolvedValue(
      makeVerdict({}, sparse),
    );
    render(
      <MemoryRouter>
        <VerdictTab instrumentId={1} thesis={null} />
      </MemoryRouter>,
    );
    expect(await screen.findByText("0.82")).toBeInTheDocument();
    // financials-suppressed quality signals
    expect(screen.getAllByText(/n\/a — financials/i).length).toBe(2);
    // unavailable positioning (3 cards)
    expect(screen.getAllByText(/unavailable/i).length).toBeGreaterThanOrEqual(3);
    // peer cohort pending note (no peer_key)
    expect(screen.getByText(/peer percentile pending/i)).toBeInTheDocument();
  });
});
