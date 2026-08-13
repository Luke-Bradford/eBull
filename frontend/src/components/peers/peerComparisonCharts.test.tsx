/**
 * Render tests for the peer-comparison charts (#594). The pure shaping is
 * exercised in `lib/peerComparison.test.ts`; these pin the empty-state guards
 * and confirm the populated branch mounts. The heatmap is hand-rolled (no
 * ResponsiveContainer) so its content is assertable; the radar + scatter use
 * ResponsiveContainer (0px in jsdom — see test/setup.ts) so we assert the guard
 * + the container element, with recharts props validated by typecheck.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  PeerRadarChart,
  PeerReturnScatter,
  SectorHeatmap,
} from "@/components/peers/peerComparisonCharts";
import {
  buildHeatmap,
  buildRadar,
  buildScatter,
} from "@/lib/peerComparison";
import type { CandleBar, PeerComparison } from "@/api/types";

const PC: PeerComparison = {
  symbol: "AAA",
  instrument_id: 1,
  cohort_sic: "3571",
  cohort_sic_label: "Electronic Computers",
  cohort_sic_level: 4,
  cohort_member_count: 951,
  factors: [
    { key: "roe", label: "ROE", instrument_value: 0.9, cohort_median: 0.1, cohort_n: 800, dev_limited: false, better_when: "higher" },
    { key: "pe", label: "P/E", instrument_value: 40, cohort_median: 50, cohort_n: 2, dev_limited: true, better_when: "lower" },
  ],
  peers: [{ instrument_id: 2, symbol: "BBB", company_name: "BBB Inc", size_proxy: 1e9, factors: { roe: 0.5, pe: 60 } }],
};

const EMPTY_PC: PeerComparison = {
  symbol: "ZZZ",
  instrument_id: 9,
  cohort_sic: "9999",
  cohort_sic_label: null,
  cohort_sic_level: 0,
  cohort_member_count: 0,
  factors: [],
  peers: [],
};

function cb(date: string, close: string): CandleBar {
  return { date, open: null, high: null, low: null, close, volume: null };
}

describe("PeerRadarChart", () => {
  it("renders the no-factors hint when nothing is comparable", () => {
    render(<PeerRadarChart radar={buildRadar(EMPTY_PC)} symbol="ZZZ" />);
    expect(screen.getByText(/No comparable factors/i)).toBeInTheDocument();
  });

  it("mounts the radar when factors are present", () => {
    const { container } = render(<PeerRadarChart radar={buildRadar(PC)} symbol="AAA" />);
    expect(screen.queryByText(/No comparable factors/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});

describe("SectorHeatmap", () => {
  it("renders the no-peers hint on empty data", () => {
    render(<SectorHeatmap heatmap={buildHeatmap(EMPTY_PC)} />);
    expect(screen.getByText(/No peers to map/i)).toBeInTheDocument();
  });

  it("pins the instrument row and renders peer rows + factor headers", () => {
    render(<SectorHeatmap heatmap={buildHeatmap(PC)} />);
    expect(screen.getByText("AAA")).toBeInTheDocument(); // instrument row
    expect(screen.getByText("BBB")).toBeInTheDocument(); // peer row
    expect(screen.getByText(/ROE/)).toBeInTheDocument(); // factor header
  });
});

describe("PeerReturnScatter", () => {
  it("renders the not-enough-history hint with no overlapping returns", () => {
    render(<PeerReturnScatter data={buildScatter("AAA", [], {})} />);
    expect(screen.getByText(/Not enough overlapping price history/i)).toBeInTheDocument();
  });

  it("mounts the scatter when returns align", () => {
    const candles: Record<string, CandleBar[]> = {
      AAA: [cb("2026-06-01", "100"), cb("2026-06-02", "110")],
      BBB: [cb("2026-06-01", "100"), cb("2026-06-02", "105")],
    };
    const { container } = render(<PeerReturnScatter data={buildScatter("AAA", ["BBB"], candles)} />);
    expect(screen.queryByText(/Not enough overlapping/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});
