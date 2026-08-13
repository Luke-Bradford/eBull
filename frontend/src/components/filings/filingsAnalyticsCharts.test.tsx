/**
 * Render test for the red-flag-trend chart (#1748). jsdom stubs
 * ResizeObserver (test/setup.ts) so ResponsiveContainer measures 0px and
 * never draws the inner series — assert the empty-state guard + the
 * container element, not chart internals (recharts props are typecheck-
 * validated). Pins the empty branch (no risk-bearing filing) so a future
 * bug shows as the hint, not a blank frame.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { RedFlagTrendChart } from "@/components/filings/filingsAnalyticsCharts";
import type { RedFlagTrendPoint } from "@/api/types";

describe("RedFlagTrendChart", () => {
  it("renders the no-red-flag hint on empty points", () => {
    render(<RedFlagTrendChart points={[]} />);
    expect(screen.getByText(/No red-flag filings/i)).toBeInTheDocument();
  });

  it("mounts the chart when scored quarters are present", () => {
    const points: RedFlagTrendPoint[] = [
      { quarter: "2025-Q1", avg_score: 1.0, n: 1 },
      { quarter: "2025-Q3", avg_score: 0.7, n: 2 },
    ];
    const { container } = render(<RedFlagTrendChart points={points} />);
    expect(screen.queryByText(/No red-flag filings/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});
