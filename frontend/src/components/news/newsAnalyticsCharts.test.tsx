/**
 * Render tests for the news-analytics charts (#593). The pure shaping is
 * exercised in `lib/newsAnalytics.test.ts`; these pin the empty-state
 * guards inside the chart components (where a bug shows as a blank recharts
 * frame, not an error) and confirm the populated branch mounts the chart.
 *
 * jsdom stubs ResizeObserver as a noop (test/setup.ts) so ResponsiveContainer
 * measures 0px and never renders the inner series — hence we assert the
 * guard hints + the container element, not chart internals (the recharts
 * props are validated by typecheck).
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  NewsVolumeChart,
  SentimentTrendChart,
  SourceBreakdownPie,
} from "@/components/news/newsAnalyticsCharts";
import {
  buildSentimentSeries,
  buildSourceBreakdown,
  buildWeeklyVolume,
} from "@/lib/newsAnalytics";
import type { NewsItem } from "@/api/types";

let seq = 0;
function n(event_time: string, sentiment_score: number | null, source = "Yahoo Finance"): NewsItem {
  seq += 1;
  return {
    news_event_id: seq,
    instrument_id: 1,
    event_time,
    source,
    headline: `h${seq}`,
    category: "general",
    sentiment_score,
    importance_score: null,
    snippet: null,
    url: null,
  };
}

describe("SentimentTrendChart", () => {
  it("renders the no-signal hint when no item is scored", () => {
    render(<SentimentTrendChart series={buildSentimentSeries([n("2026-06-22T10:00:00Z", null)])} />);
    expect(screen.getByText(/No scored sentiment/i)).toBeInTheDocument();
  });

  it("renders the no-signal hint on an empty series", () => {
    render(<SentimentTrendChart series={buildSentimentSeries([])} />);
    expect(screen.getByText(/No scored sentiment/i)).toBeInTheDocument();
  });

  it("mounts the chart (not the hint) when scored data is present", () => {
    const { container } = render(
      <SentimentTrendChart
        series={buildSentimentSeries([
          n("2026-06-22T10:00:00Z", 0.3),
          n("2026-06-23T10:00:00Z", -0.4),
        ])}
      />,
    );
    expect(screen.queryByText(/No scored sentiment/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});

describe("NewsVolumeChart", () => {
  it("renders the no-news hint on empty data", () => {
    render(<NewsVolumeChart data={buildWeeklyVolume([])} />);
    expect(screen.getByText(/No news in the window/i)).toBeInTheDocument();
  });

  it("mounts the chart when there is news", () => {
    const { container } = render(
      <NewsVolumeChart data={buildWeeklyVolume([n("2026-06-22T10:00:00Z", 0.1)])} />,
    );
    expect(screen.queryByText(/No news in the window/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});

describe("SourceBreakdownPie", () => {
  it("renders the no-sources hint on empty data", () => {
    render(<SourceBreakdownPie slices={buildSourceBreakdown([])} />);
    expect(screen.getByText(/No sources in the window/i)).toBeInTheDocument();
  });

  it("mounts the donut when sources are present", () => {
    const { container } = render(
      <SourceBreakdownPie
        slices={buildSourceBreakdown([
          n("2026-06-22T10:00:00Z", 0.1, "Yahoo Finance"),
          n("2026-06-22T11:00:00Z", 0.1, "Reuters"),
        ])}
      />,
    );
    expect(screen.queryByText(/No sources in the window/i)).not.toBeInTheDocument();
    expect(container.querySelector(".recharts-responsive-container")).not.toBeNull();
  });
});
