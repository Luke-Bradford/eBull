import { describe, expect, it } from "vitest";

import {
  buildSentimentSeries,
  buildSourceBreakdown,
  buildWeeklyVolume,
  isoWeek,
  newsCoverage,
  UNKNOWN_SOURCE,
} from "@/lib/newsAnalytics";
import type { NewsItem } from "@/api/types";

let seq = 0;
function n(
  event_time: string,
  sentiment_score: number | null,
  source: string | null = "Yahoo Finance",
): NewsItem {
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

describe("isoWeek (ISO-8601 week-year, UTC — year-boundary correctness)", () => {
  const cases: ReadonlyArray<[string, number, number]> = [
    ["2020-12-31T12:00:00Z", 2020, 53], // Thu — last week of 2020
    ["2021-01-01T12:00:00Z", 2020, 53], // Fri — still ISO 2020-W53
    ["2021-01-04T12:00:00Z", 2021, 1], // Mon — first week of 2021
    ["2019-12-30T12:00:00Z", 2020, 1], // Mon — owned by ISO 2020-W01
    ["2016-01-01T12:00:00Z", 2015, 53], // Fri — owned by ISO 2015-W53
  ];
  it.each(cases)("%s → %i-W%i", (iso, year, week) => {
    expect(isoWeek(new Date(iso))).toEqual({ year, week });
  });

  it("buckets a near-midnight event by UTC, not local time", () => {
    // 23:30Z Sunday is still that ISO week in UTC regardless of viewer tz.
    expect(isoWeek(new Date("2026-06-21T23:30:00Z"))).toEqual({ year: 2026, week: 25 });
    expect(isoWeek(new Date("2026-06-22T00:30:00Z"))).toEqual({ year: 2026, week: 26 });
  });
});

describe("buildSentimentSeries", () => {
  it("daily mean averages scored items, excludes null scores, counts all", () => {
    const s = buildSentimentSeries([
      n("2026-06-22T10:00:00Z", 0.2),
      n("2026-06-22T14:00:00Z", 0.4),
      n("2026-06-22T15:00:00Z", null), // counts toward volume, not mean
    ]);
    expect(s.points).toHaveLength(1);
    expect(s.points[0]!.mean).toBeCloseTo(0.3, 6);
    expect(s.points[0]!.count).toBe(3);
  });

  it("builds a gap-free UTC day axis and advances the rolling window over quiet days", () => {
    const s = buildSentimentSeries([
      n("2026-06-22T10:00:00Z", 1),
      n("2026-06-25T10:00:00Z", -1), // 3-day gap
    ]);
    // 22,23,24,25 inclusive
    expect(s.points.map((p) => p.date)).toEqual([
      "2026-06-22",
      "2026-06-23",
      "2026-06-24",
      "2026-06-25",
    ]);
    // Quiet days carry null daily mean but a non-null rolling (trailing window).
    expect(s.points[1]!.mean).toBeNull();
    expect(s.points[1]!.rolling).toBeCloseTo(1, 6); // only the 22's item in window
    // Day 25: rolling over 19..25 window covers both items → mean(1,-1)=0
    expect(s.points[3]!.rolling).toBeCloseTo(0, 6);
  });

  it("rolling mean uses minPeriods=1 (first day is non-null)", () => {
    const s = buildSentimentSeries([n("2026-06-22T10:00:00Z", 0.5)]);
    expect(s.points[0]!.rolling).toBeCloseTo(0.5, 6);
  });

  it("splitOffset = 1 (all emerald) when every value is non-negative", () => {
    const s = buildSentimentSeries([
      n("2026-06-22T10:00:00Z", 0.2),
      n("2026-06-23T10:00:00Z", 0.5),
    ]);
    expect(s.splitOffset).toBeCloseTo(1, 6);
  });

  it("splitOffset = 0 (all red) when every value is negative", () => {
    const s = buildSentimentSeries([
      n("2026-06-22T10:00:00Z", -0.2),
      n("2026-06-23T10:00:00Z", -0.5),
    ]);
    expect(s.splitOffset).toBeCloseTo(0, 6);
  });

  it("splitOffset = fraction of domain above zero for mixed-sign data", () => {
    // rolling values: day1 = -0.2 ; day2 = mean(-0.2, 0.6) over 7d window = 0.2.
    // To make the test deterministic, isolate days >7d apart so each day's
    // rolling == its own daily mean.
    const s = buildSentimentSeries([
      n("2026-06-01T10:00:00Z", -0.2),
      n("2026-07-01T10:00:00Z", 0.6),
    ]);
    expect(s.min).toBeCloseTo(-0.2, 6);
    expect(s.max).toBeCloseTo(0.6, 6);
    // domain [-0.2, 0.6], span 0.8, zero sits 0.6/0.8 = 0.75 from top.
    expect(s.splitOffset).toBeCloseTo(0.75, 6);
  });

  it("handles empty + all-null-score input without throwing", () => {
    expect(buildSentimentSeries([]).points).toEqual([]);
    const allNull = buildSentimentSeries([n("2026-06-22T10:00:00Z", null)]);
    expect(allNull.points).toHaveLength(1);
    expect(allNull.points[0]!.rolling).toBeNull();
    expect(allNull.splitOffset).toBe(1);
  });
});

describe("buildWeeklyVolume", () => {
  it("counts per ISO week over a gap-free weekly axis", () => {
    const v = buildWeeklyVolume([
      n("2026-06-22T10:00:00Z", 0.1), // W26
      n("2026-06-24T10:00:00Z", 0.1), // W26
      n("2026-07-06T10:00:00Z", 0.1), // W28 (skips W27)
    ]);
    expect(v).toEqual([
      { week: "2026-W26", count: 2 },
      { week: "2026-W27", count: 0 }, // quiet week renders as a 0 bar
      { week: "2026-W28", count: 1 },
    ]);
  });

  it("returns [] for empty input", () => {
    expect(buildWeeklyVolume([])).toEqual([]);
  });
});

describe("buildSourceBreakdown", () => {
  it("counts by source, coalesces null/blank to Unknown, sorts count-desc", () => {
    const slices = buildSourceBreakdown([
      n("2026-06-22T10:00:00Z", 0.1, "Yahoo Finance"),
      n("2026-06-22T11:00:00Z", 0.1, "Yahoo Finance"),
      n("2026-06-22T12:00:00Z", 0.1, "Reuters"),
      n("2026-06-22T13:00:00Z", 0.1, null),
      n("2026-06-22T14:00:00Z", 0.1, "   "),
    ]);
    // Equal counts tie-break alphabetically: "Unknown" (U) before "Yahoo …" (Y).
    expect(slices).toEqual([
      { source: UNKNOWN_SOURCE, count: 2 },
      { source: "Yahoo Finance", count: 2 },
      { source: "Reuters", count: 1 },
    ]);
  });
});

describe("newsCoverage", () => {
  it("flags limited when < 2 weeks or < 2 sources (the dev reality)", () => {
    const dev = newsCoverage([
      n("2026-06-22T10:00:00Z", 0.1, "Yahoo Finance"),
      n("2026-06-24T10:00:00Z", 0.1, "Yahoo Finance"),
    ]);
    expect(dev).toEqual({ weeks: 1, sources: 1, limited: true });
  });

  it("not limited with ≥ 2 weeks and ≥ 2 sources", () => {
    const ok = newsCoverage([
      n("2026-06-22T10:00:00Z", 0.1, "Yahoo Finance"),
      n("2026-07-06T10:00:00Z", 0.1, "Reuters"),
    ]);
    expect(ok.limited).toBe(false);
  });
});
