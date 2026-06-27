import { describe, expect, it } from "vitest";

import {
  classifySession,
  classifyUsSession,
  detectCoverageGaps,
  type MarketSpecials,
} from "./chartFormatters";

/**
 * Lock down US-equity session classification across the four windows
 * NYSE/NASDAQ define in ET wall-clock, including the DST transition
 * the implementation depends on (Intl with `America/New_York` is
 * authoritative for both EDT and EST). All inputs are constructed
 * from UTC epochs so the test is timezone-independent — what the
 * developer's machine clock says cannot affect the result.
 *
 * Quick reference (April 2026 = EDT, UTC-4):
 *   PM:     04:00–09:30 ET = 08:00–13:30 UTC
 *   RTH:    09:30–16:00 ET = 13:30–20:00 UTC
 *   AH:     16:00–20:00 ET = 20:00–24:00 UTC (rolls into next UTC day)
 *   closed: 20:00–04:00 ET = 24:00–08:00 UTC (next day)
 *   weekend (Sat/Sun in ET) = closed regardless of clock
 */
function utc(y: number, m: number, d: number, hh: number, mm: number = 0): number {
  return Math.floor(Date.UTC(y, m - 1, d, hh, mm) / 1000);
}

describe("classifyUsSession (April 2026 EDT)", () => {
  it("classifies pre-market start (04:00 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 8, 0))).toBe("pre");
  });
  it("classifies last pre-market minute (09:29 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 13, 29))).toBe("pre");
  });
  it("classifies RTH open (09:30 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 13, 30))).toBe("rth");
  });
  it("classifies mid-RTH (12:00 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 16, 0))).toBe("rth");
  });
  it("classifies last RTH minute (15:59 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 19, 59))).toBe("rth");
  });
  it("classifies AH open (16:00 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 20, 0))).toBe("ah");
  });
  it("classifies mid-AH (18:00 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 22, 0))).toBe("ah");
  });
  it("classifies last AH minute (19:59 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 23, 59))).toBe("ah");
  });
  it("classifies AH end / closed start (20:00 ET)", () => {
    // 20:00 EDT = 00:00 UTC next day.
    expect(classifyUsSession(utc(2026, 4, 22, 0, 0))).toBe("closed");
  });
  it("classifies overnight (02:00 ET)", () => {
    // 02:00 EDT Tue = 06:00 UTC Tue.
    expect(classifyUsSession(utc(2026, 4, 21, 6, 0))).toBe("closed");
  });
  it("classifies exactly midnight ET (00:00) as closed", () => {
    // 00:00 EDT Tue Apr 21 = 04:00 UTC Tue Apr 21. Boundary case
    // because some Intl runtimes emit hour="24" for midnight; the
    // `% 24` normalisation in `_nyParts` must collapse that to 0
    // so this lands in the closed window, not an unmatched branch.
    // PR #610 review WARNING.
    expect(classifyUsSession(utc(2026, 4, 21, 4, 0))).toBe("closed");
  });
  it("classifies just-before-PM (03:59 ET)", () => {
    expect(classifyUsSession(utc(2026, 4, 21, 7, 59))).toBe("closed");
  });

  it("classifies Saturday all-day as closed", () => {
    // Apr 25 2026 is a Saturday. Every hour → closed.
    for (const hh of [4, 9, 12, 16, 22]) {
      expect(classifyUsSession(utc(2026, 4, 25, hh + 4, 0))).toBe("closed");
    }
  });
  it("classifies Sunday all-day as closed", () => {
    // Apr 26 2026 is a Sunday.
    for (const hh of [4, 9, 12, 16, 22]) {
      expect(classifyUsSession(utc(2026, 4, 26, hh + 4, 0))).toBe("closed");
    }
  });
});

describe("classifyUsSession (DST handling)", () => {
  it("classifies a January day as EST (UTC-5): 09:30 ET = 14:30 UTC", () => {
    // Jan 5 2026 — Monday, EST.
    expect(classifyUsSession(utc(2026, 1, 5, 14, 30))).toBe("rth");
    expect(classifyUsSession(utc(2026, 1, 5, 14, 29))).toBe("pre");
  });
  it("classifies a July day as EDT (UTC-4): 09:30 ET = 13:30 UTC", () => {
    // Jul 6 2026 — Monday, EDT.
    expect(classifyUsSession(utc(2026, 7, 6, 13, 30))).toBe("rth");
    expect(classifyUsSession(utc(2026, 7, 6, 13, 29))).toBe("pre");
  });
});

describe("classifySession (#609 profile-aware)", () => {
  // Tue Apr 21 2026 (EDT): 13:30 UTC = 09:30 ET (RTH), 08:00 UTC = 04:00 ET (PM),
  // 21:00 UTC = 17:00 ET (AH on a normal day).
  it("continuous → every bar is rth (no PM/AH/closed)", () => {
    expect(classifySession("continuous", utc(2026, 4, 21, 8, 0))).toBe("rth"); // 04:00 ET
    expect(classifySession("continuous", utc(2026, 4, 22, 6, 0))).toBe("rth"); // 02:00 ET overnight
    expect(classifySession("continuous", utc(2026, 4, 25, 16, 0))).toBe("rth"); // Saturday
  });

  it("foreign_equity → in-session bar is rth (no PM/AH); weekend → closed", () => {
    expect(classifySession("foreign_equity", utc(2026, 4, 21, 8, 0))).toBe("rth"); // Tue
    expect(classifySession("foreign_equity", utc(2026, 4, 21, 21, 0))).toBe("rth");
    // Sat Apr 25 2026 — defensive weekend branch (backfill artifact);
    // contrast with `continuous` (Saturday → rth) asserted above.
    expect(classifySession("foreign_equity", utc(2026, 4, 25, 16, 0))).toBe("closed");
  });

  it("us_equity matches the legacy classifier with no specials", () => {
    expect(classifySession("us_equity", utc(2026, 4, 21, 8, 0))).toBe("pre");
    expect(classifySession("us_equity", utc(2026, 4, 21, 13, 30))).toBe("rth");
    expect(classifySession("us_equity", utc(2026, 4, 21, 20, 0))).toBe("ah");
  });

  it("us_equity_rth → no PM/AH; only 09:30–16:00 ET is rth", () => {
    expect(classifySession("us_equity_rth", utc(2026, 4, 21, 8, 0))).toBe("closed"); // 04:00 ET PM → closed
    expect(classifySession("us_equity_rth", utc(2026, 4, 21, 13, 30))).toBe("rth"); // 09:30 ET
    expect(classifySession("us_equity_rth", utc(2026, 4, 21, 20, 0))).toBe("closed"); // 16:00 ET AH → closed
  });

  it("full closure → closed all day even during RTH clock", () => {
    const specials: MarketSpecials = { fullClosures: new Set(["2026-04-21"]), halfDays: new Set() };
    expect(classifySession("us_equity", utc(2026, 4, 21, 16, 0), specials)).toBe("closed"); // 12:00 ET
  });

  it("half-day → RTH ends 13:00 ET; 13:00–17:00 is ah; ≥17:00 closed", () => {
    const specials: MarketSpecials = { fullClosures: new Set(), halfDays: new Set(["2026-04-21"]) };
    // 12:59 ET = 16:59 UTC → still rth.
    expect(classifySession("us_equity", utc(2026, 4, 21, 16, 59), specials)).toBe("rth");
    // 13:00 ET = 17:00 UTC → ah (early-close afternoon).
    expect(classifySession("us_equity", utc(2026, 4, 21, 17, 0), specials)).toBe("ah");
    // 16:59 ET = 20:59 UTC → still ah (bounded to 17:00 ET).
    expect(classifySession("us_equity", utc(2026, 4, 21, 20, 59), specials)).toBe("ah");
    // 17:00 ET = 21:00 UTC → closed (the prior unbounded bug tinted this).
    expect(classifySession("us_equity", utc(2026, 4, 21, 21, 0), specials)).toBe("closed");
  });

  it("half-day + us_equity_rth → ≥13:00 ET is closed (no AH)", () => {
    const specials: MarketSpecials = { fullClosures: new Set(), halfDays: new Set(["2026-04-21"]) };
    expect(classifySession("us_equity_rth", utc(2026, 4, 21, 16, 59), specials)).toBe("rth"); // 12:59 ET
    expect(classifySession("us_equity_rth", utc(2026, 4, 21, 17, 0), specials)).toBe("closed"); // 13:00 ET
  });
});

describe("detectCoverageGaps (#1754 Phase C)", () => {
  // April 21 2026 is a Tue (EDT); 14:00 UTC = 10:00 ET (RTH). interval = 60s.
  const t0 = utc(2026, 4, 21, 14, 0);
  const bar = (sec: number) => ({ time: sec });

  it("flags an intrasession hole > 2 buckets, tolerating one missing bar", () => {
    // +60 (clean), +120 (one missing — tolerated), +300 (≥2 missing — gap).
    const bars = [bar(t0), bar(t0 + 60), bar(t0 + 60 + 120), bar(t0 + 60 + 120 + 300)];
    expect(detectCoverageGaps(bars, 60, "us_equity")).toEqual([3]);
  });

  it("does NOT flag the expected overnight (cross-day) gap", () => {
    const nextDay = utc(2026, 4, 22, 14, 0); // next RTH day — huge delta, cross-day
    expect(detectCoverageGaps([bar(t0), bar(nextDay)], 60, "us_equity")).toEqual([]);
  });

  it("does NOT flag a gap touching a closed window", () => {
    const closed = utc(2026, 4, 21, 2, 0); // 22:00 ET prior — closed
    expect(detectCoverageGaps([bar(closed), bar(t0)], 60, "us_equity")).toEqual([]);
  });

  it("is disabled for non-US profiles (no precise session model)", () => {
    const bars = [bar(t0), bar(t0 + 600)];
    expect(detectCoverageGaps(bars, 60, "foreign_equity")).toEqual([]);
    expect(detectCoverageGaps(bars, 60, "continuous")).toEqual([]);
  });

  it("handles degenerate inputs", () => {
    expect(detectCoverageGaps([bar(t0)], 60, "us_equity")).toEqual([]);
    expect(detectCoverageGaps([bar(t0), bar(t0 + 600)], 0, "us_equity")).toEqual([]);
  });
});
