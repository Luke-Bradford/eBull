import { describe, expect, it } from "vitest";

import { classifyUsSession } from "./chartFormatters";

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
