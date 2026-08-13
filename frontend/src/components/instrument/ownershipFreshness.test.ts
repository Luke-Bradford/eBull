import { describe, expect, it } from "vitest";

import {
  CADENCE,
  classifyFreshness,
  formatAge,
} from "./ownershipFreshness";

const TODAY = new Date("2026-05-02T00:00:00Z");

describe("classifyFreshness", () => {
  it("returns 'unknown' for null or unparsable dates", () => {
    expect(classifyFreshness("institutions", null, TODAY)).toBe("unknown");
    expect(classifyFreshness("institutions", "not-a-date", TODAY)).toBe("unknown");
  });

  it("future dates classify as fresh — clock drift / test fixtures", () => {
    expect(classifyFreshness("insiders", "2026-06-01", TODAY)).toBe("fresh");
  });

  it("13F categories classify by their longer cadence (135 / 270 days)", () => {
    // Sanity-pin the cadence numbers so a future tweak that
    // accidentally inverts thresholds gets caught.
    expect(CADENCE.institutions.aging_days).toBe(135);
    expect(CADENCE.institutions.stale_days).toBe(270);
    expect(CADENCE.etfs.aging_days).toBe(135);

    // 30 days → fresh (well within filing-window cadence)
    expect(classifyFreshness("institutions", "2026-04-02", TODAY)).toBe("fresh");
    // 134 days → still fresh by inclusive boundary semantics
    expect(classifyFreshness("institutions", "2025-12-19", TODAY)).toBe("fresh");
    // 135 days → flips to aging
    expect(classifyFreshness("institutions", "2025-12-18", TODAY)).toBe("aging");
    // 269 days → aging
    expect(classifyFreshness("institutions", "2025-08-06", TODAY)).toBe("aging");
    // 270 days → stale
    expect(classifyFreshness("institutions", "2025-08-05", TODAY)).toBe("stale");
  });

  it("Form 4 insiders classify by their tighter cadence (30 / 90 days)", () => {
    expect(CADENCE.insiders.aging_days).toBe(30);
    expect(CADENCE.insiders.stale_days).toBe(90);

    // 2 days → fresh (typical Form 4 filing lag)
    expect(classifyFreshness("insiders", "2026-04-30", TODAY)).toBe("fresh");
    // 30 days → aging boundary
    expect(classifyFreshness("insiders", "2026-04-02", TODAY)).toBe("aging");
    // 90 days → stale boundary
    expect(classifyFreshness("insiders", "2026-02-01", TODAY)).toBe("stale");
  });

  it("Treasury classifies by quarterly cadence (100 / 200 days)", () => {
    expect(CADENCE.treasury.aging_days).toBe(100);
    expect(CADENCE.treasury.stale_days).toBe(200);

    expect(classifyFreshness("treasury", "2026-03-28", TODAY)).toBe("fresh");
    expect(classifyFreshness("treasury", "2026-01-22", TODAY)).toBe("aging"); // 100d
    expect(classifyFreshness("treasury", "2025-10-14", TODAY)).toBe("stale"); // 200d
  });
});

describe("formatAge", () => {
  it("returns null for null or unparsable input", () => {
    expect(formatAge(null, TODAY)).toBeNull();
    expect(formatAge("garbage", TODAY)).toBeNull();
  });

  it("formats young ages in days", () => {
    expect(formatAge("2026-04-30", TODAY)).toBe("2d");
    expect(formatAge("2026-03-04", TODAY)).toBe("59d");
  });

  it("formats medium ages in months", () => {
    // 60 days exactly = 2 months under round(60/30) = 2.
    expect(formatAge("2026-03-03", TODAY)).toBe("2mo");
    // ~10 months
    expect(formatAge("2025-07-05", TODAY)).toBe("10mo");
  });

  it("formats long ages in years past 18 months", () => {
    expect(formatAge("2024-05-02", TODAY)).toBe("2y");
    expect(formatAge("2022-05-02", TODAY)).toBe("4y");
  });

  it("clamps future-dated source rows to '0d' rather than emitting a negative age", () => {
    expect(formatAge("2026-06-01", TODAY)).toBe("0d");
  });
});
