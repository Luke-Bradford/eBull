import { describe, expect, it, vi } from "vitest";
import {
  formatBigMoney,
  formatBigNumber,
  formatEta,
  formatHeartbeatAge,
  formatRate,
  formatRelativeTime,
  formatTime,
} from "@/lib/format";

describe("formatTime", () => {
  it("renders HH:MM (24h) for a Date", () => {
    // 14:32 UTC — assert the minutes part survives regardless of viewer TZ.
    expect(formatTime(new Date("2026-06-07T14:32:00Z"))).toMatch(/\d{2}:32/);
  });

  it("accepts an ISO string", () => {
    expect(formatTime("2026-06-07T09:05:00Z")).toMatch(/\d{2}:05/);
  });

  it("renders em-dash for null / undefined / empty / invalid", () => {
    expect(formatTime(null)).toBe("—");
    expect(formatTime(undefined)).toBe("—");
    expect(formatTime("")).toBe("—");
    expect(formatTime("not-a-date")).toBe("—");
  });
});

describe("formatRelativeTime", () => {
  const NOW = new Date("2026-04-21T12:00:00Z");

  it("renders '—' for null / undefined / empty string", () => {
    expect(formatRelativeTime(null)).toBe("—");
    expect(formatRelativeTime(undefined)).toBe("—");
    expect(formatRelativeTime("")).toBe("—");
  });

  it("renders 'just now' for <60s delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T11:59:30Z")).toBe("just now");
    vi.useRealTimers();
  });

  it("renders minutes for <1h delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T11:55:00Z")).toBe("5m ago");
    vi.useRealTimers();
  });

  it("renders hours for <1d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T09:00:00Z")).toBe("3h ago");
    vi.useRealTimers();
  });

  it("renders days for <7d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-19T12:00:00Z")).toBe("2d ago");
    vi.useRealTimers();
  });

  it("falls back to formatDate for >=7d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const result = formatRelativeTime("2026-04-10T12:00:00Z");
    expect(result).toMatch(/2026/);
    vi.useRealTimers();
  });
});

describe("formatRate", () => {
  it("renders '—' for null", () => {
    expect(formatRate(null)).toBe("—");
  });

  it("renders sub-1000 rates with one decimal + rows/s", () => {
    expect(formatRate(8.4)).toBe("8.4 rows/s");
    expect(formatRate(0.5)).toBe("0.5 rows/s");
  });

  it("abbreviates thousands with k", () => {
    expect(formatRate(15600)).toBe("15.6k rows/s");
  });
});

describe("formatEta", () => {
  it("renders '—' for null", () => {
    expect(formatEta(null)).toBe("—");
  });

  it("renders '<1m' for sub-minute ETAs", () => {
    expect(formatEta(40)).toBe("<1m");
  });

  it("renders whole minutes under an hour", () => {
    expect(formatEta(852)).toBe("~14m");
  });

  it("renders hours + minutes over an hour", () => {
    expect(formatEta(3 * 3600 + 5 * 60)).toBe("~3h 5m");
  });
});

describe("formatHeartbeatAge", () => {
  it("renders '—' for null", () => {
    expect(formatHeartbeatAge(null)).toBe("—");
  });

  it("renders seconds under a minute", () => {
    expect(formatHeartbeatAge(0)).toBe("updated 0s ago");
    expect(formatHeartbeatAge(45)).toBe("updated 45s ago");
  });

  it("renders minutes under an hour", () => {
    expect(formatHeartbeatAge(180)).toBe("updated 3m ago");
  });

  it("renders hours past an hour", () => {
    expect(formatHeartbeatAge(7200)).toBe("updated 2h ago");
  });
});

describe("formatBigMoney", () => {
  it("prefixes the currency symbol on abbreviated magnitudes", () => {
    expect(formatBigMoney(2_138_850_000, "USD")).toBe("US$2.14B");
    expect(formatBigMoney(53_471_250, "USD")).toBe("US$53.47M");
  });

  it("defaults to GBP and renders null as em dash", () => {
    expect(formatBigMoney(1_500, undefined)).toBe("\u00a31.50K");
    expect(formatBigMoney(null, "USD")).toBe("\u2014");
  });

  it("puts a negative sign OUTSIDE the currency symbol", () => {
    // "US$-10.75B" reads as a currency called "US$-"; the sign belongs in
    // front. Net debt, investing/financing cash flow and negative FCF all
    // cross zero on the fundamentals charts (#2185); before that branch the
    // only callers passed always-positive offering proceeds, so no negative
    // had ever reached this helper.
    expect(formatBigMoney(-10_746_000_000, "USD")).toBe("-US$10.75B");
    expect(formatBigMoney(-1_500, undefined)).toBe("-\u00a31.50K");
  });

  it("signs sub-thousand magnitudes, which have no abbreviation suffix", () => {
    // The under-1e3 branch is the one the #2190 refactor rerouted. It used to
    // `return n.toFixed(0)` directly, ignoring the `sign` variable the four
    // abbreviated branches above it used — correct only because toFixed
    // carries its own sign, and the sole path where the two formatters
    // disagreed about who owns it. Both now render from one derivation, so
    // this pins the branch that had no coverage.
    expect(formatBigMoney(-500, "USD")).toBe("-US$500");
    expect(formatBigNumber(-500)).toBe("-500");
    // `-0` must format unsigned: `-0 < 0` is false, matching the old
    // `(-0).toFixed(0) === "0"` behaviour. A split that keyed on
    // `Object.is(n, -0)` or `1 / n < 0` would render "-US$0" here.
    expect(formatBigMoney(-0, "USD")).toBe("US$0");
    expect(formatBigNumber(-0)).toBe("0");
  });

  it("degrades instead of throwing on a non-ISO-4217 code", () => {
    // `new Intl.NumberFormat(_, {style:"currency", currency:"XX"})` throws
    // RangeError. These helpers run inside recharts tickFormatter/Tooltip
    // callbacks fed backend `reported_currency` (TEXT NOT NULL, no shape
    // CHECK), so a throw would take the whole chart down, not one label.
    expect(() => formatBigMoney(1_000_000, "XX")).not.toThrow();
    expect(formatBigMoney(1_000_000, "XX")).toBe("XX1.00M");
  });
});
