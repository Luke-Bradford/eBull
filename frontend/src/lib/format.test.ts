import { describe, expect, it, vi } from "vitest";
import {
  formatEta,
  formatHeartbeatAge,
  formatRate,
  formatRelativeTime,
} from "@/lib/format";

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
