import { describe, expect, it, vi } from "vitest";
import { formatRelativeTime } from "@/lib/format";

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
