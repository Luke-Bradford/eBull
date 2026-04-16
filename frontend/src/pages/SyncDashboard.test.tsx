/**
 * Unit tests for SyncDashboard helpers.
 *
 * Focus: parseUtc — the Safari-strict ISO-8601 parse used when
 * computing sync-run durations. Without timezone normalisation,
 * Safari parses offset-less strings as local time, which would
 * produce wrong durations for operators outside UTC.
 */

import { describe, expect, it } from "vitest";

import { parseUtc } from "./SyncDashboard";

describe("parseUtc", () => {
  it("parses string with explicit +00:00 offset as UTC", () => {
    const d = parseUtc("2026-04-16T12:30:00+00:00");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("parses string with Z suffix as UTC", () => {
    const d = parseUtc("2026-04-16T12:30:00Z");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("appends Z when offset is missing (Safari-safe)", () => {
    const d = parseUtc("2026-04-16T12:30:00");
    expect(d.toISOString()).toBe("2026-04-16T12:30:00.000Z");
  });

  it("preserves explicit non-UTC offset", () => {
    const d = parseUtc("2026-04-16T12:30:00+02:00");
    // 12:30 at +02:00 = 10:30 UTC
    expect(d.toISOString()).toBe("2026-04-16T10:30:00.000Z");
  });
});
