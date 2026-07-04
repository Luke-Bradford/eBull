/**
 * #1689 three-state semaphore (supersedes the #1508 C3 two-colour fold):
 * green (`current`/`working`) = ok · amber (`self_healing`) = recovering ·
 * red (`attention`) = act · muted (`stale_manual`) = aged history. `working`
 * stays calm green; `self_healing` is now its own amber tone (label "retrying")
 * so a recovering row is visible rather than hidden as calm-green.
 */

import { describe, expect, test } from "vitest";

import {
  STALE_REASON_LABEL,
  VERDICT_SORT_PRIORITY,
  VERDICT_VISUAL,
} from "./processStatus";

describe("VERDICT_VISUAL — three-state semaphore (#1689)", () => {
  test("working stays calm-green; self_healing/attention/stale_manual are each distinct", () => {
    expect(VERDICT_VISUAL.working.toneClass).toBe(VERDICT_VISUAL.current.toneClass);
    // self_healing is now amber — NOT the calm green it shared under C3.
    expect(VERDICT_VISUAL.self_healing.toneClass).not.toBe(VERDICT_VISUAL.current.toneClass);
    expect(VERDICT_VISUAL.self_healing.toneClass).not.toBe(VERDICT_VISUAL.attention.toneClass);
    expect(VERDICT_VISUAL.attention.toneClass).not.toBe(VERDICT_VISUAL.current.toneClass);
    // stale_manual is muted — distinct from calm, amber, and red.
    expect(VERDICT_VISUAL.stale_manual.toneClass).not.toBe(VERDICT_VISUAL.current.toneClass);
    expect(VERDICT_VISUAL.stale_manual.toneClass).not.toBe(VERDICT_VISUAL.self_healing.toneClass);
    expect(VERDICT_VISUAL.stale_manual.toneClass).not.toBe(VERDICT_VISUAL.attention.toneClass);
  });

  test("no verdict pulses-as-alarm", () => {
    expect(VERDICT_VISUAL.current.pulse).toBe(false);
    expect(VERDICT_VISUAL.working.pulse).toBe(false);
    expect(VERDICT_VISUAL.self_healing.pulse).toBe(false);
    expect(VERDICT_VISUAL.attention.pulse).toBe(false);
    expect(VERDICT_VISUAL.stale_manual.pulse).toBe(false);
  });

  test("distinct label text per verdict", () => {
    expect(VERDICT_VISUAL.current.label).toBe("current");
    expect(VERDICT_VISUAL.working.label).toBe("working");
    expect(VERDICT_VISUAL.self_healing.label).toBe("retrying");
    expect(VERDICT_VISUAL.attention.label).toBe("needs attention");
    expect(VERDICT_VISUAL.stale_manual.label).toBe("stale");
  });
});

describe("VERDICT_SORT_PRIORITY (#1689)", () => {
  test("only attention pins to the top; calm/recovering share rank 1", () => {
    expect(VERDICT_SORT_PRIORITY.attention).toBe(0);
    expect(VERDICT_SORT_PRIORITY.working).toBe(VERDICT_SORT_PRIORITY.current);
    expect(VERDICT_SORT_PRIORITY.self_healing).toBe(VERDICT_SORT_PRIORITY.current);
  });

  test("attention outranks the calm group; stale_manual sinks below it", () => {
    expect(VERDICT_SORT_PRIORITY.attention).toBeLessThan(VERDICT_SORT_PRIORITY.current);
    expect(VERDICT_SORT_PRIORITY.stale_manual).toBeGreaterThan(VERDICT_SORT_PRIORITY.current);
  });

  test("#1831 — paused is neutral, never pinned to the attention top", () => {
    expect(VERDICT_SORT_PRIORITY.paused).toBeGreaterThan(VERDICT_SORT_PRIORITY.attention);
  });
});

describe("VERDICT_VISUAL — paused (#1831)", () => {
  test("paused reads 'paused', is muted-grey (not red), and does not pulse", () => {
    expect(VERDICT_VISUAL.paused.label).toBe("paused");
    expect(VERDICT_VISUAL.paused.toneClass).not.toBe(VERDICT_VISUAL.attention.toneClass);
    expect(VERDICT_VISUAL.paused.pulse).toBe(false);
  });
});

describe("STALE_REASON_LABEL — watermark_gap matches backend (Task 2)", () => {
  test("watermark_gap reads 'ingest failing'", () => {
    expect(STALE_REASON_LABEL.watermark_gap).toBe("ingest failing");
  });
});
