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
  STATUS_VISUAL,
  VERDICT_SORT_PRIORITY,
  VERDICT_VISUAL,
} from "./processStatus";

describe("VERDICT_VISUAL — three-state semaphore (#1689)", () => {
  test("working stays calm-green; self_healing/attention/stale_manual are each distinct", () => {
    expect(VERDICT_VISUAL.working.tone).toBe(VERDICT_VISUAL.current.tone);
    // self_healing is now amber — NOT the calm green it shared under C3.
    expect(VERDICT_VISUAL.self_healing.tone).not.toBe(VERDICT_VISUAL.current.tone);
    expect(VERDICT_VISUAL.self_healing.tone).not.toBe(VERDICT_VISUAL.attention.tone);
    expect(VERDICT_VISUAL.attention.tone).not.toBe(VERDICT_VISUAL.current.tone);
    // stale_manual is muted — distinct from calm, amber, and red.
    expect(VERDICT_VISUAL.stale_manual.tone).not.toBe(VERDICT_VISUAL.current.tone);
    expect(VERDICT_VISUAL.stale_manual.tone).not.toBe(VERDICT_VISUAL.self_healing.tone);
    expect(VERDICT_VISUAL.stale_manual.tone).not.toBe(VERDICT_VISUAL.attention.tone);
  });

  test("the semaphore reads in SEMANTIC tones, not colour strings (#2148)", () => {
    // Pins the three-state mapping in meaning: green = ok, amber = warn,
    // red = risk, muted = neutral. Before #2148 these were hoisted class
    // strings, so the semaphore could only be asserted by inequality.
    expect(VERDICT_VISUAL.current.tone).toBe("ok");
    expect(VERDICT_VISUAL.working.tone).toBe("ok");
    expect(VERDICT_VISUAL.self_healing.tone).toBe("warn");
    expect(VERDICT_VISUAL.attention.tone).toBe("risk");
    expect(VERDICT_VISUAL.stale_manual.tone).toBe("neutral");
    expect(VERDICT_VISUAL.paused.tone).toBe("neutral");
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
  test("paused reads 'paused' and is muted-grey, not red", () => {
    expect(VERDICT_VISUAL.paused.label).toBe("paused");
    expect(VERDICT_VISUAL.paused.tone).not.toBe(VERDICT_VISUAL.attention.tone);
  });
});

describe("tone maps hold semantics, never colour (#2148)", () => {
  // The eightKSeverity.ts regression (prevention-log → "A lint gate's file-glob
  // is part of its contract") was a tone map carrying raw Tailwind into a `.ts`
  // module the dark gate did not walk. `extraClass` is the one escape hatch
  // here, so pin that it stays non-colour: colour belongs to the tone.
  const COLOUR_UTILITY = /\b(?:bg|text|border|ring|fill|stroke|from|via|to)-/;
  const ALL_VISUALS = [
    ...Object.entries(STATUS_VISUAL).map(([k, v]) => [`STATUS_VISUAL.${k}`, v] as const),
    ...Object.entries(VERDICT_VISUAL).map(([k, v]) => [`VERDICT_VISUAL.${k}`, v] as const),
  ];

  test.each(ALL_VISUALS)("%s carries no colour class", (_name, visual) => {
    expect(visual.extraClass ?? "").not.toMatch(COLOUR_UTILITY);
  });

  test("every visual names a tone and a non-empty label", () => {
    const TONES = new Set(["ok", "warn", "risk", "info", "neutral"]);
    for (const [name, visual] of ALL_VISUALS) {
      expect(TONES.has(visual.tone), `${name} tone`).toBe(true);
      expect(visual.label.length, `${name} label`).toBeGreaterThan(0);
    }
  });
});

describe("STALE_REASON_LABEL — watermark_gap matches backend (Task 2)", () => {
  test("watermark_gap reads 'ingest failing'", () => {
    expect(STALE_REASON_LABEL.watermark_gap).toBe("ingest failing");
  });
});
