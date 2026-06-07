/**
 * #1508 C3 — two-colour Processes page. `working` and `self_healing`
 * must read as the SAME calm green as `current` (keep distinct label
 * text, drop the blue/amber alarm pulse); only `attention` is the
 * alarming, pinned colour. Sort folds the three calm verdicts into one
 * group so only `attention` floats to the pinned region.
 */

import { describe, expect, test } from "vitest";

import {
  STALE_REASON_LABEL,
  VERDICT_SORT_PRIORITY,
  VERDICT_VISUAL,
} from "./processStatus";

describe("VERDICT_VISUAL — two-colour fold (C3)", () => {
  test("working and self_healing share the calm (non-attention) tone of current", () => {
    expect(VERDICT_VISUAL.working.toneClass).toBe(
      VERDICT_VISUAL.current.toneClass,
    );
    expect(VERDICT_VISUAL.self_healing.toneClass).toBe(
      VERDICT_VISUAL.current.toneClass,
    );
    expect(VERDICT_VISUAL.attention.toneClass).not.toBe(
      VERDICT_VISUAL.current.toneClass,
    );
  });

  test("the calm verdicts do not pulse-as-alarm; attention does not pulse either", () => {
    expect(VERDICT_VISUAL.current.pulse).toBe(false);
    expect(VERDICT_VISUAL.working.pulse).toBe(false);
    expect(VERDICT_VISUAL.self_healing.pulse).toBe(false);
    expect(VERDICT_VISUAL.attention.pulse).toBe(false);
  });

  test("distinct label text is preserved per verdict", () => {
    expect(VERDICT_VISUAL.current.label).toBe("current");
    expect(VERDICT_VISUAL.working.label).toBe("working");
    expect(VERDICT_VISUAL.self_healing.label).toBe("self-healing");
    expect(VERDICT_VISUAL.attention.label).toBe("needs attention");
  });
});

describe("VERDICT_SORT_PRIORITY — only attention pins (C3)", () => {
  test("only attention sorts to the pinned region", () => {
    expect(VERDICT_SORT_PRIORITY.attention).toBe(0);
    expect(VERDICT_SORT_PRIORITY.working).toBe(VERDICT_SORT_PRIORITY.current);
    expect(VERDICT_SORT_PRIORITY.self_healing).toBe(
      VERDICT_SORT_PRIORITY.current,
    );
  });

  test("attention outranks the calm group", () => {
    expect(VERDICT_SORT_PRIORITY.attention).toBeLessThan(
      VERDICT_SORT_PRIORITY.current,
    );
  });
});

describe("STALE_REASON_LABEL — watermark_gap matches backend (Task 2)", () => {
  test("watermark_gap reads 'ingest failing'", () => {
    expect(STALE_REASON_LABEL.watermark_gap).toBe("ingest failing");
  });
});
