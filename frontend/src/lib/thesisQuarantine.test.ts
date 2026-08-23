import { describe, expect, it } from "vitest";

import {
  isThesisQuarantined,
  thesisRefusalBody,
  thesisRefusalClaim,
  thesisRefusalTooltip,
  thesisSubjectState,
  THESIS_QUARANTINE_REASON,
} from "@/lib/thesisQuarantine";

describe("thesisQuarantine", () => {
  // The whole point of the predicate is the NULL/undefined direction: the
  // Python side is `is True`, so "nobody decided" must not read as "passed".
  const cases: ReadonlyArray<{
    readonly ok: boolean | null | undefined;
    readonly state: ReturnType<typeof thesisSubjectState>;
    readonly quarantined: boolean;
  }> = [
    { ok: true, state: "usable", quarantined: false },
    { ok: false, state: "unnamed_subject", quarantined: true },
    { ok: null, state: "unchecked", quarantined: true },
    { ok: undefined, state: "unchecked", quarantined: true },
  ];

  for (const c of cases) {
    it(`${String(c.ok)} → ${c.state} (quarantined: ${String(c.quarantined)})`, () => {
      expect(thesisSubjectState(c.ok)).toBe(c.state);
      expect(isThesisQuarantined(c.ok)).toBe(c.quarantined);
    });
  }

  it("agrees with the backend refusal vocabulary", () => {
    // Mirrors thesis_subject_identity.QUARANTINE_REASON — the screen and the
    // logs must say the same word, so a drift here is a real failure.
    expect(THESIS_QUARANTINE_REASON).toBe("thesis_quarantined");
  });

  it("never reports a false verdict as merely unchecked", () => {
    // The two refusing states must stay distinguishable: `false` is a decided
    // failure, `null` is an undecided row, and they carry different copy.
    expect(thesisSubjectState(false)).not.toBe(thesisSubjectState(null));
  });

  describe("refusal copy", () => {
    // Codex ckpt-2 on this ticket: the banner distinguished the two refusing
    // states correctly while two hard-coded tooltips claimed "never names its
    // instrument" for BOTH — asserting, on an undecided row, a check that never
    // ran. These pin the distinction at the single source the surfaces now read.
    it("claims a failed check ONLY on a decided failure", () => {
      expect(thesisRefusalClaim(false)).toContain("never names its own instrument");
    });

    it.each([null, undefined] as const)(
      "does not claim a failed check on an undecided row (%s)",
      (ok) => {
        expect(thesisRefusalClaim(ok)).toContain("has not been checked");
        expect(thesisRefusalClaim(ok)).not.toContain("never names");
      },
    );

    it("names the machine-readable reason on every refusing surface", () => {
      for (const ok of [false, null, undefined] as const) {
        expect(thesisRefusalTooltip(ok)).toContain(THESIS_QUARANTINE_REASON);
        expect(thesisRefusalBody(ok)).toContain(THESIS_QUARANTINE_REASON);
      }
    });

    it("does not enumerate the consumers, which cannot be kept in step from here", () => {
      // Review NITPICK on PR #2897: the copy used to name "portfolio, scoring,
      // entry timing, alerts and reporting" with no link to the Python modules
      // it claimed, so a consumer change would leave operator-facing text
      // quietly wrong. The census now lives in
      // tests/test_thesis_subject_identity_consumers.py, where it is checkable.
      for (const ok of [false, null] as const) {
        const copy = thesisRefusalBody(ok);
        for (const consumer of ["portfolio", "scoring", "entry timing", "reporting"]) {
          expect(copy).not.toContain(consumer);
        }
      }
    });

    it("keeps the tooltip a prefix of the banner body — one claim, two lengths", () => {
      // If these ever diverge, the same row says two different things
      // depending on which surface the operator happens to be looking at.
      for (const ok of [false, null] as const) {
        expect(thesisRefusalBody(ok).startsWith(thesisRefusalTooltip(ok))).toBe(true);
      }
    });
  });
});
