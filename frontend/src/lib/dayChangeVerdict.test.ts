import { describe, expect, it } from "vitest";

import { dayChangeVerdictTitle } from "./dayChangeVerdict";

describe("dayChangeVerdictTitle", () => {
  it("says nothing for a clean window", () => {
    expect(dayChangeVerdictTitle("ok", [])).toBeUndefined();
  });

  it("says nothing when the field is absent (older response, or <2 closes)", () => {
    // ⚠ Absent data is NOT a verdict. A missing day change with no verdict must
    // not acquire an explanation it does not have.
    expect(dayChangeVerdictTitle(null, null)).toBeUndefined();
    expect(dayChangeVerdictTitle(undefined, undefined)).toBeUndefined();
  });

  it("explains a suppressed day change as a suppression, not a warning", () => {
    const title = dayChangeVerdictTitle("quarantined", ["quarantined_transition"]);
    expect(title).toBe(
      "No day change shown: the move between these two closes is not a return (quarantined transition).",
    );
  });

  it("distinguishes unverified from quarantined", () => {
    // ⚠ `unverified` is the ordinary state of the newest bar between a close and
    // the next verdict refresh — 55% of the list on the live corpus. Its value
    // still renders, so its text must not read as a defect.
    const title = dayChangeVerdictTitle("unverified", ["coverage_after_last_bar"]);
    expect(title).toBe("Not yet verified: the latest close has not been checked yet.");
  });

  it("joins every reason, because the clauses compose", () => {
    const title = dayChangeVerdictTitle("quarantined", [
      "horizon_stretched",
      "quarantined_transition",
    ]);
    expect(title).toContain("too far apart");
    expect(title).toContain("not a return");
  });

  it("falls back to silence on an unknown code rather than printing the raw enum", () => {
    expect(dayChangeVerdictTitle("quarantined", ["some_future_rule"])).toBeUndefined();
  });
});
