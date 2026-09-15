/**
 * #3046 — human text for a day-change window's quarantine verdict.
 *
 * The API returns a closed vocabulary (`app/services/price_window_verdict.py`).
 * Rendering it raw would put `quarantined_transition` in front of an operator, so
 * this maps each code to a sentence that says what is actually known.
 *
 * ⚠ CONTAINMENT, NOT DIAGNOSIS. The rule set "NEVER identifies a cause"
 * (`price_quarantine.py:5-6`), so no string here claims the price is wrong — only
 * that the ratio between the two closes is not a return.
 *
 * ⚠ `unverified` is NOT a warning. It means the window sits outside the interval
 * the quarantine rules have evaluated, which is the ordinary state of the newest
 * bar between a close and the next verdict refresh. Its value renders normally;
 * the text exists so the state is legible rather than silent.
 */
const REASON_TEXT: Record<string, string> = {
  bar_return_unusable: "a bar in this window has an unusable close",
  unresolved_break: "an unresolved price-scale break sits inside this window",
  quarantined_transition:
    "the move between these two closes is not a return (quarantined transition)",
  horizon_stretched:
    "these closes are too far apart to be a day change (the series has a gap)",
  coverage_missing: "this instrument has not been checked for price defects",
  coverage_before_first_bar: "the earlier close is before the checked range",
  coverage_after_last_bar: "the latest close has not been checked yet",
  verdict_deferred: "the verdict for this window is still provisional",
};

export function dayChangeVerdictTitle(
  verdict: string | null | undefined,
  reasons: string[] | null | undefined,
): string | undefined {
  if (!verdict || verdict === "ok") return undefined;
  const parts = (reasons ?? [])
    .map((code) => REASON_TEXT[code])
    .filter((text): text is string => Boolean(text));
  if (parts.length === 0) return undefined;
  const prefix =
    verdict === "quarantined" ? "No day change shown: " : "Not yet verified: ";
  return prefix + parts.join("; ") + ".";
}
