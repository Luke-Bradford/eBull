/**
 * Thesis subject-identity verdict, as the frontend reads it (#2306).
 *
 * The rule and the verdict are the BACKEND's: #2431 refuses a memo that never
 * names its own instrument, #2436 stores that verdict on the row
 * (`theses.subject_identity_ok`) and every deterministic consumer fails closed
 * on it — `portfolio.py`, `scoring.py`, `entry_timing.py`, `alerts.py`,
 * `reporting.py`. Nothing here re-derives the rule; this module only mirrors
 * the READ predicate so the screen agrees with the engine instead of
 * contradicting it.
 *
 * Canonical Python counterpart:
 * `app/services/thesis_subject_identity.py::is_thesis_usable` — `is True`.
 */

/** Verdict states, in the vocabulary the stored column can express. */
export type ThesisSubjectState = "usable" | "unnamed_subject" | "unchecked";

/**
 * ⚠ `false` is `unnamed_subject`, NOT `misattributed`.
 *
 * The stored rule records only that the memo never named its own instrument.
 * It does NOT establish that some other company was positively identified —
 * that is a strictly narrower class (#2306 measured 19 such memos against
 * 1,512 rows the rule refuses). Naming the state for the stronger claim would
 * put the conflation into the type system, where every later reader inherits
 * it.
 */
export function thesisSubjectState(
  ok: boolean | null | undefined,
): ThesisSubjectState {
  if (ok === true) return "usable";
  if (ok === false) return "unnamed_subject";
  return "unchecked";
}

/**
 * May this thesis's stance, confidence or valuation band be read as a verdict?
 *
 * ⚠ FAIL-CLOSED ON NULL AND UNDEFINED, matching `is_thesis_usable`. NULL means
 * *nobody has decided*, which is not *passed*; `undefined` means the field was
 * absent from the payload, which is the same thing with less provenance. The
 * wire types declare the field required, so `undefined` should not occur —
 * this accepts it so a hand-built fixture cannot fake a pass.
 *
 * ⚠⚠ NOT SUFFICIENT ALONE ON A LIST SURFACE. The theses library gives a row to
 * held instruments that have no thesis at all, with every thesis field null
 * (`app/api/theses.py:235-239`). This returns `true` for those, for the trivial
 * reason that there is nothing to check — rendering a quarantine marker there
 * would invent a defect. Gate on `thesis_id !== null` as well. Detail surfaces
 * need no such gate: they already early-return on a null thesis.
 */
export function isThesisQuarantined(ok: boolean | null | undefined): boolean {
  return ok !== true;
}

/**
 * The machine-readable reason the deterministic layer reports when it refuses a
 * thesis. Mirrors `thesis_subject_identity.QUARANTINE_REASON` so the screen and
 * the logs say the same word.
 */
export const THESIS_QUARANTINE_REASON = "thesis_quarantined";

/** Who refuses the row — the verified consumer census, not a paraphrase. */
const REFUSED_BY =
  `The deterministic layer refuses it (${THESIS_QUARANTINE_REASON}) — ` +
  "portfolio, scoring, entry timing, alerts and reporting.";

/**
 * The claim a refusing state actually supports, in one sentence.
 *
 * ⚠⚠ THE TWO REFUSING STATES DO NOT SUPPORT THE SAME CLAIM, and every surface
 * must respect that. `false` is a decided failure — the rule ran and the memo
 * did not name its instrument. `null` is an UNDECIDED row: saying "never names
 * its instrument" there asserts a check that never happened. Caught by Codex
 * checkpoint 2 on this ticket, where the banner distinguished them correctly
 * and two hard-coded tooltips did not. Copy lives here, once, so the next
 * surface cannot reintroduce the split.
 */
export function thesisRefusalClaim(ok: boolean | null | undefined): string {
  return thesisSubjectState(ok) === "unnamed_subject"
    ? "This memo never names its own instrument, so its figures may describe a different company."
    : "This memo has not been checked against its instrument, so its figures are unverified.";
}

/** Hover copy for the compact markers: the claim plus who refuses it. */
export function thesisRefusalTooltip(ok: boolean | null | undefined): string {
  return `${thesisRefusalClaim(ok)} ${REFUSED_BY}`;
}

/** Body copy for the full banner: the claim, who refuses it, and why it is still shown. */
export function thesisRefusalBody(ok: boolean | null | undefined): string {
  return `${thesisRefusalTooltip(ok)} It is shown here for evidence only.`;
}
