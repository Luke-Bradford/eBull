/**
 * Humanize the deterministic scorer's audit output (#1908 PR-4).
 *
 * The Verdict tab used to render `scores.explanation` raw — scorer internals
 * verbatim, e.g.
 *
 *   "value: base_value missing; bear_value missing; fundamentals fallback
 *    (no thesis); penalties fired: high_realized_volatility (total deduction:
 *    0.04)"
 *
 * The penalty/reward half of that string is ALREADY structured on the same
 * row: `scores.penalties_json` (`app/services/scoring.py::_insert_score`)
 * carries `{name, deduction|addition, reason, kind}` per entry. So the chips
 * below are built from the STRUCTURED field — this module never parses the
 * explanation text. The raw string stays available verbatim, behind an
 * expander, because it is the audit trail.
 *
 * Names are stable identifiers emitted by `_compute_penalties` /
 * `_realized_risk_penalties` / the Calmar reward. `PENALTY_LABEL` covers the
 * nine the scorer can emit today; anything new degrades to a de-snake-cased
 * label rather than disappearing — the scorer is free to add a penalty
 * without a frontend release.
 */
import type { ScorePenaltyItem } from "@/api/types";
import type { BadgeTone } from "@/components/ui/Badge";

/**
 * Operator-facing copy per scorer identifier. Sources:
 * `app/services/scoring.py` — `stale_thesis`, `missing_critical_data`,
 * `wide_spread`, `high_red_flag`, `extreme_dilution`, `low_confidence`
 * (`_compute_penalties`); `high_realized_volatility`, `deep_drawdown`
 * (`_realized_risk_penalties`, v1.2+); `strong_calmar` (v1.3+ reward).
 */
const PENALTY_LABEL: Record<string, string> = {
  stale_thesis: "Stale thesis",
  missing_critical_data: "Missing critical data",
  wide_spread: "Wide spread",
  high_red_flag: "High red-flag score",
  extreme_dilution: "Extreme dilution",
  low_confidence: "Low thesis confidence",
  high_realized_volatility: "High realized volatility",
  deep_drawdown: "Deep drawdown",
  strong_calmar: "Strong Calmar",
};

/**
 * Reward vs penalty. `kind` is authoritative when present; rows written before
 * #1635 have no `kind` at all and are penalties, so fall back to "carries an
 * `addition`" rather than assuming either way.
 */
export function isReward(item: ScorePenaltyItem): boolean {
  if (item.kind === "reward") return true;
  if (item.kind === "penalty") return false;
  return typeof item.addition === "number";
}

/** `high_realized_volatility` → "High realized volatility". */
export function humanizePenaltyName(name: string): string {
  const known = PENALTY_LABEL[name];
  if (known !== undefined) return known;
  const words = name.replace(/_/g, " ").trim();
  if (words === "") return name;
  return words.charAt(0).toUpperCase() + words.slice(1);
}

export interface ScoreChip {
  /** React key — the scorer name, unique per row in practice. */
  readonly key: string;
  readonly label: string;
  /** Signed magnitude, e.g. "−0.04" / "+0.03". Empty when neither
   *  magnitude is present (a malformed row still renders its label). */
  readonly delta: string;
  readonly tone: BadgeTone;
  /** The scorer's own `reason` — surfaced on hover, not truncated. */
  readonly title: string;
}

/**
 * Structured penalties/rewards → chips. Rewards read `ok`, penalties `risk`.
 *
 * Uses U+2212 MINUS for the deduction (not a hyphen) so the sign aligns in a
 * tabular-nums row and cannot be misread as a dash.
 */
export function toScoreChips(items: ScorePenaltyItem[] | null): ScoreChip[] {
  if (items === null) return [];
  return items.map((item, i) => {
    const reward = isReward(item);
    const magnitude = reward ? item.addition : item.deduction;
    return {
      key: `${item.name}-${i}`,
      label: humanizePenaltyName(item.name),
      delta:
        typeof magnitude === "number"
          ? `${reward ? "+" : "−"}${Math.abs(magnitude).toFixed(2)}`
          : "",
      tone: reward ? ("ok" as BadgeTone) : ("risk" as BadgeTone),
      title: item.reason,
    };
  });
}
