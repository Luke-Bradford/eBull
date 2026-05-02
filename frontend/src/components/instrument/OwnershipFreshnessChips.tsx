/**
 * Per-category freshness chip strip for the ownership card (#767).
 *
 * Renders one chip per non-empty category (Institutions / ETFs /
 * Insiders / Treasury) with the source-row date, an age delta, and a
 * colour code:
 *
 *   * fresh   — neutral slate
 *   * aging   — amber (cadence-window exceeded; expected if upstream
 *     filing window hasn't closed yet)
 *   * stale   — red (clearly past expected cadence)
 *   * unknown — slate without an age delta (no date supplied)
 *
 * Renders nothing when no categories are present so the empty-state
 * card body owns the messaging.
 */

import {
  classifyFreshness,
  formatAge,
  type FreshnessLevel,
} from "@/components/instrument/ownershipFreshness";
import type { SunburstRings } from "@/components/instrument/ownershipRings";

export interface OwnershipFreshnessChipsProps {
  readonly rings: SunburstRings;
  /** Reference clock. Defaulted by the consumer to ``new Date()``;
   *  required here so tests can pin time without patching globals. */
  readonly today: Date;
}

const LEVEL_CLASSES: Record<FreshnessLevel, string> = {
  fresh:
    "border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400",
  aging:
    "border-amber-300 bg-amber-50 text-amber-800 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-300",
  stale:
    "border-red-300 bg-red-50 text-red-800 dark:border-red-700 dark:bg-red-950/40 dark:text-red-300",
  unknown:
    "border-dashed border-slate-300 bg-transparent text-slate-500 dark:border-slate-600 dark:text-slate-400",
};

export function OwnershipFreshnessChips({
  rings,
  today,
}: OwnershipFreshnessChipsProps): JSX.Element | null {
  if (rings.categories.length === 0) return null;
  return (
    <ul
      className="flex flex-wrap gap-1.5 text-xs"
      aria-label="Per-category data freshness"
    >
      {rings.categories.map((cat) => {
        const level = classifyFreshness(cat.key, cat.as_of_date, today);
        const age = formatAge(cat.as_of_date, today);
        const cls = LEVEL_CLASSES[level];
        const title = buildChipTitle(cat.label, cat.as_of_date, level);
        return (
          <li key={cat.key}>
            <span
              className={`inline-flex items-baseline gap-1 rounded border px-1.5 py-0.5 ${cls}`}
              title={title}
              data-freshness-level={level}
            >
              <span className="font-medium">{cat.label}</span>
              {age !== null ? (
                <span className="font-mono">{age}</span>
              ) : (
                <span className="font-mono text-slate-400 dark:text-slate-500">—</span>
              )}
            </span>
          </li>
        );
      })}
    </ul>
  );
}

function buildChipTitle(
  label: string,
  as_of_date: string | null,
  level: FreshnessLevel,
): string {
  // ``unknown`` is also reached when the as_of_date is non-null but
  // unparsable (Codex review of #767). Echoing a malformed string in
  // a tooltip just confuses the operator — collapse both cases to
  // "no date on file" so the chip and tooltip stay consistent.
  const date_part =
    level === "unknown" || as_of_date === null ? "no date on file" : `as of ${as_of_date}`;
  switch (level) {
    case "fresh":
      return `${label}: ${date_part}`;
    case "aging":
      return `${label}: ${date_part} — past expected refresh cadence`;
    case "stale":
      return `${label}: ${date_part} — clearly stale, check upstream ingest`;
    case "unknown":
      return `${label}: ${date_part}`;
  }
}
