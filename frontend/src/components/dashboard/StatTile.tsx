/**
 * StatTile — the established stat-tile chrome, extracted from
 * SummaryCards' private Card (#1592 child 2, spec §6.2): hairline
 * top-rule, small-caps label, tabular-nums value, optional tone and
 * `hint` slot (benchmark delta / denominators / caveats).
 *
 * Consumers: SummaryCards, RollingPnlStrip, the period-statement page,
 * RiskPage. Keep the hairline — it IS the tile chrome.
 *
 * **Every stat row shares this component on purpose (#1908 PR-5).** In an
 * editorial-chrome system the hairline rule is the only grouping signal
 * there is, so two stat rows must share both the tile geometry AND the
 * column grid or their rules break at different x-positions and the spread
 * stops reading as one document. A near-copy tile is how that drifted
 * before: `RollingPnlStrip` had its own `Pill` with different padding and
 * light-only tone colours.
 */
import type { ReactNode } from "react";

export function StatTile({
  label,
  value,
  hint,
  tone,
  size = "lg",
  toneHint = false,
}: {
  label: string;
  value: string;
  hint?: ReactNode;
  /** Semantic colour of the value. `muted` is an explicit "no signal here"
   *  (a zero delta) — distinct from omitting `tone`, which renders the
   *  full-strength default used by non-directional stats like Total AUM. */
  tone?: "positive" | "negative" | "muted";
  /** Type scale of the value. `lg` (default) is a headline stat; `md` is a
   *  supporting row that must not compete with the headline above it. */
  size?: "lg" | "md";
  /** Carry `tone` onto the hint as well. Opt-in, because the hint slot holds
   *  two different kinds of thing: a RESTATEMENT of the value in another unit
   *  (a signed %, which shares the value's signal and should share its colour)
   *  or a CAVEAT / denominator ("vs book vol 12%", "Budget unavailable"),
   *  which must stay muted so it doesn't read as a second signal. */
  toneHint?: boolean;
}) {
  const toneClass =
    tone === "positive"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "negative"
        ? "text-red-600 dark:text-red-400"
        : tone === "muted"
          ? "text-slate-600 dark:text-slate-400"
          : "text-slate-900 dark:text-slate-100";
  const sizeClass = size === "md" ? "text-lg" : "text-2xl";
  const hintToneClass = toneHint ? toneClass : "text-slate-500 dark:text-slate-400";
  return (
    <div className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">
        {label}
      </div>
      <div className={`mt-1 ${sizeClass} font-semibold tabular-nums ${toneClass}`}>
        {value}
      </div>
      {hint ? (
        <div className={`mt-1 text-xs tabular-nums ${hintToneClass}`}>{hint}</div>
      ) : null}
    </div>
  );
}

/**
 * The shared stat-row grid. Both dashboard stat rows use it so their
 * hairline rules land on the same x-positions — the alignment IS the
 * grouping. A row with fewer than four tiles fills from the left and leaves
 * the trailing columns empty rather than redistributing (which is what
 * pushed the rolling-P&L rules out of alignment with the summary row above).
 */
export const STAT_ROW_GRID = "grid grid-cols-1 gap-x-6 sm:grid-cols-2 lg:grid-cols-4";
