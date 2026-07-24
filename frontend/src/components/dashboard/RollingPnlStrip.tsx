/**
 * RollingPnlStrip — 1d / 1w / 1m unrealised P&L pills on the
 * dashboard (#315 Phase 2). Sits under SummaryCards.
 *
 * Values come from /portfolio/rolling-pnl. Rendered as three side-by-side
 * pills showing money delta + percentage. Low `coverage` (few positions
 * had a prior close) surfaces a muted "(n of m)" suffix so the operator
 * knows whether to trust the number.
 *
 * Silent-when-loading (skeleton), compact error state (inline retry).
 * Never blanks the dashboard on its own failure.
 */
import { fetchRollingPnl } from "@/api/portfolio";
import type { RollingPnlPeriod } from "@/api/types";
import { formatMoney, formatPct } from "@/lib/format";
import { SectionSkeleton } from "@/components/dashboard/Section";
import { STAT_ROW_GRID, StatTile } from "@/components/dashboard/StatTile";
import { useAsync } from "@/lib/useAsync";

const LABELS: Record<string, string> = {
  "1d": "1 day",
  "1w": "1 week",
  "1m": "1 month",
};

function Pill({
  period,
  currency,
}: {
  period: RollingPnlPeriod;
  currency: string;
}) {
  // Zero delta is neutral, not positive — avoids the odd "+£0.00"
  // rendering (Codex #388 round-2 finding).
  const sign: "pos" | "neg" | "neutral" =
    period.pnl > 0 ? "pos" : period.pnl < 0 ? "neg" : "neutral";
  // #1908 PR-5: this was a near-copy of StatTile with its own padding and
  // LIGHT-ONLY tone classes (`text-emerald-700` / `text-red-700` with no
  // `dark:` partner — the dark gate's checks only cover bg/border/hover, so
  // nothing caught it). Reusing StatTile fixes the dark-mode contrast and
  // guarantees this row's hairlines keep aligning with the summary row.
  //
  // Three props carry behaviour the private tile had and must not lose:
  //   - `size="md"`   — supporting row, must not shout as loud as the headline.
  //   - `toneHint`    — the % RESTATES the money delta, so it carries the same
  //                     signal and the same colour (review round 1 WARNING) —
  //                     but ONLY when there IS a percentage. A null pnl_pct
  //                     renders the em-dash no-data placeholder, and a dash
  //                     painted emerald/rose reads as a signal that does not
  //                     exist (review round 2 NITPICK).
  //   - `tone="muted"` on a zero delta — an explicit "no direction here",
  //                     not the full-strength default (review round 1 NITPICK).
  return (
    <div data-testid={`rolling-pnl-${period.period}`}>
      <StatTile
        label={LABELS[period.period] ?? period.period}
        value={`${sign === "pos" ? "+" : ""}${formatMoney(period.pnl, currency)}`}
        // formatPct already signs positives — don't double-prefix.
        hint={period.pnl_pct === null ? "—" : formatPct(period.pnl_pct)}
        tone={sign === "pos" ? "positive" : sign === "neg" ? "negative" : "muted"}
        size="md"
        toneHint={period.pnl_pct !== null}
      />
    </div>
  );
}

export function RollingPnlStrip(): JSX.Element | null {
  const { data, loading, error } = useAsync(fetchRollingPnl, []);

  if (loading) {
    return (
      <div className={STAT_ROW_GRID}>
        {[0, 1, 2].map((i) => (
          <div key={i} className="border-t border-slate-200 dark:border-slate-800 px-1 pt-3 pb-1">
            <SectionSkeleton rows={1} />
          </div>
        ))}
      </div>
    );
  }

  if (error !== null || data === null) {
    // Silent-on-error rather than cluttering the dashboard — the
    // SummaryCards' total P&L card already reports a number. This
    // strip is supplementary context; if it fails, hide it.
    return null;
  }

  return (
    <div className={STAT_ROW_GRID}>
      {data.periods.map((period) => (
        <Pill
          key={period.period}
          period={period}
          currency={data.display_currency}
        />
      ))}
    </div>
  );
}
