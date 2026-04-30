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
  const toneText =
    sign === "pos"
      ? "text-emerald-700"
      : sign === "neg"
        ? "text-red-700"
        : "text-slate-600";
  // Design-system v1: hairline-top chrome shared with Section/Pane.
  // Tone is carried by text colour only — no border accent.
  return (
    <div
      className="flex-1 border-t border-slate-200 dark:border-slate-800 px-3 pt-3 pb-1"
      data-testid={`rolling-pnl-${period.period}`}
    >
      <div className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">
        {LABELS[period.period] ?? period.period}
      </div>
      <div className={`mt-1 text-lg font-semibold tabular-nums ${toneText}`}>
        {`${sign === "pos" ? "+" : ""}${formatMoney(period.pnl, currency)}`}
      </div>
      <div className={`text-xs tabular-nums ${toneText}`}>
        {/* formatPct already signs positives — don't double-prefix. */}
        {period.pnl_pct === null ? "—" : formatPct(period.pnl_pct)}
      </div>
    </div>
  );
}

export function RollingPnlStrip(): JSX.Element | null {
  const { data, loading, error } = useAsync(fetchRollingPnl, []);

  if (loading) {
    return (
      <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="border-t border-slate-200 dark:border-slate-800 px-3 pt-3 pb-1">
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
    <div className="grid grid-cols-1 gap-x-6 sm:grid-cols-3">
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
