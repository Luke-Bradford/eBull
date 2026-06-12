/**
 * §4.5 Holdings & exposure — period-end table (never truncated: a
 * statement that hides positions isn't a record, §6.8), weight bars
 * with a dark-aware track, concentration line phrased for small n,
 * and sector exposure bars in ONE neutral fill (accent rotation is
 * for series identity, not categories — §6.3).
 */
import { Link } from "react-router-dom";

import type { HoldingV2, RiskV2 } from "@/api/reportSnapshot";
import { NilLine } from "@/components/reports/StatementChrome";
import { dec } from "@/components/reports/snapshotMath";
import { formatMoney, formatNumber, formatPct, formatUnsignedPct } from "@/lib/format";
import { useChartTheme } from "@/lib/useChartTheme";

function WeightBar({ fraction }: { fraction: number }) {
  return (
    <div className="h-1.5 w-16 rounded bg-slate-100 dark:bg-slate-800">
      <div
        className="h-1.5 rounded bg-slate-400 dark:bg-slate-500"
        style={{ width: `${Math.min(100, Math.max(0, fraction * 100))}%` }}
      />
    </div>
  );
}

export function HoldingsSection({
  holdings,
  risk,
  currency,
}: {
  holdings: HoldingV2[];
  /** Monthly carries the risk key; weekly passes null and the
   *  concentration line derives from the table itself. */
  risk: RiskV2 | null;
  currency: string;
}) {
  const theme = useChartTheme();

  if (holdings.length === 0) {
    return <NilLine>No open positions at period end.</NilLine>;
  }

  const top5Fraction =
    risk !== null
      ? dec(risk.concentration_top5_pct)
      : (() => {
          const weights = holdings
            .map((h) => dec(h.weight_pct))
            .filter((w): w is number => w !== null)
            .sort((a, b) => b - a);
          if (weights.length === 0) return null;
          return weights.slice(0, 5).reduce((a, b) => a + b, 0);
        })();

  const sectorEntries = risk !== null ? Object.entries(risk.sector_exposure) : [];

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
              <th className="px-2 py-2 font-medium">Symbol</th>
              <th className="px-2 py-2 font-medium">Name</th>
              <th className="px-2 py-2 font-medium">Sector</th>
              <th className="px-2 py-2 text-right font-medium">Units</th>
              <th className="px-2 py-2 text-right font-medium">Price</th>
              <th className="px-2 py-2 text-right font-medium">Market value</th>
              <th className="px-2 py-2 text-right font-medium">Weight</th>
              <th className="px-2 py-2 text-right font-medium">Since entry</th>
              <th className="px-2 py-2 text-right font-medium">Period contribution</th>
            </tr>
          </thead>
          <tbody>
            {holdings.map((h) => {
              const weight = dec(h.weight_pct);
              const contribution = dec(h.period_contribution);
              const bps = dec(h.period_contribution_bps);
              return (
                <tr key={h.instrument_id} className="border-t border-slate-100 dark:border-slate-800/60">
                  <td className="px-2 py-2">
                    <Link
                      to={`/instrument/${encodeURIComponent(h.symbol)}`}
                      className="font-medium text-blue-600 hover:underline dark:text-blue-400"
                    >
                      {h.symbol}
                    </Link>
                  </td>
                  <td className="px-2 py-2 text-slate-600 dark:text-slate-300">{h.company_name}</td>
                  <td className="px-2 py-2 text-slate-500">{h.sector ?? "—"}</td>
                  <td className="px-2 py-2 text-right tabular-nums">{formatNumber(dec(h.units))}</td>
                  <td className="px-2 py-2 text-right tabular-nums">{formatMoney(dec(h.price), currency)}</td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatMoney(dec(h.market_value), currency)}
                  </td>
                  <td className="px-2 py-2">
                    <div className="flex items-center justify-end gap-2 tabular-nums">
                      {formatUnsignedPct(weight)}
                      {weight !== null ? <WeightBar fraction={weight} /> : null}
                    </div>
                  </td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatPct(dec(h.since_entry_return_pct))}
                  </td>
                  <td className="px-2 py-2 text-right tabular-nums">
                    {formatMoney(contribution, currency)}
                    {bps !== null ? (
                      <span className="ml-1 text-xs text-slate-500">({formatNumber(bps, 0)} bps)</span>
                    ) : null}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-slate-500">
        {top5Fraction !== null
          ? holdings.length <= 5
            ? `Top 5 concentration = ${formatUnsignedPct(top5Fraction)} of portfolio value (only ${holdings.length} holding${holdings.length === 1 ? "" : "s"})`
            : `Top 5 concentration = ${formatUnsignedPct(top5Fraction)} of portfolio value`
          : "Top 5 concentration unavailable"}
      </p>

      {sectorEntries.length > 0 ? (
        <div className="max-w-md space-y-1">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Sector exposure
          </h3>
          {sectorEntries.map(([sector, raw]) => {
            const fraction = dec(raw);
            return (
              <div key={sector} className="flex items-center gap-2 text-xs">
                <span className="w-32 truncate text-slate-600 dark:text-slate-300">{sector}</span>
                <div className="h-2 flex-1 rounded bg-slate-100 dark:bg-slate-800">
                  <div
                    className="h-2 rounded"
                    style={{
                      width: `${Math.min(100, Math.max(0, (fraction ?? 0) * 100))}%`,
                      backgroundColor: theme.accent[1],
                    }}
                  />
                </div>
                <span className="w-14 text-right tabular-nums text-slate-500">
                  {formatUnsignedPct(fraction)}
                </span>
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
