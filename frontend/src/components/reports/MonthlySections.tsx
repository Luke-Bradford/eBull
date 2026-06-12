/**
 * Monthly-only statement sections (#1592 child 2, spec §4):
 * §4.3 Rolling returns · §4.7 Dividends & income · §4.8 Charges ·
 * §4.9 Risk & trade statistics · §4.10 Model & thesis review.
 *
 * Basis reminders: rolling/attribution/gross-return fields are
 * FRACTION-basis (formatPct); `win_rate_pct` / `hit_rate_pct` are
 * pre-multiplied 0–100 strings (literal "%" suffix) — see
 * reportSnapshot.ts module header.
 */
import { Link } from "react-router-dom";

import type {
  AttributionSummaryV2,
  CostsV2,
  IncomeV2,
  MonthlySnapshotV2,
  RiskV2,
  RollingReturnsV2,
  ThesisSummaryV2,
  TradeStatsV2,
} from "@/api/reportSnapshot";
import { DegradedBadge, Fn, NilLine, ScopeCaveat, type NoteIndex } from "@/components/reports/StatementChrome";
import { dec } from "@/components/reports/snapshotMath";
import { formatDate, formatMoney, formatNumber, formatPct } from "@/lib/format";

const ROLLING_WINDOWS: ReadonlyArray<readonly [keyof RollingReturnsV2, string]> = [
  ["1m", "1 month"],
  ["3m", "3 months"],
  ["6m", "6 months"],
  ["1y", "1 year"],
  ["si", "Since inception"],
];

export function RollingReturnsSection({ rolling }: { rolling: RollingReturnsV2 }) {
  return (
    <table className="w-full max-w-lg text-sm">
      <thead>
        <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
          <th className="px-2 py-2 font-medium">Window</th>
          <th className="px-2 py-2 text-right font-medium">Portfolio</th>
          <th className="px-2 py-2 text-right font-medium">Benchmark</th>
          <th className="px-2 py-2 text-right font-medium">Excess</th>
        </tr>
      </thead>
      <tbody>
        {ROLLING_WINDOWS.map(([key, label]) => {
          const cell = rolling[key];
          return (
            <tr key={key} className="border-t border-slate-100 dark:border-slate-800/60">
              <td className="px-2 py-2 text-slate-600 dark:text-slate-300">{label}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatPct(dec(cell.portfolio))}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatPct(dec(cell.benchmark))}</td>
              <td className="px-2 py-2 text-right tabular-nums">{formatPct(dec(cell.excess))}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function IncomeSection({ income, marker }: { income: IncomeV2; marker: NoteIndex }) {
  if (income.items.length === 0) {
    return <NilLine>No dividends declared this period.</NilLine>;
  }
  const totals = Object.entries(income.estimated_totals);
  return (
    <div className="space-y-2">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
            <th className="px-2 py-2 font-medium">Symbol</th>
            <th className="px-2 py-2 font-medium">Ex-date</th>
            <th className="px-2 py-2 text-right font-medium">DPS</th>
            <th className="px-2 py-2 text-right font-medium">Units</th>
            <th className="px-2 py-2 text-right font-medium">Estimated amount</th>
          </tr>
        </thead>
        <tbody>
          {income.items.map((item, i) => (
            <tr key={`${item.instrument_id}-${item.ex_date}-${i}`} className="border-t border-slate-100 dark:border-slate-800/60">
              <td className="px-2 py-2">
                <Link
                  to={`/instrument/${encodeURIComponent(item.symbol)}`}
                  className="font-medium text-blue-600 hover:underline dark:text-blue-400"
                >
                  {item.symbol}
                </Link>
              </td>
              <td className="px-2 py-2 text-slate-500">{formatDate(item.ex_date)}</td>
              <td className="px-2 py-2 text-right tabular-nums">
                {formatMoney(dec(item.dps_declared), item.currency)}
              </td>
              <td className="px-2 py-2 text-right tabular-nums">{formatNumber(dec(item.units))}</td>
              <td className="px-2 py-2 text-right tabular-nums">
                {formatMoney(dec(item.estimated_amount), item.currency)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-sm font-medium">
        Estimated income:{" "}
        <span className="tabular-nums">
          {totals.map(([ccy, total]) => formatMoney(dec(total), ccy)).join(" + ")}
        </span>
        {marker.income !== undefined ? <Fn n={marker.income} /> : null}
      </p>
    </div>
  );
}

export function ChargesSection({
  costs,
  currency,
  fxUnavailable,
  marker,
}: {
  costs: CostsV2;
  currency: string;
  fxUnavailable: boolean;
  marker: NoteIndex;
}) {
  return (
    <div className="space-y-1">
      <p className="text-sm">
        Fees paid in period:{" "}
        <span className="tabular-nums">{formatMoney(dec(costs.fees_total), currency)}</span>{" "}
        <span className="text-xs text-slate-500">
          ({costs.fill_count} fill{costs.fill_count === 1 ? "" : "s"})
        </span>
        {fxUnavailable ? <DegradedBadge>FX unavailable</DegradedBadge> : null}
      </p>
      <ScopeCaveat>
        Own-platform fees only — broker-side fees are invisible until the trade ledger (#1593)
        <Fn n={marker.scope1593} />
      </ScopeCaveat>
    </div>
  );
}

export function RiskStatsSection({
  risk,
  tradeStats,
  bestTrade,
  worstTrade,
  marker,
}: {
  risk: RiskV2;
  tradeStats: TradeStatsV2;
  bestTrade: MonthlySnapshotV2["best_trade"];
  worstTrade: MonthlySnapshotV2["worst_trade"];
  marker: NoteIndex;
}) {
  // win_rate_pct is a PERCENT-basis string ("66.67") — literal suffix.
  const winRate =
    tradeStats.total_closed >= 5 && tradeStats.win_rate_pct !== null
      ? `${tradeStats.win_rate_pct}%`
      : null;
  return (
    <div className="space-y-3 text-sm">
      <dl className="max-w-md space-y-0.5">
        <Row label="Volatility">
          {risk.insufficient_history
            ? `Insufficient history (${risk.observations} period${risk.observations === 1 ? "" : "s"})`
            : formatPct(dec(risk.volatility))}
          {marker.smallN !== undefined ? <Fn n={marker.smallN} /> : null}
        </Row>
        <Row label="Max drawdown">
          {risk.insufficient_history
            ? `Insufficient history (${risk.observations} period${risk.observations === 1 ? "" : "s"})`
            : formatPct(dec(risk.max_drawdown))}
        </Row>
        <Row label="Observation basis">{risk.observation_label}</Row>
        <Row label="Turnover">(requires trade ledger, #1593)</Row>
      </dl>

      <div>
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Closed-trade review
        </h3>
        {tradeStats.total_closed === 0 ? (
          <NilLine>No closed trades during this period.</NilLine>
        ) : (
          <dl className="max-w-md space-y-0.5">
            <Row label="Win rate">
              {winRate !== null
                ? `${winRate} (${tradeStats.winners} of ${tradeStats.total_closed} closed trades)`
                : `${tradeStats.winners} of ${tradeStats.total_closed} closed trades (% suppressed below 5)`}
              {marker.smallN !== undefined ? <Fn n={marker.smallN} /> : null}
            </Row>
            <Row label="Payoff ratio">
              {dec(tradeStats.payoff_ratio) !== null
                ? `${formatNumber(dec(tradeStats.payoff_ratio), 2)} (avg win ${formatPct(dec(tradeStats.avg_win_pct))} / avg loss ${formatPct(dec(tradeStats.avg_loss_pct))})`
                : "—"}
            </Row>
            <Row label="Average holding period">
              {tradeStats.avg_holding_days !== null
                ? `${formatNumber(tradeStats.avg_holding_days, 0)} days (${tradeStats.total_closed} trade${tradeStats.total_closed === 1 ? "" : "s"})`
                : "—"}
            </Row>
            <Row label="Best closed trade (gross)">
              {bestTrade !== null
                ? `${bestTrade.symbol} ${formatPct(dec(bestTrade.gross_return_pct))}`
                : "—"}
            </Row>
            <Row label="Worst closed trade (gross)">
              {worstTrade !== null
                ? `${worstTrade.symbol} ${formatPct(dec(worstTrade.gross_return_pct))}`
                : "—"}
            </Row>
          </dl>
        )}
      </div>
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-4">
      <dt className="text-slate-500">{label}</dt>
      <dd className="text-right tabular-nums">{children}</dd>
    </div>
  );
}

export function ModelThesisSection({
  attribution,
  thesis,
  scoreChanges,
}: {
  attribution: AttributionSummaryV2;
  thesis: ThesisSummaryV2;
  scoreChanges: MonthlySnapshotV2["score_changes"];
}) {
  const components: ReadonlyArray<readonly [string, string | null]> = [
    ["Gross return", attribution.avg_gross_return_pct],
    ["Market", attribution.avg_market_return_pct],
    ["Sector", attribution.avg_sector_return_pct],
    ["Model alpha", attribution.avg_model_alpha_pct],
    ["Timing alpha", attribution.avg_timing_alpha_pct],
    ["Cost drag", attribution.avg_cost_drag_pct],
  ];
  return (
    <div className="space-y-3 text-sm">
      <div>
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Attribution decomposition{" "}
          <span className="normal-case text-slate-400">
            (equal-weighted, n={attribution.positions_attributed} closed trades)
          </span>
        </h3>
        {attribution.positions_attributed === 0 ? (
          <NilLine>No closed trades to attribute this period.</NilLine>
        ) : (
          <dl className="max-w-md space-y-0.5">
            {components.map(([label, raw]) => (
              <Row key={label} label={label}>
                {formatPct(dec(raw))}
              </Row>
            ))}
          </dl>
        )}
      </div>

      <div>
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Thesis outcomes
        </h3>
        {thesis.total === 0 ? (
          <NilLine>No thesis outcomes to review this period.</NilLine>
        ) : (
          <dl className="max-w-md space-y-0.5">
            <Row label="Hit">{`${thesis.hits} of ${thesis.evaluated} evaluated`}</Row>
            <Row label="Miss">{String(thesis.misses)}</Row>
            <Row label="Not yet evaluable">{String(thesis.not_evaluable)}</Row>
            <Row label="Buy hit rate">
              {thesis.buy.hit_rate_pct !== null
                ? `${thesis.buy.hit_rate_pct}% (${thesis.buy.hits} of ${thesis.buy.n})`
                : `n=${thesis.buy.n}`}
            </Row>
            <Row label="Avoid hit rate">
              {thesis.avoid.hit_rate_pct !== null
                ? `${thesis.avoid.hit_rate_pct}% (${thesis.avoid.hits} of ${thesis.avoid.n})`
                : `n=${thesis.avoid.n}`}
            </Row>
          </dl>
        )}
      </div>

      <div>
        <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">
          Rank movers
        </h3>
        {scoreChanges.length === 0 ? (
          <NilLine>No significant rank movements this period.</NilLine>
        ) : (
          <ul className="max-w-md space-y-0.5">
            {scoreChanges.map((s) => (
              <li key={`${s.instrument_id}-${s.scored_at}`} className="flex items-baseline justify-between gap-4">
                <Link
                  to={`/instrument/${encodeURIComponent(s.symbol)}`}
                  className="font-medium text-blue-600 hover:underline dark:text-blue-400"
                >
                  {s.symbol}
                </Link>
                <span className="tabular-nums text-slate-600 dark:text-slate-300">
                  rank {s.rank ?? "—"}{" "}
                  {s.rank_delta !== null ? (
                    <span className="text-slate-500">
                      ({s.rank_delta > 0 ? `+${s.rank_delta}` : s.rank_delta})
                    </span>
                  ) : null}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
