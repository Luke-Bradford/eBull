import { useState } from "react";

import {
  fetchFiredSignals,
  fetchStrategyOverview,
} from "@/api/strategies";
import type { StrategyEvidenceWindow, StrategyOverview } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { formatDate, formatNumber } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

function number(value: string | null, suffix = ""): string {
  if (value === null) return "—";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${formatNumber(parsed, 2)}${suffix}` : "—";
}

function EvidenceTable({ windows }: { readonly windows: StrategyEvidenceWindow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-xs">
        <thead className="text-slate-500 dark:text-slate-400">
          <tr>
            <th className="pb-2 pr-4 font-medium">Window</th>
            <th className="pb-2 pr-4 font-medium">Status</th>
            <th className="pb-2 pr-4 font-medium">Trades</th>
            <th className="pb-2 pr-4 font-medium">Expectancy</th>
            <th className="pb-2 pr-4 font-medium">Sharpe</th>
            <th className="pb-2 pr-4 font-medium">Max drawdown</th>
            <th className="pb-2 font-medium">Refusals</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {windows.map((window) => {
            const representative = window.arms.find(
              (arm) => arm.ambiguity_arm === "worst_case" && arm.quarantine_arm === "masked",
            );
            return (
              <tr key={window.window_id}>
                <td className="py-2 pr-4">
                  <div className="font-medium text-slate-800 dark:text-slate-200">{window.label}</div>
                  <div className="text-slate-500">
                    {formatDate(window.window_start)}–{formatDate(window.window_end)}
                  </div>
                </td>
                <td className="py-2 pr-4">{window.status}</td>
                <td className="py-2 pr-4 tabular-nums">{representative?.trade_count ?? "—"}</td>
                <td className="py-2 pr-4 tabular-nums">
                  {number(representative?.expectancy_per_trade_pct ?? null, "%")}
                </td>
                <td className="py-2 pr-4 tabular-nums">{number(representative?.sharpe ?? null)}</td>
                <td className="py-2 pr-4 tabular-nums">
                  {number(representative?.max_drawdown_pct ?? null, "%")}
                </td>
                <td className="max-w-xs py-2 text-slate-500">
                  {representative?.promotion_refusals.join(", ") ?? "Recent evidence not computed"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function StrategyPanel({ strategy }: { readonly strategy: StrategyOverview }) {
  return (
    <article className="border-t border-slate-200 pt-3 dark:border-slate-800">
      <header className="mb-3 flex items-baseline justify-between gap-2">
        <h2 className="text-sm font-semibold text-slate-700 dark:text-slate-300">{strategy.title}</h2>
        <span className="text-xs text-slate-500 dark:text-slate-400">
          {strategy.runnable ? "runnable" : "excluded"} · scan {strategy.scan.status} · frontier{" "}
          {formatDate(strategy.scan.frontier_date)}
        </span>
      </header>
      <div className="mb-3 grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
        <div>
          <div className="text-xs text-slate-500">Fired entries</div>
          <div className="tabular-nums">{formatNumber(strategy.scan.fired_entries, 0)}</div>
        </div>
        <div>
          <div className="text-xs text-slate-500">Fired exits</div>
          <div className="tabular-nums">{formatNumber(strategy.scan.fired_exits, 0)}</div>
        </div>
        <div>
          <div className="text-xs text-slate-500">Not evaluable</div>
          <div className="tabular-nums">{formatNumber(strategy.scan.not_evaluable, 0)}</div>
        </div>
        <div>
          <div className="text-xs text-slate-500">Legacy result rows</div>
          <div className="tabular-nums">{formatNumber(strategy.legacy_result_count, 0)}</div>
        </div>
      </div>
      {strategy.exclusion_reason ? (
        <p className="mb-3 text-xs text-amber-700 dark:text-amber-400">
          Backtest exclusion: {strategy.exclusion_reason}
        </p>
      ) : null}
      <EvidenceTable windows={strategy.evidence_windows} />
      <details className="mt-3 text-xs text-slate-500">
        <summary className="cursor-pointer">Allocation refusals and provenance</summary>
        <p className="mt-2">{strategy.allocation_refusals.join(", ")}</p>
        <p className="mt-1 break-all">Strategy version: {strategy.strategy_version}</p>
      </details>
    </article>
  );
}

export function StrategiesPage() {
  const [cursor, setCursor] = useState<number | null>(null);
  const [cursorHistory, setCursorHistory] = useState<Array<number | null>>([]);
  const overview = useAsync(fetchStrategyOverview, []);
  const signals = useAsync(() => fetchFiredSignals(cursor), [cursor]);

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Strategies</h1>
        <p className="mt-1 max-w-4xl text-sm text-slate-600 dark:text-slate-400">
          Read-only forward observation. Every current-version fired signal is shown whether funded or
          unfunded; historical backtests remain separate. Allocation and broker execution are disabled.
        </p>
      </header>

      <Section title="Strategy evidence" action="Exact current versions">
        {overview.loading ? <SectionSkeleton rows={8} /> : null}
        {overview.error ? <SectionError onRetry={overview.refetch} /> : null}
        {overview.data?.strategies.length === 0 ? (
          <EmptyState
            title="No registered strategies"
            description="Add a strategy to the manifest before evidence can be monitored."
          />
        ) : null}
        {overview.data && overview.data.strategies.length > 0 ? (
          <div className="space-y-6">
            {overview.data.strategies.map((strategy) => (
              <StrategyPanel key={strategy.strategy_id} strategy={strategy} />
            ))}
          </div>
        ) : null}
      </Section>

      <Section title="Fired signals" action="Forward observation · all unfunded">
        {signals.loading ? <SectionSkeleton rows={6} /> : null}
        {signals.error ? <SectionError onRetry={signals.refetch} /> : null}
        {signals.data?.items.length === 0 ? (
          <EmptyState title="No fired signals" description="The current strategy versions have not fired yet." />
        ) : null}
        {signals.data && signals.data.items.length > 0 ? (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-xs">
                <thead className="text-slate-500 dark:text-slate-400">
                  <tr>
                    <th className="pb-2 pr-4 font-medium">Signal</th>
                    <th className="pb-2 pr-4 font-medium">Instrument</th>
                    <th className="pb-2 pr-4 font-medium">Fill</th>
                    <th className="pb-2 pr-4 font-medium">Outcome</th>
                    <th className="pb-2 font-medium">Funding</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                  {signals.data.items.map((signal) => (
                    <tr key={signal.signal_id}>
                      <td className="py-2 pr-4">
                        <div>{signal.strategy_id}</div>
                        <div className="text-slate-500">
                          {signal.signal_kind} · {formatDate(signal.signal_bar_date)}
                        </div>
                      </td>
                      <td className="py-2 pr-4">
                        <div className="font-medium">{signal.symbol}</div>
                        <div className="text-slate-500">{signal.company_name ?? `#${signal.instrument_id}`}</div>
                      </td>
                      <td className="py-2 pr-4 tabular-nums">
                        {formatDate(signal.fill_bar_date)} · {number(signal.fill_price)}
                      </td>
                      <td className="py-2 pr-4">
                        {signal.outcome ?? "pending"}
                        {signal.gross_return_pct ? ` · ${number(signal.gross_return_pct, "%")}` : ""}
                      </td>
                      <td className="py-2 text-slate-500">unfunded · execution disabled</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-3 flex gap-2">
              <button
                type="button"
                disabled={cursorHistory.length === 0}
                onClick={() => {
                  const previous = cursorHistory.at(-1) ?? null;
                  setCursorHistory((history) => history.slice(0, -1));
                  setCursor(previous);
                }}
                className="border border-slate-300 px-3 py-1 text-xs disabled:opacity-40 dark:border-slate-700"
              >
                Newer
              </button>
              <button
                type="button"
                disabled={signals.data.next_cursor === null}
                onClick={() => {
                  setCursorHistory((history) => [...history, cursor]);
                  setCursor(signals.data?.next_cursor ?? null);
                }}
                className="border border-slate-300 px-3 py-1 text-xs disabled:opacity-40 dark:border-slate-700"
              >
                Older
              </button>
            </div>
          </>
        ) : null}
      </Section>
    </div>
  );
}
