import { useEffect, useState } from "react";
import type { FormEvent } from "react";

import {
  fetchFiredSignals,
  fetchStrategyOverview,
  updateStrategyAllocation,
} from "@/api/strategies";
import type {
  StrategyEntryBlock,
  StrategyEvidenceWindow,
  StrategyOverview,
} from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { formatDate, formatMoney, formatNumber, formatPct } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

function decimal(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function pctPoints(value: string | null): string {
  const parsed = decimal(value);
  return formatPct(parsed === null ? null : parsed / 100);
}

function fraction(value: string | null): string {
  return formatPct(decimal(value));
}

function money(value: string | null, currency = "USD"): string {
  return formatMoney(decimal(value), currency);
}

function humanizeCode(value: string): string {
  const text = value.replaceAll("_", " ");
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function shadowOutcome(value: string | null): string {
  if (value === null) return "Pending";
  if (value === "tp_hit") return "Reached its goal";
  if (value === "sl_hit" || value === "expired") return "Didn't reach its goal";
  if (value === "ambiguous") return "Could not determine";
  return "Not resolved";
}

function statusTone(status: "missing" | "partial" | "complete"): BadgeTone {
  if (status === "complete") return "ok";
  if (status === "partial") return "warn";
  return "neutral";
}

function EvidenceTable({ windows }: { readonly windows: StrategyEvidenceWindow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-slate-500 dark:text-slate-400">
          <tr>
            <th className="px-2 py-2 font-medium">Recent test window</th>
            <th className="px-2 py-2 font-medium">Coverage</th>
            <th className="px-2 py-2 text-right font-medium">Trades</th>
            <th className="px-2 py-2 text-right font-medium">Per trade</th>
            <th className="px-2 py-2 text-right font-medium">Worst dip</th>
            <th className="px-2 py-2 font-medium">Evidence decision</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {windows.map((window) => {
            const representative = window.arms.find(
              (arm) => arm.ambiguity_arm === "worst_case" && arm.quarantine_arm === "masked",
            );
            const refused = representative ? representative.promotion_refusals.length > 0 : true;
            return (
              <tr key={window.window_id}>
                <td className="px-2 py-2">
                  <div className="font-medium text-slate-800 dark:text-slate-200">{window.label}</div>
                  <div className="text-xs text-slate-500">
                    {formatDate(window.window_start)}–{formatDate(window.window_end)}
                  </div>
                </td>
                <td className="px-2 py-2">
                  <Badge tone={statusTone(window.status)}>{window.status}</Badge>
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {representative?.trade_count ?? "—"}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {pctPoints(representative?.expectancy_per_trade_pct ?? null)}
                </td>
                <td className="px-2 py-2 text-right tabular-nums">
                  {pctPoints(representative?.max_drawdown_pct ?? null)}
                </td>
                <td className="px-2 py-2">
                  <Badge tone={representative === undefined ? "neutral" : refused ? "risk" : "ok"}>
                    {representative === undefined ? "Not measured" : refused ? "Not eligible" : "Passed"}
                  </Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Metric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="border-t border-slate-200 pt-2 dark:border-slate-800">
      <div className="text-xs text-slate-500 dark:text-slate-400">{label}</div>
      <div className="mt-1 text-sm font-semibold tabular-nums text-slate-800 dark:text-slate-200">
        {value}
      </div>
      {detail ? <div className="mt-0.5 text-xs text-slate-500">{detail}</div> : null}
    </div>
  );
}

function AllocationControl({
  strategy,
  onUpdated,
}: {
  readonly strategy: StrategyOverview;
  readonly onUpdated: () => void;
}) {
  const [limit, setLimit] = useState(strategy.allocation.capital_limit);
  const [enabled, setEnabled] = useState(strategy.allocation.enabled);
  const [reason, setReason] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [failed, setFailed] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    setLimit(strategy.allocation.capital_limit);
    setEnabled(strategy.allocation.enabled);
  }, [strategy.allocation.capital_limit, strategy.allocation.enabled, strategy.allocation.revision]);

  const parsedLimit = Number(limit);
  const canReduceRisk =
    strategy.allocation.deployment_id !== null &&
    parsedLimit <= Number(strategy.allocation.capital_limit) &&
    (!enabled || strategy.allocation.enabled);
  const canSubmit =
    !submitting &&
    reason.trim().length > 0 &&
    Number.isFinite(parsedLimit) &&
    parsedLimit >= 0 &&
    (!enabled || parsedLimit > 0) &&
    (strategy.allocation_ready || canReduceRisk);
  const controlsAvailable = strategy.allocation_ready || strategy.allocation.deployment_id !== null;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setFailed(false);
    setSaved(false);
    try {
      await updateStrategyAllocation(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        capital_limit: parsedLimit.toFixed(6),
        enabled,
        reason: reason.trim(),
      });
      setReason("");
      setSaved(true);
      onUpdated();
    } catch (error) {
      console.error("Strategy allocation update failed:", error);
      setFailed(true);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={(event) => void submit(event)} className="mt-4 border-t border-slate-200 pt-3 dark:border-slate-800">
      <div className="mb-2 flex items-center justify-between gap-3">
        <div>
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Capital allocation</h3>
          <p className="mt-1 text-xs text-slate-500">
            Operator-set ceiling only. Capital is never moved automatically between strategies.
          </p>
        </div>
        <Badge tone={strategy.allocation_ready ? "ok" : "risk"}>
          {strategy.allocation_ready ? "Available" : "Unavailable"}
        </Badge>
      </div>
      <div className="grid gap-3 sm:grid-cols-[minmax(0,12rem)_auto_minmax(0,1fr)_auto] sm:items-end">
        <label className="text-xs font-medium text-slate-600 dark:text-slate-300">
          Maximum USD capital
          <input
            type="number"
            min="0"
            step="0.01"
            value={limit}
            disabled={!controlsAvailable || submitting}
            onChange={(event) => setLimit(event.target.value)}
            className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-2 py-2 text-sm tabular-nums outline-none focus:border-blue-600 focus:ring-2 focus:ring-blue-200 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-950 dark:focus:ring-blue-900"
          />
        </label>
        <label className="flex min-h-11 cursor-pointer items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
          <input
            type="checkbox"
            checked={enabled}
            disabled={!controlsAvailable || submitting}
            onChange={(event) => setEnabled(event.target.checked)}
            className="h-4 w-4"
          />
          Enabled
        </label>
        <label className="text-xs font-medium text-slate-600 dark:text-slate-300">
          Reason for this audited change
          <input
            type="text"
            maxLength={1000}
            value={reason}
            disabled={!controlsAvailable || submitting}
            onChange={(event) => setReason(event.target.value)}
            className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-2 py-2 text-sm outline-none focus:border-blue-600 focus:ring-2 focus:ring-blue-200 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-950 dark:focus:ring-blue-900"
          />
        </label>
        <button
          type="submit"
          disabled={!canSubmit}
          className="min-h-11 cursor-pointer border border-blue-700 bg-blue-700 px-3 py-2 text-sm font-medium text-white transition-colors duration-200 hover:bg-blue-800 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:cursor-not-allowed disabled:opacity-40 dark:focus:ring-blue-900"
        >
          {submitting ? "Saving…" : "Save allocation"}
        </button>
      </div>
      {!strategy.allocation_ready ? (
        <p className="mt-2 text-xs text-red-700 dark:text-red-300">
          Allocation needs complete recent evidence, a paper promotion, a pinned passing result, a current scan,
          and an execution policy. Existing allocations can still be disabled.
        </p>
      ) : null}
      {failed ? <p className="mt-2 text-xs text-red-700 dark:text-red-300">The allocation was not changed. Refresh and try again.</p> : null}
      {saved ? <p className="mt-2 text-xs text-emerald-700 dark:text-emerald-300">Allocation revision saved and audited.</p> : null}
    </form>
  );
}

function StrategyPanel({ strategy, onUpdated }: { readonly strategy: StrategyOverview; readonly onUpdated: () => void }) {
  const pnl = strategy.pnl;
  const attribution = strategy.attribution;
  return (
    <article className="border-t border-slate-200 pt-3 dark:border-slate-800">
      <header className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold text-slate-700 dark:text-slate-300">{strategy.title}</h2>
          <Badge tone={strategy.runnable ? "info" : "neutral"}>{strategy.runnable ? "Runnable" : "Excluded"}</Badge>
          <Badge tone={strategy.allocation.enabled ? "ok" : "neutral"}>
            {strategy.allocation.enabled ? "Funded" : "Shadow only"}
          </Badge>
        </div>
        <span className="text-xs text-slate-500 dark:text-slate-400">
          {strategy.stage ?? "not promoted"} · scan {strategy.scan.status} · frontier {formatDate(strategy.scan.frontier_date)}
        </span>
      </header>

      <div className="mb-4 grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-4">
        <Metric
          label="Made you"
          value={money(pnl.total_pnl, pnl.currency)}
          detail={pnl.complete ? "Exact owned lifecycle" : "Incomplete broker evidence"}
        />
        <Metric
          label="Shadow per signal"
          value={pctPoints(attribution.shadow_average_return_pct)}
          detail={`${formatNumber(attribution.resolved_entries, 0)} resolved · gross price move`}
        />
        <Metric
          label="Captured"
          value={fraction(attribution.funded_capture_rate)}
          detail={`${formatNumber(attribution.funded_entries, 0)} of ${formatNumber(attribution.fired_entries, 0)} entries`}
        />
        <Metric
          label="Filled"
          value={fraction(attribution.fill_rate)}
          detail={`${formatNumber(attribution.broker_rejected_entries, 0)} broker rejected`}
        />
        <Metric label="Allocated ceiling" value={money(strategy.allocation.capital_limit, "USD")} />
        <Metric label="Reserved" value={money(strategy.allocation.reserved_capital, "USD")} />
        <Metric label="Currently invested" value={money(strategy.allocation.invested_capital, "USD")} />
        <Metric label="Remaining" value={money(strategy.allocation.remaining_capital, "USD")} />
      </div>

      {strategy.exclusion_reason ? (
        <p className="mb-3 text-xs text-amber-700 dark:text-amber-300">Backtest exclusion: {strategy.exclusion_reason}</p>
      ) : null}
      <EvidenceTable windows={strategy.evidence_windows} />

      <details className="mt-3 text-xs text-slate-500">
        <summary className="cursor-pointer">Attribution and evidence detail</summary>
        <div className="mt-2 grid gap-2 sm:grid-cols-2">
          <p>Funded shadow gross result: {pctPoints(attribution.funded_shadow_average_return_pct)}</p>
          <p>Skipped shadow gross result: {pctPoints(attribution.rejected_shadow_average_return_pct)}</p>
          <p>Skipped minus funded: {pctPoints(attribution.opportunity_gap_pct)}</p>
          <p>Average slippage: {pctPoints(attribution.average_slippage_pct)}</p>
          <p>Average stressed entry cost: {money(attribution.average_stressed_cost_usd, "USD")}</p>
          <p>Highest observed account drawdown: {pctPoints(attribution.max_observed_account_drawdown_pct)}</p>
        </div>
        {pnl.incomplete_reasons.length > 0 ? <p className="mt-2">P&amp;L gaps: {pnl.incomplete_reasons.join(", ")}</p> : null}
        <p className="mt-2">Allocation refusals: {strategy.allocation_refusals.join(", ") || "none"}</p>
        <p className="mt-1 break-all">Strategy version: {strategy.strategy_version}</p>
      </details>

      <AllocationControl strategy={strategy} onUpdated={onUpdated} />
    </article>
  );
}

function EntryBlockBanner({ state, stale }: { readonly state: StrategyEntryBlock; readonly stale: boolean }) {
  if (!state.new_entries_blocked) return null;
  return (
    <div className="border border-red-300 bg-red-50 px-4 py-3 text-sm text-red-800 dark:border-red-800 dark:bg-red-950/40 dark:text-red-200">
      <div className="font-semibold">New strategy entries are blocked</div>
      <div className="mt-1 text-xs">
        {state.global_kill_reason ?? (state.execution_block_reasons.join(", ") || "A safety gate is active.")}
        {stale ? " (stale — refreshing)" : ""}
      </div>
    </div>
  );
}

export function StrategiesPage() {
  const [cursor, setCursor] = useState<number | null>(null);
  const [cursorHistory, setCursorHistory] = useState<Array<number | null>>([]);
  const [cachedEntryBlock, setCachedEntryBlock] = useState<StrategyEntryBlock | null>(null);
  const overview = useAsync(fetchStrategyOverview, []);
  // useAsync captures fn via a ref — a fresh arrow per render is safe.
  const signals = useAsync(() => fetchFiredSignals(cursor), [cursor]);

  useEffect(() => {
    if (overview.data !== null) setCachedEntryBlock(overview.data.entry_block);
  }, [overview.data]);
  const displayedEntryBlock = overview.data?.entry_block ?? cachedEntryBlock;

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Strategies</h1>
        <p className="mt-1 max-w-4xl text-sm text-slate-600 dark:text-slate-400">
          Monitor every fired signal, compare the simultaneous shadow record with funded trades, and set audited
          paper-capital ceilings. Manual positions remain in the account portfolio and never enter strategy P&amp;L.
        </p>
      </header>

      {displayedEntryBlock ? (
        <EntryBlockBanner state={displayedEntryBlock} stale={overview.data === null} />
      ) : null}

      <Section title="Strategy picker" action="Exact current versions">
        {overview.loading ? <SectionSkeleton rows={8} /> : null}
        {overview.error ? <SectionError onRetry={overview.refetch} /> : null}
        {overview.data?.strategies.length === 0 ? (
          <EmptyState title="No registered strategies" description="Add a strategy to the manifest before evidence can be monitored." />
        ) : null}
        {overview.data && overview.data.strategies.length > 0 ? (
          <div className="space-y-6">
            {overview.data.strategies.map((strategy) => (
              <StrategyPanel key={strategy.strategy_id} strategy={strategy} onUpdated={overview.refetch} />
            ))}
          </div>
        ) : null}
      </Section>

      <Section title="Fired signals" action="Funded, rejected and shadow outcomes">
        {signals.loading ? <SectionSkeleton rows={6} /> : null}
        {signals.error ? <SectionError onRetry={signals.refetch} /> : null}
        {signals.data?.items.length === 0 ? (
          <EmptyState title="No fired signals" description="The current strategy versions have not fired yet." />
        ) : null}
        {signals.data && signals.data.items.length > 0 ? (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-slate-500 dark:text-slate-400">
                  <tr>
                    <th className="px-2 py-2 font-medium">Signal</th>
                    <th className="px-2 py-2 font-medium">Instrument</th>
                    <th className="px-2 py-2 text-right font-medium">Expected fill</th>
                    <th className="px-2 py-2 text-right font-medium">Actual fill</th>
                    <th className="px-2 py-2 font-medium">Shadow outcome</th>
                    <th className="px-2 py-2 font-medium">Capital decision</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                  {signals.data.items.map((signal) => (
                    <tr key={signal.signal_id}>
                      <td className="px-2 py-2">
                        <div>{signal.strategy_id}</div>
                        <div className="text-xs text-slate-500">{signal.signal_kind} · {formatDate(signal.signal_bar_date)}</div>
                      </td>
                      <td className="px-2 py-2">
                        <div className="font-medium">{signal.symbol}</div>
                        <div className="text-xs text-slate-500">{signal.company_name ?? `#${signal.instrument_id}`}</div>
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums">
                        <div>{formatNumber(decimal(signal.fill_price), 4)}</div>
                        <div className="text-xs text-slate-500">{formatDate(signal.fill_bar_date)}</div>
                      </td>
                      <td className="px-2 py-2 text-right tabular-nums">
                        <div>{formatNumber(decimal(signal.actual_fill_price), 4)}</div>
                        <div className="text-xs text-slate-500">slippage {pctPoints(signal.slippage_pct)}</div>
                      </td>
                      <td className="px-2 py-2">
                        <div>{shadowOutcome(signal.outcome)}</div>
                        <div className="text-xs tabular-nums text-slate-500">{pctPoints(signal.gross_return_pct)}</div>
                      </td>
                      <td className="px-2 py-2">
                        <Badge tone={signal.funding_status === "funded" ? "ok" : signal.funding_status === "rejected" ? "risk" : "neutral"}>
                          {signal.funding_status === "funded" ? "Funded" : signal.funding_status === "rejected" ? "Not funded" : "No capital needed"}
                        </Badge>
                        <div className="mt-1 text-xs text-slate-500">{humanizeCode(signal.funding_reason)}</div>
                      </td>
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
                className="min-h-11 cursor-pointer border border-slate-300 px-3 py-2 text-xs transition-colors duration-200 hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800 dark:focus:ring-blue-900"
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
                className="min-h-11 cursor-pointer border border-slate-300 px-3 py-2 text-xs transition-colors duration-200 hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-blue-300 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800 dark:focus:ring-blue-900"
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
