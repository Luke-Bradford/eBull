import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { fetchFiredSignals, fetchStrategyOverview, fetchStrategyPnlHistory, updateStrategyAllocation, updateStrategyPaperPool } from "@/api/strategies";
import type { FiredSignal, StrategyEvidenceWindow, StrategyOverview, StrategyOverviewResponse, StrategyResultArm } from "@/api/types";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { STAT_ROW_GRID, StatTile } from "@/components/dashboard/StatTile";
import { EmptyState } from "@/components/states/EmptyState";
import { Badge } from "@/components/ui/Badge";
import { formatDate, formatMoney, formatNumber, formatPct } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";
import { useChartTheme } from "@/lib/useChartTheme";

function number(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function money(value: string | null): string {
  return formatMoney(number(value), "USD");
}

function pctFraction(value: number | null): string {
  return formatPct(value);
}

function pctPoints(value: string | null): string {
  const parsed = number(value);
  return formatPct(parsed === null ? null : parsed / 100);
}

function representativeArm(strategy: StrategyOverview): StrategyResultArm | null {
  const windows = [
    ...strategy.evidence_windows.filter((window) => window.status === "complete"),
    ...strategy.evidence_windows.filter((window) => window.status !== "complete"),
  ];
  for (const window of windows) {
    const arm = window.arms.find(
      (candidate) => candidate.ambiguity_arm === "worst_case" && candidate.quarantine_arm === "masked",
    );
    if (arm) return arm;
  }
  return null;
}

function strategyDisplayStats(strategy: StrategyOverview) {
  if (strategy.attribution.resolved_entries > 0) {
    return {
      basis: "Observed",
      successRate: number(strategy.attribution.win_rate),
      averageReturn: strategy.attribution.shadow_average_return_pct,
    };
  }
  const arm = representativeArm(strategy);
  if (!arm) return { basis: "Awaiting backtest", successRate: null, averageReturn: null };
  const resolved = Math.max(0, arm.trade_count - arm.open_trade_count - arm.unpriced_trade_count);
  return {
    basis: "Backtest",
    successRate: resolved > 0 ? Math.max(0, resolved - arm.losing_trade_count) / resolved : null,
    averageReturn: arm.expectancy_per_trade_pct,
  };
}

function aggregate(overview: StrategyOverviewResponse) {
  const pnl = overview.strategies.map((strategy) => number(strategy.pnl.total_pnl));
  const observedCount = overview.strategies.reduce(
    (sum, strategy) => sum + strategy.attribution.resolved_entries,
    0,
  );
  let resolved = observedCount;
  let winners = overview.strategies.reduce(
    (sum, strategy) => sum + strategy.attribution.winning_entries,
    0,
  );
  let weightedReturn: number | null = 0;
  for (const strategy of overview.strategies) {
    if (strategy.attribution.resolved_entries === 0) continue;
    const average = number(strategy.attribution.shadow_average_return_pct);
    if (average === null) {
      weightedReturn = null;
      break;
    }
    weightedReturn += average * strategy.attribution.resolved_entries;
  }
  if (observedCount === 0) {
    weightedReturn = 0;
    for (const strategy of overview.strategies) {
      const arm = representativeArm(strategy);
      if (!arm) continue;
      const count = Math.max(0, arm.trade_count - arm.open_trade_count - arm.unpriced_trade_count);
      resolved += count;
      winners += Math.max(0, count - arm.losing_trade_count);
      weightedReturn += Number(arm.expectancy_per_trade_pct) * count;
    }
  }
  return {
    totalPnl: pnl.every((value) => value !== null) ? pnl.reduce<number>((sum, value) => sum + (value ?? 0), 0) : null,
    successRate: resolved ? winners / resolved : null,
    averageReturn: resolved && weightedReturn !== null ? weightedReturn / resolved / 100 : null,
    activePositions: overview.strategies.reduce((sum, strategy) => sum + strategy.pnl.active_position_count, 0),
    performanceBasis: observedCount > 0 ? "Observed results" : resolved > 0 ? "Backtest evidence" : "No results yet",
  };
}

function PnlTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ value?: number }>; label?: string }) {
  if (!active || !payload?.length) return null;
  return <ChartTooltip><div className="text-slate-500">{formatDate(label ?? null)}</div><div className="font-semibold">{formatMoney(payload[0]?.value ?? null, "USD")}</div></ChartTooltip>;
}

function PnlChart({ history }: { history: Array<{ date: string; total_pnl: string }> }) {
  const theme = useChartTheme();
  const data = history.map((point) => ({ date: point.date, pnl: number(point.total_pnl) }));
  return <div className="mt-5 h-44 border-t border-slate-200 pt-3 dark:border-slate-800"><ResponsiveContainer width="100%" height="100%"><LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}><XAxis dataKey="date" tickFormatter={(value: string) => formatDate(value)} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} /><YAxis tickFormatter={(value: number) => `$${formatNumber(value, 0)}`} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} width={52} /><Tooltip content={<PnlTooltip />} /><Line type="stepAfter" dataKey="pnl" stroke={theme.primaryLine} strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} /></LineChart></ResponsiveContainer></div>;
}

function AutomationControl({ overview, onUpdated }: { overview: StrategyOverviewResponse; onUpdated: () => void }) {
  const pool = overview.paper_pool;
  const [enabled, setEnabled] = useState(pool.enabled && overview.execution_enabled);
  const [limit, setLimit] = useState(pool.capital_limit);
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  useEffect(() => { setEnabled(pool.enabled && overview.execution_enabled); setLimit(pool.capital_limit); }, [pool.enabled, pool.capital_limit, overview.execution_enabled]);
  const parsed = Number(limit);
  const valid = Number.isFinite(parsed) && parsed >= 0 && (!enabled || parsed > 0);
  const dirty = enabled !== (pool.enabled && overview.execution_enabled) || parsed !== Number(pool.capital_limit);
  async function save(event: FormEvent) {
    event.preventDefault();
    if (!valid || !dirty || saving) return;
    setSaving(true); setFailed(false);
    try {
      await updateStrategyPaperPool({ enabled, capital_limit: parsed.toFixed(6), reason: "Automated strategy workspace update" });
      onUpdated();
    } catch (error) { console.error("Automation update failed:", error); setFailed(true); }
    finally { setSaving(false); }
  }
  return <form onSubmit={(event) => void save(event)} className="flex flex-wrap items-end gap-3 border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
    <label className="flex min-h-11 cursor-pointer items-center gap-2 pr-4 text-sm font-semibold"><input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} className="h-5 w-5" />Automation {enabled ? "on" : "off"}</label>
    <label className="min-w-52 flex-1 text-xs font-medium text-slate-600 dark:text-slate-300">Trading capital (USD)<input type="number" min="0" step="0.01" value={limit} onChange={(event) => setLimit(event.target.value)} className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm tabular-nums dark:border-slate-700 dark:bg-slate-950" /></label>
    <button type="submit" disabled={!valid || !dirty || saving} className="min-h-11 border border-blue-700 bg-blue-700 px-4 text-sm font-medium text-white disabled:opacity-40">{saving ? "Saving…" : "Apply"}</button>
    <div className="ml-auto grid grid-cols-3 gap-5 text-xs"><div><span className="block text-slate-500">Working</span><strong>{money(pool.invested_capital)}</strong></div><div><span className="block text-slate-500">Reserved</span><strong>{money(pool.reserved_capital)}</strong></div><div><span className="block text-slate-500">Available</span><strong>{money(pool.remaining_capital)}</strong></div></div>
    {overview.entry_block.new_entries_blocked ? <p className="w-full text-xs text-amber-700 dark:text-amber-300"><strong>New entries are waiting on a safety check.</strong> Existing automated positions remain managed. {overview.entry_block.execution_block_reasons[0] ?? overview.entry_block.global_kill_reason}</p> : null}
    {failed ? <p className="w-full text-xs text-red-700 dark:text-red-300">The automation settings were not changed.</p> : null}
  </form>;
}

function StrategyRow({ strategy, poolLimit, onUpdated, expanded, onExpand }: { strategy: StrategyOverview; poolLimit: string; onUpdated: () => void; expanded: boolean; onExpand: () => void }) {
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  const stats = strategyDisplayStats(strategy);
  const availableCapital = Math.max(Number(strategy.allocation.capital_limit), Number(poolLimit));
  const canToggle = strategy.allocation.enabled || (
    (strategy.allocation.deployment_id !== null || strategy.allocation_ready) && availableCapital > 0
  );
  const timeToOutcome = strategy.attribution.median_days_to_outcome === null
    ? strategy.exit_timing
    : `${formatNumber(number(strategy.attribution.median_days_to_outcome), 1)} market days`;
  const timeBasis = strategy.attribution.median_days_to_outcome === null ? "Rule" : "Observed";
  async function toggle() {
    if (!canToggle || saving) return;
    setSaving(true); setFailed(false);
    const enabled = !strategy.allocation.enabled;
    const current = Number(strategy.allocation.capital_limit);
    const capital = enabled && current <= 0 ? Number(poolLimit) : current;
    try {
      await updateStrategyAllocation(strategy.strategy_id, { strategy_version: strategy.strategy_version, capital_limit: Math.max(0, capital).toFixed(6), enabled, reason: `${enabled ? "Enabled" : "Paused"} from automated strategy workspace` });
      onUpdated();
    } catch (error) { console.error("Strategy toggle failed:", error); setFailed(true); }
    finally { setSaving(false); }
  }
  return <div className="border-t border-slate-200 dark:border-slate-800"><article className="grid gap-4 py-4 lg:grid-cols-[minmax(16rem,1.5fr)_repeat(5,minmax(6rem,0.7fr))_auto] lg:items-center">
    <div><div className="flex items-center gap-2"><h3 className="text-sm font-semibold">{strategy.title}</h3><Badge tone={strategy.allocation_ready ? "ok" : "neutral"}>{strategy.allocation_ready ? "Ready" : "Learning"}</Badge></div><p className="mt-1 max-w-sm text-xs text-slate-500">{strategy.description}</p></div>
    <div><span className="text-xs text-slate-500">P&amp;L</span><strong className="block">{money(strategy.pnl.total_pnl)}</strong></div>
    <div><span className="text-xs text-slate-500">Success</span><strong className="block">{pctFraction(stats.successRate)}</strong><span className="text-[10px] text-slate-500">{stats.basis}</span></div>
    <div><span className="text-xs text-slate-500">Avg / trade</span><strong className="block">{pctPoints(stats.averageReturn)}</strong><span className="text-[10px] text-slate-500">{stats.basis}</span></div>
    <div><span className="text-xs text-slate-500">Time to outcome</span><strong className="block text-xs">{timeToOutcome}</strong><span className="text-[10px] text-slate-500">{timeBasis}</span></div>
    <div><span className="text-xs text-slate-500">Matches / 30d</span><strong className="block">{formatNumber(strategy.attribution.signals_last_30_days, 0)}</strong></div>
    <div className="flex items-center justify-end gap-2"><label title={!canToggle ? "Assign trading capital and complete the strategy checks before enabling." : undefined} className={`flex min-h-11 items-center gap-2 text-xs ${canToggle ? "cursor-pointer" : "cursor-not-allowed opacity-50"}`}><input type="checkbox" checked={strategy.allocation.enabled} disabled={!canToggle || saving} onChange={() => void toggle()} className="h-5 w-5" />{strategy.allocation.enabled ? "On" : "Off"}</label><button type="button" aria-expanded={expanded} onClick={onExpand} className="min-h-11 border border-slate-300 px-3 text-xs dark:border-slate-700">{expanded ? "Close" : "Breakdown"}</button></div>
    {failed ? <p className="text-xs text-red-700 dark:text-red-300 lg:col-span-7">This strategy was not changed.</p> : null}
  </article></div>;
}

function EvidenceCard({ window }: { window: StrategyEvidenceWindow }) {
  const arm = window.arms.find((candidate) => candidate.ambiguity_arm === "worst_case" && candidate.quarantine_arm === "masked");
  if (!arm) return <p className="text-sm text-slate-500">This evidence window has not completed.</p>;
  const resolved = Math.max(0, arm.trade_count - arm.open_trade_count - arm.unpriced_trade_count);
  const success = resolved ? (resolved - arm.losing_trade_count) / resolved : null;
  return (
    <div className={STAT_ROW_GRID}>
      <StatTile
        label="Period"
        value={window.label}
        hint={`${formatDate(window.window_start)}–${formatDate(window.window_end)}`}
        size="md"
      />
      <StatTile label="Trades" value={formatNumber(resolved, 0)} size="md" />
      <StatTile label="Success" value={pctFraction(success)} size="md" />
      <StatTile
        label="Average / trade"
        value={pctPoints(arm.expectancy_per_trade_pct)}
        size="md"
      />
      <StatTile label="Worst dip" value={pctPoints(arm.max_drawdown_pct)} size="md" />
    </div>
  );
}

function StrategyBreakdown({ strategy }: { strategy: StrategyOverview }) {
  const [page, setPage] = useState(0);
  const window = strategy.evidence_windows[page];
  return (
    <section className="mb-4 border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-950/40">
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h4 className="text-sm font-semibold">Historical breakdown</h4>
          <p className="text-xs text-slate-500">
            Evidence only; instruments and signal events live in Activity.
          </p>
        </div>
        <span className="text-xs text-slate-500">
          {strategy.evidence_windows.length
            ? `${page + 1} of ${strategy.evidence_windows.length}`
            : "No windows"}
        </span>
      </div>
      {window ? <EvidenceCard window={window} /> : null}
      <div className="mt-4 flex gap-2">
        <button
          type="button"
          disabled={page === 0}
          onClick={() => setPage((value) => value - 1)}
          className="min-h-11 border border-slate-300 px-3 text-xs hover:bg-slate-100 disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          Previous
        </button>
        <button
          type="button"
          disabled={page >= strategy.evidence_windows.length - 1}
          onClick={() => setPage((value) => value + 1)}
          className="min-h-11 border border-slate-300 px-3 text-xs hover:bg-slate-100 disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          Next
        </button>
      </div>
      <details className="mt-4 text-xs text-slate-500">
        <summary className="cursor-pointer">Audit detail</summary>
        <p className="mt-2">
          Stage: {strategy.stage ?? "not promoted"} · version: {strategy.strategy_version}
        </p>
        <p>Blockers: {strategy.allocation_refusals.join(", ") || "none"}</p>
      </details>
    </section>
  );
}

function ActivityView({ strategies }: { strategies: StrategyOverview[] }) {
  const [strategyId, setStrategyId] = useState(strategies[0]?.strategy_id ?? "");
  const [cursor, setCursor] = useState<number | null>(null);
  const [history, setHistory] = useState<Array<number | null>>([]);
  const signals = useAsync(() => fetchFiredSignals(cursor, strategyId), [cursor, strategyId]);
  function select(value: string) {
    setStrategyId(value);
    setCursor(null);
    setHistory([]);
  }
  function newer() {
    const previous = history.at(-1) ?? null;
    setHistory((items) => items.slice(0, -1));
    setCursor(previous);
  }
  function older() {
    setHistory((items) => [...items, cursor]);
    setCursor(signals.data?.next_cursor ?? null);
  }
  return (
    <section className="border-t border-slate-200 py-4 dark:border-slate-800">
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <label className="text-xs font-medium text-slate-600 dark:text-slate-300">
          Strategy
          <select
            value={strategyId}
            onChange={(event) => select(event.target.value)}
            className="mt-1 block min-h-11 min-w-64 border border-slate-300 bg-white px-3 dark:border-slate-700 dark:bg-slate-950"
          >
            {strategies.map((strategy) => (
              <option key={strategy.strategy_id} value={strategy.strategy_id}>
                {strategy.title}
              </option>
            ))}
          </select>
        </label>
        <p className="text-xs text-slate-500">15 events per page · newest first</p>
      </div>
      {signals.loading ? (
        <SectionSkeleton rows={6} />
      ) : signals.error ? (
        <SectionError onRetry={signals.refetch} />
      ) : !signals.data?.items.length ? (
        <EmptyState
          title="No signal activity"
          description="This strategy has not produced a matching event yet."
        />
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-xs text-slate-500">
                <tr>
                  <th className="px-2 py-2">Instrument</th>
                  <th className="px-2 py-2">Date</th>
                  <th className="px-2 py-2">Outcome</th>
                  <th className="px-2 py-2 text-right">Return</th>
                  <th className="px-2 py-2 text-right">Capital decision</th>
                </tr>
              </thead>
              <tbody>
                {signals.data.items.map((signal: FiredSignal) => (
                  <tr key={signal.signal_id} className="border-t border-slate-200 dark:border-slate-800">
                    <td className="px-2 py-2">
                      <strong>{signal.symbol}</strong>
                      <div className="text-xs text-slate-500">{signal.company_name}</div>
                    </td>
                    <td className="px-2 py-2">{formatDate(signal.signal_bar_date)}</td>
                    <td className="px-2 py-2">{signal.outcome ?? "In progress"}</td>
                    <td className="px-2 py-2 text-right tabular-nums">
                      {pctPoints(signal.gross_return_pct)}
                    </td>
                    <td className="px-2 py-2 text-right">
                      {signal.funding_status === "funded" ? "Used capital" : "Observed only"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="mt-3 flex gap-2">
            <button
              type="button"
              disabled={!history.length}
              onClick={newer}
              className="min-h-11 border border-slate-300 px-3 text-xs hover:bg-slate-100 disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
            >
              Newer
            </button>
            <button
              type="button"
              disabled={signals.data.next_cursor === null}
              onClick={older}
              className="min-h-11 border border-slate-300 px-3 text-xs hover:bg-slate-100 disabled:opacity-40 dark:border-slate-700 dark:hover:bg-slate-800"
            >
              Older
            </button>
          </div>
        </>
      )}
    </section>
  );
}

export function StrategiesPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const [tab, setTab] = useState<"overview" | "activity">("overview");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const summary = useMemo(() => overview.data ? aggregate(overview.data) : null, [overview.data]);
  return (
    <div className="space-y-5">
      <header>
        <h1 className="text-xl font-semibold">Automated strategies</h1>
        <p className="mt-1 max-w-3xl text-sm text-slate-600 dark:text-slate-400">
          Allocate capital once, choose the strategies allowed to act, and monitor how the
          automated portfolio performs.
        </p>
      </header>
      {overview.loading ? (
        <SectionSkeleton rows={7} />
      ) : overview.error ? (
        <SectionError onRetry={overview.refetch} />
      ) : overview.data && summary ? (
        <>
          {!overview.data.demo_connection && !overview.data.live_strategy_activation_available ? (
            <div className="border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200">
              <strong>Real-money strategy activation is unavailable.</strong>
            </div>
          ) : null}
          <nav className="flex border-b border-slate-200 dark:border-slate-800">
            <button
              type="button"
              onClick={() => setTab("overview")}
              className={`min-h-11 border-b-2 px-4 text-sm ${tab === "overview" ? "border-blue-600 font-semibold" : "border-transparent text-slate-500"}`}
            >
              Overview
            </button>
            <button
              type="button"
              onClick={() => setTab("activity")}
              className={`min-h-11 border-b-2 px-4 text-sm ${tab === "activity" ? "border-blue-600 font-semibold" : "border-transparent text-slate-500"}`}
            >
              Activity
            </button>
          </nav>
          {tab === "overview" ? (
            <>
              <section className="py-1">
                <div className={STAT_ROW_GRID}>
                  <StatTile
                    label="Automated P&L"
                    value={formatMoney(summary.totalPnl, "USD")}
                    hint="Closed + current positions"
                  />
                  <StatTile
                    label="Capital assigned"
                    value={money(overview.data.paper_pool.capital_limit)}
                  />
                  <StatTile
                    label="Capital working"
                    value={money(overview.data.paper_pool.invested_capital)}
                  />
                  <StatTile label="Open positions" value={formatNumber(summary.activePositions, 0)} />
                  <StatTile
                    label="Success rate"
                    value={pctFraction(summary.successRate)}
                    hint={summary.performanceBasis}
                    size="md"
                  />
                  <StatTile
                    label="Average / trade"
                    value={pctFraction(summary.averageReturn)}
                    hint={summary.performanceBasis}
                    size="md"
                  />
                </div>
                {pnlHistory.loading ? (
                  <p className="mt-4 text-xs text-slate-500">Loading P&amp;L history…</p>
                ) : pnlHistory.error ? (
                  <SectionError onRetry={pnlHistory.refetch} />
                ) : pnlHistory.data?.points.length ? (
                  <PnlChart history={pnlHistory.data.points} />
                ) : null}
              </section>
              <AutomationControl overview={overview.data} onUpdated={overview.refetch} />
              <section className="border-t border-slate-200 dark:border-slate-800">
                <div className="py-4">
                  <h2 className="text-sm font-semibold">Strategies</h2>
                  <p className="mt-1 text-xs text-slate-500">
                    Backtest figures fill the early record; observed results replace them as
                    automated outcomes resolve.
                  </p>
                </div>
                {overview.data.strategies.length ? (
                  overview.data.strategies.map((strategy) => (
                    <div key={strategy.strategy_id}>
                      <StrategyRow
                        strategy={strategy}
                        poolLimit={overview.data?.paper_pool.capital_limit ?? "0"}
                        onUpdated={overview.refetch}
                        expanded={expandedId === strategy.strategy_id}
                        onExpand={() =>
                          setExpandedId((current) =>
                            current === strategy.strategy_id ? null : strategy.strategy_id,
                          )
                        }
                      />
                      {expandedId === strategy.strategy_id ? (
                        <StrategyBreakdown strategy={strategy} />
                      ) : null}
                    </div>
                  ))
                ) : (
                  <EmptyState
                    title="No registered strategies"
                    description="Strategies appear here after they are registered."
                  />
                )}
              </section>
            </>
          ) : overview.data.strategies.length ? (
            <ActivityView strategies={overview.data.strategies} />
          ) : (
            <EmptyState
              title="No strategy activity"
              description="Register a strategy before viewing its signal activity."
            />
          )}
        </>
      ) : null}
    </div>
  );
}
