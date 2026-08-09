import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import {
  fetchFiredSignals,
  fetchStrategyOverview,
  fetchStrategyPnlHistory,
  updateStrategyAllocation,
  updateStrategyPaperPool,
} from "@/api/strategies";
import type {
  FiredSignal,
  StrategyEvidenceWindow,
  StrategyOverview,
  StrategyOverviewResponse,
} from "@/api/types";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
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

function money(value: string | null, currency = "USD"): string {
  return formatMoney(number(value), currency);
}

function points(value: string | null): string {
  const parsed = number(value);
  return formatPct(parsed === null ? null : parsed / 100);
}

function fraction(value: string | null): string {
  return formatPct(number(value));
}

function aggregate(overview: StrategyOverviewResponse) {
  const totalPnlValues = overview.strategies.map((strategy) => number(strategy.pnl.total_pnl));
  const totalPnl = totalPnlValues.every((value) => value !== null)
    ? totalPnlValues.reduce<number>((sum, value) => sum + (value ?? 0), 0)
    : null;
  const resolved = overview.strategies.reduce((sum, strategy) => sum + strategy.attribution.resolved_entries, 0);
  const winners = overview.strategies.reduce((sum, strategy) => sum + strategy.attribution.winning_entries, 0);
  return {
    totalPnl,
    winRate: resolved > 0 ? winners / resolved : null,
    activePositions: overview.strategies.reduce((sum, strategy) => sum + strategy.pnl.active_position_count, 0),
    enabledStrategies: overview.strategies.filter((strategy) => strategy.allocation.enabled).length,
  };
}

function PnlTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ value?: number }>; label?: string }) {
  if (!active || !payload?.length) return null;
  return (
    <ChartTooltip>
      <div className="text-slate-500">{formatDate(label ?? null)}</div>
      <div className="font-semibold text-slate-800 dark:text-slate-100">{formatMoney(payload[0]?.value ?? null, "USD")}</div>
    </ChartTooltip>
  );
}

function PnlChart({ points: history }: { points: Array<{ date: string; total_pnl: string }> }) {
  const theme = useChartTheme();
  const data = history.map((point) => ({ date: point.date, pnl: number(point.total_pnl) }));
  if (data.length === 0) {
    return <div className="flex h-56 items-center justify-center text-sm text-slate-500">The P&amp;L line begins when the first automated position closes.</div>;
  }
  return (
    <div className="h-56 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 12, right: 12, bottom: 0, left: 0 }}>
          <XAxis dataKey="date" tickFormatter={(value: string) => formatDate(value)} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} />
          <YAxis tickFormatter={(value: number) => `$${formatNumber(value, 0)}`} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} width={54} />
          <Tooltip content={<PnlTooltip />} cursor={{ stroke: theme.crosshair }} />
          <Line type="stepAfter" dataKey="pnl" stroke={theme.primaryLine} strokeWidth={2} dot={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function PoolControl({ overview, onUpdated }: { overview: StrategyOverviewResponse; onUpdated: () => void }) {
  const pool = overview.paper_pool;
  const [enabled, setEnabled] = useState(pool.enabled);
  const [limit, setLimit] = useState(pool.capital_limit);
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    setEnabled(pool.enabled);
    setLimit(pool.capital_limit);
  }, [pool.enabled, pool.capital_limit]);
  const parsed = Number(limit);
  const valid = Number.isFinite(parsed) && parsed >= 0 && (!enabled || parsed > 0);
  const dirty = enabled !== pool.enabled || parsed !== Number(pool.capital_limit);

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!valid || saving) return;
    setSaving(true);
    setFailed(false);
    try {
      await updateStrategyPaperPool({
        enabled,
        capital_limit: parsed.toFixed(6),
        reason: "Strategy workspace paper-pool update",
      });
      onUpdated();
    } catch (error) {
      console.error("Paper pool update failed:", error);
      setFailed(true);
    } finally {
      setSaving(false);
    }
  }

  return (
    <form onSubmit={(event) => void save(event)} className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="text-sm font-semibold text-slate-800 dark:text-slate-100">Automatic paper trading</div>
          <p className="mt-1 max-w-xl text-xs text-slate-500">One shared ceiling across the strategies you switch on. New trades stop when the pot is committed; owned positions continue to be protected.</p>
        </div>
        <label className="flex min-h-11 cursor-pointer items-center gap-2 text-sm font-medium text-slate-700 dark:text-slate-200">
          <input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} className="h-5 w-5" />
          {enabled ? "Running" : "Paused"}
        </label>
      </div>
      <div className="mt-4 grid gap-3 sm:grid-cols-[minmax(12rem,18rem)_auto_1fr] sm:items-end">
        <label className="text-xs font-medium text-slate-600 dark:text-slate-300">
          Shared paper capital (USD)
          <input type="number" min="0" step="0.01" value={limit} onChange={(event) => setLimit(event.target.value)} className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm tabular-nums dark:border-slate-700 dark:bg-slate-950" />
        </label>
        <button type="submit" disabled={!valid || !dirty || saving} className="min-h-11 border border-blue-700 bg-blue-700 px-4 py-2 text-sm font-medium text-white disabled:opacity-40">
          {saving ? "Saving…" : "Save"}
        </button>
        <div className="grid grid-cols-3 gap-3 text-xs text-slate-500">
          <div><span className="block">Working</span><strong className="text-sm text-slate-800 dark:text-slate-100">{money(pool.invested_capital)}</strong></div>
          <div><span className="block">Reserved</span><strong className="text-sm text-slate-800 dark:text-slate-100">{money(pool.reserved_capital)}</strong></div>
          <div><span className="block">Available</span><strong className="text-sm text-slate-800 dark:text-slate-100">{money(pool.remaining_capital)}</strong></div>
        </div>
      </div>
      {!overview.execution_enabled ? <p className="mt-3 text-xs text-amber-700 dark:text-amber-300">System-wide automatic trading is off, so this paper pot will remain paused until that safety control is enabled.</p> : null}
      {failed ? <p className="mt-2 text-xs text-red-700 dark:text-red-300">The paper pot was not changed. Refresh and try again.</p> : null}
    </form>
  );
}

function StrategyRow({ strategy, poolLimit, onUpdated, onOpen }: { strategy: StrategyOverview; poolLimit: string; onUpdated: () => void; onOpen: () => void }) {
  const [saving, setSaving] = useState(false);
  const canToggle = strategy.allocation.deployment_id !== null || strategy.allocation_ready;
  async function toggle() {
    if (!canToggle || saving) return;
    setSaving(true);
    const enabled = !strategy.allocation.enabled;
    const current = Number(strategy.allocation.capital_limit);
    const shared = Number(poolLimit);
    const capitalLimit = enabled && current <= 0 ? Math.max(shared, 0) : current;
    try {
      await updateStrategyAllocation(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        capital_limit: capitalLimit.toFixed(6),
        enabled,
        reason: `${enabled ? "Enabled" : "Paused"} from strategy workspace`,
      });
      onUpdated();
    } catch (error) {
      console.error("Strategy toggle failed:", error);
    } finally {
      setSaving(false);
    }
  }
  return (
    <article className="grid gap-4 border-t border-slate-200 py-4 dark:border-slate-800 lg:grid-cols-[minmax(15rem,1.5fr)_repeat(5,minmax(6rem,0.7fr))_auto] lg:items-center">
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-sm font-semibold text-slate-800 dark:text-slate-100">{strategy.title}</h2>
          <Badge tone={strategy.allocation_ready ? "ok" : "neutral"}>{strategy.allocation_ready ? "Ready" : "Learning"}</Badge>
        </div>
        <div className="mt-1 text-xs text-slate-500">{strategy.scan.status === "current" ? "Up to date" : "Awaiting a current scan"} · {strategy.pnl.active_position_count} open</div>
      </div>
      <div><div className="text-xs text-slate-500">P&amp;L</div><div className="mt-1 font-semibold tabular-nums">{money(strategy.pnl.total_pnl)}</div></div>
      <div><div className="text-xs text-slate-500">Win rate</div><div className="mt-1 font-semibold tabular-nums">{fraction(strategy.attribution.win_rate)}</div></div>
      <div><div className="text-xs text-slate-500">Average return</div><div className="mt-1 font-semibold tabular-nums">{points(strategy.attribution.shadow_average_return_pct)}</div></div>
      <div><div className="text-xs text-slate-500">Typical result</div><div className="mt-1 font-semibold tabular-nums">{strategy.attribution.median_days_to_outcome === null ? "—" : `${formatNumber(number(strategy.attribution.median_days_to_outcome), 0)} days`}</div></div>
      <div><div className="text-xs text-slate-500">Last 30 days</div><div className="mt-1 font-semibold tabular-nums">{strategy.attribution.signals_last_30_days} signals</div></div>
      <div className="flex items-center justify-end gap-3">
        <label className={`flex min-h-11 items-center gap-2 text-xs font-medium ${canToggle ? "cursor-pointer" : "cursor-not-allowed opacity-50"}`}>
          <input type="checkbox" checked={strategy.allocation.enabled} disabled={!canToggle || saving} onChange={() => void toggle()} className="h-5 w-5" />
          {strategy.allocation.enabled ? "On" : "Off"}
        </label>
        <button type="button" onClick={onOpen} className="min-h-11 border border-slate-300 px-3 py-2 text-xs font-medium dark:border-slate-700">Details</button>
      </div>
    </article>
  );
}

function EvidencePage({ window }: { window: StrategyEvidenceWindow }) {
  const arm = window.arms.find((item) => item.ambiguity_arm === "worst_case" && item.quarantine_arm === "masked");
  return (
    <div className="grid grid-cols-2 gap-3 border border-slate-200 p-3 text-sm dark:border-slate-800">
      <div className="col-span-2"><div className="font-medium">{window.label}</div><div className="text-xs text-slate-500">{formatDate(window.window_start)}–{formatDate(window.window_end)}</div></div>
      <div><span className="text-xs text-slate-500">Trades</span><strong className="block">{arm?.trade_count ?? "—"}</strong></div>
      <div><span className="text-xs text-slate-500">Per trade</span><strong className="block">{points(arm?.expectancy_per_trade_pct ?? null)}</strong></div>
      <div><span className="text-xs text-slate-500">Worst dip</span><strong className="block">{points(arm?.max_drawdown_pct ?? null)}</strong></div>
      <div><span className="text-xs text-slate-500">Evidence</span><strong className="block">{window.status}</strong></div>
    </div>
  );
}

function SignalLedger({ strategyId }: { strategyId: string }) {
  const [cursor, setCursor] = useState<number | null>(null);
  const [history, setHistory] = useState<Array<number | null>>([]);
  const signals = useAsync(() => fetchFiredSignals(cursor, strategyId), [cursor, strategyId]);
  if (signals.loading) return <SectionSkeleton rows={4} />;
  if (signals.error) return <SectionError onRetry={signals.refetch} />;
  if (!signals.data?.items.length) return <p className="text-sm text-slate-500">No fired signals yet.</p>;
  return (
    <div>
      <div className="space-y-2">
        {signals.data.items.map((signal: FiredSignal) => (
          <div key={signal.signal_id} className="flex items-center justify-between gap-3 border-t border-slate-200 py-2 text-xs dark:border-slate-800">
            <div><strong className="text-slate-800 dark:text-slate-100">{signal.symbol}</strong><div className="text-slate-500">{formatDate(signal.signal_bar_date)}</div></div>
            <div className="text-right"><div>{signal.outcome ?? "Waiting"}</div><div className="text-slate-500">{points(signal.gross_return_pct)}</div></div>
          </div>
        ))}
      </div>
      <div className="mt-3 flex gap-2">
        <button type="button" disabled={history.length === 0} onClick={() => { const previous = history.at(-1) ?? null; setHistory((items) => items.slice(0, -1)); setCursor(previous); }} className="min-h-11 border border-slate-300 px-3 text-xs disabled:opacity-40 dark:border-slate-700">Newer</button>
        <button type="button" disabled={signals.data.next_cursor === null} onClick={() => { setHistory((items) => [...items, cursor]); setCursor(signals.data?.next_cursor ?? null); }} className="min-h-11 border border-slate-300 px-3 text-xs disabled:opacity-40 dark:border-slate-700">Older</button>
      </div>
    </div>
  );
}

function StrategyDrawer({ strategy, onClose }: { strategy: StrategyOverview; onClose: () => void }) {
  const [page, setPage] = useState(0);
  const windows = strategy.evidence_windows;
  const window = windows[page];
  return (
    <div className="fixed inset-0 z-50 bg-slate-950/50" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose(); }}>
      <aside role="dialog" aria-modal="true" aria-label={`${strategy.title} details`} className="ml-auto h-full w-full max-w-xl overflow-y-auto bg-white p-5 shadow-2xl dark:bg-slate-950">
        <div className="flex items-start justify-between gap-4">
          <div><h2 className="text-lg font-semibold">{strategy.title}</h2><p className="mt-1 text-xs text-slate-500">Performance and evidence; technical mechanics stay out of the workspace.</p></div>
          <button type="button" onClick={onClose} aria-label="Close strategy details" className="min-h-11 min-w-11 border border-slate-300 text-xl dark:border-slate-700">×</button>
        </div>
        <div className="mt-5 grid grid-cols-2 gap-4 sm:grid-cols-4">
          <div><span className="text-xs text-slate-500">P&amp;L</span><strong className="block">{money(strategy.pnl.total_pnl)}</strong></div>
          <div><span className="text-xs text-slate-500">Win rate</span><strong className="block">{fraction(strategy.attribution.win_rate)}</strong></div>
          <div><span className="text-xs text-slate-500">Working</span><strong className="block">{money(strategy.allocation.invested_capital)}</strong></div>
          <div><span className="text-xs text-slate-500">Signals</span><strong className="block">{strategy.attribution.fired_entries}</strong></div>
        </div>
        <section className="mt-7">
          <div className="mb-3 flex items-center justify-between"><h3 className="text-sm font-semibold">Backtest record</h3><span className="text-xs text-slate-500">{windows.length ? `${page + 1} of ${windows.length}` : "No windows"}</span></div>
          {window ? <EvidencePage window={window} /> : <p className="text-sm text-slate-500">No evidence window is available.</p>}
          {windows.length > 1 ? <div className="mt-2 flex gap-2"><button type="button" disabled={page === 0} onClick={() => setPage((value) => value - 1)} className="min-h-11 border border-slate-300 px-3 text-xs disabled:opacity-40 dark:border-slate-700">Previous</button><button type="button" disabled={page >= windows.length - 1} onClick={() => setPage((value) => value + 1)} className="min-h-11 border border-slate-300 px-3 text-xs disabled:opacity-40 dark:border-slate-700">Next</button></div> : null}
        </section>
        <section className="mt-7"><h3 className="mb-3 text-sm font-semibold">Recent signals</h3><SignalLedger strategyId={strategy.strategy_id} /></section>
        <details className="mt-7 text-xs text-slate-500"><summary className="cursor-pointer">Technical audit detail</summary><p className="mt-2">Stage: {strategy.stage ?? "not promoted"}</p><p>Version: {strategy.strategy_version}</p><p>Allocation blockers: {strategy.allocation_refusals.join(", ") || "none"}</p>{strategy.exclusion_reason ? <p>Exclusion: {strategy.exclusion_reason}</p> : null}</details>
      </aside>
    </div>
  );
}

export function StrategiesPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const summary = useMemo(() => overview.data ? aggregate(overview.data) : null, [overview.data]);
  const selected = overview.data?.strategies.find((strategy) => strategy.strategy_id === selectedId) ?? null;

  return (
    <div className="space-y-6">
      <header><h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Automated strategies</h1><p className="mt-1 max-w-3xl text-sm text-slate-600 dark:text-slate-400">See how the automated paper portfolio is working, choose which proven strategies may act, and set one shared capital limit.</p></header>
      {overview.loading ? <SectionSkeleton rows={8} /> : null}
      {overview.error ? <SectionError onRetry={overview.refetch} /> : null}
      {overview.data && summary ? <>
        {overview.data.entry_block.new_entries_blocked ? <div className="border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200"><strong>New entries are paused.</strong> Existing automated positions are still managed. {overview.data.entry_block.execution_block_reasons[0] ?? overview.data.entry_block.global_kill_reason}</div> : null}
        <section className="grid gap-4 lg:grid-cols-[minmax(0,1.8fr)_minmax(18rem,0.8fr)]">
          <div className="border border-slate-200 bg-white p-4 dark:border-slate-800 dark:bg-slate-900">
            <div className="flex flex-wrap items-end justify-between gap-4"><div><div className="text-xs font-medium uppercase tracking-wide text-slate-500">Automated P&amp;L</div><div className="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-white">{formatMoney(summary.totalPnl, "USD")}</div></div><div className="flex gap-6 text-sm"><div><span className="block text-xs text-slate-500">Win rate</span><strong>{formatPct(summary.winRate)}</strong></div><div><span className="block text-xs text-slate-500">Open positions</span><strong>{summary.activePositions}</strong></div><div><span className="block text-xs text-slate-500">Strategies on</span><strong>{summary.enabledStrategies}</strong></div></div></div>
            {pnlHistory.loading ? <SectionSkeleton rows={4} /> : pnlHistory.error ? <SectionError onRetry={pnlHistory.refetch} /> : <PnlChart points={pnlHistory.data?.points ?? []} />}
          </div>
          <PoolControl overview={overview.data} onUpdated={overview.refetch} />
        </section>
        <section className="border border-slate-200 bg-white px-4 dark:border-slate-800 dark:bg-slate-900">
          <div className="flex items-center justify-between py-4"><div><h2 className="text-sm font-semibold">Strategies</h2><p className="mt-1 text-xs text-slate-500">Switch on only the approaches you want competing for the shared pot.</p></div><Badge tone={overview.data.paper_pool.enabled ? "ok" : "neutral"}>{overview.data.paper_pool.enabled ? "Pool running" : "Pool paused"}</Badge></div>
          {overview.data.strategies.length === 0 ? <EmptyState title="No registered strategies" description="Strategies appear here after they are registered." /> : overview.data.strategies.map((strategy) => <StrategyRow key={strategy.strategy_id} strategy={strategy} poolLimit={overview.data?.paper_pool.capital_limit ?? "0"} onUpdated={overview.refetch} onOpen={() => setSelectedId(strategy.strategy_id)} />)}
        </section>
        {!overview.data.live_strategy_activation_available ? <p className="text-xs text-slate-500">Real-money activation remains unavailable until the broker cost and live-order contract passes validation. This workspace controls paper trading only.</p> : null}
      </> : null}
      {selected ? <StrategyDrawer strategy={selected} onClose={() => setSelectedId(null)} /> : null}
    </div>
  );
}
