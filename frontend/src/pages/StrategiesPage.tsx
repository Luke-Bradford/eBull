import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import {
  fetchStrategyOverview,
  fetchStrategyPnlHistory,
  updateStrategyAllocation,
  updateStrategyPaperPool,
} from "@/api/strategies";
import type {
  StrategyEvidenceWindow,
  StrategyOverview,
  StrategyOverviewResponse,
  StrategyResultArm,
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

function money(value: string | null): string {
  return formatMoney(number(value), "USD");
}

function pctPoints(value: string | null): string {
  const parsed = number(value);
  return formatPct(parsed === null ? null : parsed / 100);
}

function primaryEvidence(strategy: StrategyOverview): StrategyEvidenceWindow | null {
  return strategy.evidence_windows.find((window) => window.window_id === "primary")
    ?? strategy.evidence_windows.find((window) => window.status === "complete")
    ?? null;
}

function representativeArm(strategy: StrategyOverview): StrategyResultArm | null {
  return primaryEvidence(strategy)?.arms.find(
    (arm) => arm.ambiguity_arm === "worst_case" && arm.quarantine_arm === "masked",
  ) ?? null;
}

function completedEvidenceCount(strategy: StrategyOverview): number {
  return strategy.evidence_windows.filter((window) => window.status === "complete").length;
}

function validationState(strategy: StrategyOverview): {
  label: string;
  tone: "ok" | "warn" | "risk" | "neutral";
  explanation: string;
} {
  if (strategy.allocation_ready) {
    return { label: "Approved", tone: "ok", explanation: "All automation gates have passed." };
  }
  const arm = representativeArm(strategy);
  if (!strategy.runnable || !arm) {
    return {
      label: "Not tested",
      tone: "neutral",
      explanation: !strategy.runnable
        ? "The rule cannot yet run end to end."
        : "No completed valid backtest is available.",
    };
  }
  const expectancy = number(arm.expectancy_per_trade_pct);
  const ciHigh = number(arm.expectancy_ci_high_pct);
  if ((expectancy !== null && expectancy <= 0) || (ciHigh !== null && ciHigh <= 0)) {
    return {
      label: "Rejected",
      tone: "risk",
      explanation: "The current rule does not show positive expectancy after costs.",
    };
  }
  const ciLow = number(arm.expectancy_ci_low_pct);
  if (ciLow === null || ciLow <= 0) {
    return {
      label: "Not proven",
      tone: "warn",
      explanation: "The estimate is positive, but its confidence range still includes a loss.",
    };
  }
  return {
    label: "Checks incomplete",
    tone: "warn",
    explanation: "The return evidence is positive, but required validation checks remain.",
  };
}

const REFUSAL_LABELS: Record<string, string> = {
  strategy_not_runnable: "Rule is not runnable end to end",
  recent_evidence_incomplete: "Recent evidence windows are incomplete",
  recent_evidence_gate_refused: "Recent evidence failed its promotion gate",
  recent_net_expectancy_not_positive: "Net expectancy is not positive",
  paper_promotion_missing: "No approved deployment exists",
  pinned_promotion_evidence_invalid: "Pinned evidence is no longer valid",
  execution_policy_missing: "Execution and risk policy is missing",
  universe_basis_not_survivorship_free: "Point-in-time universe is not complete",
  carry_unmodelled: "Holding and financing costs are not modelled",
  synthetic_control_not_run: "Random-entry control has not passed",
};

function refusalLabel(refusal: string): string {
  return REFUSAL_LABELS[refusal] ?? refusal.replaceAll("_", " ");
}

function aggregate(overview: StrategyOverviewResponse) {
  const pnlValues = overview.strategies.map((strategy) => number(strategy.pnl.total_pnl));
  const resolved = overview.strategies.reduce(
    (sum, strategy) => sum + strategy.attribution.resolved_entries,
    0,
  );
  const winners = overview.strategies.reduce(
    (sum, strategy) => sum + strategy.attribution.winning_entries,
    0,
  );
  let weightedReturn = 0;
  let averageReturnKnown = resolved > 0;
  for (const strategy of overview.strategies) {
    if (strategy.attribution.resolved_entries === 0) continue;
    const average = number(strategy.attribution.shadow_average_return_pct);
    if (average === null) {
      averageReturnKnown = false;
      break;
    }
    weightedReturn += average * strategy.attribution.resolved_entries;
  }
  const fired = overview.strategies.reduce(
    (sum, strategy) => sum + strategy.attribution.fired_entries,
    0,
  );
  return {
    totalPnl: pnlValues.every((value) => value !== null)
      ? pnlValues.reduce<number>((sum, value) => sum + (value ?? 0), 0)
      : null,
    resolved,
    winners,
    unsuccessful: Math.max(0, resolved - winners),
    awaitingOutcome: Math.max(0, fired - resolved),
    successRate: resolved > 0 ? winners / resolved : null,
    averageReturn: averageReturnKnown ? weightedReturn / resolved / 100 : null,
    activePositions: overview.strategies.reduce(
      (sum, strategy) => sum + strategy.pnl.active_position_count,
      0,
    ),
    approved: overview.strategies.filter((strategy) => strategy.allocation_ready).length,
  };
}

function Metric({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div>
      <span className="block text-[10px] font-semibold uppercase tracking-wider text-slate-500">
        {label}
      </span>
      <strong className="mt-1 block text-xl tabular-nums text-slate-900 dark:text-slate-100">
        {value}
      </strong>
      {hint ? <span className="mt-0.5 block text-xs text-slate-500">{hint}</span> : null}
    </div>
  );
}

function PnlTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ value?: number }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <ChartTooltip>
      <div className="text-slate-500">{formatDate(label ?? null)}</div>
      <div className="font-semibold">{formatMoney(payload[0]?.value ?? null, "USD")}</div>
    </ChartTooltip>
  );
}

function PnlChart({ history }: { history: Array<{ date: string; total_pnl: string }> }) {
  const theme = useChartTheme();
  const data = history.map((point) => ({ date: point.date, pnl: number(point.total_pnl) }));
  return (
    <div className="mt-6 h-52 border-t border-slate-200 pt-4 dark:border-slate-800">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <XAxis dataKey="date" tickFormatter={(value: string) => formatDate(value)} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} />
          <YAxis tickFormatter={(value: number) => `$${formatNumber(value, 0)}`} tick={{ fontSize: 10, fill: theme.textMuted }} stroke={theme.gridLine} width={52} />
          <Tooltip content={<PnlTooltip />} />
          <Line type="stepAfter" dataKey="pnl" stroke={theme.primaryLine} strokeWidth={2} dot={{ r: 2 }} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function EmptyPnlChart() {
  return (
    <div className="mt-6 flex h-52 items-center justify-center border-t border-slate-200 bg-[linear-gradient(to_bottom,transparent_31px,rgb(226_232_240/0.55)_32px)] bg-[size:100%_32px] pt-4 text-center dark:border-slate-800 dark:bg-[linear-gradient(to_bottom,transparent_31px,rgb(30_41_59/0.45)_32px)]">
      <div className="bg-white/90 px-5 py-3 dark:bg-slate-900/90">
        <p className="text-sm font-medium text-slate-700 dark:text-slate-200">No automated P&amp;L yet</p>
        <p className="mt-1 max-w-sm text-xs text-slate-500">
          The performance line begins when an automated position records a result.
        </p>
      </div>
    </div>
  );
}

function AutomationControl({
  overview,
  approvedCount,
  onUpdated,
}: {
  overview: StrategyOverviewResponse;
  approvedCount: number;
  onUpdated: () => void;
}) {
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
  const canEnable = approvedCount > 0;

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!valid || !dirty || saving || (enabled && !canEnable)) return;
    setSaving(true);
    setFailed(false);
    try {
      await updateStrategyPaperPool({
        enabled,
        capital_limit: parsed.toFixed(6),
        reason: "Automated strategy workspace update",
      });
      onUpdated();
    } catch (error) {
      console.error("Automation update failed:", error);
      setFailed(true);
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-sm font-semibold">Automation</h2>
          <p className="mt-1 text-xs text-slate-500">One capital limit shared by approved strategies.</p>
        </div>
        <Badge tone={enabled ? "ok" : "neutral"}>{enabled ? "On" : "Off"}</Badge>
      </div>
      <form onSubmit={(event) => void save(event)} className="mt-5 space-y-4">
        <label className={`flex min-h-11 items-center gap-2 text-sm font-semibold ${canEnable || enabled ? "cursor-pointer" : "cursor-not-allowed opacity-60"}`}>
          <input
            type="checkbox"
            checked={enabled}
            disabled={(!canEnable && !enabled) || saving}
            onChange={(event) => setEnabled(event.target.checked)}
            className="h-5 w-5"
          />
          Allow new automated entries
        </label>
        <div className="flex flex-wrap items-end gap-2">
          <label className="w-48 text-xs font-medium text-slate-600 dark:text-slate-300">
            Trading capital (USD)
            <input
              type="number"
              min="0"
              step="0.01"
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
              className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm tabular-nums dark:border-slate-700 dark:bg-slate-950"
            />
          </label>
          <button type="submit" disabled={!valid || !dirty || saving || (enabled && !canEnable)} className="min-h-11 cursor-pointer border border-blue-700 bg-blue-700 px-4 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-40">
            {saving ? "Saving…" : "Save"}
          </button>
        </div>
      </form>
      <dl className="mt-5 grid grid-cols-3 gap-3 border-t border-slate-200 pt-4 text-xs dark:border-slate-800">
        <div><dt className="text-slate-500">Working</dt><dd className="font-semibold tabular-nums">{money(pool.invested_capital)}</dd></div>
        <div><dt className="text-slate-500">Reserved</dt><dd className="font-semibold tabular-nums">{money(pool.reserved_capital)}</dd></div>
        <div><dt className="text-slate-500">Available</dt><dd className="font-semibold tabular-nums">{money(pool.remaining_capital)}</dd></div>
      </dl>
      {!canEnable ? (
        <p className="mt-4 text-xs text-amber-700 dark:text-amber-300">
          Automation stays off until at least one strategy passes validation.
        </p>
      ) : null}
      {overview.entry_block.new_entries_blocked ? (
        <p className="mt-4 text-xs text-amber-700 dark:text-amber-300">
          <strong>New entries are paused by a safety control.</strong> Existing automated positions remain managed.
        </p>
      ) : null}
      {failed ? <p className="mt-4 text-xs text-red-700 dark:text-red-300">The automation settings were not changed.</p> : null}
    </section>
  );
}

function SignalValidation({ overview }: { overview: StrategyOverviewResponse }) {
  const summary = aggregate(overview);
  return (
    <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Forward signal validation</h2>
          <p className="mt-1 max-w-2xl text-xs text-slate-500">
            A signal is recorded only after every rule aligns. It then remains an open observation until its exit rule resolves.
          </p>
        </div>
        <Badge tone="info">Completed daily bars</Badge>
      </div>
      <dl className="mt-5 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div><dt className="text-xs text-slate-500">Open observations</dt><dd className="mt-1 text-lg font-semibold tabular-nums">{formatNumber(summary.awaitingOutcome, 0)}</dd></div>
        <div><dt className="text-xs text-slate-500">Completed</dt><dd className="mt-1 text-lg font-semibold tabular-nums">{formatNumber(summary.resolved, 0)}</dd></div>
        <div><dt className="text-xs text-slate-500">Successful</dt><dd className="mt-1 text-lg font-semibold tabular-nums">{formatNumber(summary.winners, 0)}</dd></div>
        <div><dt className="text-xs text-slate-500">Unsuccessful</dt><dd className="mt-1 text-lg font-semibold tabular-nums">{formatNumber(summary.unsuccessful, 0)}</dd></div>
      </dl>
      <p className="mt-4 border-t border-slate-200 pt-4 text-xs text-slate-500 dark:border-slate-800">
        There is no “pending strategy” state and no near-trigger forecast in the current daily evaluator.
      </p>
    </section>
  );
}

function StrategyToggle({
  strategy,
  poolLimit,
  onUpdated,
}: {
  strategy: StrategyOverview;
  poolLimit: string;
  onUpdated: () => void;
}) {
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  async function toggle() {
    if (saving || (!strategy.allocation.enabled && !strategy.allocation_ready)) return;
    setSaving(true);
    setFailed(false);
    const enabled = !strategy.allocation.enabled;
    const current = Number(strategy.allocation.capital_limit);
    const capital = enabled && current <= 0 ? Number(poolLimit) : current;
    try {
      await updateStrategyAllocation(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        capital_limit: Math.max(0, capital).toFixed(6),
        enabled,
        reason: `${enabled ? "Enabled" : "Paused"} from automated strategy workspace`,
      });
      onUpdated();
    } catch (error) {
      console.error("Strategy toggle failed:", error);
      setFailed(true);
    } finally {
      setSaving(false);
    }
  }
  return (
    <div>
      <label className="flex min-h-11 cursor-pointer items-center justify-end gap-2 text-xs font-medium">
        <input type="checkbox" checked={strategy.allocation.enabled} disabled={saving} onChange={() => void toggle()} className="h-5 w-5" />
        {strategy.allocation.enabled ? "Enabled" : "Paused"}
      </label>
      {failed ? <p className="text-xs text-red-700 dark:text-red-300">Not changed</p> : null}
    </div>
  );
}

function ApprovedStrategy({
  strategy,
  poolLimit,
  onUpdated,
}: {
  strategy: StrategyOverview;
  poolLimit: string;
  onUpdated: () => void;
}) {
  return (
    <article className="grid gap-4 border-t border-slate-200 py-4 dark:border-slate-800 lg:grid-cols-[minmax(16rem,1.4fr)_repeat(4,minmax(6rem,0.65fr))_auto] lg:items-center">
      <div>
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold">{strategy.title}</h3>
          <Badge tone={strategy.allocation_ready ? "ok" : "warn"}>
            {strategy.allocation_ready ? "Approved" : "Managing existing position"}
          </Badge>
        </div>
        <p className="mt-1 max-w-md text-xs text-slate-500">{strategy.description}</p>
      </div>
      <div><span className="text-xs text-slate-500">P&amp;L</span><strong className="block tabular-nums">{money(strategy.pnl.total_pnl)}</strong></div>
      <div><span className="text-xs text-slate-500">Completed</span><strong className="block tabular-nums">{formatNumber(strategy.attribution.resolved_entries, 0)}</strong></div>
      <div><span className="text-xs text-slate-500">Success</span><strong className="block tabular-nums">{formatPct(number(strategy.attribution.win_rate))}</strong></div>
      <div><span className="text-xs text-slate-500">Average / trade</span><strong className="block tabular-nums">{pctPoints(strategy.attribution.shadow_average_return_pct)}</strong></div>
      <StrategyToggle strategy={strategy} poolLimit={poolLimit} onUpdated={onUpdated} />
    </article>
  );
}

function EvidenceDetail({ strategy }: { strategy: StrategyOverview }) {
  const window = primaryEvidence(strategy);
  const arm = representativeArm(strategy);
  const completed = completedEvidenceCount(strategy);
  const failures = strategy.allocation_refusals.map(refusalLabel);
  const failedOutcomes = Math.max(0, strategy.attribution.resolved_entries - strategy.attribution.winning_entries);
  if (!window || !arm) {
    return (
      <div className="border-t border-slate-200 px-4 py-4 text-sm text-slate-500 dark:border-slate-800">
        No completed valid evidence is available. {failures[0] ?? "The research run has not finished."}
      </div>
    );
  }
  const resolved = Math.max(0, arm.trade_count - arm.open_trade_count - arm.unpriced_trade_count);
  return (
    <div className="border-t border-slate-200 px-4 py-4 dark:border-slate-800">
      <div className="grid gap-5 lg:grid-cols-[1.3fr_1fr]">
        <div>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500">Primary evidence</h4>
          <p className="mt-1 text-xs text-slate-500">{window.label} · {formatDate(window.window_start)}–{formatDate(window.window_end)} · pessimistic execution arm</p>
          <dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <div><dt className="text-xs text-slate-500">Trades</dt><dd className="font-semibold tabular-nums">{formatNumber(resolved, 0)}</dd></div>
            <div><dt className="text-xs text-slate-500">Profit factor</dt><dd className="font-semibold tabular-nums">{formatNumber(number(arm.profit_factor), 2)}</dd></div>
            <div><dt className="text-xs text-slate-500">Vs buy &amp; hold</dt><dd className="font-semibold tabular-nums">{pctPoints(arm.return_vs_buy_and_hold_pct)}</dd></div>
            <div><dt className="text-xs text-slate-500">Deflated Sharpe</dt><dd className="font-semibold tabular-nums">{formatNumber(number(arm.deflated_sharpe), 2)}</dd></div>
          </dl>
        </div>
        <div>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500">Why it cannot use capital</h4>
          <ul className="mt-2 space-y-1 text-xs text-slate-600 dark:text-slate-300">
            {(failures.length ? failures : ["No remaining blockers"]).slice(0, 4).map((failure) => <li key={failure}>• {failure}</li>)}
          </ul>
          {failures.length > 4 ? <p className="mt-1 text-xs text-slate-500">+ {failures.length - 4} more checks</p> : null}
        </div>
      </div>
      <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2 border-t border-slate-200 pt-3 text-xs text-slate-500 dark:border-slate-800">
        <span>Evidence windows complete: <strong className="text-slate-700 dark:text-slate-200">{completed}/{strategy.evidence_windows.length}</strong></span>
        <span>Forward observations: <strong className="text-slate-700 dark:text-slate-200">{strategy.attribution.fired_entries}</strong></span>
        <span>Completed outcomes: <strong className="text-slate-700 dark:text-slate-200">{strategy.attribution.resolved_entries}</strong></span>
        <span>Successful / unsuccessful: <strong className="text-slate-700 dark:text-slate-200">{strategy.attribution.winning_entries} / {failedOutcomes}</strong></span>
      </div>
    </div>
  );
}

function ResearchCandidate({ strategy }: { strategy: StrategyOverview }) {
  const [expanded, setExpanded] = useState(false);
  const arm = representativeArm(strategy);
  const validation = validationState(strategy);
  const ci = arm && arm.expectancy_ci_low_pct !== null && arm.expectancy_ci_high_pct !== null
    ? `${pctPoints(arm.expectancy_ci_low_pct)} to ${pctPoints(arm.expectancy_ci_high_pct)}`
    : "—";
  return (
    <article className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="grid gap-4 p-4 lg:grid-cols-[minmax(16rem,1.5fr)_repeat(3,minmax(7rem,0.7fr))_auto] lg:items-center">
        <div>
          <div className="flex flex-wrap items-center gap-2"><h3 className="text-sm font-semibold">{strategy.title}</h3><Badge tone={validation.tone}>{validation.label}</Badge></div>
          <p className="mt-1 max-w-lg text-xs text-slate-500">{validation.explanation}</p>
        </div>
        <div><span className="text-xs text-slate-500">Expected / trade</span><strong className="block tabular-nums">{pctPoints(arm?.expectancy_per_trade_pct ?? null)}</strong><span className="text-[10px] text-slate-500">After modelled costs</span></div>
        <div><span className="text-xs text-slate-500">95% range</span><strong className="block text-xs tabular-nums">{ci}</strong><span className="text-[10px] text-slate-500">Must clear 0%</span></div>
        <div><span className="text-xs text-slate-500">Worst drawdown</span><strong className="block tabular-nums">{pctPoints(arm?.max_drawdown_pct ?? null)}</strong><span className="text-[10px] text-slate-500">Backtest</span></div>
        <button type="button" aria-expanded={expanded} onClick={() => setExpanded((value) => !value)} className="min-h-11 cursor-pointer border border-slate-300 px-3 text-xs hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800">
          {expanded ? "Hide evidence" : "View evidence"}
        </button>
      </div>
      {expanded ? <EvidenceDetail strategy={strategy} /> : null}
    </article>
  );
}

export function StrategiesPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const summary = useMemo(() => overview.data ? aggregate(overview.data) : null, [overview.data]);
  const approvedStrategies = overview.data?.strategies.filter((strategy) => strategy.allocation_ready || strategy.allocation.enabled) ?? [];
  const researchCandidates = overview.data?.strategies.filter((strategy) => !strategy.allocation_ready && !strategy.allocation.enabled) ?? [];

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-xl font-semibold">Automated strategies</h1>
        <p className="mt-1 max-w-3xl text-sm text-slate-600 dark:text-slate-400">
          Monitor the automated pot and control only strategies that have passed validation.
        </p>
      </header>
      {overview.loading ? (
        <SectionSkeleton rows={7} />
      ) : overview.error ? (
        <SectionError onRetry={overview.refetch} />
      ) : overview.data && summary ? (
        <>
          {summary.approved === 0 ? (
            <div className="flex flex-wrap items-center justify-between gap-2 border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200">
              <span><strong>No strategies are approved for automation.</strong> Research candidates cannot use capital.</span>
              <span className="text-xs">0 of {overview.data.strategies.length} ready</span>
            </div>
          ) : null}
          {!overview.data.demo_connection && !overview.data.live_strategy_activation_available ? (
            <div className="border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200">
              <strong>Real-money strategy activation is unavailable.</strong>
            </div>
          ) : null}

          <div className="grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(19rem,1fr)]">
            <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <h2 className="text-sm font-semibold">Portfolio performance</h2>
                  <p className="mt-1 text-xs text-slate-500">Automated positions only; research backtests are excluded.</p>
                </div>
                <Badge tone="neutral">{overview.data.demo_connection ? "Demo account" : "Connected account"}</Badge>
              </div>
              <div className="mt-5 grid grid-cols-2 gap-5 sm:grid-cols-4">
                <Metric label="Total P&L" value={formatMoney(summary.totalPnl, "USD")} hint="Realised + open" />
                <Metric label="Average / trade" value={formatPct(summary.averageReturn)} hint="Completed outcomes" />
                <Metric label="Success rate" value={formatPct(summary.successRate)} hint={`${formatNumber(summary.resolved, 0)} completed`} />
                <Metric label="Open positions" value={formatNumber(summary.activePositions, 0)} />
              </div>
              {pnlHistory.loading ? (
                <div className="mt-6 flex h-52 items-center justify-center border-t border-slate-200 text-xs text-slate-500 dark:border-slate-800">Loading P&amp;L history…</div>
              ) : pnlHistory.error ? (
                <div className="mt-6 border-t border-slate-200 pt-4 dark:border-slate-800"><SectionError onRetry={pnlHistory.refetch} /></div>
              ) : pnlHistory.data?.points.length ? (
                <PnlChart history={pnlHistory.data.points} />
              ) : (
                <EmptyPnlChart />
              )}
            </section>
            <AutomationControl overview={overview.data} approvedCount={summary.approved} onUpdated={overview.refetch} />
          </div>

          <SignalValidation overview={overview.data} />

          <section>
            <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
              <div>
                <h2 className="text-sm font-semibold">Approved &amp; managed strategies</h2>
                <p className="mt-1 text-xs text-slate-500">Approved strategies may use the shared pot; an invalidated strategy remains visible only while it manages an existing position.</p>
              </div>
              <span className="text-xs text-slate-500">{summary.approved} approved</span>
            </div>
            {approvedStrategies.length ? (
              <div>
                {approvedStrategies.map((strategy) => (
                  <ApprovedStrategy key={strategy.strategy_id} strategy={strategy} poolLimit={overview.data?.paper_pool.capital_limit ?? "0"} onUpdated={overview.refetch} />
                ))}
              </div>
            ) : (
              <EmptyState title="Nothing can trade yet" description="Candidates move here only after recent evidence, risk, cost and execution checks all pass." />
            )}
          </section>

          <section>
            <div className="mb-3 flex flex-wrap items-end justify-between gap-2 border-t border-slate-200 pt-5 dark:border-slate-800">
              <div>
                <h2 className="text-sm font-semibold">Research pipeline</h2>
                <p className="mt-1 max-w-3xl text-xs text-slate-500">
                  These rules are measured, not selectable. Current evaluation uses completed daily bars; it does not predict which rule is about to fire.
                </p>
              </div>
              <span className="text-xs text-slate-500">{researchCandidates.length} candidates</span>
            </div>
            <div className="space-y-2">
              {researchCandidates.map((strategy) => <ResearchCandidate key={strategy.strategy_id} strategy={strategy} />)}
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}
