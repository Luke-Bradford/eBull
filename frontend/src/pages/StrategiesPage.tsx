import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import {
  closeStrategyOwnedPosition,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
  requestStrategyEvidenceRefresh,
  updateStrategyAllocation,
  updateStrategyPaperPool,
  updateStrategySizing,
} from "@/api/strategies";
import type {
  StrategyEvidenceWindow,
  StrategyOverview,
  StrategyOverviewResponse,
  StrategyOwnedPosition,
  StrategyResultArm,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { LiveQuoteProvider, useLiveTick } from "@/components/quotes/LiveQuoteProvider";
import { EmptyState } from "@/components/states/EmptyState";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatDate, formatMoney, formatNumber, formatPct } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";
import { useChartTheme } from "@/lib/useChartTheme";
import { liveTickPriceIn } from "@/lib/useLiveQuote";

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

/** The declared id is `primary-2022-plus`, not `primary` (#2624).
 *
 * `app/services/strategy_recent_evidence.py` declares exactly eight ids and
 * `primary` is not among them, so this lookup used to be dead and the function
 * always fell through to "first window with status complete". That is
 * order-dependent: whenever the primary window is `partial` while a calendar-year
 * window is `complete`, the headline "Expected / trade" silently described 2022
 * alone under a label that said primary. */
const PRIMARY_WINDOW_ID = "primary-2022-plus";

function primaryEvidence(strategy: StrategyOverview): StrategyEvidenceWindow | null {
  return strategy.evidence_windows.find((window) => window.window_id === PRIMARY_WINDOW_ID)
    ?? strategy.evidence_windows.find((window) => window.status === "complete")
    ?? null;
}

/** Scope 2: say a version rotated, never "never run".
 *
 * A registry-touching merge mints a new `strategy_version` and the watermark is
 * keyed on it, so the new version starts a track record beside the old one. The
 * page used to show only the current version, so the operator saw an empty card
 * — indistinguishable from a broken system. */
function ScanRotationNote({ strategy }: { strategy: StrategyOverview }) {
  const rotation = strategy.scan.rotation;
  if (strategy.scan.status !== "rotated" || !rotation) return null;
  const scannedThrough = rotation.previous_frontier_date
    ? ` scanned through ${formatDate(rotation.previous_frontier_date)} under the previous version`
    : " scanned under the previous version";
  return (
    <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
      <strong className="font-semibold text-slate-700 dark:text-slate-200">Version rotated.</strong>
      {` This strategy${scannedThrough} (${shortVersion(rotation.previous_version)}). A new track record is starting; it has not scanned yet.`}
    </p>
  );
}

function shortVersion(version: string): string {
  const suffix = version.split("+").pop() ?? version;
  return suffix.slice(0, 8);
}

/** Scope 1: where the track record went, and why its numbers are not shown.
 *
 * Deliberately no metrics. Every version replaced before today sits on a
 * different `cost_model_id` / `return_basis`, and those pins are the result
 * identity — so the honest render names the difference rather than splicing an
 * old expectancy in beside a new one. */
function PriorVersionsBlock({ strategy }: { strategy: StrategyOverview }) {
  if (strategy.prior_versions.length === 0) return null;
  return (
    <div className="mt-4 border-t border-slate-200 pt-3 dark:border-slate-800">
      {/* One short line, not a paragraph: this block repeats on every card, and
          the per-row reason already carries the detail. */}
      <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500">
        Previous versions <span className="font-normal normal-case tracking-normal">· figures not shown, measured on another basis</span>
      </h4>
      <ul className="mt-2 space-y-1 text-xs text-slate-600 dark:text-slate-300">
        {strategy.prior_versions.map((prior) => (
          <li key={prior.strategy_version} className="tabular-nums">
            <span className="font-mono">{shortVersion(prior.strategy_version)}</span>
            {` · ${prior.result_count} stored result${prior.result_count === 1 ? "" : "s"}`}
            {prior.last_scan_frontier_date ? ` · scanned through ${formatDate(prior.last_scan_frontier_date)}` : " · never scanned"}
            {prior.comparable
              ? " · same measurement basis"
              : prior.incomparable_reasons.length
                ? ` · different ${prior.incomparable_reasons.map(pinLabel).join(", ")}`
                : " · no stored results to compare"}
          </li>
        ))}
      </ul>
    </div>
  );
}

const PIN_LABELS: Record<string, string> = {
  namespace: "sample",
  corpus_version: "corpus",
  cost_model_id: "cost model",
  sizing_rule: "sizing rule",
  benchmark_rule: "benchmark",
  return_basis: "return basis",
  position_rule_set_version: "position rules",
  outcome_rule_set_version: "outcome rules",
  input_rule_set_version: "input rules",
};

function pinLabel(pin: string): string {
  return PIN_LABELS[pin] ?? pin.replace(/_/g, " ");
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
  if (strategy.purpose === "harness_validation") {
    return {
      label: "Validation control",
      tone: "neutral",
      explanation: "Tests the research harness and can never use capital.",
    };
  }
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
  harness_validation_only: "Validation control; permanently barred from capital",
  strategy_not_capital_candidate: "Strategy has not been admitted as a capital candidate",
  strategy_not_runnable: "Rule is not runnable end to end",
  recent_evidence_incomplete: "Recent evidence windows are incomplete",
  recent_evidence_gate_refused: "Recent evidence failed its promotion gate",
  recent_net_expectancy_not_positive: "Net expectancy is not positive",
  paper_promotion_missing: "No approved deployment exists",
  pinned_promotion_evidence_invalid: "Pinned evidence is no longer valid",
  execution_policy_missing: "Execution and risk policy is missing",
  universe_basis_not_survivorship_free: "Point-in-time universe is not complete",
  carry_unmodelled: "Overnight financing cost is not modelled",
  fx_unmodelled: "Currency conversion cost is not modelled",
  trial_register_superseded: "Evidence does not match the current experiment register",
  synthetic_control_not_run: "Random-entry control has not passed",
  prospective_assessment_policy_missing: "Prospective forecast acceptance limits are missing",
  prospective_assessment_missing: "No current prospective forecast assessment exists",
  prospective_assessment_not_passed: "Recent forecast probabilities failed validation",
  prospective_assessment_stale: "Prospective forecast validation is stale",
};

function refusalLabel(refusal: string): string {
  return REFUSAL_LABELS[refusal] ?? refusal.replaceAll("_", " ");
}

function aggregate(overview: StrategyOverviewResponse) {
  const pnlValues = overview.strategies.map((strategy) => number(strategy.pnl.total_pnl));
  const forwardStrategies = overview.strategies.filter((strategy) => strategy.forward_outcome_supported);
  const resolved = forwardStrategies.reduce(
    (sum, strategy) => sum + strategy.attribution.resolved_entries,
    0,
  );
  const winners = forwardStrategies.reduce(
    (sum, strategy) => sum + strategy.attribution.winning_entries,
    0,
  );
  let weightedReturn = 0;
  let averageReturnKnown = resolved > 0;
  for (const strategy of forwardStrategies) {
    if (strategy.attribution.resolved_entries === 0) continue;
    const average = number(strategy.attribution.shadow_average_return_pct);
    if (average === null) {
      averageReturnKnown = false;
      break;
    }
    weightedReturn += average * strategy.attribution.resolved_entries;
  }
  const fired = forwardStrategies.reduce(
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

const READINESS_COPY: Record<StrategyOverviewResponse["automation_readiness"]["state"], string> = {
  no_capital_candidates: "No validated capital candidate exists. Research controls are measuring the machinery only.",
  historical_validation_incomplete: "Candidate research has not cleared the recent after-cost validation gates.",
  assessment_policy_missing: "Prospective forecast acceptance limits have not been registered.",
  prospective_evidence_missing: "A historically valid candidate is waiting for prospective forecast outcomes.",
  prospective_evidence_failed: "Recent forecast probabilities did not remain accurate enough to receive capital.",
  prospective_evidence_stale: "The last passing prospective assessment is no longer current.",
  candidate_evidence_incomplete: "No historically valid candidate has a fresh passing forecast assessment yet.",
  ready: "At least one candidate has current historical, execution and prospective forecast authority.",
};

function AutomationReadiness({ overview }: { overview: StrategyOverviewResponse }) {
  const readiness = overview.automation_readiness;
  return (
    <section className={`border px-5 py-4 ${readiness.ready ? "border-emerald-300 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/30" : "border-amber-300 bg-amber-50 dark:border-amber-800 dark:bg-amber-950/30"}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm font-semibold">{readiness.ready ? "Automation evidence is current" : "Automation is not ready"}</h2>
            <Badge tone={readiness.ready ? "ok" : "warn"}>{readiness.prospectively_ready_candidate_count} ready</Badge>
          </div>
          <p className="mt-1 max-w-3xl text-xs text-slate-600 dark:text-slate-300">{READINESS_COPY[readiness.state]}</p>
        </div>
        <div className="grid grid-cols-3 gap-5 text-right text-xs">
          <div><span className="block text-slate-500">Candidates</span><strong className="tabular-nums">{readiness.capital_candidate_count}</strong></div>
          <div><span className="block text-slate-500">Outcomes scored</span><strong className="tabular-nums">{readiness.resolved_forecasts}</strong></div>
          <div><span className="block text-slate-500">Fresh passing scopes</span><strong className="tabular-nums">{readiness.fresh_passed_scope_count}</strong></div>
        </div>
      </div>
    </section>
  );
}

function AccountEvidence({ overview }: { overview: StrategyOverviewResponse }) {
  const evidence = overview.account_equity_evidence;
  // No `?? "USD"`. The broker reports the account currency (#2602 item 2) and a null
  // here means it reported one we cannot name -- painting a $ on it would re-assert
  // the assumption this panel exists to test.
  const currency = evidence.currency;
  if (evidence.status === "unavailable") {
    return (
      <div className="mt-5 border-t border-slate-200 pt-3 text-xs text-slate-500 dark:border-slate-800">
        <span className="font-medium text-slate-700 dark:text-slate-300">Account evidence</span>
        <span className="ml-2">Official account equity starts collecting with the next portfolio sync.</span>
      </div>
    );
  }
  return (
    <div className="mt-5 flex flex-wrap items-end justify-between gap-3 border-t border-slate-200 pt-3 text-xs dark:border-slate-800">
      <div>
        <span className="font-medium text-slate-700 dark:text-slate-300">Account evidence</span>
        <span className="ml-2 text-slate-500">
          {evidence.days_collected} daily official {evidence.days_collected === 1 ? "snapshot" : "snapshots"}
        </span>
      </div>
      <div className="flex gap-5 text-right">
        <div>
          <span className="block text-slate-500">Broker equity</span>
          {currency === null ? (
            <strong className="text-amber-700 dark:text-amber-300">Currency unverified</strong>
          ) : (
            <strong>{formatMoney(evidence.official_equity === null ? null : Number(evidence.official_equity), currency)}</strong>
          )}
        </div>
        {evidence.local_eod_value !== null && evidence.local_eod_currency !== null ? (
          <div>
            <span className="block text-slate-500">Local valuation</span>
            <strong>{formatMoney(Number(evidence.local_eod_value), evidence.local_eod_currency)}</strong>
          </div>
        ) : null}
        <div className="self-end text-amber-700 dark:text-amber-300">Reconciliation collecting</div>
      </div>
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

function PnlChart({ history }: { history: Array<{ date: string; total_pnl: string | null }> }) {
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
          The performance line begins after the first end-of-day strategy-pot snapshot.
        </p>
      </div>
    </div>
  );
}

function StrategyPositionRow({
  position,
  onClose,
}: {
  position: StrategyOwnedPosition;
  onClose: (position: StrategyOwnedPosition) => void;
}) {
  const tick = useLiveTick(position.instrument_id);
  const livePrice = liveTickPriceIn(tick, position.currency);
  const snapshotPrice = number(position.current_price);
  const parsedLivePrice = livePrice === null ? null : Number(livePrice.value);
  const currentPrice = parsedLivePrice !== null && Number.isFinite(parsedLivePrice)
    ? parsedLivePrice
    : snapshotPrice;
  const assigned = number(position.assigned_value);
  const units = number(position.units);
  const openRate = number(position.open_rate);
  const liveValuationAvailable = assigned !== null
    && units !== null
    && openRate !== null
    && currentPrice !== null
    && position.direction !== null;
  const currentValue = liveValuationAvailable
    ? assigned + units * (
      position.direction === "long" ? currentPrice - openRate : openRate - currentPrice
    )
    : number(position.current_value);
  const pnl = currentValue !== null && assigned !== null
    ? currentValue - assigned
    : number(position.unrealised_pnl);
  const pnlRatio = pnl !== null && assigned !== null && assigned !== 0 ? pnl / assigned : null;
  const closing = position.trade_status === "closing";
  const needsReconciliation = position.trade_status === "reconcile_required" || !position.valuation_available;

  return (
    <tr className="border-t border-slate-200 dark:border-slate-800">
      <td className="px-4 py-3">
        <div className="flex items-center gap-2">
          <strong className="text-sm">{position.symbol}</strong>
          <Badge tone={closing ? "info" : needsReconciliation ? "warn" : "neutral"}>
            {closing ? "Closing" : needsReconciliation ? "Check required" : position.direction ?? "Owned"}
          </Badge>
        </div>
        <span className="mt-0.5 block max-w-48 truncate text-xs text-slate-500">
          {position.company_name ?? `Position #${position.broker_position_id}`}
        </span>
      </td>
      <td className="px-4 py-3 text-xs text-slate-600 dark:text-slate-300">
        <span className="block font-medium text-slate-800 dark:text-slate-100">{position.strategy_title}</span>
        <span className="text-slate-500">Opened {formatDate(position.opened_at)}</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">{formatMoney(assigned, position.currency)}</td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">{formatMoney(currentValue, position.currency)}</td>
      <td className={`px-4 py-3 text-right text-sm font-semibold tabular-nums ${pnl !== null && pnl < 0 ? "text-red-600 dark:text-red-300" : pnl !== null && pnl > 0 ? "text-emerald-700 dark:text-emerald-300" : ""}`}>
        <span className="block">{formatMoney(pnl, position.currency)}</span>
        <span className="text-xs">{formatPct(pnlRatio)}</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">
        <span className="block">{formatMoney(number(position.stop_loss_rate), position.currency)}</span>
        <span className="text-[10px] text-slate-500">Stop loss</span>
      </td>
      <td className="px-4 py-3 text-right text-sm tabular-nums">
        <span className="block">{formatMoney(number(position.take_profit_rate), position.currency)}</span>
        <span className="text-[10px] text-slate-500">Take profit</span>
      </td>
      <td className="px-4 py-3 text-right">
        <button
          type="button"
          disabled={closing}
          onClick={() => onClose(position)}
          className="min-h-11 cursor-pointer border border-slate-300 px-3 text-xs font-medium hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          {closing ? "Closing…" : "Close"}
        </button>
      </td>
    </tr>
  );
}

function OpenStrategyPositions({
  positions,
  onClose,
}: {
  positions: StrategyOwnedPosition[];
  onClose: (position: StrategyOwnedPosition) => void;
}) {
  return (
    <section className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-2 px-5 py-4">
        <div>
          <h2 className="text-sm font-semibold">Open automated positions</h2>
          <p className="mt-1 text-xs text-slate-500">
            Exact strategy-owned trades only. They also remain visible in the main Portfolio.
          </p>
        </div>
        <span className="text-xs text-slate-500">{positions.length} open</span>
      </div>
      {positions.length === 0 ? (
        <div className="border-t border-slate-200 px-5 py-5 text-sm text-slate-500 dark:border-slate-800">
          No automated positions are open. This section appears when an approved strategy receives a broker position.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[64rem]">
            <thead className="border-t border-slate-200 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:border-slate-800">
              <tr>
                <th className="px-4 py-2">Position</th>
                <th className="px-4 py-2">Strategy</th>
                <th className="px-4 py-2 text-right">Assigned</th>
                <th className="px-4 py-2 text-right">Current</th>
                <th className="px-4 py-2 text-right">Gain / loss</th>
                <th className="px-4 py-2 text-right">SL</th>
                <th className="px-4 py-2 text-right">TP</th>
                <th className="px-4 py-2"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {positions.map((position) => (
                <StrategyPositionRow
                  key={`${position.strategy_trade_id}:${position.broker_position_id}`}
                  position={position}
                  onClose={onClose}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function StrategyCloseModal({
  position,
  onRequestClose,
  onAccepted,
}: {
  position: StrategyOwnedPosition | null;
  onRequestClose: () => void;
  onAccepted: () => void;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setSubmitting(false);
    setError(null);
  }, [position]);

  async function submit() {
    if (position === null || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      await closeStrategyOwnedPosition(position.strategy_trade_id, position.broker_position_id);
      onAccepted();
      onRequestClose();
    } catch (caught) {
      setError(caught instanceof ApiError ? caught.message : "The close request could not be submitted.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Modal
      isOpen={position !== null}
      onRequestClose={submitting ? () => undefined : onRequestClose}
      label={`Close ${position?.symbol ?? "automated position"}`}
    >
      {position ? (
        <div className="space-y-4">
          <div>
            <h2 className="text-base font-semibold">Close {position.symbol}</h2>
            <p className="mt-1 text-xs text-slate-500">
              {position.strategy_title} · broker position #{position.broker_position_id}
            </p>
          </div>
          <p className="text-sm text-slate-700 dark:text-slate-200">
            This submits a full close to the connected demo account. The strategy will show this position as Closing until the exact broker order reconciles.
          </p>
          <p className="text-xs text-slate-500">
            A separate manual position in {position.symbol} is not part of this request and will remain untouched.
          </p>
          {error ? <p role="alert" className="text-xs text-red-700 dark:text-red-300">{error}</p> : null}
          <div className="flex justify-end gap-2 border-t border-slate-200 pt-4 dark:border-slate-800">
            <button type="button" disabled={submitting} onClick={onRequestClose} className="min-h-11 cursor-pointer border border-slate-300 px-4 text-sm disabled:opacity-50 dark:border-slate-700">Cancel</button>
            <button type="button" disabled={submitting} onClick={() => void submit()} className="min-h-11 cursor-pointer border border-red-700 bg-red-700 px-4 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50">
              {submitting ? "Submitting…" : "Close position"}
            </button>
          </div>
        </div>
      ) : null}
    </Modal>
  );
}

function AutomationControl({
  overview,
  onUpdated,
}: {
  overview: StrategyOverviewResponse;
  onUpdated: () => void;
}) {
  const pool = overview.paper_pool;
  const [enabled, setEnabled] = useState(pool.enabled && overview.execution_enabled);
  const [limit, setLimit] = useState(pool.capital_limit);
  const [capitalMode, setCapitalMode] = useState(pool.capital_mode);
  const [riskProfile, setRiskProfile] = useState(pool.mandate.risk_profile);
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    setEnabled(pool.enabled && overview.execution_enabled);
    setLimit(pool.capital_limit);
    setCapitalMode(pool.capital_mode);
    setRiskProfile(pool.mandate.risk_profile);
  }, [pool.enabled, pool.capital_limit, pool.capital_mode, pool.mandate.risk_profile, overview.execution_enabled]);
  const parsed = Number(limit);
  const valid = Number.isFinite(parsed) && parsed >= 0 && (!enabled || parsed > 0);
  const effectiveEnabled = pool.enabled && overview.execution_enabled;
  const dirty = enabled !== effectiveEnabled
    || parsed !== Number(pool.capital_limit)
    || capitalMode !== pool.capital_mode
    || riskProfile !== pool.mandate.risk_profile;
  const canEnable = overview.automation_readiness.ready && overview.execution_enabled && riskProfile !== "unconfigured";
  const selectedMandate = riskProfile === pool.mandate.risk_profile && pool.mandate.configured
    ? pool.mandate
    : pool.available_mandates.find((mandate) => mandate.risk_profile === riskProfile) ?? null;

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!valid || !dirty || saving || (enabled && !canEnable)) return;
    setSaving(true);
    setFailed(false);
    try {
      await updateStrategyPaperPool({
        enabled,
        capital_limit: parsed.toFixed(6),
        capital_mode: capitalMode,
        risk_profile: riskProfile,
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
          <label className="w-52 text-xs font-medium text-slate-600 dark:text-slate-300">
            Profit treatment
            <select
              value={capitalMode}
              onChange={(event) => setCapitalMode(event.target.value as "fixed" | "compound")}
              className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
            >
              <option value="fixed">Keep principal limit fixed</option>
              <option value="compound">Reinvest realised P&amp;L</option>
            </select>
          </label>
          <label className="w-48 text-xs font-medium text-slate-600 dark:text-slate-300">
            Risk profile
            <select
              value={riskProfile}
              onChange={(event) => setRiskProfile(event.target.value as "unconfigured" | "cautious" | "balanced" | "growth")}
              className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
            >
              <option value="unconfigured">Choose risk profile</option>
              <option value="cautious">Cautious</option>
              <option value="balanced">Balanced</option>
              <option value="growth">Growth</option>
            </select>
          </label>
          <button type="submit" disabled={!valid || !dirty || saving || (enabled && !canEnable)} className="min-h-11 cursor-pointer border border-blue-700 bg-blue-700 px-4 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-40">
            {saving ? "Saving…" : "Save"}
          </button>
        </div>
      </form>
      {selectedMandate ? (
        <div className="mt-5 border-t border-slate-200 pt-4 dark:border-slate-800">
          <p className="text-xs text-slate-500">Policy ceilings, not return forecasts. Long-only and unleveraged in this version.</p>
          <dl className="mt-3 grid grid-cols-2 gap-3 text-xs sm:grid-cols-4">
            <div><dt className="text-slate-500">Target volatility</dt><dd className="font-semibold">{pctPoints(selectedMandate.target_volatility_pct)}</dd></div>
            <div><dt className="text-slate-500">Max drawdown</dt><dd className="font-semibold">{pctPoints(selectedMandate.max_portfolio_drawdown_pct)}</dd></div>
            <div><dt className="text-slate-500">Max loss / position</dt><dd className="font-semibold">{pctPoints(selectedMandate.max_loss_per_position_pct)}</dd></div>
            <div><dt className="text-slate-500">Max daily loss</dt><dd className="font-semibold">{pctPoints(selectedMandate.max_daily_loss_pct)}</dd></div>
            <div><dt className="text-slate-500">Active risk budget</dt><dd className="font-semibold">{pctPoints(selectedMandate.active_risk_budget_pct)}</dd></div>
            <div><dt className="text-slate-500">Cash reserve</dt><dd className="font-semibold">{pctPoints(selectedMandate.cash_reserve_pct)}</dd></div>
            <div><dt className="text-slate-500">Concurrent positions</dt><dd className="font-semibold">{selectedMandate.max_concurrent_positions}</dd></div>
            <div><dt className="text-slate-500">Authority</dt><dd className="font-semibold">Long only · No leverage</dd></div>
          </dl>
        </div>
      ) : null}
      <dl className="mt-5 grid grid-cols-2 gap-3 border-t border-slate-200 pt-4 text-xs sm:grid-cols-4 dark:border-slate-800">
        <div><dt className="text-slate-500">Risk base</dt><dd className="font-semibold tabular-nums">{money(pool.effective_capital)}</dd></div>
        <div><dt className="text-slate-500">Working</dt><dd className="font-semibold tabular-nums">{money(pool.invested_capital)}</dd></div>
        <div><dt className="text-slate-500">Reserved</dt><dd className="font-semibold tabular-nums">{money(pool.reserved_capital)}</dd></div>
        <div><dt className="text-slate-500">Available</dt><dd className="font-semibold tabular-nums">{money(pool.remaining_capital)}</dd></div>
      </dl>
      {!canEnable ? (
        <p className="mt-4 text-xs text-amber-700 dark:text-amber-300">
          {!overview.execution_enabled
            ? "System-wide automatic trading is off. Enable that safety control before allowing new entries."
            : riskProfile === "unconfigured"
              ? "Choose a risk profile before allowing new entries."
            : "Automation stays off until at least one strategy passes validation."}
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

function SignalValidation({ overview, strategies }: { overview: StrategyOverviewResponse; strategies: StrategyOverview[] }) {
  const summary = aggregate({ ...overview, strategies });
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
  const availableCapital = Math.max(Number(strategy.allocation.capital_limit), Number(poolLimit));
  const canEnable = strategy.allocation_ready && availableCapital > 0;
  async function toggle() {
    if (saving || (!strategy.allocation.enabled && !canEnable)) return;
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
        <input type="checkbox" checked={strategy.allocation.enabled} disabled={saving || (!strategy.allocation.enabled && !canEnable)} onChange={() => void toggle()} className="h-5 w-5" />
        {strategy.allocation.enabled ? "Enabled" : "Paused"}
      </label>
      {failed ? <p className="text-xs text-red-700 dark:text-red-300">Not changed</p> : null}
    </div>
  );
}

function StrategySizingControl({ strategy, onUpdated }: { strategy: StrategyOverview; onUpdated: () => void }) {
  const allocation = strategy.allocation;
  const [mode, setMode] = useState<"percent" | "fixed">(allocation.ticket_sizing_mode ?? "percent");
  const [value, setValue] = useState(allocation.ticket_value ?? "");
  const [maximum, setMaximum] = useState(allocation.max_ticket_amount ?? "");
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    setMode(allocation.ticket_sizing_mode ?? "percent");
    setValue(allocation.ticket_value ?? "");
    setMaximum(allocation.max_ticket_amount ?? "");
  }, [allocation.ticket_sizing_mode, allocation.ticket_value, allocation.max_ticket_amount]);
  const parsedValue = Number(value);
  const parsedMaximum = Number(maximum);
  const valid = Number.isFinite(parsedValue) && parsedValue > 0
    && Number.isFinite(parsedMaximum) && parsedMaximum > 0
    && (mode !== "percent" || parsedValue <= 100)
    && (mode !== "fixed" || parsedMaximum >= parsedValue);

  async function save(event: FormEvent) {
    event.preventDefault();
    if (!valid || saving) return;
    setSaving(true);
    setFailed(false);
    try {
      await updateStrategySizing(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        ticket_sizing_mode: mode,
        ticket_value: parsedValue.toFixed(6),
        max_ticket_amount: parsedMaximum.toFixed(6),
        reason: "Per-signal sizing updated from automated strategy workspace",
      });
      onUpdated();
    } catch (error) {
      console.error("Strategy sizing update failed:", error);
      setFailed(true);
    } finally {
      setSaving(false);
    }
  }

  if (!allocation.policy_configured) return null;
  return (
    <details className="mt-2 text-xs text-slate-500">
      <summary className="cursor-pointer">
        Per signal: {allocation.ticket_sizing_mode === "fixed" ? money(allocation.ticket_value) : `${allocation.ticket_value ?? "—"}%`}
        {allocation.max_ticket_amount ? ` · max ${money(allocation.max_ticket_amount)}` : ""}
      </summary>
      <form onSubmit={(event) => void save(event)} className="mt-3 flex flex-wrap items-end gap-2">
        <label>
          Method
          <select value={mode} onChange={(event) => setMode(event.target.value as "percent" | "fixed")} className="mt-1 block min-h-10 border border-slate-300 bg-white px-2 dark:border-slate-700 dark:bg-slate-950">
            <option value="percent">Percent</option>
            <option value="fixed">Fixed USD</option>
          </select>
        </label>
        <label>
          {mode === "percent" ? "Percent" : "Amount"}
          <input type="number" min="0.01" max={mode === "percent" ? "100" : undefined} step="0.01" value={value} onChange={(event) => setValue(event.target.value)} className="mt-1 block min-h-10 w-24 border border-slate-300 bg-white px-2 tabular-nums dark:border-slate-700 dark:bg-slate-950" />
        </label>
        <label>
          Hard max USD
          <input type="number" min="0.01" step="0.01" value={maximum} onChange={(event) => setMaximum(event.target.value)} className="mt-1 block min-h-10 w-28 border border-slate-300 bg-white px-2 tabular-nums dark:border-slate-700 dark:bg-slate-950" />
        </label>
        <button type="submit" disabled={!valid || saving} className="min-h-10 border border-slate-300 px-3 font-medium text-slate-700 disabled:opacity-40 dark:border-slate-700 dark:text-slate-200">{saving ? "Saving…" : "Save sizing"}</button>
      </form>
      {failed ? <p className="mt-2 text-red-700 dark:text-red-300">Sizing was not changed.</p> : null}
    </details>
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
        <StrategySizingControl strategy={strategy} onUpdated={onUpdated} />
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
    // The blank card #2624 is about. A rotation lands here with EVERY window
    // `missing`, so this branch has to explain the rotation rather than imply
    // the research never ran.
    return (
      <div className="border-t border-slate-200 px-4 py-4 text-sm text-slate-500 dark:border-slate-800">
        <ScanRotationNote strategy={strategy} />
        <p className={strategy.scan.status === "rotated" ? "mt-2" : undefined}>
          No completed valid evidence is available under the current version. {failures[0] ?? "The research run has not finished."}
        </p>
        <PriorVersionsBlock strategy={strategy} />
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
      <PriorVersionsBlock strategy={strategy} />
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

function ValidationControl({ strategy }: { strategy: StrategyOverview }) {
  const arm = representativeArm(strategy);
  // ⚠ This — NOT `EvidenceDetail` — is where the four strategies that exist are
  // rendered, because all of them are `harness_validation` (#2624). Putting the
  // rotation notice only on the capital-candidate path would have shipped a
  // payload nothing displays; caught by walking the real page, not by the
  // component tests, whose fixture is a candidate.
  return (
    <article className="border-t border-slate-200 py-3 dark:border-slate-800">
      <div className="grid gap-3 sm:grid-cols-[minmax(14rem,1.5fr)_repeat(3,minmax(6rem,0.6fr))] sm:items-center">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold">{strategy.title}</h3>
            <Badge tone="neutral">Control</Badge>
          </div>
          <p className="mt-1 text-xs text-slate-500">
            Harness evidence only · never eligible for capital
            {strategy.forward_outcome_supported ? " · forward outcomes measured" : " · backtest only"}
          </p>
        </div>
        <div><span className="text-xs text-slate-500">Expected / trade</span><strong className="block tabular-nums">{pctPoints(arm?.expectancy_per_trade_pct ?? null)}</strong></div>
        <div><span className="text-xs text-slate-500">Worst drawdown</span><strong className="block tabular-nums">{pctPoints(arm?.max_drawdown_pct ?? null)}</strong></div>
        <div><span className="text-xs text-slate-500">Evidence windows</span><strong className="block tabular-nums">{completedEvidenceCount(strategy)}/{strategy.evidence_windows.length}</strong></div>
      </div>
      <ScanRotationNote strategy={strategy} />
      <PriorVersionsBlock strategy={strategy} />
    </article>
  );
}

export function StrategiesPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, []);
  const [closeFor, setCloseFor] = useState<StrategyOwnedPosition | null>(null);
  const [refreshingEvidence, setRefreshingEvidence] = useState(false);
  const [refreshEvidenceError, setRefreshEvidenceError] = useState<string | null>(null);
  const summary = useMemo(() => overview.data ? aggregate(overview.data) : null, [overview.data]);
  const capitalCandidates = overview.data?.strategies.filter((strategy) => strategy.purpose === "capital_candidate") ?? [];
  const approvedStrategies = capitalCandidates.filter((strategy) => strategy.allocation_ready || strategy.allocation.enabled);
  const researchCandidates = capitalCandidates.filter((strategy) => !strategy.allocation_ready && !strategy.allocation.enabled);
  const forwardCandidates = capitalCandidates.filter((strategy) => strategy.forward_outcome_supported);
  const validationControls = overview.data?.strategies.filter((strategy) => strategy.purpose === "harness_validation") ?? [];

  async function refreshEvidence() {
    setRefreshingEvidence(true);
    setRefreshEvidenceError(null);
    try {
      await requestStrategyEvidenceRefresh();
      await overview.refetch();
    } catch (error) {
      setRefreshEvidenceError(error instanceof ApiError ? error.message : "Could not queue evidence refresh.");
    } finally {
      setRefreshingEvidence(false);
    }
  }

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
          <AutomationReadiness overview={overview.data} />
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
                <>
                  <PnlChart history={pnlHistory.data.points} />
                  <p className="mt-2 text-xs text-slate-500">
                    Daily realised plus open P&amp;L from exact automated positions; manual positions are excluded.
                    Gaps mean an owned mark or close could not reconcile. Capital changes are recorded separately
                    from performance; total return and benchmark comparison remain unavailable until distribution
                    and benchmark accounting reconcile.
                  </p>
                </>
              ) : (
                <EmptyPnlChart />
              )}
              <AccountEvidence overview={overview.data} />
            </section>
            <AutomationControl overview={overview.data} onUpdated={overview.refetch} />
          </div>

          {ownedPositions.loading ? (
            <SectionSkeleton rows={3} />
          ) : ownedPositions.error ? (
            <SectionError onRetry={ownedPositions.refetch} />
          ) : ownedPositions.data ? (
            <LiveQuoteProvider instrumentIds={ownedPositions.data.live_quote_instrument_ids}>
              <OpenStrategyPositions positions={ownedPositions.data.positions} onClose={setCloseFor} />
            </LiveQuoteProvider>
          ) : null}

          {forwardCandidates.length ? <SignalValidation overview={overview.data} strategies={forwardCandidates} /> : null}

          {approvedStrategies.length ? (
            <section>
              <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
                <div>
                  <h2 className="text-sm font-semibold">Approved &amp; managed strategies</h2>
                  <p className="mt-1 text-xs text-slate-500">Approved strategies may use the shared pot; an invalidated strategy remains visible only while it manages an existing position.</p>
                </div>
                <span className="text-xs text-slate-500">{summary.approved} approved</span>
              </div>
              <div>
                {approvedStrategies.map((strategy) => (
                  <ApprovedStrategy key={strategy.strategy_id} strategy={strategy} poolLimit={overview.data?.paper_pool.capital_limit ?? "0"} onUpdated={overview.refetch} />
                ))}
              </div>
            </section>
          ) : null}

          {(researchCandidates.length || validationControls.length) ? (
            <details className="group border-t border-slate-200 pt-5 dark:border-slate-800">
              <summary className="flex min-h-11 cursor-pointer list-none flex-wrap items-center justify-between gap-3 marker:hidden">
                <span>
                  <span className="block text-sm font-semibold">Research &amp; validation</span>
                  <span className="mt-1 block text-xs text-slate-500">Supporting evidence and harness controls; none can use capital.</span>
                </span>
                <span className="flex items-center gap-2 text-xs text-slate-500">
                  <span>{researchCandidates.length} {researchCandidates.length === 1 ? "candidate" : "candidates"} · {validationControls.length} {validationControls.length === 1 ? "control" : "controls"}</span>
                  <span aria-hidden="true" className="transition-transform group-open:rotate-90">›</span>
                </span>
              </summary>
              <div className="mt-4 space-y-6">
                <section>
                  <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
                    <div>
                      <h2 className="text-sm font-semibold">Research pipeline</h2>
                      <p className="mt-1 max-w-3xl text-xs text-slate-500">
                        These rules are measured, not selectable. Current evaluation uses completed daily bars; it does not predict which rule is about to fire.
                      </p>
                    </div>
                    <div className="flex items-center gap-3 text-xs text-slate-500">
                      <span>
                        Evidence {overview.data.evidence_refresh.completed_windows}/{overview.data.evidence_refresh.total_windows}
                        {overview.data.evidence_refresh.partial_windows ? ` · ${overview.data.evidence_refresh.partial_windows} partial` : ""}
                        {` · frozen through ${formatDate(overview.data.evidence_refresh.frozen_through)}`}
                      </span>
                      <button
                        type="button"
                        className="border border-slate-300 px-3 py-1.5 font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:text-slate-200 dark:hover:bg-slate-900"
                        disabled={refreshingEvidence || overview.data.evidence_refresh.status === "queued" || overview.data.evidence_refresh.status === "running" || overview.data.evidence_refresh.partial_windows > 0}
                        onClick={refreshEvidence}
                      >
                        {refreshingEvidence || overview.data.evidence_refresh.status === "queued"
                          ? "Refresh queued"
                          : overview.data.evidence_refresh.status === "running"
                            ? "Refreshing…"
                            : "Refresh evidence"}
                      </button>
                    </div>
                  </div>
                  {refreshEvidenceError ? <p className="mb-3 text-xs text-red-600 dark:text-red-400">{refreshEvidenceError}</p> : null}
                  {overview.data.evidence_refresh.status === "failed" ? (
                    <p className="mb-3 text-xs text-red-600 dark:text-red-400">
                      Last refresh failed: {overview.data.evidence_refresh.last_error ?? "See process history."}
                    </p>
                  ) : null}
                  {overview.data.evidence_refresh.partial_windows > 0 ? (
                    <p className="mb-3 text-xs text-amber-700 dark:text-amber-300">
                      A partial immutable window needs operator repair before refresh can resume.
                    </p>
                  ) : null}
                  <div className="space-y-2">
                    {researchCandidates.length
                      ? researchCandidates.map((strategy) => <ResearchCandidate key={strategy.strategy_id} strategy={strategy} />)
                      : <EmptyState title="No capital candidates" description="The current bounded research programme produced no strategy safe enough to allocate capital." />}
                  </div>
                </section>

                {validationControls.length ? (
                  <section>
                    <div className="border-t border-slate-200 pt-5 dark:border-slate-800">
                      <h2 className="text-sm font-semibold">Validation controls</h2>
                      <p className="mt-1 max-w-3xl text-xs text-slate-500">
                        Published baseline rules retained to test the backtester, cost model and outcome pipeline. They are not trading recommendations and cannot be enabled.
                      </p>
                    </div>
                    <div className="mt-3">
                      {validationControls.map((strategy) => <ValidationControl key={strategy.strategy_id} strategy={strategy} />)}
                    </div>
                  </section>
                ) : null}
              </div>
            </details>
          ) : null}

          <StrategyCloseModal
            position={closeFor}
            onRequestClose={() => setCloseFor(null)}
            onAccepted={() => {
              ownedPositions.refetch();
              overview.refetch();
              pnlHistory.refetch();
            }}
          />
        </>
      ) : null}
    </div>
  );
}
