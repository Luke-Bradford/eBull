import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import {
  advanceStrategyPromotion,
  closeStrategyOwnedPosition,
  createStrategyPaperSetup,
  fetchFiredSignals,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
  requestStrategyEvidenceRefresh,
  updateStrategyAllocation,
  updateStrategyPaperPool,
  updateStrategySizing,
} from "@/api/strategies";
import type {
  FiredSignal,
  FiredSignalsResponse,
  StrategyEvidenceWindow,
  StrategyFireRate,
  StrategyControlledTrial,
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
import { formatDate, formatMoney, formatNumber, formatPct, formatUnsignedPct } from "@/lib/format";
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
 * `app/services/strategy_recent_evidence.py` declares the bounded historical
 * ids, and `primary` is not among them, so this lookup used to be dead and the function
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
  ambiguity_rule_version: "ambiguity rule",
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

/** The metric set that carries a holding period, mirroring
 *  `app/services/strategy_statistics.py::METRIC_SET_ID`. A row stamped with
 *  anything else predates the measurement, which is a DIFFERENT statement from
 *  "this strategy holds for zero days" — hence naming the version in the cell. */
const HOLD_PERIOD_METRIC_SET = "criterion7-v2";

const SHARE_UNAVAILABLE_LABELS: Record<
  NonNullable<StrategyFireRate["share_unavailable_reason"]>,
  string
> = {
  never_scanned: "Not scanned yet",
  no_evaluable_decisions: "No evaluable decisions",
};

const WEEKLY_RATE_UNAVAILABLE_LABELS: Record<
  NonNullable<StrategyFireRate["weekly_rate_unavailable_reason"]>,
  string
> = {
  never_scanned: "Not scanned yet",
  single_scan_day: "1 scan day — needs a span",
};

function CatalogFact({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className="tabular-nums text-slate-700 dark:text-slate-200">{value}</dd>
      <dd className="text-[10px] text-slate-500">{note}</dd>
    </div>
  );
}

/** The catalog facts #2623 asks for: how often the rule fires, how long it holds,
 *  and how often it won — on every card, because all four strategies today are
 *  `harness_validation` and so render through `ValidationControl` (#2624).
 *
 * ⚠⚠ `median_hold_days` is deliberately unreachable without its two exclusion
 * counts. The median is right-censored and the direction of that bias is NOT
 * determinable a priori — a position opened just before the window end is still
 * open and also short — so `open_trade_count` and `unpriced_trade_count` are
 * separate exclusions, neither implying the other. Keeping all three in one
 * component is what makes "never render the median alone" structural rather than
 * a convention the next edit can forget.
 *
 * ⚠ Every blank names its OWN reason. The three nulls are independent: the fire
 * share and the weekly rate carry separate reason enums (#2623 gap 2), and the
 * holding period's blank is explained by `metric_set_id`, not by either of them. */
function StrategyCatalogFacts({
  strategy,
  arm,
}: {
  strategy: StrategyOverview;
  arm: StrategyResultArm | null;
}) {
  const fireRate = strategy.fire_rate;
  const share = number(fireRate.fired_share_of_evaluable);
  const perWeek = number(fireRate.entries_per_calendar_week);
  const shareReason = fireRate.share_unavailable_reason;
  const weeklyReason = fireRate.weekly_rate_unavailable_reason;
  const median = arm ? number(arm.median_hold_days) : null;
  const p25 = arm ? number(arm.hold_days_p25) : null;
  const p75 = arm ? number(arm.hold_days_p75) : null;
  const excluded = arm ? arm.open_trade_count + arm.unpriced_trade_count : 0;

  // The SHARE is the headline because it is dimensionless. `entries_per_calendar_week`
  // is throughput and rises with the universe, so leading on it would make a
  // strategy look busier purely because more instruments were listed.
  const firesValue = share === null
    ? (shareReason ? SHARE_UNAVAILABLE_LABELS[shareReason] : "—")
    // ⚠ UNSIGNED. A fire propensity is a composition, not a return, and
    // `formatPct` carries `signDisplay: "exceptZero"` — it would render a
    // perfectly ordinary 52% share as "+52.10%".
    : formatUnsignedPct(share);
  const firesNote = perWeek === null
    ? (weeklyReason ? WEEKLY_RATE_UNAVAILABLE_LABELS[weeklyReason] : "No weekly rate")
    : `${formatNumber(perWeek, 2)} / week`;

  let turnaroundValue: string;
  let turnaroundNote: string;
  if (arm === null) {
    turnaroundValue = "—";
    turnaroundNote = "No completed evidence";
  } else if (median === null) {
    // Two different nulls. A pre-`criterion7-v2` row was written before the
    // holding period was measured at all; a row stamped WITH it and still null
    // simply closed no trades (`sql/347` allows that case and no other).
    turnaroundValue = "Not measured";
    turnaroundNote = arm.metric_set_id === HOLD_PERIOD_METRIC_SET
      ? "No completed trades"
      : `Result version ${arm.metric_set_id}`;
    if (excluded > 0) {
      turnaroundNote += ` · ${formatNumber(arm.open_trade_count, 0)} open, ${formatNumber(arm.unpriced_trade_count, 0)} unpriced`;
    }
  } else {
    turnaroundValue = `${formatNumber(median, 1)} days`;
    const range = p25 !== null && p75 !== null
      ? `${formatNumber(p25, 1)}–${formatNumber(p75, 1)} typical`
      : "No range";
    turnaroundNote = `${range} · ${formatNumber(arm.open_trade_count, 0)} open, ${formatNumber(arm.unpriced_trade_count, 0)} unpriced excluded`;
  }

  // ⚠ `trade_count` is ALREADY the resolved count — `backtest_run` appends a
  // return only on a realised close, so open and unpriced positions were never
  // in it. Subtracting them here (as this page used to) understates the figure.
  //
  // ⚠⚠ "Not lost", NOT "won", and the label is the fix rather than a hedge.
  // `strategy_statistics` computes `losing_trades` as `value < 0.0` strictly, so
  // a breakeven trade is in `trade_count` and in neither the losing count nor any
  // stored winning count — there is no winning count. `trade_count - losing` is
  // therefore the NON-LOSING share, and calling it a win rate would report a
  // breakeven as a win. A true win rate needs a strictly-positive count stored at
  // the producer, which is a metric-set change and populate-forward-only; naming
  // what we actually have is the #2602-item-5 posture: never substitute.
  const notLost = arm ? arm.trade_count - arm.losing_trade_count : 0;
  // Unsigned for the same reason as the share: a not-lost rate is a composition.
  const successValue = arm && arm.trade_count > 0 ? formatUnsignedPct(notLost / arm.trade_count) : "—";
  const successNote = arm && arm.trade_count > 0
    ? `${formatNumber(notLost, 0)} of ${formatNumber(arm.trade_count, 0)} not at a loss`
    : "No completed backtest trades";

  return (
    <dl className="mt-2 flex flex-wrap gap-x-6 gap-y-2 text-xs">
      <CatalogFact label="Fires" value={firesValue} note={firesNote} />
      <CatalogFact label="Turnaround" value={turnaroundValue} note={turnaroundNote} />
      <CatalogFact label="Not lost" value={successValue} note={successNote} />
    </dl>
  );
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
  paper_forward_evidence_missing: "Paper approval has no durable forward-evidence record",
  pinned_promotion_evidence_invalid: "Pinned evidence is no longer valid",
  execution_policy_missing: "Execution and risk policy is missing",
  universe_basis_not_survivorship_free: "Point-in-time universe is not complete",
  carry_unmodelled: "Overnight financing cost is not modelled",
  fx_unmodelled: "Currency conversion cost is not modelled",
  trial_register_superseded: "Evidence does not match the current experiment register",
  synthetic_control_not_run: "Random-entry control has not passed",
  prospective_assessment_policy_missing: "Prospective forecast acceptance limits are missing",
  prospective_assessment_missing: "No current prospective forecast assessment exists",
  prospective_assessment_ambiguous: "More than one current prospective assessment scope exists",
  prospective_assessment_includes_pre_forward_observations: "Prospective evidence includes observations from before forward admission",
  prospective_assessment_window_invalid: "Prospective assessment window is temporally inconsistent",
  prospective_assessment_not_passed: "Recent forecast probabilities failed validation",
  prospective_assessment_stale: "Prospective forecast validation is stale",
  preregistration_declaration_missing: "The capital candidate has no frozen preregistration",
  declaration_digest_mismatch: "The frozen preregistration failed its integrity check",
  declaration_no_longer_coherent: "The frozen preregistration no longer matches the evidence contract",
  declaration_not_capital_candidate: "The frozen preregistration is not for a capital candidate",
  forward_observation_missing: "Forward observation has not started",
  forward_observation_future_dated: "Forward observation is future-dated",
  forward_decision_dates_insufficient: "The preregistered forward decision-date floor is not met",
  forward_calendar_weeks_insufficient: "The preregistered forward calendar-time floor is not met",
  funding_not_reconciled_to_trade: "Funded decision has not reconciled to a strategy trade",
  trade_not_reconciled_to_position: "Strategy trade has not reconciled to a broker position",
  position_ownership_ambiguous: "More than one broker-position ownership record exists",
  position_ownership_incomplete: "Broker-position ownership is incomplete",
  released_position_missing_close_history: "Released position is missing broker close history",
  realised_pnl_missing_from_history: "Broker close history is missing realised P&L",
  fees_missing_from_history: "Broker close history is missing fees",
  closed_trade_has_active_ownership: "Closed trade still has active broker-position ownership",
  released_ownership_trade_not_closed: "Released broker-position ownership belongs to a trade not marked closed",
  position_operation_rejected: "Latest broker-position operation was rejected",
  position_operation_reconciliation_required: "Latest broker-position operation requires reconciliation",
  position_operation_error: "Latest broker-position operation recorded an error",
  position_operation_reconciliation_not_found: "Latest operation was not found at the broker",
  position_operation_reconciliation_ambiguous: "Latest operation has ambiguous broker reconciliation",
  position_operation_reconciliation_error: "Latest operation reconciliation failed",
  entry_order_reconciliation_not_found: "Entry order was not found at the broker",
  entry_order_reconciliation_ambiguous: "Entry order has ambiguous broker reconciliation",
  entry_order_reconciliation_rejected: "Entry order was rejected by the broker",
  entry_order_reconciliation_error: "Entry order reconciliation failed",
};

function refusalLabel(refusal: string): string {
  return REFUSAL_LABELS[refusal] ?? refusal.replaceAll("_", " ");
}

const ACCOUNT_EVIDENCE_REASON_LABELS: Record<string, string> = {
  official_account_equity_missing: "Official account equity starts collecting with the next portfolio sync.",
  account_currency_assumed_not_observed: "This snapshot predates observed broker account currency; its currency cannot be trusted.",
  account_currency_not_documented: "The broker reported an account currency that is not documented for this USD-only trading lane.",
  same_day_local_eod_snapshot_missing: "The same-day local end-of-day valuation is missing.",
  local_eod_currency_mismatch: "The broker account and local valuation currencies do not match.",
  local_eod_valuation_incomplete: "The local valuation is missing at least one price or currency conversion.",
  local_eod_effective_time_unknown: "The effective dates of the local valuation marks were not recorded.",
};

function accountEvidenceReasonLabel(
  reason: string,
  evidence: StrategyOverviewResponse["account_equity_evidence"],
): string {
  if (reason === "local_eod_marks_carried_forward") {
    const stale = evidence.local_eod_stale_mark_positions;
    const priced = evidence.local_eod_positions_priced;
    if (typeof stale === "number" && typeof priced === "number") {
      return `${stale} of ${priced} priced ${priced === 1 ? "position uses" : "positions use"} a carried-forward closing mark.`;
    }
    return "The local valuation includes carried-forward closing marks.";
  }
  // An upstream reason added before this UI is deployed must remain visible.
  return ACCOUNT_EVIDENCE_REASON_LABELS[reason] ?? reason;
}

function AccountEvidenceReasons({
  evidence,
}: {
  evidence: StrategyOverviewResponse["account_equity_evidence"];
}) {
  if (evidence.incomplete_reasons.length === 0) return null;
  return (
    <ul className="mt-2 space-y-1 text-amber-700 dark:text-amber-300">
      {evidence.incomplete_reasons.map((reason) => (
        <li key={reason}>{accountEvidenceReasonLabel(reason, evidence)}</li>
      ))}
    </ul>
  );
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
        <AccountEvidenceReasons evidence={evidence} />
      </div>
    );
  }
  return (
    <div className="mt-5 border-t border-slate-200 pt-3 text-xs dark:border-slate-800">
      <div className="flex flex-wrap items-end justify-between gap-3">
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
          <div className="self-end text-amber-700 dark:text-amber-300">
            {evidence.incomplete_reasons.length > 0 ? "Reconciliation incomplete" : "Comparison tolerance not defined"}
          </div>
        </div>
      </div>
      <AccountEvidenceReasons evidence={evidence} />
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

function fundingPresentation(signal: FiredSignal): { label: string; tone: "ok" | "neutral" | "warn" } {
  if (signal.funding_status === "funded") return { label: "Funded", tone: "ok" };
  if (signal.funding_status === "rejected") return { label: "Not funded", tone: "warn" };
  return { label: "No capital needed", tone: "neutral" };
}

function executionLabel(signal: FiredSignal): string {
  if (signal.execution_status) return signal.execution_status.replaceAll("_", " ");
  if (signal.funding_status === "rejected") return "No order submitted";
  if (signal.funding_status === "not_applicable") return "Exit observation";
  return "Awaiting broker state";
}

function outcomeLabel(signal: FiredSignal): string {
  if (signal.outcome === null) return "Outcome unresolved";
  return signal.outcome.replaceAll("_", " ");
}

function lifecycleTone(signal: FiredSignal): "ok" | "neutral" | "warn" | "risk" | "info" {
  const lifecycle = signal.trade_lifecycle;
  if (lifecycle === null) return "neutral";
  if (lifecycle.trade_status === "failed" || lifecycle.trade_status === "reconcile_required") return "risk";
  if (lifecycle.incomplete_reasons.length > 0) return "risk";
  if (lifecycle.trade_status === "closed") return "ok";
  if (lifecycle.trade_status === "closing") return "warn";
  if (lifecycle.trade_status === "open") return "info";
  return "neutral";
}

function lifecycleLabel(signal: FiredSignal): string {
  const lifecycle = signal.trade_lifecycle;
  if (lifecycle === null) return signal.funding_status === "funded" ? "Trade unavailable" : "No funded trade";
  return lifecycle.trade_status?.replaceAll("_", " ") ?? "Trade unavailable";
}

function StrategyActivity({
  response,
  strategyTitles,
  onRetry,
}: {
  response: FiredSignalsResponse;
  strategyTitles: Record<string, string>;
  onRetry: () => void;
}) {
  const [olderItems, setOlderItems] = useState<FiredSignal[]>([]);
  const [nextCursor, setNextCursor] = useState(response.next_cursor);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreFailed, setLoadMoreFailed] = useState(false);

  useEffect(() => {
    setOlderItems([]);
    setNextCursor(response.next_cursor);
    setLoadMoreFailed(false);
  }, [response]);

  const items = [...response.items, ...olderItems];

  async function loadMore() {
    if (nextCursor === null || loadingMore) return;
    setLoadingMore(true);
    setLoadMoreFailed(false);
    try {
      const page = await fetchFiredSignals(nextCursor);
      const seen = new Set(items.map((item) => item.signal_id));
      setOlderItems((current) => [...current, ...page.items.filter((item) => !seen.has(item.signal_id))]);
      setNextCursor(page.next_cursor);
    } catch (error) {
      console.error("Strategy activity pagination failed:", error);
      setLoadMoreFailed(true);
    } finally {
      setLoadingMore(false);
    }
  }

  return (
    <section className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-3 px-5 py-4">
        <div>
          <h2 className="text-sm font-semibold">Generated trade activity</h2>
          <p className="mt-1 max-w-3xl text-xs text-slate-500">
            Fired signals through allocation and demo execution. The final column is the rule-defined signal outcome, not realised broker P&amp;L. Rejected decisions are retained as the unfunded shadow record; they never created an order.
          </p>
        </div>
        <button type="button" onClick={onRetry} className="min-h-10 border border-slate-300 px-3 text-xs font-medium hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800">
          Refresh activity
        </button>
      </div>
      {items.length === 0 ? (
        <div className="border-t border-slate-200 px-5 py-5 dark:border-slate-800">
          <EmptyState title="No generated decisions yet" description="Fired entry and exit signals will appear here after the daily strategy scan." />
        </div>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[88rem]">
              <thead className="border-t border-slate-200 text-left text-[10px] font-semibold uppercase tracking-wider text-slate-500 dark:border-slate-800">
                <tr>
                  <th className="px-4 py-2">Signal</th>
                  <th className="px-4 py-2">Strategy</th>
                  <th className="px-4 py-2">Allocation</th>
                  <th className="px-4 py-2">Execution</th>
                  <th className="px-4 py-2">Trade lifecycle</th>
                  <th className="px-4 py-2 text-right">Model / actual fill</th>
                  <th className="px-4 py-2 text-right">Broker close / P&amp;L</th>
                  <th className="px-4 py-2 text-right">Signal outcome</th>
                </tr>
              </thead>
              <tbody>
                {items.map((signal) => {
                  const funding = fundingPresentation(signal);
                  const operationalLifecycleReasons = (signal.trade_lifecycle?.incomplete_reasons ?? []).filter(
                    (reason) => reason.startsWith("entry_order_") || reason.startsWith("position_operation_"),
                  );
                  return (
                    <tr key={signal.signal_id} className="border-t border-slate-200 align-top dark:border-slate-800">
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <strong className="text-sm">{signal.symbol}</strong>
                          <Badge tone="neutral">{signal.signal_kind}</Badge>
                        </div>
                        <span className="mt-1 block text-xs text-slate-500">Signal #{signal.signal_id} · {formatDate(signal.signal_bar_date)}</span>
                      </td>
                      <td className="px-4 py-3 text-xs">
                        <span className="block font-medium text-slate-800 dark:text-slate-100">{strategyTitles[signal.strategy_id] ?? signal.strategy_id}</span>
                        <span className="text-slate-500">{shortVersion(signal.strategy_version)} · {signal.universe}</span>
                      </td>
                      <td className="px-4 py-3 text-xs">
                        <Badge tone={funding.tone}>{funding.label}</Badge>
                        <span className="mt-1 block max-w-64 text-slate-500">{refusalLabel(signal.funding_reason)}</span>
                        {signal.funded_amount !== null ? <span className="mt-1 block tabular-nums">{money(signal.funded_amount)} assigned</span> : null}
                      </td>
                      <td className="px-4 py-3 text-xs">
                        <span className="capitalize">{executionLabel(signal)}</span>
                        <span className="mt-1 block text-slate-500">{signal.strategy_trade_id === null ? "No strategy trade" : `Trade #${signal.strategy_trade_id}`}</span>
                      </td>
                      <td className="px-4 py-3 text-xs">
                        <Badge tone={lifecycleTone(signal)}><span className="capitalize">{lifecycleLabel(signal)}</span></Badge>
                        {signal.trade_lifecycle ? (
                          <>
                            <span className="mt-1 block text-slate-500">
                              {signal.trade_lifecycle.broker_position_id !== null
                                ? `Position #${signal.trade_lifecycle.broker_position_id} · ${signal.trade_lifecycle.ownership_status}`
                                : signal.trade_lifecycle.ownership_count > 1
                                  ? `${signal.trade_lifecycle.ownership_count} ownership records · ambiguous`
                                  : "Broker position unavailable"}
                            </span>
                            {signal.trade_lifecycle.latest_operation_type ? (
                              <>
                                <span className="mt-1 block capitalize">
                                  {signal.trade_lifecycle.latest_operation_type.replaceAll("_", " ")} · {signal.trade_lifecycle.latest_operation_status?.replaceAll("_", " ") ?? "state unavailable"}
                                </span>
                                <span className="block text-slate-500">
                                  {signal.trade_lifecycle.latest_operation_id === null ? "Operation identity unavailable" : `Operation #${signal.trade_lifecycle.latest_operation_id}`}
                                  {signal.trade_lifecycle.latest_operation_order_id === null ? "" : ` · order #${signal.trade_lifecycle.latest_operation_order_id}`}
                                </span>
                              </>
                            ) : null}
                            {signal.trade_lifecycle.latest_operation_trigger ? <span className="block text-slate-500">{refusalLabel(signal.trade_lifecycle.latest_operation_trigger)}</span> : null}
                            {signal.trade_lifecycle.latest_operation_error ? <span className="block text-red-700 dark:text-red-300">{refusalLabel(signal.trade_lifecycle.latest_operation_error)}</span> : null}
                            {signal.trade_lifecycle.latest_reconciliation_state ? (
                              <span className="block text-slate-500 capitalize">
                                Reconciliation {signal.trade_lifecycle.latest_reconciliation_state.replaceAll("_", " ")}
                                {signal.trade_lifecycle.latest_reconciliation_broker_status ? ` · broker ${signal.trade_lifecycle.latest_reconciliation_broker_status.replaceAll("_", " ")}` : ""}
                              </span>
                            ) : null}
                            {signal.trade_lifecycle.latest_reconciliation_error ? <span className="block text-red-700 dark:text-red-300">{refusalLabel(signal.trade_lifecycle.latest_reconciliation_error)}</span> : null}
                            {operationalLifecycleReasons.length > 0 ? (
                              <span className="block text-red-700 dark:text-red-300">{operationalLifecycleReasons.map(refusalLabel).join(" · ")}</span>
                            ) : null}
                          </>
                        ) : null}
                      </td>
                      <td className="px-4 py-3 text-right text-xs tabular-nums">
                        <span className="block">{money(signal.fill_price)}</span>
                        <span className="text-slate-500">{signal.actual_fill_price === null ? "Actual —" : `${money(signal.actual_fill_price)} actual`}</span>
                        {signal.slippage_pct !== null ? <span className="block text-slate-500">{pctPoints(signal.slippage_pct)} slippage</span> : null}
                      </td>
                      <td className="px-4 py-3 text-right text-xs">
                        {signal.trade_lifecycle === null ? (
                          <span className="text-slate-500">Not applicable</span>
                        ) : signal.trade_lifecycle.close_history_status === "not_applicable" ? (
                          <span className="text-slate-500">No broker position opened</span>
                        ) : signal.trade_lifecycle.close_history_status === "not_closed" ? (
                          <span className="text-slate-500">No broker close yet</span>
                        ) : signal.trade_lifecycle.close_history_status === "complete" ? (
                          <>
                            <span className="block">{signal.trade_lifecycle.close_event_count ?? 0} close event{signal.trade_lifecycle.close_event_count === 1 ? "" : "s"}</span>
                            <span className="mt-1 block font-semibold tabular-nums">{money(signal.trade_lifecycle.realised_pnl_usd)} realised</span>
                            <span className="block text-slate-500 tabular-nums">{money(signal.trade_lifecycle.observed_fees_usd)} fees</span>
                          </>
                        ) : (
                          <>
                            <span className="block text-red-700 dark:text-red-300">Close history {signal.trade_lifecycle.close_history_status}</span>
                            <span className="mt-1 block max-w-64 text-slate-500">{signal.trade_lifecycle.incomplete_reasons.map(refusalLabel).join(" · ")}</span>
                          </>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right text-xs">
                        <span className="block capitalize">{outcomeLabel(signal)}</span>
                        <span className="mt-1 block tabular-nums">{pctPoints(signal.gross_return_pct)}</span>
                        {signal.outcome_reason ? <span className="block text-slate-500">{refusalLabel(signal.outcome_reason)}</span> : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-between gap-3 border-t border-slate-200 px-5 py-3 dark:border-slate-800">
            <span className="text-xs text-slate-500">{items.length} decision{items.length === 1 ? "" : "s"} shown</span>
            {nextCursor !== null ? (
              <button type="button" disabled={loadingMore} onClick={() => void loadMore()} className="min-h-10 border border-slate-300 px-3 text-xs font-medium disabled:opacity-50 dark:border-slate-700">
                {loadingMore ? "Loading…" : loadMoreFailed ? "Retry older activity" : "Load older activity"}
              </button>
            ) : null}
          </div>
          {loadMoreFailed ? <p role="alert" className="px-5 pb-3 text-xs text-red-700 dark:text-red-300">Older activity could not be loaded. Current rows remain unchanged.</p> : null}
        </>
      )}
    </section>
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
  // This form owns both the paper-pool switch and the system-wide automatic
  // trading flag through one atomic endpoint. Requiring execution_enabled here
  // creates a circular first-enable gate: the flag can never become true
  // because the control that changes it is disabled. The server independently
  // rechecks evidence readiness, the demo boundary, and live-off state on every enable.
  const canEnable = overview.automation_readiness.ready
    && overview.demo_connection
    && !overview.live_execution_enabled
    && riskProfile !== "unconfigured";
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
          {!overview.demo_connection
            ? "Paper automation can only be enabled while connected to the demo environment."
            : overview.live_execution_enabled
              ? "Turn off system-wide live trading before enabling paper automation."
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
        <StrategyCatalogFacts strategy={strategy} arm={representativeArm(strategy)} />
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
  // ⚠ `trade_count` IS the resolved count and always was. `backtest_run` appends
  // to `book.returns` only inside `if realised`, and `trade_count = len(net_returns)`,
  // so open and unpriced positions were never summands — they are reported
  // ALONGSIDE it, never inside it. Subtracting them double-counted the exclusion
  // and understated the headline on 300 of the 324 stored rows, by up to 2,296
  // trades. Measured with:
  //   select count(*) filter (where open_trade_count + unpriced_trade_count > 0),
  //          max(open_trade_count + unpriced_trade_count)
  //   from strategy_results_store;
  const resolved = arm.trade_count;
  return (
    <div className="border-t border-slate-200 px-4 py-4 dark:border-slate-800">
      <div className="grid gap-5 lg:grid-cols-[1.3fr_1fr]">
        <div>
          <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-500">Primary evidence</h4>
          <p className="mt-1 text-xs text-slate-500">{window.label} · {formatDate(window.window_start)}–{formatDate(window.window_end)} · pessimistic execution arm</p>
          <dl className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
            <div>
              <dt className="text-xs text-slate-500">Trades</dt>
              <dd className="font-semibold tabular-nums">{formatNumber(resolved, 0)}</dd>
              <dd className="text-[10px] text-slate-500">{formatNumber(arm.open_trade_count, 0)} open, {formatNumber(arm.unpriced_trade_count, 0)} unpriced excluded</dd>
            </div>
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

const PROMOTION_ACTION_LABEL = {
  register_candidate: "Register candidate",
  validate_historical: "Approve historical evidence",
  start_forward_observation: "Start forward observation",
  approve_paper: "Approve for paper trading",
} as const;

type PaperSetupValues = {
  capital_limit: string;
  ticket_sizing_mode: "percent" | "fixed";
  ticket_value: string;
  max_ticket_amount: string;
  stop_loss_pct: string;
  take_profit_pct: string;
  max_quote_age_seconds: string;
  max_scan_age_seconds: string;
  max_halt_feed_age_seconds: string;
  max_cost_age_seconds: string;
  max_reconciliation_age_seconds: string;
  max_instrument_exposure_pct: string;
  max_portfolio_exposure_pct: string;
  max_drawdown_pct: string;
  min_net_expectancy_pct: string;
  cost_stress_multiplier: string;
  reason: string;
};

const EMPTY_PAPER_SETUP: PaperSetupValues = {
  capital_limit: "", ticket_sizing_mode: "percent", ticket_value: "", max_ticket_amount: "",
  stop_loss_pct: "", take_profit_pct: "", max_quote_age_seconds: "", max_scan_age_seconds: "",
  max_halt_feed_age_seconds: "", max_cost_age_seconds: "", max_reconciliation_age_seconds: "",
  max_instrument_exposure_pct: "", max_portfolio_exposure_pct: "", max_drawdown_pct: "",
  min_net_expectancy_pct: "", cost_stress_multiplier: "", reason: "",
};

const PAPER_SETUP_FIELDS: { key: Exclude<keyof PaperSetupValues, "ticket_sizing_mode" | "reason">; label: string; step?: string }[] = [
  { key: "capital_limit", label: "Strategy capital limit (USD)", step: "0.01" },
  { key: "ticket_value", label: "Per-signal size (% or USD)", step: "0.01" },
  { key: "max_ticket_amount", label: "Hard ticket maximum (USD)", step: "0.01" },
  { key: "stop_loss_pct", label: "Stop loss (%)", step: "0.01" },
  { key: "take_profit_pct", label: "Take profit (%)", step: "0.01" },
  { key: "max_quote_age_seconds", label: "Maximum quote age (seconds)" },
  { key: "max_scan_age_seconds", label: "Maximum scan age (seconds)" },
  { key: "max_halt_feed_age_seconds", label: "Maximum halt-feed age (seconds)" },
  { key: "max_cost_age_seconds", label: "Maximum cost age (seconds)" },
  { key: "max_reconciliation_age_seconds", label: "Maximum reconciliation age (seconds)" },
  { key: "max_instrument_exposure_pct", label: "Maximum instrument exposure (%)", step: "0.01" },
  { key: "max_portfolio_exposure_pct", label: "Maximum portfolio exposure (%)", step: "0.01" },
  { key: "max_drawdown_pct", label: "Maximum drawdown (%)", step: "0.01" },
  { key: "min_net_expectancy_pct", label: "Minimum net expectancy (%)", step: "0.01" },
  { key: "cost_stress_multiplier", label: "Cost stress multiplier", step: "0.01" },
];

function InitialPaperSetup({ strategy, onUpdated }: { strategy: StrategyOverview; onUpdated: () => void }) {
  const [values, setValues] = useState<PaperSetupValues>(EMPTY_PAPER_SETUP);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const complete = Object.entries(values).every(([key, value]) => key === "ticket_sizing_mode" || value.trim());
  async function save(event: FormEvent) {
    event.preventDefault();
    if (!complete || saving) return;
    setSaving(true);
    setError(null);
    try {
      await createStrategyPaperSetup(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        ...values,
        max_quote_age_seconds: Number(values.max_quote_age_seconds),
        max_scan_age_seconds: Number(values.max_scan_age_seconds),
        max_halt_feed_age_seconds: Number(values.max_halt_feed_age_seconds),
        max_cost_age_seconds: Number(values.max_cost_age_seconds),
        max_reconciliation_age_seconds: Number(values.max_reconciliation_age_seconds),
      });
      onUpdated();
    } catch (caught) {
      setError(caught instanceof ApiError ? caught.message : "Initial paper limits were not created.");
    } finally {
      setSaving(false);
    }
  }
  return (
    <details className="border-t border-slate-200 p-4 dark:border-slate-800">
      <summary className="cursor-pointer text-xs font-semibold">Set explicit first paper limits</summary>
      <p className="mt-2 text-xs text-slate-500">Every field is an operator decision. Saving creates a disabled deployment; it does not start trading.</p>
      <form onSubmit={(event) => void save(event)} className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <label className="text-xs">Sizing method<select value={values.ticket_sizing_mode} onChange={(event) => setValues((current) => ({ ...current, ticket_sizing_mode: event.target.value as "percent" | "fixed" }))} className="mt-1 block min-h-10 w-full border border-slate-300 bg-white px-2 dark:border-slate-700 dark:bg-slate-950"><option value="percent">Percent of sleeve</option><option value="fixed">Fixed USD</option></select></label>
        {PAPER_SETUP_FIELDS.map((field) => (
          <label key={field.key} className="text-xs">{field.label}<input type="number" step={field.step ?? "1"} value={values[field.key]} onChange={(event) => setValues((current) => ({ ...current, [field.key]: event.target.value }))} className="mt-1 block min-h-10 w-full border border-slate-300 bg-white px-2 dark:border-slate-700 dark:bg-slate-950" /></label>
        ))}
        <label className="text-xs sm:col-span-2 lg:col-span-3">Operator reason<input value={values.reason} maxLength={1000} onChange={(event) => setValues((current) => ({ ...current, reason: event.target.value }))} className="mt-1 block min-h-10 w-full border border-slate-300 bg-white px-2 dark:border-slate-700 dark:bg-slate-950" /></label>
        <button type="submit" disabled={!complete || saving} className="min-h-10 border border-slate-300 px-3 text-xs font-medium disabled:opacity-40 dark:border-slate-700">{saving ? "Saving explicit limits…" : "Create disabled paper setup"}</button>
      </form>
      {error ? <p className="mt-2 text-xs text-red-700 dark:text-red-300">{error}</p> : null}
    </details>
  );
}

function ResearchCandidate({ strategy, onUpdated }: { strategy: StrategyOverview; onUpdated: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const [reason, setReason] = useState("");
  const [promoting, setPromoting] = useState(false);
  const [promotionError, setPromotionError] = useState<string | null>(null);
  const arm = representativeArm(strategy);
  const validation = validationState(strategy);
  const ci = arm && arm.expectancy_ci_low_pct !== null && arm.expectancy_ci_high_pct !== null
    ? `${pctPoints(arm.expectancy_ci_low_pct)} to ${pctPoints(arm.expectancy_ci_high_pct)}`
    : "—";
  async function promote() {
    if (!strategy.next_promotion_action || !reason.trim()) return;
    setPromoting(true);
    setPromotionError(null);
    try {
      await advanceStrategyPromotion(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        action: strategy.next_promotion_action,
        reason: reason.trim(),
      });
      setReason("");
      onUpdated();
    } catch (error) {
      setPromotionError(error instanceof ApiError ? error.message : "Strategy was not advanced.");
    } finally {
      setPromoting(false);
    }
  }
  return (
    <article className="border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="grid gap-4 p-4 lg:grid-cols-[minmax(16rem,1.5fr)_repeat(3,minmax(7rem,0.7fr))_auto] lg:items-center">
        <div>
          <div className="flex flex-wrap items-center gap-2"><h3 className="text-sm font-semibold">{strategy.title}</h3><Badge tone={validation.tone}>{validation.label}</Badge></div>
          <p className="mt-1 max-w-lg text-xs text-slate-500">{validation.explanation}</p>
          <StrategyCatalogFacts strategy={strategy} arm={arm} />
        </div>
        <div><span className="text-xs text-slate-500">Expected / trade</span><strong className="block tabular-nums">{pctPoints(arm?.expectancy_per_trade_pct ?? null)}</strong><span className="text-[10px] text-slate-500">After modelled costs</span></div>
        <div><span className="text-xs text-slate-500">95% range</span><strong className="block text-xs tabular-nums">{ci}</strong><span className="text-[10px] text-slate-500">Must clear 0%</span></div>
        <div><span className="text-xs text-slate-500">Worst drawdown</span><strong className="block tabular-nums">{pctPoints(arm?.max_drawdown_pct ?? null)}</strong><span className="text-[10px] text-slate-500">Backtest</span></div>
        <button type="button" aria-expanded={expanded} onClick={() => setExpanded((value) => !value)} className="min-h-11 cursor-pointer border border-slate-300 px-3 text-xs hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800">
          {expanded ? "Hide evidence" : "View evidence"}
        </button>
      </div>
      {expanded ? <EvidenceDetail strategy={strategy} /> : null}
      {strategy.next_promotion_action ? (
        <div className="border-t border-slate-200 p-4 dark:border-slate-800">
          <div className="flex flex-wrap items-end gap-3">
            <label className="min-w-64 flex-1 text-xs text-slate-600 dark:text-slate-300">
              Operator reason
              <input value={reason} onChange={(event) => setReason(event.target.value)} maxLength={1000} className="mt-1 block min-h-10 w-full border border-slate-300 bg-white px-2 dark:border-slate-700 dark:bg-slate-950" />
            </label>
            <button type="button" onClick={() => void promote()} disabled={promoting || !reason.trim() || strategy.promotion_refusals.length > 0} className="min-h-10 border border-slate-300 px-3 text-xs font-medium disabled:opacity-40 dark:border-slate-700">
              {promoting ? "Checking evidence…" : PROMOTION_ACTION_LABEL[strategy.next_promotion_action]}
            </button>
          </div>
          {strategy.promotion_refusals.length ? <p className="mt-2 text-xs text-amber-700 dark:text-amber-300">Cannot advance: {strategy.promotion_refusals.map(refusalLabel).join(" · ")}</p> : null}
          {promotionError ? <p className="mt-2 text-xs text-red-700 dark:text-red-300">{promotionError}</p> : null}
          <p className="mt-2 text-[10px] text-slate-500">The server selects and pins the complete evidence bundle. This control cannot enable live trading.</p>
        </div>
      ) : null}
      {strategy.stage === "paper_enabled" && strategy.allocation_refusals.includes("execution_policy_missing") ? <InitialPaperSetup strategy={strategy} onUpdated={onUpdated} /> : null}
    </article>
  );
}

function ValidationControl({ strategy }: { strategy: StrategyOverview }) {
  const arm = representativeArm(strategy);
  // ⚠ This — NOT `EvidenceDetail` — is where manifest strategies still marked
  // `harness_validation` are rendered (#2624). Putting the rotation notice
  // only on the capital-candidate path would have shipped a
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
          <StrategyCatalogFacts strategy={strategy} arm={arm} />
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

const CONTROLLED_TRIAL_STATE: Record<StrategyControlledTrial["state"], string> = {
  not_run: "Not run",
  structural_refused: "Structural gate refused",
  structural_passed_outcomes_pending: "Structure committed; outcomes pending",
  historical_conjuncts_failed: "Historical conjuncts failed",
  historical_conjuncts_passed: "Historical conjuncts passed",
  evidence_inconsistent: "Evidence inconsistent",
};

function ControlledTrialPanel({ trials }: { trials: StrategyControlledTrial[] }) {
  return (
    <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Controlled research trials</h2>
          <p className="mt-1 max-w-3xl text-xs text-slate-500">
            Preregistered historical falsification only. These verdicts are not promotion authority and cannot enable paper or live trading.
          </p>
        </div>
        <Badge tone="neutral">Read only</Badge>
      </div>
      <div className="mt-4 space-y-4">
        {trials.map((trial) => {
          const tone = trial.state === "historical_conjuncts_passed"
            ? "ok"
            : trial.state === "not_run" || trial.state === "structural_passed_outcomes_pending"
              ? "warn"
              : "risk";
          return (
            <article key={`${trial.trial_id}:${trial.strategy_version}`} className="border-t border-slate-200 pt-4 dark:border-slate-800">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <h3 className="text-sm font-medium">MT-1 volatility-managed relative strength</h3>
                  <p className="mt-1 text-xs text-slate-500">Negative control: S-8 range mean reversion</p>
                </div>
                <Badge tone={tone}>{CONTROLLED_TRIAL_STATE[trial.state]}</Badge>
              </div>
              <dl className="mt-3 grid gap-3 text-xs sm:grid-cols-4">
                <div><dt className="text-slate-500">Structural fan</dt><dd className="mt-1 font-medium tabular-nums">{trial.structural_cells}/4 cells</dd></div>
                <div><dt className="text-slate-500">Outcome fan</dt><dd className="mt-1 font-medium tabular-nums">{trial.result_cells.length}/4 cells</dd></div>
                <div><dt className="text-slate-500">Holdout evaluations</dt><dd className="mt-1 font-medium tabular-nums">{trial.holdout_evaluations}</dd></div>
                <div><dt className="text-slate-500">Holdout accesses</dt><dd className="mt-1 font-medium tabular-nums">{trial.holdout_accesses}</dd></div>
              </dl>
              {trial.result_cells.length ? (
                <ul className="mt-3 grid gap-2 text-xs sm:grid-cols-2 lg:grid-cols-4">
                  {trial.result_cells.map((cell) => (
                    <li key={`${cell.ambiguity_arm}:${cell.quarantine_arm}`} className="border border-slate-200 px-3 py-2 dark:border-slate-800">
                      <span className="block text-slate-500">{cell.ambiguity_arm.replaceAll("_", " ")} · {cell.quarantine_arm}</span>
                      <strong className={cell.historical_conjuncts_pass ? "text-emerald-700 dark:text-emerald-300" : "text-red-700 dark:text-red-300"}>
                        {cell.historical_conjuncts_pass ? "Conjuncts passed" : "Conjuncts failed"}
                      </strong>
                    </li>
                  ))}
                </ul>
              ) : null}
              {trial.refusal_detail ? <p className="mt-3 text-xs text-red-700 dark:text-red-300">{trial.refusal_detail}</p> : null}
              {trial.integrity_refusals.length ? (
                <p className="mt-3 text-xs text-red-700 dark:text-red-300">Integrity refusal: {trial.integrity_refusals.join(", ")}</p>
              ) : null}
            </article>
          );
        })}
      </div>
    </section>
  );
}

export function StrategiesPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, []);
  const activity = useAsync(() => fetchFiredSignals(null), []);
  const [closeFor, setCloseFor] = useState<StrategyOwnedPosition | null>(null);
  const [refreshingEvidence, setRefreshingEvidence] = useState(false);
  const [refreshEvidenceError, setRefreshEvidenceError] = useState<string | null>(null);
  const summary = useMemo(() => overview.data ? aggregate(overview.data) : null, [overview.data]);
  const strategyTitles = useMemo(
    () => Object.fromEntries((overview.data?.strategies ?? []).map((strategy) => [strategy.strategy_id, strategy.title])),
    [overview.data],
  );
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

          {activity.loading ? (
            <SectionSkeleton rows={4} />
          ) : activity.error ? (
            <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
              <h2 className="mb-3 text-sm font-semibold">Generated trade activity</h2>
              <SectionError onRetry={activity.refetch} />
            </section>
          ) : activity.data ? (
            <StrategyActivity response={activity.data} strategyTitles={strategyTitles} onRetry={activity.refetch} />
          ) : null}

          {overview.data.controlled_trials.length ? <ControlledTrialPanel trials={overview.data.controlled_trials} /> : null}

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
                      ? researchCandidates.map((strategy) => <ResearchCandidate key={strategy.strategy_id} strategy={strategy} onUpdated={overview.refetch} />)
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
              activity.refetch();
            }}
          />
        </>
      ) : null}
    </div>
  );
}
