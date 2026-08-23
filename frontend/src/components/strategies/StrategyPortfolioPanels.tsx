import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import { updateStrategyPaperPool } from "@/api/strategies";
import type { StrategyOverviewResponse } from "@/api/types";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { Badge } from "@/components/ui/Badge";
import { formatDate, formatMoney, formatNumber } from "@/lib/format";
import { money, number, pctPoints } from "@/lib/strategyFormat";
import { useChartTheme } from "@/lib/useChartTheme";

/**
 * Panels that belong to the fenced-off pot, moved off the research lens (#2868).
 *
 * The split was cosmetic until these came with it: `StrategiesPage` kept
 * fetching and rendering portfolio performance, the P&L chart, account
 * evidence and the pot's own funding control, so selecting Research
 * re-rendered the whole portfolio dataset and its broker-facing controls
 * underneath the candidate list (Codex ckpt-2 P2).
 */
const RECONCILIATION_COPY: Record<
  StrategyOverviewResponse["account_equity_evidence"]["reconciliation_state"],
  string
> = {
  unavailable: "No official snapshot",
  refused: "Reconciliation refused",
  reconciled: "Reconciled within tolerance",
  diverged: "Diverged beyond tolerance",
};

const RECONCILIATION_TONE: Record<
  StrategyOverviewResponse["account_equity_evidence"]["reconciliation_state"],
  string
> = {
  // Amber = the comparison could not run; the operator waits or repairs an input.
  unavailable: "text-amber-700 dark:text-amber-300",
  refused: "text-amber-700 dark:text-amber-300",
  reconciled: "text-emerald-700 dark:text-emerald-300",
  // Rose = it ran and the books disagree. That is a finding, and it is the only one of
  // the four states that says something is wrong rather than something is missing.
  diverged: "text-rose-700 dark:text-rose-300",
};

const ACCOUNT_EVIDENCE_REASON_LABELS: Record<string, string> = {
  official_account_equity_missing: "Official account equity starts collecting with the next portfolio sync.",
  account_currency_assumed_not_observed: "This snapshot predates observed broker account currency; its currency cannot be trusted.",
  account_currency_not_documented: "The broker reported an account currency that is not documented for this USD-only trading lane.",
  same_day_local_eod_snapshot_missing: "The same-day local end-of-day valuation is missing.",
  // Retired as a refusal in #2602 item 4 — a display currency differing from the account
  // currency is the ordinary configured state. The label stays so any row still carrying
  // the slug renders rather than falling back to the raw text.
  local_eod_currency_mismatch: "The broker account and local valuation currencies do not match.",
  local_eod_valuation_incomplete: "The local valuation is missing at least one price or currency conversion.",
  local_eod_effective_time_unknown: "The effective dates of the local valuation marks were not recorded.",
  account_currency_fx_rate_missing: "No exchange rate converts the local valuation into the broker account currency.",
  official_direct_position_value_not_recorded:
    "This broker snapshot predates the direct-position split, so it cannot be compared like for like.",
  official_direct_short_positions_unvalued:
    "The account holds direct short positions, which the official direct-holding value does not cover.",
  official_pending_orders_outstanding:
    "Orders are pending, so the broker's cash figure is already net of commitments the local ledger has not seen.",
  direct_position_count_mismatch:
    "The broker and the local book disagree on how many direct positions are open.",
  mark_rounding_tolerance_not_recorded:
    "This local valuation predates the recorded rounding allowance, so no tolerance can be applied to it.",
  reconciliation_inputs_out_of_bounds:
    "A stored reconciliation input is outside its safe range; the comparison is refused rather than reported.",
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

export function aggregate(overview: StrategyOverviewResponse) {
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

export function Metric({ label, value, hint }: { label: string; value: string; hint?: string }) {
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


export function AccountEvidence({ overview }: { overview: StrategyOverviewResponse }) {
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
          {/* The residual is dominated by copy-trader mirrors and pending orders, but it
              also absorbs any error on the official side — so it is described as what is
              NOT in the local book, never asserted to BE the non-engine holdings. */}
          {evidence.residual_not_in_local_book !== null && currency !== null ? (
            <div>
              <span className="block text-slate-500">Not in local book</span>
              <strong>{formatMoney(Number(evidence.residual_not_in_local_book), currency)}</strong>
            </div>
          ) : null}
          {/* Three tones, not two. `diverged` is a FINDING — the comparison ran and the
              books disagree — while `refused` and `unavailable` mean it could not run at
              all. Painting them the same amber makes the one state that demands action
              look like the two that demand patience. */}
          <div className={`self-end ${RECONCILIATION_TONE[evidence.reconciliation_state]}`}>
            {RECONCILIATION_COPY[evidence.reconciliation_state]}
            {evidence.difference !== null && evidence.tolerance !== null && currency !== null ? (
              <span className="ml-1 tabular-nums text-slate-500">
                {formatMoney(Number(evidence.difference), currency)} vs{" "}
                {formatMoney(Number(evidence.tolerance), currency)}
              </span>
            ) : null}
          </div>
        </div>
      </div>
      <AccountEvidenceReasons evidence={evidence} />
      <p className="mt-2 text-[10px] uppercase tracking-wider text-slate-400">
        Rule {evidence.reconciliation_rule_version}
      </p>
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

export function PnlChart({ history }: { history: Array<{ date: string; total_pnl: string | null }> }) {
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

export function EmptyPnlChart() {
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

export function AutomationControl({
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
  const [approvalMode, setApprovalMode] = useState(pool.approval_mode);
  const [riskProfile, setRiskProfile] = useState(pool.mandate.risk_profile);
  const [saving, setSaving] = useState(false);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    setEnabled(pool.enabled && overview.execution_enabled);
    setLimit(pool.capital_limit);
    setCapitalMode(pool.capital_mode);
    setApprovalMode(pool.approval_mode);
    setRiskProfile(pool.mandate.risk_profile);
  }, [
    pool.enabled,
    pool.capital_limit,
    pool.capital_mode,
    pool.approval_mode,
    pool.mandate.risk_profile,
    overview.execution_enabled,
  ]);
  const parsed = Number(limit);
  const valid = Number.isFinite(parsed) && parsed >= 0 && (!enabled || parsed > 0);
  const effectiveEnabled = pool.enabled && overview.execution_enabled;
  // #2843. An unconfigured mandate cannot carry a policy approver, so an
  // `unconfigured` selection means `manual` whatever the approval select last held.
  // DERIVED, not reset in an effect: an effect rewriting `approvalMode` would
  // clobber the operator's own selection the moment they picked a profile again,
  // and it would need the previous value to undo itself. The server owns the rule
  // (`configure_paper_pool`, mapped to a 409) — this only stops the form OFFERING
  // an invalid pair and then surfacing a raw 409 for it.
  const effectiveApprovalMode = riskProfile === "unconfigured" ? "manual" : approvalMode;
  const dirty = enabled !== effectiveEnabled
    || parsed !== Number(pool.capital_limit)
    || capitalMode !== pool.capital_mode
    || effectiveApprovalMode !== pool.approval_mode
    || riskProfile !== pool.mandate.risk_profile;
  // ⚠ `overview.execution_enabled` is this form's OUTPUT, never its input
  // (#2766). `PUT /strategies/paper-pool` writes `runtime_config
  // .enable_auto_trading` and the pool's `enabled` to the SAME value in one
  // transaction (`app/api/strategies.py::update_strategy_paper_pool`), and no
  // other control in the app sets that flag. Requiring it here made the first
  // enable unreachable from the repository's fail-closed default — the page
  // demanded the flag be true before it would call the only endpoint that
  // turns it true. The backend's own first-enable gate is `readiness.ready`,
  // which is what this mirrors. `enable_live_trading` is untouched by the flow.
  const accountEligible = overview.demo_connection || overview.live_strategy_activation_available;
  const canEnable = overview.automation_readiness.ready && accountEligible && riskProfile !== "unconfigured";
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
        approval_mode: effectiveApprovalMode,
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
          {/* #2843. This selects WHO may approve a stage promotion, never WHAT
              qualifies — every evidence bar is identical under both values.
              `autonomous` needs a configured risk profile, mirroring the
              server's own refusal rather than restating its reasoning. */}
          <label className="w-56 text-xs font-medium text-slate-600 dark:text-slate-300">
            Promotion approval
            <select
              value={effectiveApprovalMode}
              onChange={(event) => setApprovalMode(event.target.value as "manual" | "autonomous")}
              className="mt-1 min-h-11 w-full border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
            >
              <option value="manual">Operator approves each stage</option>
              <option value="autonomous" disabled={riskProfile === "unconfigured"}>
                Approve on evidence, hands off
              </option>
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
          {!accountEligible
            ? "This account cannot run strategy automation. Connect the demo account, or complete real-money activation first."
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
