import { useState } from "react";

import { postKillSwitch } from "@/api/config";
import {
  closeStrategyOwnedPosition,
  fetchCoreSleeve,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
  rebalanceCoreSleeve,
  updateCoreMandate,
} from "@/api/strategies";
import type { CoreSleeveResponse, StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { ApiError } from "@/api/client";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { EmptyState } from "@/components/states/EmptyState";
import { OpenStrategyPositions, StrategyCloseModal } from "@/components/strategies/StrategyPositions";
import {
  AccountEvidence,
  AutomationControl,
  BenchmarkRefusals,
  EmptyPnlChart,
  PnlChart,
} from "@/components/strategies/StrategyPortfolioPanels";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatMoney, formatNumber, formatPct } from "@/lib/format";
import { aggregate } from "@/lib/strategyAggregate";
import { number } from "@/lib/strategyFormat";
import { strategyPortfolioStatus } from "@/lib/strategyPortfolioStatus";
import { useAsync } from "@/lib/useAsync";

/**
 * The fenced-off pot — a control panel, not a status page (#2868, reshaped on
 * operator feedback 2026-08-23: *"This looks more like a wiki, a guide, not a
 * user interface. Toggles and summaries. What can be configured, not
 * narrated."*).
 *
 * The intended use is small: put money in, turn it on, say what happens to
 * profits, and then watch numbers and be able to stop. Everything else is the
 * scripts' job. So the page is ordered controls → numbers → holdings, and
 * anything that cannot be acted on is one line, not a paragraph.
 *
 * ⚠ Blockers are ACTIONS or FACTS, never instructions. A blocker the operator
 * can clear carries its own control (the kill switch); one they cannot (no
 * strategy has earned capital yet) is a single line of fact. The previous
 * version narrated all five as an ordered lesson, which is what the feedback
 * above was about.
 */
function BlockerRow({
  tone,
  label,
  action,
}: {
  tone: "risk" | "warn";
  label: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="flex min-h-11 flex-wrap items-center justify-between gap-3 border-t border-slate-200 py-2 text-sm dark:border-slate-800">
      <span className="flex items-center gap-2">
        {/* Not "halted" — the header badge already says that. These name the
            KIND of blocker: something switched off vs something not set up. */}
        <Badge tone={tone}>{tone === "risk" ? "blocked" : "setup"}</Badge>
        <span className="text-slate-700 dark:text-slate-200">{label}</span>
      </span>
      {action}
    </div>
  );
}

function CoreSleeveControl({
  sleeve,
  busy,
  setBusy,
  onError,
  onUpdated,
}: {
  sleeve: CoreSleeveResponse;
  busy: boolean;
  setBusy: (busy: boolean) => void;
  onError: (message: string | null) => void;
  onUpdated: () => void;
}) {
  const mandate = sleeve.mandate;
  const [enabled, setEnabled] = useState(mandate.enabled ?? false);
  const [target, setTarget] = useState(mandate.core_target_pct ?? "80");
  const [reserve, setReserve] = useState(mandate.liquidity_reserve_pct ?? "10");
  const [band, setBand] = useState(mandate.rebalance_band_pct ?? "5");
  const [minimum, setMinimum] = useState(mandate.min_rebalance_amount ?? "25");
  const [reason, setReason] = useState("");
  const [confirmRebalance, setConfirmRebalance] = useState(false);
  const [outcome, setOutcome] = useState<string | null>(null);
  const policyUpgradeRequired = sleeve.blockers.some(
    (blocker) => blocker.code === "core_mandate_policy_unsupported",
  );
  const mandateDirty =
    policyUpgradeRequired ||
    enabled !== (mandate.enabled ?? false) ||
    Number(target) !== Number(mandate.core_target_pct ?? "80") ||
    Number(reserve) !== Number(mandate.liquidity_reserve_pct ?? "10") ||
    Number(band) !== Number(mandate.rebalance_band_pct ?? "5") ||
    Number(minimum) !== Number(mandate.min_rebalance_amount ?? "25");

  async function saveMandate() {
    setBusy(true);
    onError(null);
    setOutcome(null);
    try {
      await updateCoreMandate({
        enabled,
        core_instrument_id: sleeve.selected_instrument_id ?? mandate.core_instrument_id ?? null,
        core_target_pct: target,
        liquidity_reserve_pct: reserve,
        rebalance_band_pct: band,
        min_rebalance_amount: minimum,
        reason: reason.trim(),
        provider: "etoro",
        environment: "demo",
      });
      setReason("");
      onUpdated();
    } catch (error) {
      onError(error instanceof ApiError ? error.message : "The core mandate could not be saved.");
    } finally {
      setBusy(false);
    }
  }

  async function rebalance() {
    setBusy(true);
    onError(null);
    setOutcome(null);
    try {
      const result = await rebalanceCoreSleeve();
      const label =
        result.state === "submitted"
          ? "Broker accepted; fill reconciliation is pending."
          : result.state === "submission_uncertain"
            ? "Submission outcome is uncertain; reconciliation is required before retrying."
            : result.reason_code === "core_order_reconciled"
              ? "The existing broker order was reconciled; holdings and fill state have been refreshed."
              : result.state === "held"
                ? "No trade required; the sleeve remains inside its band."
              : `Rebalance refused: ${result.reason_code}.`;
      setOutcome(label);
      onUpdated();
    } catch (error) {
      onError(error instanceof ApiError ? error.message : "The demo rebalance could not be evaluated.");
    } finally {
      setBusy(false);
      setConfirmRebalance(false);
    }
  }

  return (
    <>
      <div className="mt-5 border-t border-slate-200 pt-4 dark:border-slate-800">
        {!sleeve.can_configure ? (
          <p className="mb-3 text-xs text-slate-500">
            Save these values as a disabled draft now. Enabling and demo rebalancing remain locked until the core instrument passes #2833.
          </p>
        ) : null}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {[
            ["Core target %", target, setTarget],
            ["Cash reserve %", reserve, setReserve],
            ["Rebalance band %", band, setBand],
            ["Minimum amount", minimum, setMinimum],
          ].map(([label, value, setter]) => (
            <label key={label as string} className="text-xs font-medium text-slate-600 dark:text-slate-300">
              {label as string}
              <input
                type="number"
                min="0"
                step="0.01"
                value={value as string}
                onChange={(event) => (setter as (value: string) => void)(event.target.value)}
                className="mt-1 min-h-11 w-full rounded-md border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
              />
            </label>
          ))}
        </div>
        <label className="mt-3 flex min-h-11 items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={enabled}
            disabled={!sleeve.can_configure && !enabled}
            onChange={(event) => setEnabled(event.target.checked)}
            className="disabled:cursor-not-allowed disabled:opacity-50"
          />
          Enable demo core sleeve
        </label>
        <label className="mt-3 block text-xs font-medium text-slate-600 dark:text-slate-300">
          Audit reason
          <input
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            className="mt-1 min-h-11 w-full rounded-md border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
          />
        </label>
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            type="button"
            disabled={busy || reason.trim().length === 0 || (enabled && !sleeve.can_configure) || (mandate.configured && !mandateDirty)}
            onClick={() => void saveMandate()}
            className="min-h-11 rounded-md border border-slate-300 px-3 text-sm font-medium disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700"
          >
            {busy ? "Saving…" : "Save mandate"}
          </button>
          <button
            type="button"
            disabled={busy || (!sleeve.can_rebalance && !sleeve.can_resume) || (!sleeve.can_resume && mandateDirty)}
            onClick={() => setConfirmRebalance(true)}
            className="min-h-11 rounded-md bg-sky-700 px-3 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50"
          >
            {sleeve.can_resume ? "Resume demo order" : "Rebalance demo now"}
          </button>
        </div>
      </div>
      {outcome ? <p role="status" className="mt-3 text-sm text-slate-700 dark:text-slate-200">{outcome}</p> : null}
      <Modal isOpen={confirmRebalance} onRequestClose={() => setConfirmRebalance(false)} labelledBy="core-rebalance-title">
        <h2 id="core-rebalance-title" className="text-base font-semibold">
          {sleeve.can_resume ? `Resume demo order ${sleeve.pending_order_id}?` : `Rebalance ${sleeve.selected_symbol} in demo?`}
        </h2>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          {sleeve.can_resume
            ? "The server will look up and reconcile the already-authorised order using its original request ID and account credentials. It cannot create a second order."
            : `The server will size within the ${target}% core mandate and every live guard. This path can buy only; it cannot sell or use alpha signals.`}
        </p>
        <div className="mt-4 flex justify-end gap-2">
          <button type="button" onClick={() => setConfirmRebalance(false)} className="min-h-11 rounded-md border border-slate-300 px-3 text-sm dark:border-slate-700">Cancel</button>
          <button type="button" disabled={busy} onClick={() => void rebalance()} className="min-h-11 rounded-md bg-sky-700 px-3 text-sm font-medium text-white disabled:opacity-50">{busy ? "Evaluating…" : sleeve.can_resume ? "Confirm resume" : "Confirm demo rebalance"}</button>
        </div>
      </Modal>
    </>
  );
}

export function StrategyPortfolioLens() {
  const overview = useAsync(fetchStrategyOverview, []);
  const coreSleeve = useAsync(fetchCoreSleeve, []);
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const [closeFor, setCloseFor] = useState<StrategyOwnedPosition | null>(null);
  const [confirmCloseAll, setConfirmCloseAll] = useState(false);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  if (overview.loading) return <SectionSkeleton rows={6} />;
  if (overview.error || !overview.data) return <SectionError onRetry={overview.refetch} />;

  const data: StrategyOverviewResponse = overview.data;
  const status = strategyPortfolioStatus(data);
  const summary = aggregate(data);
  const pool = data.paper_pool;
  const positions = ownedPositions.data?.positions ?? [];
  /** ⚠ Bulk close operates on CLOSABLE positions only. A row whose trade is
   *  already `closing` disables its own Close button, and resubmitting it makes
   *  the endpoint reject — which, because the loop stops on first failure,
   *  would strand every genuinely open position behind it (Codex ckpt-2). */
  const closable = positions.filter((position) => position.trade_status !== "closing");
  const killActive = data.entry_block.global_kill_active;

  async function clearKillSwitch() {
    setBusy(true);
    setActionError(null);
    try {
      await postKillSwitch({
        active: false,
        reason: "Operator cleared the automated-pot kill switch from the portfolio panel",
        activated_by: "operator",
      });
      await overview.refetch();
    } catch (error) {
      setActionError(error instanceof ApiError ? error.message : "The kill switch could not be cleared.");
    } finally {
      setBusy(false);
    }
  }

  /** Sequential, not parallel: each close is a broker order, and a partial
   *  failure must leave the rest closed rather than racing an unknown number
   *  of submissions. Stops on the first failure and reports what remains. */
  async function closeAll() {
    setBusy(true);
    setActionError(null);
    let closed = 0;
    try {
      for (const position of closable) {
        await closeStrategyOwnedPosition(position.strategy_trade_id, position.broker_position_id);
        closed += 1;
      }
    } catch (error) {
      const detail = error instanceof ApiError ? error.message : "A close request failed.";
      setActionError(`${detail} ${closed} of ${closable.length} closed; the rest are still open.`);
    } finally {
      setBusy(false);
      setConfirmCloseAll(false);
      void ownedPositions.refetch();
      void overview.refetch();
      void pnlHistory.refetch();
    }
  }

  return (
    <div className="space-y-8">
      <section aria-labelledby="pot-state">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 id="pot-state" className="flex items-center gap-2 text-lg font-semibold">
            {status.headline}
            <Badge tone={status.tone}>{status.trading ? "live" : "halted"}</Badge>
          </h2>
          {closable.length > 0 ? (
            <button
              type="button"
              disabled={busy}
              onClick={() => setConfirmCloseAll(true)}
              className="min-h-11 rounded-md border border-rose-300 px-3 text-sm font-medium text-rose-700 hover:bg-rose-50 disabled:opacity-50 dark:border-rose-800 dark:text-rose-300 dark:hover:bg-rose-950/40"
            >
              Close all {closable.length} positions
            </button>
          ) : null}
        </div>

        <div className="mt-4 grid grid-cols-2 gap-x-6 lg:grid-cols-4">
          <StatTile label="Pot" value={formatMoney(number(pool.effective_capital), pool.currency)} hint={pool.capital_mode === "compound" ? "Compounding" : "Fixed limit"} />
          <StatTile
            label="P&L"
            value={formatMoney(summary.totalPnl, pool.currency)}
            hint={formatPct(summary.averageReturn) + " / trade"}
            tone={summary.totalPnl === null || summary.totalPnl === 0 ? "muted" : summary.totalPnl > 0 ? "positive" : "negative"}
            toneHint
          />
          <StatTile label="Open" value={formatNumber(summary.activePositions, 0)} hint={`${formatNumber(summary.approved, 0)} strategies approved`} />
          <StatTile label="Available" value={formatMoney(number(pool.capital_observation_complete === false ? null : pool.remaining_capital), pool.currency)} hint={pool.capital_observation_complete === false ? "Checked at action" : "To deploy"} />
        </div>

        {actionError ? (
          <p role="alert" className="mt-3 text-sm text-rose-700 dark:text-rose-300">
            {actionError}
          </p>
        ) : null}

        {status.blockers.length ? (
          <div className="mt-5" aria-label="Blocking conditions">
            {killActive ? (
              <BlockerRow
                tone="risk"
                label={`Kill switch on — ${data.entry_block.global_kill_reason ?? "no reason recorded"}`}
                action={
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => void clearKillSwitch()}
                    className="min-h-11 rounded-md border border-slate-300 px-3 text-sm font-medium hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
                  >
                    Clear
                  </button>
                }
              />
            ) : null}
            {/* Only what the Setup form below does NOT already show. Capital,
                mandate and the on/off switch are all fields down there, so
                listing them here too is narration of a control the operator can
                already see — which is exactly the feedback this panel answers.
                What stays is what no field can fix: evidence a strategy has
                not earned yet. */}
            {status.blockers
              .filter((blocker) => blocker.key === "no_approved_strategies")
              .map((blocker) => (
                <BlockerRow
                  key={blocker.key}
                  tone="warn"
                  label={blocker.detail ? `${blocker.label} — ${blocker.detail}` : blocker.label}
                />
              ))}
          </div>
        ) : null}
      </section>

      <section aria-labelledby="pot-setup">
        <h2 id="pot-setup" className="sr-only">
          Setup
        </h2>
        <div className="space-y-4">
          {coreSleeve.loading ? <SectionSkeleton rows={3} /> : null}
          {coreSleeve.error ? <SectionError onRetry={coreSleeve.refetch} /> : null}
          {coreSleeve.data ? (
            <section className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <h2 className="text-sm font-semibold">Core &amp; cash</h2>
                  <p className="mt-1 text-xs text-slate-500">
                    Deterministic fallback when no strategy has earned capital.
                  </p>
                </div>
                <Badge tone={coreSleeve.data.state === "ready" ? "ok" : "warn"}>
                  {coreSleeve.data.state === "ready" ? "Ready" : "Cash"}
                </Badge>
              </div>
              <div className="mt-5 grid grid-cols-2 gap-x-6 sm:grid-cols-3">
                <StatTile
                  size="md"
                  label="Evidence"
                  value={`${coreSleeve.data.observed_trading_days} / ${coreSleeve.data.required_trading_days}`}
                  hint="Trading days"
                />
                <StatTile
                  size="md"
                  label="Instrument"
                  value={coreSleeve.data.selected_symbol ?? "Cash"}
                  hint={coreSleeve.data.selected_symbol ? "Evidence-selected" : "No sleeve adopted"}
                />
                <StatTile
                  size="md"
                  label="Cost ceiling"
                  value={`${(coreSleeve.data.max_cost_bps / 100).toFixed(2)}%`}
                  hint="Preregistered #2833 bar"
                />
              </div>
              <div className="mt-4">
                {coreSleeve.data.blockers.map((blocker) => (
                  <BlockerRow key={blocker.code} tone="warn" label={blocker.detail} />
                ))}
              </div>
              <p className="mt-4 border-t border-slate-200 pt-3 text-xs text-slate-500 dark:border-slate-800">
                Demo only · buy only · no alpha signal. {coreSleeve.data.household_tax_caveat}
              </p>
              <CoreSleeveControl
                sleeve={coreSleeve.data}
                busy={busy}
                setBusy={setBusy}
                onError={setActionError}
                onUpdated={() => {
                  void coreSleeve.refetch();
                  void ownedPositions.refetch();
                  void overview.refetch();
                  void pnlHistory.refetch();
                }}
              />
            </section>
          ) : null}
          <AutomationControl
            overview={data}
            coreSleeve={coreSleeve.data}
            onUpdated={() => {
              void overview.refetch();
              void coreSleeve.refetch();
            }}
          />
        </div>
      </section>

      <section aria-labelledby="pot-performance">
        <h2 id="pot-performance" className="text-sm font-semibold">
          Portfolio performance
        </h2>
        {/* The pot's trade record, as numbers rather than sentences. Automated
            positions only; research backtests are excluded by `aggregate`. */}
        <div className="mt-3 grid grid-cols-2 gap-x-6 sm:grid-cols-3">
          <StatTile size="md" label="Total P&L" value={formatMoney(summary.totalPnl, pool.currency)} hint="Realised + open" />
          <StatTile size="md" label="Average / trade" value={formatPct(summary.averageReturn)} hint="Completed outcomes" />
          <StatTile size="md" label="Win rate" value={formatPct(summary.successRate)} hint={`${formatNumber(summary.resolved, 0)} completed`} />
        </div>
        {pnlHistory.loading ? (
          <div className="flex h-52 items-center justify-center text-xs text-slate-500">Loading…</div>
        ) : pnlHistory.error ? (
          <SectionError onRetry={pnlHistory.refetch} />
        ) : pnlHistory.data?.points.length ? (
          <PnlChart history={pnlHistory.data.points} />
        ) : (
          <EmptyPnlChart />
        )}
        {/* #2602 item 5. Sourced from `overview`, not `pnlHistory` — see the
            component. A benchmark that is absent must say so by name in every
            branch above, including the error one. */}
        <BenchmarkRefusals refusals={data.benchmark_refusals} />
        {/* Whether our P&L agrees with the broker's own equity. Kept when the
            page was trimmed to a control panel: it is the reason to trust the
            number above it, not commentary about it. */}
        <AccountEvidence overview={data} />
      </section>

      <section aria-labelledby="pot-holdings">
        <h2 id="pot-holdings" className="sr-only">
          Holdings
        </h2>
        {ownedPositions.loading ? <SectionSkeleton rows={3} /> : null}
        {ownedPositions.error ? <SectionError onRetry={ownedPositions.refetch} /> : null}
        {ownedPositions.data && positions.length > 0 ? (
          <LiveQuoteProvider instrumentIds={ownedPositions.data.live_quote_instrument_ids}>
            <OpenStrategyPositions positions={positions} onClose={setCloseFor} />
          </LiveQuoteProvider>
        ) : null}
        {ownedPositions.data && positions.length === 0 ? (
          <EmptyState title="Nothing held" description="Positions opened by an approved strategy appear here." />
        ) : null}
      </section>

      <StrategyCloseModal
        position={closeFor}
        onRequestClose={() => setCloseFor(null)}
        onAccepted={() => {
          setCloseFor(null);
          void ownedPositions.refetch();
          void overview.refetch();
          void pnlHistory.refetch();
        }}
      />

      <Modal isOpen={confirmCloseAll} onRequestClose={() => setConfirmCloseAll(false)} labelledBy="close-all-title">
          <h2 id="close-all-title" className="text-base font-semibold">
            Close all {closable.length} automated positions?
          </h2>
          <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
            Each is submitted to the broker one at a time. Manual positions are not touched. This
            does not stop the strategies — turn automation off if you also want new entries to stop.
          </p>
          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => setConfirmCloseAll(false)}
              className="min-h-11 rounded-md border border-slate-300 px-3 text-sm dark:border-slate-700"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => void closeAll()}
              className="min-h-11 rounded-md bg-rose-600 px-3 text-sm font-medium text-white hover:bg-rose-700 disabled:opacity-50"
            >
              {busy ? "Closing…" : `Close ${closable.length}`}
            </button>
          </div>
      </Modal>
    </div>
  );
}
