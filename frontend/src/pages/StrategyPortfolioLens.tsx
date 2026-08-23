import { useState } from "react";

import { postKillSwitch } from "@/api/config";
import {
  closeStrategyOwnedPosition,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
} from "@/api/strategies";
import type { StrategyOverviewResponse, StrategyOwnedPosition } from "@/api/types";
import { ApiError } from "@/api/client";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { EmptyState } from "@/components/states/EmptyState";
import { OpenStrategyPositions, StrategyCloseModal } from "@/components/strategies/StrategyPositions";
import {
  AccountEvidence,
  AutomationControl,
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

export function StrategyPortfolioLens() {
  const overview = useAsync(fetchStrategyOverview, []);
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
          <StatTile label="Available" value={formatMoney(number(pool.remaining_capital), pool.currency)} hint="To deploy" />
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
        <AutomationControl overview={data} onUpdated={overview.refetch} />
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
