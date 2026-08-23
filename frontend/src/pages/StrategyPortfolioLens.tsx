import { useState } from "react";
import { Link } from "react-router-dom";

import { fetchStrategyOverview, fetchStrategyOwnedPositions, fetchStrategyPnlHistory } from "@/api/strategies";
import type { StrategyOwnedPosition, StrategyPortfolioMandate } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { EmptyState } from "@/components/states/EmptyState";
import { OpenStrategyPositions, StrategyCloseModal } from "@/components/strategies/StrategyPositions";
import {
  AccountEvidence,
  aggregate,
  AutomationControl,
  EmptyPnlChart,
  Metric,
  PnlChart,
} from "@/components/strategies/StrategyPortfolioPanels";
import { Badge } from "@/components/ui/Badge";
import { formatMoney, formatNumber, formatPct } from "@/lib/format";
import { strategyPortfolioStatus } from "@/lib/strategyPortfolioStatus";
import { useAsync } from "@/lib/useAsync";

/**
 * The fenced-off pot (#2868) — the lens the operator opens.
 *
 * Answers three questions in this order: is it trading, how much is in it, and
 * what does it hold. Everything about WHICH strategies might one day earn the
 * capital lives on the research lens; this page deliberately never mentions
 * evidence windows, walk-forward splits or regime breakdowns.
 *
 * ⚠ Status leads, money follows (operator decision 2026-08-23). The honest
 * answer is "not trading" today and will be for a while, and the reasons are
 * a chain — so a row of zeroed money tiles above the reason is a page that
 * makes the operator hunt for the one sentence that explains all of them.
 *
 * Editorial chrome only: `StatTile` hairlines, no bounded cards. The page this
 * was split out of used `border … bg-white p-5` throughout, which contradicts
 * the settled design-system v1 surface model — not carried over.
 */
function number(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function MandateLine({ mandate }: { mandate: StrategyPortfolioMandate }) {
  if (!mandate.configured) {
    return <p className="text-sm text-slate-500">No risk mandate set — the pot has no drawdown or position limits.</p>;
  }
  const limits = [
    mandate.target_volatility_pct === null ? null : `${mandate.target_volatility_pct}% target vol`,
    mandate.max_portfolio_drawdown_pct === null ? null : `${mandate.max_portfolio_drawdown_pct}% max drawdown`,
    mandate.max_concurrent_positions === null ? null : `${mandate.max_concurrent_positions} positions max`,
  ].filter((part): part is string => part !== null);
  return (
    <p className="text-sm text-slate-600 dark:text-slate-300">
      <span className="font-medium capitalize">{mandate.risk_profile}</span>
      {limits.length ? <span className="text-slate-500"> — {limits.join(" · ")}</span> : null}
    </p>
  );
}

export function StrategyPortfolioLens() {
  const overview = useAsync(fetchStrategyOverview, []);
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const [closeFor, setCloseFor] = useState<StrategyOwnedPosition | null>(null);

  if (overview.loading) return <SectionSkeleton rows={6} />;
  if (overview.error || !overview.data) return <SectionError onRetry={overview.refetch} />;

  const data = overview.data;
  const status = strategyPortfolioStatus(data);
  const summary = aggregate(data);
  const pool = data.paper_pool;
  const positions = ownedPositions.data?.positions ?? [];

  return (
    <div className="space-y-8">
      <section aria-labelledby="pot-status">
        <div className="flex flex-wrap items-center gap-3">
          <h2 id="pot-status" className="text-lg font-semibold">
            {status.headline}
          </h2>
          <Badge tone={status.tone}>{status.trading ? "live" : "halted"}</Badge>
        </div>
        {status.blockers.length ? (
          <>
            <p className="mt-2 text-sm text-slate-500">
              Each step below is only reachable once the one above it is cleared.
            </p>
            <ol aria-label="Reasons the pot is not trading" className="mt-3 space-y-2">
              {status.blockers.map((blocker, index) => (
                // The key pairs kind with label: `entries_blocked` is emitted once per
                // backend reason, so several blockers can share a key and React would
                // then reuse or drop a row on the next payload (Codex ckpt-2).
                <li key={`${blocker.key}:${blocker.label}`} className="flex gap-3 border-t border-slate-200 pt-2 text-sm dark:border-slate-800">
                  <span className="tabular-nums text-slate-400">{index + 1}</span>
                  <span>
                    <span className="text-slate-800 dark:text-slate-100">{blocker.label}</span>
                    {blocker.detail ? (
                      <span className="mt-0.5 block text-xs text-slate-500">{blocker.detail}</span>
                    ) : null}
                  </span>
                </li>
              ))}
            </ol>
          </>
        ) : (
          <p className="mt-2 text-sm text-slate-500">Capital is assigned and at least one strategy may trade it.</p>
        )}
      </section>

      <section aria-labelledby="pot-money">
        <h2 id="pot-money" className="sr-only">
          Pot
        </h2>
        <div className="grid grid-cols-2 gap-x-6 lg:grid-cols-4">
          <StatTile label="Assigned" value={formatMoney(number(pool.effective_capital), pool.currency)} hint={pool.capital_mode === "compound" ? "Expanding" : "Fixed"} />
          <StatTile label="Invested" value={formatMoney(number(pool.invested_capital), pool.currency)} />
          {/* The overview's own count, NOT positions.length: the positions request
              is separate, so while it is pending or failed the list is empty and a
              tile reading 0 would be a false statement sitting next to an error
              (Codex ckpt-2). Both numbers come from strategy-owned trades. */}
          <StatTile label="Open positions" value={formatNumber(summary.activePositions, 0)} />
          <StatTile label="Remaining" value={formatMoney(number(pool.remaining_capital), pool.currency)} hint="Assigned less invested" />
        </div>
        <div className="mt-4 border-t border-slate-200 pt-3 dark:border-slate-800">
          <p className="text-xs uppercase tracking-wide text-slate-500">Mandate</p>
          <div className="mt-1">
            <MandateLine mandate={pool.mandate} />
          </div>
        </div>
      </section>

      <section aria-labelledby="pot-performance">
        <h2 id="pot-performance" className="text-sm font-semibold">
          Portfolio performance
        </h2>
        <p className="mt-1 text-xs text-slate-500">Automated positions only; research backtests are excluded.</p>
        {/* Average/trade and success rate are the pot's own record. They came
            across with the section rather than being dropped in the split. */}
        <div className="mt-4 grid grid-cols-2 gap-5 sm:grid-cols-3">
          <Metric label="Total P&L" value={formatMoney(summary.totalPnl, "USD")} hint="Realised + open" />
          <Metric label="Average / trade" value={formatPct(summary.averageReturn)} hint="Completed outcomes" />
          <Metric
            label="Success rate"
            value={formatPct(summary.successRate)}
            hint={`${formatNumber(summary.resolved, 0)} completed`}
          />
        </div>
        {pnlHistory.loading ? (
          <div className="flex h-52 items-center justify-center text-xs text-slate-500">Loading P&amp;L history…</div>
        ) : pnlHistory.error ? (
          <SectionError onRetry={pnlHistory.refetch} />
        ) : pnlHistory.data?.points.length ? (
          <>
            <PnlChart history={pnlHistory.data.points} />
            <p className="mt-2 text-xs text-slate-500">
              Daily realised plus open P&amp;L from exact automated positions; manual positions are excluded.
              Gaps mean an owned mark or close could not reconcile.
            </p>
          </>
        ) : (
          <EmptyPnlChart />
        )}
        <AccountEvidence overview={data} />
      </section>

      <AutomationControl overview={data} onUpdated={overview.refetch} />

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
          <EmptyState
            title="Nothing held"
            description="This pot has never opened a position. Strategy-owned trades appear here once capital is assigned and a strategy fires."
          />
        ) : null}
      </section>

      <p className="text-sm text-slate-500">
        Candidate strategies and their evidence live on the{" "}
        <Link className="underline" to="/strategies?view=research">
          research lens
        </Link>
        .
      </p>

      <StrategyCloseModal
        position={closeFor}
        onRequestClose={() => setCloseFor(null)}
        onAccepted={() => {
          setCloseFor(null);
          void ownedPositions.refetch();
          void overview.refetch();
          // The chart is on this lens now, so it is this callback's job to
          // refresh it — otherwise it keeps painting the pre-close valuation
          // until a reload (Codex ckpt-2).
          void pnlHistory.refetch();
        }}
      />
    </div>
  );
}
