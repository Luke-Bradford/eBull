import { useState } from "react";

import { fetchBudget } from "@/api/budget";
import { fetchPortfolio } from "@/api/portfolio";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchSystemStatus } from "@/api/system";
import { fetchWatchlist, removeFromWatchlist } from "@/api/watchlist";
import { useConfig } from "@/lib/ConfigContext";
import { useAsync } from "@/lib/useAsync";
import { ErrorBanner } from "@/components/states/ErrorBanner";
import { AlertsStrip } from "@/components/dashboard/AlertsStrip";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { PortfolioValueChart } from "@/components/dashboard/PortfolioValueChart";
import { RollingPnlStrip } from "@/components/dashboard/RollingPnlStrip";
import { SummaryCards } from "@/components/dashboard/SummaryCards";
import { PositionsTable } from "@/components/dashboard/PositionsTable";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { RecentRecommendations } from "@/components/dashboard/RecentRecommendations";
import { BootstrapProgress, isBootstrapping } from "@/components/dashboard/BootstrapProgress";
import { WatchlistPanel } from "@/components/dashboard/WatchlistPanel";

/**
 * Operator dashboard — command center (#315 Phase 1).
 *
 * Landing view answers "how's my fund today + what needs me next 5 min?"
 * Ops-health panels (sync layers, job runs) live on /admin and no longer
 * clutter the dashboard. Budget rolls into the summary strip; the old
 * standalone Budget section retired for the same reason — Deployment
 * card already surfaces the same number.
 *
 * Each endpoint owns its request lifecycle; a partial failure leaves
 * unrelated panels rendering. The page-level banner only fires when
 * every data source failed.
 *
 * Layout:
 *   ┌ SummaryCards (AUM · Cash · P&L · Deployment) ┐  ← portfolio-gated
 *   │ RollingPnlStrip (1d · 1w · 1m)               │  ← portfolio-gated
 *   │ PortfolioValueChart                          │  ← portfolio-gated
 *   │ Positions                                    │  ← portfolio-gated
 *   │──────────────────────────────────────────────│
 *   │ AlertsStrip (guard rejections)               │  ← self-isolating, always mounted
 *   │ Needs action (proposed recs)                 │
 *   │                                              │
 *   │ Watchlist                                    │
 *   └──────────────────────────────────────────────┘
 *
 * Note: AlertsStrip is intentionally outside the portfolio-gated block so
 * a /portfolio failure never suppresses guard-rejection alerts. RollingPnlStrip
 * and PortfolioValueChart remain portfolio-gated (pre-existing behaviour from
 * Phase 2); their isolation is tracked as tech-debt.
 */
export function DashboardPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  // "Needs action" = recommendations the operator hasn't triaged yet.
  // Filter to status=proposed so executed / rejected rows don't
  // crowd the decision queue.
  const recs = useAsync(
    () =>
      fetchRecommendations(
        { action: null, status: "proposed", instrument_id: null },
        0,
        10,
      ),
    [],
  );
  // `system` + `config` are kept so BootstrapProgress can detect a
  // first-run install, and so the all-endpoints-failed banner fires
  // when the backend is unreachable. The System status panel itself
  // moved to /admin in Phase 1.
  const system = useAsync(fetchSystemStatus, []);
  const config = useConfig();
  const budget = useAsync(fetchBudget, []);
  const watchlist = useAsync(fetchWatchlist, []);
  const [watchlistError, setWatchlistError] = useState<string | null>(null);

  const handleRemove = async (symbol: string) => {
    setWatchlistError(null);
    try {
      await removeFromWatchlist(symbol);
      watchlist.refetch();
    } catch (err) {
      const message =
        err instanceof Error ? err.message : `Failed to remove ${symbol}`;
      setWatchlistError(message);
      watchlist.refetch();
    }
  };

  const allFailed =
    portfolio.error !== null &&
    recs.error !== null &&
    system.error !== null &&
    config.error !== null &&
    budget.error !== null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Dashboard</h1>
      </div>

      {allFailed ? (
        <ErrorBanner message="The API is unreachable. Check that the backend is running and the auth token is configured." />
      ) : null}

      {!system.loading && system.data !== null && isBootstrapping(system.data) ? (
        <BootstrapProgress system={system.data} />
      ) : null}

      {portfolio.error !== null ? (
        // Single error surface covers both SummaryCards and Positions —
        // they share the `/portfolio` fetch so duplicating the retry
        // affordance would just confuse the operator (Codex #387
        // review).
        <div className="border-t border-slate-200 pt-3">
          <SectionError onRetry={portfolio.refetch} />
        </div>
      ) : (
        <>
          <SummaryCards
            data={portfolio.loading ? null : portfolio.data}
            budgetData={budget.loading || budget.error !== null ? null : budget.data}
            budgetError={budget.error !== null}
          />
          {/* Rolling P&L: 1d/1w/1m pills showing short-horizon
              movement. Hidden on error so a failed rolling fetch
              doesn't blank the rest of the page. */}
          <RollingPnlStrip />
          {/* Portfolio value over time (positions + cash). Mounted
              under the pills so the operator sees the cockpit row
              (totals → short-horizon delta → long-horizon trajectory)
              before scrolling into the action queue. */}
          <PortfolioValueChart />
          <Section title="Positions">
            {portfolio.loading ? (
              <SectionSkeleton rows={4} />
            ) : (
              <LiveQuoteProvider
                instrumentIds={portfolio.data?.live_quote_instrument_ids ?? []}
              >
                <PositionsTable
                  positions={portfolio.data?.positions ?? []}
                  mirrors={portfolio.data?.mirrors ?? []}
                />
              </LiveQuoteProvider>
            )}
          </Section>
        </>
      )}

      {/* Needs-action surface — guard rejections since last visit.
          Self-isolating: handles its own loading/error/empty states.
          Mounted outside the portfolio-gated branch so a failed
          /portfolio fetch never suppresses guard-rejection alerts. */}
      <AlertsStrip />

      <Section
        title={`Needs action${recs.data ? ` · ${recs.data.total}` : ""}`}
      >
        {recs.loading ? (
          <SectionSkeleton rows={4} />
        ) : recs.error !== null ? (
          <SectionError onRetry={recs.refetch} />
        ) : (
          <RecentRecommendations items={recs.data?.items ?? []} />
        )}
      </Section>

      <Section title={`Watchlist${watchlist.data ? ` · ${watchlist.data.total}` : ""}`}>
        {watchlistError !== null && (
          <div className="mb-2 rounded border border-red-200 bg-red-50 p-2 text-xs text-red-700">
            {watchlistError}
            <button
              type="button"
              className="ml-2 underline"
              onClick={() => setWatchlistError(null)}
            >
              dismiss
            </button>
          </div>
        )}
        {watchlist.loading ? (
          <SectionSkeleton rows={3} />
        ) : watchlist.error !== null ? (
          <SectionError onRetry={watchlist.refetch} />
        ) : (
          <WatchlistPanel
            items={watchlist.data?.items ?? []}
            onRemove={handleRemove}
          />
        )}
      </Section>
    </div>
  );
}
