import { fetchBudget } from "@/api/budget";
import { fetchPortfolio } from "@/api/portfolio";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchSystemStatus } from "@/api/system";
import { fetchConfig } from "@/api/config";
import { fetchWatchlist, removeFromWatchlist } from "@/api/watchlist";
import { useAsync } from "@/lib/useAsync";
import { ErrorBanner } from "@/components/states/ErrorBanner";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { SummaryCards } from "@/components/dashboard/SummaryCards";
import { PositionsTable } from "@/components/dashboard/PositionsTable";
import { RecentRecommendations } from "@/components/dashboard/RecentRecommendations";
import { BudgetOverviewPanel } from "@/components/dashboard/BudgetOverviewPanel";
import { SystemStatusPanel } from "@/components/dashboard/SystemStatusPanel";
import { BootstrapProgress, isBootstrapping } from "@/components/dashboard/BootstrapProgress";
import { WatchlistPanel } from "@/components/dashboard/WatchlistPanel";

/**
 * Operator dashboard (#60).
 *
 * Each of the four endpoints owns its own request lifecycle so a partial
 * failure (e.g. /system/status down while /portfolio is fine) does not
 * blank unrelated panels. The top-of-page banner only appears when *all
 * four* requests have failed — that's the "API unreachable" signal from
 * the issue.
 *
 * This page is strictly read-only. The kill switch toggle and any config
 * mutation lives on the admin page (#64) — see SystemStatusPanel.
 */
export function DashboardPage() {
  const portfolio = useAsync(fetchPortfolio, []);
  // The arrow function below is intentionally not memoised: useAsync
  // captures the latest `fn` via a ref (see frontend/src/lib/useAsync.ts),
  // so a new identity per render is harmless and does not trigger refetch.
  const recs = useAsync(
    () => fetchRecommendations({ action: null, status: null, instrument_id: null }, 0, 10),
    [],
  );
  const system = useAsync(fetchSystemStatus, []);
  const config = useAsync(fetchConfig, []);
  const budget = useAsync(fetchBudget, []);
  const watchlist = useAsync(fetchWatchlist, []);

  const handleRemove = async (symbol: string) => {
    try {
      await removeFromWatchlist(symbol);
      watchlist.refetch();
    } catch {
      // Surface via the existing error slot on next render; for now no-op.
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

      {/* First-run bootstrap progress — shown when all data layers are
          empty (credentials saved but pipeline not yet populated). The
          panel derives its stage from /system/status layer + job states
          and disappears once data starts flowing. */}
      {!system.loading && system.data !== null && isBootstrapping(system.data) ? (
        <BootstrapProgress system={system.data} />
      ) : null}

      {/* Portfolio summary cards share a single /portfolio fetch. */}
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <div className="space-y-6 xl:col-span-2">
          {portfolio.error !== null ? (
            <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
              <SectionError onRetry={portfolio.refetch} />
            </div>
          ) : (
            <>
              <SummaryCards
                data={portfolio.loading ? null : portfolio.data}
                budgetData={budget.loading || budget.error !== null ? null : budget.data}
                budgetError={budget.error !== null}
              />
              <Section title="Positions">
                {portfolio.loading ? (
                  <SectionSkeleton rows={4} />
                ) : (
                  <PositionsTable
                    positions={portfolio.data?.positions ?? []}
                    mirrors={portfolio.data?.mirrors ?? []}
                  />
                )}
              </Section>
            </>
          )}
        </div>

        <div className="space-y-6">
          <Section title="System status">
            {/* No combined loading gate: each endpoint's loading / error /
                data state must drive its own render branch so a slow or
                retrying endpoint cannot hide the already-resolved side.
                See docs/review-prevention-log.md "Duplicate error widgets…"
                and the related round-3 PR-#89 finding on shared loading
                gates. */}
            <SystemStatusPanel
              system={system.error !== null ? null : system.data}
              config={config.error !== null ? null : config.data}
              systemLoading={system.loading}
              configLoading={config.loading}
              systemError={system.error !== null}
              configError={config.error !== null}
              onRetrySystem={system.refetch}
              onRetryConfig={config.refetch}
            />
          </Section>

          <Section title="Budget">
            <BudgetOverviewPanel
              budget={budget.error !== null ? null : budget.data}
              loading={budget.loading}
              hasError={budget.error !== null}
              onRetry={budget.refetch}
            />
          </Section>
        </div>
      </div>

      <Section title={`Watchlist${watchlist.data ? ` (${watchlist.data.total})` : ""}`}>
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

      <Section title="Recent recommendations">
        {recs.loading ? (
          <SectionSkeleton rows={4} />
        ) : recs.error !== null ? (
          <SectionError onRetry={recs.refetch} />
        ) : (
          <RecentRecommendations items={recs.data?.items ?? []} />
        )}
      </Section>
    </div>
  );
}
