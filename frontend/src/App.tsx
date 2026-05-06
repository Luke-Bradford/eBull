import { Route, Routes } from "react-router-dom";
import { AppShell } from "@/layout/AppShell";
import { ErrorBoundary } from "@/components/states/ErrorBoundary";
import { RequireAuth } from "@/components/RequireAuth";
import { ConfigProvider } from "@/lib/ConfigContext";
import { DisplayCurrencyProvider } from "@/lib/DisplayCurrencyContext";
import { DashboardPage } from "@/pages/DashboardPage";
import { RankingsPage } from "@/pages/RankingsPage";
import { InstrumentDetailRedirect } from "@/pages/InstrumentDetailRedirect";
import { InstrumentPage } from "@/pages/InstrumentPage";
import { Tenk10KDrilldownPage } from "@/pages/Tenk10KDrilldownPage";
import { EightKListPage } from "@/pages/EightKListPage";
import { ChartPage } from "@/pages/ChartPage";
import { DividendsPage } from "@/pages/DividendsPage";
import { FundamentalsPage } from "@/pages/FundamentalsPage";
import { InsiderPage } from "@/pages/InsiderPage";
import { OwnershipPage } from "@/pages/OwnershipPage";
import { ReportsPage } from "@/pages/ReportsPage";
import { RecommendationsPage } from "@/pages/RecommendationsPage";
import { AdminPage } from "@/pages/AdminPage";
import { AdminJobDetailPage } from "@/pages/AdminJobDetailPage";
import { IngestHealthPage } from "@/pages/IngestHealthPage";
import { CoverageInsufficientPage } from "@/pages/CoverageInsufficientPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { LoginPage } from "@/pages/LoginPage";
import { SetupPage } from "@/pages/SetupPage";
import { OperatorsPage } from "@/pages/OperatorsPage";
import { CopyTradingPage } from "@/pages/CopyTradingPage";
import { InstrumentsPage } from "@/pages/InstrumentsPage";
import { PortfolioPage } from "@/pages/PortfolioPage";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/setup" element={<SetupPage />} />
        <Route
          element={
            <RequireAuth>
              <ConfigProvider>
                <DisplayCurrencyProvider>
                  <AppShell />
                </DisplayCurrencyProvider>
              </ConfigProvider>
            </RequireAuth>
          }
        >
          <Route index element={<DashboardPage />} />
          <Route path="portfolio" element={<PortfolioPage />} />
          {/* Legacy per-instrument routes redirect to the canonical
              `/instrument/:symbol` research page. Introduced in Slice 3
              of the per-stock research spec; Slice 5 deleted
              `InstrumentDetailPage`/`PositionDetailPage` but kept
              these shims so operator bookmarks still resolve. Remove
              once access logs show zero traffic on these paths. */}
          <Route
            path="portfolio/:instrumentId"
            element={<InstrumentDetailRedirect search="?tab=positions" />}
          />
          <Route path="rankings" element={<RankingsPage />} />
          <Route path="instruments" element={<InstrumentsPage />} />
          <Route
            path="instruments/:instrumentId"
            element={<InstrumentDetailRedirect />}
          />
          <Route path="instrument/:symbol" element={<InstrumentPage />} />
          <Route
            path="instrument/:symbol/filings/10-k"
            element={<Tenk10KDrilldownPage />}
          />
          <Route
            path="instrument/:symbol/filings/8-k"
            element={<EightKListPage />}
          />
          <Route
            path="instrument/:symbol/dividends"
            element={<DividendsPage />}
          />
          <Route
            path="instrument/:symbol/insider"
            element={<InsiderPage />}
          />
          <Route
            path="instrument/:symbol/ownership"
            element={<OwnershipPage />}
          />
          <Route
            path="instrument/:symbol/fundamentals"
            element={<FundamentalsPage />}
          />
          <Route
            path="instrument/:symbol/chart"
            element={<ChartPage />}
          />
          <Route path="copy-trading/:mirrorId" element={<CopyTradingPage />} />
          <Route path="recommendations" element={<RecommendationsPage />} />
          <Route path="reports" element={<ReportsPage />} />
          <Route path="admin" element={<AdminPage />} />
          <Route path="admin/jobs/:name" element={<AdminJobDetailPage />} />
          <Route path="admin/ingest-health" element={<IngestHealthPage />} />
          <Route
            path="admin/coverage/insufficient"
            element={<CoverageInsufficientPage />}
          />
          <Route path="operators" element={<OperatorsPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
