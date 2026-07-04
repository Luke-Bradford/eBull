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
import { FilingsAnalyticsPage } from "@/pages/FilingsAnalyticsPage";
import { ChartPage } from "@/pages/ChartPage";
import { RiskPage } from "@/pages/RiskPage";
import { DividendsPage } from "@/pages/DividendsPage";
import { FundamentalsPage } from "@/pages/FundamentalsPage";
import { InsiderPage } from "@/pages/InsiderPage";
import { OwnershipPage } from "@/pages/OwnershipPage";
import { NewsAnalysisPage } from "@/pages/NewsAnalysisPage";
import { PeersPage } from "@/pages/PeersPage";
import { ReportsPage } from "@/pages/ReportsPage";
import { TaxPage } from "@/pages/TaxPage";
import { RecommendationsPage } from "@/pages/RecommendationsPage";
import { AdminPage } from "@/pages/AdminPage";
import { AdminJobDetailPage } from "@/pages/AdminJobDetailPage";
import { ProcessDetailPage } from "@/pages/ProcessDetailPage";
import { IngestHealthPage } from "@/pages/IngestHealthPage";
import { CoverageInsufficientPage } from "@/pages/CoverageInsufficientPage";
import { CapabilityOverridesPage } from "@/pages/CapabilityOverridesPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { BrokerSetupPage } from "@/pages/BrokerSetupPage";
import { LoginPage } from "@/pages/LoginPage";
import { SetupPage } from "@/pages/SetupPage";
import { OperatorsPage } from "@/pages/OperatorsPage";
import { CopyTradingPage } from "@/pages/CopyTradingPage";
import { InstrumentsPage } from "@/pages/InstrumentsPage";
import { PortfolioPage } from "@/pages/PortfolioPage";
import { CalendarPage } from "@/pages/CalendarPage";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/setup" element={<SetupPage />} />
        {/* Chrome-free broker-credentials setup. Requires auth but
            renders outside AppShell so the operator sees only the
            key-entry form until eToro creds are validated + saved. */}
        <Route
          path="/setup/broker"
          element={
            <RequireAuth>
              <BrokerSetupPage />
            </RequireAuth>
          }
        />
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
          <Route path="calendar" element={<CalendarPage />} />
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
            path="instrument/:symbol/filings/analytics"
            element={<FilingsAnalyticsPage />}
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
          <Route
            path="instrument/:symbol/risk"
            element={<RiskPage />}
          />
          <Route
            path="instrument/:symbol/news-analysis"
            element={<NewsAnalysisPage />}
          />
          <Route
            path="instrument/:symbol/peers"
            element={<PeersPage />}
          />
          <Route path="copy-trading/:mirrorId" element={<CopyTradingPage />} />
          <Route path="recommendations" element={<RecommendationsPage />} />
          <Route path="reports" element={<ReportsPage />} />
          <Route path="tax" element={<TaxPage />} />
          <Route path="admin" element={<AdminPage />} />
          <Route path="admin/jobs/:name" element={<AdminJobDetailPage />} />
          <Route
            path="admin/processes/:id"
            element={<ProcessDetailPage />}
          />
          <Route path="admin/ingest-health" element={<IngestHealthPage />} />
          <Route
            path="admin/coverage/insufficient"
            element={<CoverageInsufficientPage />}
          />
          <Route
            path="admin/capability-overrides"
            element={<CapabilityOverridesPage />}
          />
          <Route path="operators" element={<OperatorsPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
