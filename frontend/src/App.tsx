import { Route, Routes } from "react-router-dom";
import { AppShell } from "@/layout/AppShell";
import { ErrorBoundary } from "@/components/states/ErrorBoundary";
import { RequireAuth } from "@/components/RequireAuth";
import { DisplayCurrencyProvider } from "@/lib/DisplayCurrencyContext";
import { DashboardPage } from "@/pages/DashboardPage";
import { RankingsPage } from "@/pages/RankingsPage";
import { InstrumentDetailPage } from "@/pages/InstrumentDetailPage";
import { RecommendationsPage } from "@/pages/RecommendationsPage";
import { AdminPage } from "@/pages/AdminPage";
import { CoverageInsufficientPage } from "@/pages/CoverageInsufficientPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { LoginPage } from "@/pages/LoginPage";
import { SetupPage } from "@/pages/SetupPage";
import { RecoverPage } from "@/pages/RecoverPage";
import { OperatorsPage } from "@/pages/OperatorsPage";
import { CopyTradingPage } from "@/pages/CopyTradingPage";
import { InstrumentsPage } from "@/pages/InstrumentsPage";
import { PortfolioPage } from "@/pages/PortfolioPage";
import { PositionDetailPage } from "@/pages/PositionDetailPage";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/setup" element={<SetupPage />} />
        <Route path="/recover" element={<RecoverPage />} />
        <Route
          element={
            <RequireAuth>
              <DisplayCurrencyProvider>
                <AppShell />
              </DisplayCurrencyProvider>
            </RequireAuth>
          }
        >
          <Route index element={<DashboardPage />} />
          <Route path="portfolio" element={<PortfolioPage />} />
          <Route path="portfolio/:instrumentId" element={<PositionDetailPage />} />
          <Route path="rankings" element={<RankingsPage />} />
          <Route path="instruments" element={<InstrumentsPage />} />
          <Route path="instruments/:instrumentId" element={<InstrumentDetailPage />} />
          <Route path="copy-trading/:mirrorId" element={<CopyTradingPage />} />
          <Route path="recommendations" element={<RecommendationsPage />} />
          <Route path="admin" element={<AdminPage />} />
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
