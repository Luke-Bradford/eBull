import { Route, Routes } from "react-router-dom";
import { AppShell } from "@/layout/AppShell";
import { ErrorBoundary } from "@/components/states/ErrorBoundary";
import { RequireAuth } from "@/components/RequireAuth";
import { DashboardPage } from "@/pages/DashboardPage";
import { RankingsPage } from "@/pages/RankingsPage";
import { InstrumentDetailPage } from "@/pages/InstrumentDetailPage";
import { RecommendationsPage } from "@/pages/RecommendationsPage";
import { AdminPage } from "@/pages/AdminPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { NotFoundPage } from "@/pages/NotFoundPage";
import { LoginPage } from "@/pages/LoginPage";
import { SetupPage } from "@/pages/SetupPage";
import { OperatorsPage } from "@/pages/OperatorsPage";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/setup" element={<SetupPage />} />
        <Route
          element={
            <RequireAuth>
              <AppShell />
            </RequireAuth>
          }
        >
          <Route index element={<DashboardPage />} />
          <Route path="rankings" element={<RankingsPage />} />
          <Route path="instruments/:instrumentId" element={<InstrumentDetailPage />} />
          <Route path="recommendations" element={<RecommendationsPage />} />
          <Route path="admin" element={<AdminPage />} />
          <Route path="operators" element={<OperatorsPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
