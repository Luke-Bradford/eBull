import { Outlet } from "react-router-dom";
import { BootstrapNudgeBanner } from "@/components/dashboard/BootstrapNudgeBanner";
import { Sidebar } from "@/layout/Sidebar";
import { Header } from "@/layout/Header";

export function AppShell() {
  return (
    <div className="flex h-screen w-screen overflow-hidden">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <Header />
        {/* #997 — first-install bootstrap nudge. Renders below the
            Header on every authenticated route while
            bootstrap_state.status !== 'complete'. Self-hides when
            bootstrap is complete or the operator dismisses it for
            the current session. */}
        <BootstrapNudgeBanner />
        {/* No top padding: pages with sticky headers (e.g. SummaryStrip
            on the instrument page) must be able to flush with the
            <Header> bar above. Pages that need top breathing room add
            their own `pt-6` to the root container. */}
        <main className="flex-1 overflow-auto px-6 pb-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
