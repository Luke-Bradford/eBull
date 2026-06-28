import { Outlet } from "react-router-dom";
import { BootstrapNudgeBanner } from "@/components/dashboard/BootstrapNudgeBanner";
import { OpenFigiKeyNudgeBanner } from "@/components/dashboard/OpenFigiKeyNudgeBanner";
import { OpenFigiKeyDriftHealBanner } from "@/components/dashboard/OpenFigiKeyDriftHealBanner";
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
        {/* #1344 — pre-bootstrap nudge to set OPENFIGI_API_KEY (faster
            CUSIP resolution). Self-hides when a key is configured, once
            bootstrap leaves the pending/partial_error window, or when
            dismissed (persistent localStorage). */}
        <OpenFigiKeyNudgeBanner />
        {/* #1791 — mid-run drift-heal sibling of the pre-flight nudge.
            Re-surfaces (amber) when bootstrap stage S13 is still running
            5+ min with no key — disjoint window from the pre-flight nudge
            above (which only shows pending/partial_error), so they never
            stack. Per-run sessionStorage dismiss. */}
        <OpenFigiKeyDriftHealBanner />
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
