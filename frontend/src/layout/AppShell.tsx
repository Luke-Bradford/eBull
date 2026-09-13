import { Outlet } from "react-router-dom";
import { BootstrapNudgeBanner } from "@/components/dashboard/BootstrapNudgeBanner";
import { OpenFigiKeyNudgeBanner } from "@/components/dashboard/OpenFigiKeyNudgeBanner";
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
        {/* No top padding: pages with sticky headers (e.g. SummaryStrip
            on the instrument page) must be able to flush with the
            <Header> bar above. Pages that need top breathing room add
            their own `pt-6` to the root container.

            `relative` is load-bearing (#2982), and it is the SAME rule
            #1858 established one level down on `Section`'s scrollable
            body: a scroll container must establish a positioning
            context. Without it, any `position:absolute` descendant with
            no nearer positioned ancestor — a Tailwind `sr-only` heading
            is the usual one, since `sr-only` is `position:absolute` —
            resolves its containing block to the INITIAL containing
            block, not this <main>. It then escapes this element's
            `overflow-auto` clip AND the shell's `overflow-hidden`
            (the shell is not its containing block either), so its
            static position extends documentElement.scrollHeight and the
            whole app shell scrolls, sliding the <aside> off-screen.
            Measured on /strategies at 1600x1000: docScrollHeight 2542
            against a 1000px viewport, from the single <h2 className=
            "sr-only"> at StrategyPortfolioLens.tsx:572.

            Fixing it HERE closes the class for all 13 `sr-only` usages
            rather than the one heading. No visual change: `relative`
            with no insets leaves in-flow content exactly where it was,
            and it does NOT capture `position:fixed` descendants, so
            Modal's `fixed inset-0` overlay still anchors to the
            viewport. See docs/review-prevention-log.md. */}
        <main className="relative flex-1 overflow-auto px-6 pb-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
