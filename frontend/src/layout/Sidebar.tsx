import { useState } from "react";
import { NavLink, useLocation } from "react-router-dom";

type NavItem = { to: string; label: string; end?: boolean };

/** #3423 — the one page the operator needs: put money in, hands off. */
const PRIMARY_ITEM: NavItem = { to: "/", label: "Invest", end: true };

/** Everything else, behind "Advanced" (operator north star, 2026-09-26). */
const ADVANCED_ITEMS: NavItem[] = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/portfolio", label: "Portfolio" },
  { to: "/calendar", label: "Calendar" },
  { to: "/strategies", label: "Strategies" },
  // #1917 — one Research item; Instruments/Rankings/Theses/Recommendations are
  // now view presets under /research.
  { to: "/research", label: "Research" },
  { to: "/reports", label: "Reports" },
  { to: "/tax", label: "Tax" },
  { to: "/admin", label: "Admin" },
  { to: "/operators", label: "Operators" },
  { to: "/settings", label: "Settings" },
];

function navClass({ isActive }: { isActive: boolean }): string {
  return [
    "rounded-md px-3 py-2 text-sm font-medium",
    isActive
      ? "bg-slate-900 text-white dark:bg-slate-100 dark:text-slate-900"
      : "text-slate-700 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800",
  ].join(" ");
}

export function Sidebar() {
  const { pathname } = useLocation();
  // Open on any page that is not Invest, so the active item is never hidden.
  const [advancedOpen, setAdvancedOpen] = useState(pathname !== "/");
  const showAdvanced = advancedOpen || pathname !== "/";
  return (
    <aside className="flex w-56 flex-col border-r border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="px-5 py-4 text-lg font-semibold tracking-tight text-slate-900 dark:text-slate-100">
        eBull
      </div>
      <nav className="flex flex-col gap-1 px-2">
        <NavLink to={PRIMARY_ITEM.to} end={PRIMARY_ITEM.end} className={navClass}>
          {PRIMARY_ITEM.label}
        </NavLink>
        <button
          type="button"
          aria-expanded={showAdvanced}
          aria-controls="sidebar-advanced"
          disabled={pathname !== "/"}
          onClick={() => setAdvancedOpen((open) => !open)}
          className="mt-3 flex min-h-11 items-center justify-between rounded-md px-3 text-xs font-semibold uppercase tracking-wide text-slate-500 hover:bg-slate-100 disabled:cursor-default disabled:hover:bg-transparent dark:hover:bg-slate-800 dark:disabled:hover:bg-transparent"
        >
          Advanced
          <span aria-hidden="true">{showAdvanced ? "−" : "+"}</span>
        </button>
        {showAdvanced ? (
          <div id="sidebar-advanced" className="flex flex-col gap-1">
            {ADVANCED_ITEMS.map((item) => (
              <NavLink key={item.to} to={item.to} end={item.end} className={navClass}>
                {item.label}
              </NavLink>
            ))}
          </div>
        ) : null}
      </nav>
      {/* Lightweight Charts attribution (#2151). The library is Apache-2.0,
          and TradingView's terms (node_modules/lightweight-charts/README.md
          §License) require the attribution notice plus "a link to
          https://www.tradingview.com/ to the page of your website ... that is
          available to your users". The per-chart `attributionLogo` option is
          documented as ONE sufficient way to meet that link requirement —
          "if you already fulfill this requirement then you can disable this
          attribution logo" (LayoutOptions.attributionLogo). This shell-level
          link is how we fulfil it, so the charts set the option false. The
          notice itself lives in the repo-root NOTICE file.
          Keep this link on every authenticated page; removing it while the
          charts have the logo disabled would put us out of compliance. */}
      <div className="mt-auto px-5 py-4 text-xs text-slate-500 dark:text-slate-400">
        Charts by{" "}
        <a
          href="https://www.tradingview.com/"
          target="_blank"
          rel="noreferrer noopener"
          className="text-blue-600 hover:underline dark:text-blue-400"
        >
          TradingView
        </a>
      </div>
    </aside>
  );
}
