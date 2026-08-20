import { NavLink } from "react-router-dom";

const NAV_ITEMS: { to: string; label: string; end?: boolean }[] = [
  { to: "/", label: "Dashboard", end: true },
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

export function Sidebar() {
  return (
    <aside className="flex w-56 flex-col border-r border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="px-5 py-4 text-lg font-semibold tracking-tight text-slate-900 dark:text-slate-100">
        eBull
      </div>
      <nav className="flex flex-col gap-1 px-2">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            className={({ isActive }) =>
              [
                "rounded-md px-3 py-2 text-sm font-medium",
                isActive
                  ? "bg-slate-900 text-white dark:bg-slate-100 dark:text-slate-900"
 : "text-slate-700 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800",
              ].join(" ")
            }
          >
            {item.label}
          </NavLink>
        ))}
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
