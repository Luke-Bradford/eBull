import { NavLink } from "react-router-dom";

const NAV_ITEMS: { to: string; label: string; end?: boolean }[] = [
  { to: "/", label: "Dashboard", end: true },
  { to: "/portfolio", label: "Portfolio" },
  { to: "/instruments", label: "Instruments" },
  { to: "/rankings", label: "Rankings" },
  { to: "/recommendations", label: "Recommendations" },
  { to: "/admin", label: "Admin" },
  { to: "/operators", label: "Operators" },
  { to: "/settings", label: "Settings" },
];

export function Sidebar() {
  return (
    <aside className="flex w-56 flex-col border-r border-slate-200 bg-white">
      <div className="px-5 py-4 text-lg font-semibold tracking-tight">eBull</div>
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
                  ? "bg-slate-900 text-white"
                  : "text-slate-700 hover:bg-slate-100",
              ].join(" ")
            }
          >
            {item.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
