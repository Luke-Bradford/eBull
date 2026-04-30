import { useSession } from "@/lib/session";

import { NotificationBell } from "./NotificationBell";
import { ThemeToggle } from "./ThemeToggle";

export function Header() {
  const { status, operator, logout } = useSession();
  const connected = status === "authenticated";

  return (
    <header className="flex h-14 items-center justify-between border-b border-slate-200 bg-white px-6 dark:border-slate-800 dark:bg-slate-900">
      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Operator console</div>
      <div className="flex items-center gap-3 text-xs">
        {/* Theme toggle stays available pre-auth too — it's a local
            UI preference, not session-gated. */}
        <ThemeToggle />
        {/* Bell only when authenticated — the alerts endpoints are
         *  session-protected; rendering it on the login page would
         *  trigger silent 401s every 30s. */}
        {connected && <NotificationBell />}
        <span
          className={[
            "inline-block h-2 w-2 rounded-full",
            connected ? "bg-emerald-500" : "bg-slate-300 dark:bg-slate-600",
          ].join(" ")}
          aria-hidden
        />
        <span className="text-slate-600 dark:text-slate-400">
          {connected && operator ? operator.username : "disconnected"}
        </span>
        {connected && (
          <button
            type="button"
            onClick={() => {
              void logout();
            }}
            className="rounded border border-slate-300 px-2 py-0.5 text-slate-600 hover:bg-slate-50 dark:bg-slate-900/40 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800"
          >
            Sign out
          </button>
        )}
      </div>
    </header>
  );
}
