import { useSession } from "@/lib/session";

import { NotificationBell } from "./NotificationBell";

export function Header() {
  const { status, operator, logout } = useSession();
  const connected = status === "authenticated";

  return (
    <header className="flex h-14 items-center justify-between border-b border-slate-200 bg-white px-6">
      <div className="text-sm font-medium text-slate-500">Operator console</div>
      <div className="flex items-center gap-3 text-xs">
        {/* Bell only when authenticated — the alerts endpoints are
         *  session-protected; rendering it on the login page would
         *  trigger silent 401s every 30s. */}
        {connected && <NotificationBell />}
        <span
          className={[
            "inline-block h-2 w-2 rounded-full",
            connected ? "bg-emerald-500" : "bg-slate-300",
          ].join(" ")}
          aria-hidden
        />
        <span className="text-slate-600">
          {connected && operator ? operator.username : "disconnected"}
        </span>
        {connected && (
          <button
            type="button"
            onClick={() => {
              void logout();
            }}
            className="rounded border border-slate-300 px-2 py-0.5 text-slate-600 hover:bg-slate-50"
          >
            Sign out
          </button>
        )}
      </div>
    </header>
  );
}
