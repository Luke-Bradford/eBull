import { hasAuthToken } from "@/api/client";

export function Header() {
  // Placeholder connected/disconnected indicator. Real bearer-token wiring
  // lands with #58 — for now this just reflects whether setAuthToken() has
  // been called in-session.
  // TODO(#58): hasAuthToken() reads a module-level variable at render time
  // and is NOT reactive — calling setAuthToken() will not re-render this
  // component without a route change or full reload. Replace with a reactive
  // store (e.g. a tiny context, zustand, or a TanStack Query auth state)
  // when wiring the real auth flow.
  const connected = hasAuthToken();
  return (
    <header className="flex h-14 items-center justify-between border-b border-slate-200 bg-white px-6">
      <div className="text-sm font-medium text-slate-500">Operator console</div>
      <div className="flex items-center gap-2 text-xs">
        <span
          className={[
            "inline-block h-2 w-2 rounded-full",
            connected ? "bg-emerald-500" : "bg-slate-300",
          ].join(" ")}
          aria-hidden
        />
        <span className="text-slate-600">{connected ? "connected" : "disconnected"}</span>
      </div>
    </header>
  );
}
