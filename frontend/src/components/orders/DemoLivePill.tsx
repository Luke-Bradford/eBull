/**
 * DemoLivePill — safety-state indicator for the operator's trade-mode
 * (issue #313, §9 of the spec).
 *
 * Follows .claude/skills/frontend/safety-state-ui.md literally:
 *   - Cache the last confirmed boolean (true OR false) in local state.
 *   - Display `fresh ?? cached`; never disappears during refetch.
 *   - Cache is updated only on non-null fresh values; transient nulls
 *     (loading, error) leave the cache untouched.
 *   - A stale marker renders whenever the cache is the source of truth.
 *
 * The scary state for this app is LIVE (real-money orders). If the
 * /config endpoint transiently fails while LIVE was confirmed true,
 * we must keep rendering LIVE rather than falling back to DEMO.
 */
import { useEffect, useState } from "react";

import { useConfig } from "@/lib/ConfigContext";

export function DemoLivePill(): JSX.Element {
  const config = useConfig();
  const liveFlag: boolean | null =
    config.data?.runtime?.enable_live_trading ?? null;

  const [cached, setCached] = useState<boolean | null>(null);
  useEffect(() => {
    // Only update the cache on confirmed fresh values. A null (loading
    // or error) leaves the cache untouched so the pill never
    // disappears behind a transient /config hiccup.
    if (liveFlag !== null) setCached(liveFlag);
  }, [liveFlag]);

  const fresh = liveFlag !== null;
  const isLive = liveFlag ?? cached ?? false;

  return (
    <span
      className={
        isLive
          ? "inline-flex items-center gap-1 rounded border border-red-300 bg-red-50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-red-700"
          : "inline-flex items-center gap-1 rounded border border-blue-300 bg-blue-50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-blue-700"
      }
      data-testid="demo-live-pill"
      data-live={isLive ? "true" : "false"}
    >
      {isLive ? "LIVE" : "DEMO MODE"}
      {!fresh && cached !== null ? (
        <span className="text-[9px] uppercase text-amber-600">(stale)</span>
      ) : null}
    </span>
  );
}
