/**
 * NotificationBell — header-bar bell with aggregate unseen-count
 * badge across the three existing alert feeds (#646).
 *
 * Surfaces what already exists rather than adding new infrastructure:
 *   - GET /alerts/guard-rejections          (#399)
 *   - GET /alerts/position-alerts           (#396/#401)
 *   - GET /alerts/coverage-status-drops     (#397/#402)
 *
 * Each feed already exposes `unseen_count` per the per-operator
 * cursor pattern. The bell sums them, shows a small red badge when
 * non-zero, and navigates to /dashboard on click — that's where the
 * existing AlertsStrip renders the unified feed in detail.
 *
 * Polls every 30s. Failures are silent (each feed is best-effort);
 * a single feed erroring just contributes 0 to the count and the
 * badge keeps reflecting the others. Same posture as AlertsStrip.
 */

import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  fetchCoverageStatusDrops,
  fetchGuardRejections,
  fetchPositionAlerts,
} from "@/api/alerts";

const POLL_INTERVAL_MS = 30_000;

/**
 * Resolve a single feed's unseen_count to a number, swallowing any
 * fetch error so one broken feed doesn't black out the badge for
 * the others. Mirrors AlertsStrip's per-feed best-effort posture.
 */
async function safeUnseen(load: () => Promise<{ unseen_count: number }>): Promise<number> {
  try {
    const r = await load();
    const n = r.unseen_count;
    return Number.isFinite(n) && n > 0 ? n : 0;
  } catch {
    return 0;
  }
}

export function NotificationBell(): JSX.Element {
  const navigate = useNavigate();
  const [count, setCount] = useState<number>(0);

  const refresh = useCallback(async () => {
    const [guard, position, coverage] = await Promise.all([
      safeUnseen(fetchGuardRejections),
      safeUnseen(fetchPositionAlerts),
      safeUnseen(fetchCoverageStatusDrops),
    ]);
    setCount(guard + position + coverage);
  }, []);

  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => {
      void refresh();
    }, POLL_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, [refresh]);

  // Cap displayed count at 99+ — past the badge's visual budget,
  // operator gets the click signal anyway.
  const display = count > 99 ? "99+" : String(count);

  return (
    <button
      type="button"
      onClick={() => navigate("/")}
      aria-label={count > 0 ? `${count} unread notifications` : "Notifications"}
      data-testid="notification-bell"
      data-unseen-count={count}
      title={count > 0 ? `${count} unread — click to open the dashboard` : "No unread alerts"}
      className={[
        "relative rounded p-1 text-slate-600 transition hover:bg-slate-50",
        count > 0 ? "text-red-700" : "",
      ].join(" ")}
    >
      {/* Inline SVG bell — keeps the component dependency-free. */}
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="h-4 w-4"
        aria-hidden
      >
        <path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9" />
        <path d="M10.3 21a1.94 1.94 0 0 0 3.4 0" />
      </svg>
      {count > 0 && (
        <span
          data-testid="notification-bell-badge"
          className={[
            "absolute -right-1 -top-1 flex min-w-[16px] items-center justify-center",
            "rounded-full bg-red-600 px-1 text-[10px] font-medium leading-none text-white",
            "tabular-nums",
          ].join(" ")}
        >
          {display}
        </span>
      )}
    </button>
  );
}
