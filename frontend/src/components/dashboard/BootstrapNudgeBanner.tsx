/**
 * Top-of-page banner that nudges the operator to run the first-install
 * bootstrap when ``bootstrap_state.status`` is anything but ``complete``.
 *
 * Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.
 *
 * Behaviour:
 *
 *  * Polls /system/bootstrap/status at 60s (idle cadence).
 *  * Hidden when status is ``complete``.
 *  * Hidden when the operator has dismissed it for the current session
 *    via sessionStorage; reload re-shows it as long as bootstrap is
 *    still incomplete (we never want "dismiss forever" — the actual
 *    fix path is the admin panel).
 *  * Click "Open admin" → navigates to ``/admin``.
 */

import { Link } from "react-router-dom";
import { useEffect, useState } from "react";

import {
  fetchBootstrapStatus,
  type BootstrapStatusResponse,
} from "@/api/bootstrap";

const SESSION_DISMISS_KEY = "bootstrapBannerDismissed";

const STATUS_COPY: Record<BootstrapStatusResponse["status"], string> = {
  pending: "First-install bootstrap has not been run yet — the system is waiting for you to populate the universe + filings before scheduled jobs can be useful.",
  running: "First-install bootstrap is in progress.",
  complete: "",
  partial_error: "First-install bootstrap finished with errors. Some scheduled jobs are still gated until you retry the failed stages or mark complete.",
};

const STATUS_TONE: Record<BootstrapStatusResponse["status"], string> = {
  pending: "border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/40 text-amber-900 dark:text-amber-100",
  running: "border-sky-200 dark:border-sky-800 bg-sky-50 dark:bg-sky-950/40 text-sky-900 dark:text-sky-100",
  complete: "",
  partial_error: "border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/40 text-red-900 dark:text-red-100",
};

export function BootstrapNudgeBanner() {
  const [data, setData] = useState<BootstrapStatusResponse | null>(null);
  const [dismissed, setDismissed] = useState<boolean>(() => {
    try {
      return window.sessionStorage.getItem(SESSION_DISMISS_KEY) === "1";
    } catch {
      return false;
    }
  });

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const result = await fetchBootstrapStatus();
        if (!cancelled) setData(result);
      } catch {
        // Banner is best-effort. A failure to fetch (no session, network
        // hiccup) just leaves the banner hidden — no user-facing error.
      }
    };
    void load();
    const id = window.setInterval(() => void load(), 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  if (data === null) return null;
  if (data.status === "complete") return null;
  if (dismissed) return null;

  const handleDismiss = () => {
    try {
      window.sessionStorage.setItem(SESSION_DISMISS_KEY, "1");
    } catch {
      // sessionStorage not available — fall through to the in-state
      // dismiss only.
    }
    setDismissed(true);
  };

  return (
    <div
      role="status"
      className={`flex flex-wrap items-center justify-between gap-3 border-b px-6 py-2 text-sm ${
        STATUS_TONE[data.status]
      }`}
    >
      <span>{STATUS_COPY[data.status]}</span>
      <div className="flex items-center gap-3">
        <Link
          to="/admin"
          className="rounded bg-white/60 px-2 py-1 text-xs font-medium hover:bg-white"
        >
          Open admin
        </Link>
        <button
          type="button"
          onClick={handleDismiss}
          className="text-xs underline hover:no-underline"
        >
          Dismiss
        </button>
      </div>
    </div>
  );
}
