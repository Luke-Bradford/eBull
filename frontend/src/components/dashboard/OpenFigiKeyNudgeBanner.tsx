/**
 * Pre-bootstrap nudge to set ``OPENFIGI_API_KEY`` (#1344).
 *
 * OpenFIGI CUSIP resolution (bootstrap stage S13,
 * ``cusip_resolver_post_bulk_sweep``) runs ~10× faster wall-clock with a
 * key configured (app/config.py: ~48 min → ~5 min on a ~12k unresolved
 * backlog). Operators don't learn this until S13 crawls — this banner
 * surfaces the recommendation *before* bootstrap runs.
 *
 * Source numbers: rate limit is 100× (250 vs 25,000 mappings/min,
 * settled-decision 635); the operator-facing figure is the ~10×
 * wall-clock estimate, the honest end-to-end number.
 *
 * Behaviour:
 *  * Polls /system/bootstrap/status at 60s (mirrors BootstrapNudgeBanner).
 *  * Shown only when ``openfigi_key_present === false`` AND a (re-)run may
 *    still execute S13 (``cusip_resolver_post_bulk_sweep``) without a key:
 *      - ``pending`` — fresh install, full run ahead.
 *      - ``partial_error`` — BUT only if S13 has not already terminally
 *        succeeded. A ``partial_error`` can originate in a different/later
 *        lane while S13 already finished; retry reruns only failed +
 *        later-same-lane stages, so a succeeded S13 will NOT rerun and the
 *        key can no longer help — hide the nudge (Codex ckpt-2).
 *    ``running`` (mid-run, too late) and ``complete`` (re-run is a
 *    deliberate operator action) are excluded.
 *  * ``openfigi_key_present`` reflects the key as of API process start —
 *    backend ``settings`` is process-global and not hot-reloaded, so a
 *    newly-set key shows only after the operator restarts the API. The
 *    same ``settings`` value drives the S13 resolver, so "no key" here
 *    truthfully predicts what the sweep will run with.
 *  * Dismiss is persistent (localStorage): an advisory env nudge, not an
 *    action-required gate — a deliberate "no key" choice should stick
 *    across reloads. (Contrast BootstrapNudgeBanner's intentionally
 *    non-permanent sessionStorage dismiss.)
 */

import { useEffect, useState } from "react";

import {
  fetchBootstrapStatus,
  type BootstrapStatusResponse,
} from "@/api/bootstrap";

const DISMISS_KEY = "openfigiKeyNudgeDismissed";
const OPENFIGI_KEY_URL = "https://www.openfigi.com/api";
// S13 — the OpenFIGI CUSIP post-bulk sweep stage (the key only speeds
// this one stage). Mirrors app/services/bootstrap_orchestrator.py
// JOB_CUSIP_RESOLVER_POST_BULK_SWEEP.
const S13_STAGE_KEY = "cusip_resolver_post_bulk_sweep";

/** Whether a (re-)run may still execute S13 without a key — the only
 *  state in which setting the key still helps. */
function nudgeApplies(data: BootstrapStatusResponse): boolean {
  if (data.openfigi_key_present) return false;
  if (data.status === "pending") return true;
  if (data.status === "partial_error") {
    // Hide if S13 already terminally succeeded — retry won't rerun it,
    // so the key can no longer help.
    const s13 = data.stages.find((s) => s.stage_key === S13_STAGE_KEY);
    const s13Done = s13?.status === "success" || s13?.status === "skipped";
    return !s13Done;
  }
  // running (too late this run) / complete (deliberate re-run) → no nudge.
  return false;
}

export function OpenFigiKeyNudgeBanner() {
  const [data, setData] = useState<BootstrapStatusResponse | null>(null);
  const [dismissed, setDismissed] = useState<boolean>(() => {
    try {
      return window.localStorage.getItem(DISMISS_KEY) === "1";
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
        // Best-effort banner — a fetch failure (no session, network
        // hiccup) just leaves it hidden, no user-facing error.
      }
    };
    void load();
    const id = window.setInterval(() => void load(), 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  if (dismissed) return null;
  if (data === null) return null;
  if (!nudgeApplies(data)) return null;

  const handleDismiss = () => {
    try {
      window.localStorage.setItem(DISMISS_KEY, "1");
    } catch {
      // localStorage unavailable — fall through to in-state dismiss only.
    }
    setDismissed(true);
  };

  return (
    <div
      role="status"
      className="flex flex-wrap items-center justify-between gap-3 border-b border-indigo-200 dark:border-indigo-800 bg-indigo-50 dark:bg-indigo-950/40 px-6 py-2 text-sm text-indigo-900 dark:text-indigo-100"
    >
      <span>
        💡 Recommended: set <code className="font-mono">OPENFIGI_API_KEY</code>{" "}
        and restart the API before bootstrap — CUSIP resolution runs ~10×
        faster with a (free) key.
      </span>
      <div className="flex items-center gap-3">
        <a
          href={OPENFIGI_KEY_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="rounded bg-white/60 dark:bg-slate-900/60 px-2 py-1 text-xs font-medium hover:bg-white dark:hover:bg-slate-900"
        >
          Get a free key
        </a>
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
