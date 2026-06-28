/**
 * Mid-run drift-heal nudge to set ``OPENFIGI_API_KEY`` (#1791, follow-up to
 * #1344). Re-surfaces the key recommendation when bootstrap stage S13
 * (``cusip_resolver_post_bulk_sweep``) is *still running* several minutes
 * in with no key — catching the operator who dismissed/missed the
 * pre-flight nudge (``OpenFigiKeyNudgeBanner``) and is now watching S13
 * crawl. Spec: docs/specs/frontend/2026-06-28-openfigi-key-driftheal-nudge.md.
 *
 * Show condition (ALL): no key, top-level ``status === "running"``, the S13
 * stage is ``running`` with a finite ``started_at`` more than ``THRESHOLD``
 * ago, and not dismissed for this ``current_run_id``.
 *
 * Threshold = 5 min, deliberately aligned to the orchestrator's
 * ``_REAPER_GRACE_SECONDS`` (bootstrap_orchestrator.py:2669), NOT the
 * issue's "2 min". ``reap_orphaned_running_stages`` resets a ``running``
 * stage only when its worker is provably dead (the ``openfigi`` lane
 * advisory lock is unheld). A genuinely-slow keyless sweep (~48 min) holds
 * that lock the whole time, so the reaper never touches it — meaning a
 * stage still ``running`` past the 5-min grace is provably a LIVE, slow
 * worker, i.e. exactly the "no key" cause. Firing before 5 min would
 * mis-attribute a just-crashed-but-not-yet-reaped stage to "missing key".
 *
 * Dismiss is per-run (sessionStorage, keyed by ``current_run_id``): silence
 * the current run's nag but re-arm on the next run (a fresh run id). Plain
 * sessionStorage ``"1"`` would wrongly suppress later runs in the same tab
 * session. Distinct from the pre-flight nudge's persistent localStorage
 * dismiss (a deliberate "no key" choice that sticks across reloads).
 */

import { useEffect, useRef, useState } from "react";

import {
  fetchBootstrapStatus,
  type BootstrapStatusResponse,
} from "@/api/bootstrap";
import {
  OPENFIGI_KEY_URL,
  S13_STAGE_KEY,
} from "@/components/dashboard/openfigiNudge";

const DISMISS_RUN_KEY = "openfigiKeyDriftHealDismissedRunId";
// Aligned to bootstrap_orchestrator.py::_REAPER_GRACE_SECONDS (300s) — see
// module docstring for why this, not the issue's 2 min.
const THRESHOLD_MS = 5 * 60_000;

/** sessionStorage run id the operator last dismissed for, or null. */
function readDismissedRunId(): number | null {
  try {
    const raw = window.sessionStorage.getItem(DISMISS_RUN_KEY);
    if (raw === null) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

/** Whether the mid-run drift-heal nudge applies to this snapshot. */
function nudgeApplies(
  data: BootstrapStatusResponse,
  dismissedRunId: number | null,
  nowMs: number,
): boolean {
  if (data.openfigi_key_present) return false;
  // Top-level guard: only when bootstrap is genuinely mid-run. Defends
  // against a stale/projected S13 row leaking from a non-running snapshot.
  if (data.status !== "running") return false;
  // Per-run dismiss: hidden only while the dismissed id matches the live
  // run. A new run (fresh current_run_id) re-arms the nudge.
  if (
    data.current_run_id !== null &&
    data.current_run_id === dismissedRunId
  ) {
    return false;
  }
  const s13 = data.stages.find((s) => s.stage_key === S13_STAGE_KEY);
  if (s13 === undefined || s13.status !== "running") return false;
  if (s13.started_at === null) return false;
  const startedMs = new Date(s13.started_at).getTime();
  if (!Number.isFinite(startedMs)) return false; // unparseable → hidden
  // Strict ``>``: at exactly the threshold the nudge is still hidden.
  return nowMs - startedMs > THRESHOLD_MS;
}

export function OpenFigiKeyDriftHealBanner() {
  const [data, setData] = useState<BootstrapStatusResponse | null>(null);
  const [dismissedRunId, setDismissedRunId] = useState<number | null>(() =>
    readDismissedRunId(),
  );
  // Monotonic request id: a slower older poll must not overwrite a newer
  // snapshot (out-of-order resolution would briefly re-show a stale banner).
  const latestReqRef = useRef(0);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const reqId = ++latestReqRef.current;
      try {
        const result = await fetchBootstrapStatus();
        // Ignore if unmounted or a newer poll already resolved.
        if (!cancelled && reqId === latestReqRef.current) setData(result);
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

  if (data === null) return null;
  if (!nudgeApplies(data, dismissedRunId, Date.now())) return null;

  const handleDismiss = () => {
    if (data.current_run_id !== null) {
      try {
        window.sessionStorage.setItem(
          DISMISS_RUN_KEY,
          String(data.current_run_id),
        );
      } catch {
        // sessionStorage unavailable — fall through to in-state dismiss.
      }
      setDismissedRunId(data.current_run_id);
    }
  };

  return (
    <div
      role="status"
      className="flex flex-wrap items-center justify-between gap-3 border-b border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-950/40 px-6 py-2 text-sm text-amber-900 dark:text-amber-100"
    >
      <span>
        ⏳ OpenFIGI CUSIP resolution (S13) has been running several minutes
        with no <code className="font-mono">OPENFIGI_API_KEY</code> — it runs
        ~10× faster with a (free) key. Set the key and restart the API to
        speed up the next run.
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
