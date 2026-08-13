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
 *  * Pre-flight dismiss is persistent (localStorage): an advisory env
 *    nudge, not an action-required gate — a deliberate "no key" choice
 *    should stick across reloads. (Contrast BootstrapNudgeBanner's
 *    intentionally non-permanent sessionStorage dismiss.)
 *
 * Drift-heal mode (#1791): re-surface the nudge mid-run when S13 has been
 * ``running`` over 2 min with no key — catching the operator who
 * dismissed/missed the pre-flight nudge and is now watching S13 crawl.
 * Distinct from pre-flight:
 *  * Fires only on ``status === "running"`` with the S13 stage itself
 *    ``running`` and ``now - started_at > 2 min``.
 *  * ``started_at`` is a TIMESTAMPTZ (sql/129_bootstrap_state.sql) → ISO
 *    string with offset → ``new Date()`` is an unambiguous absolute instant
 *    (no naive-local trap, prevention-log #822). The only residual skew is
 *    browser-vs-server clock; accepted for an advisory banner whose poll is
 *    already ±60 s fuzzy (normal NTP skew is seconds).
 *  * Dismiss is run-scoped sessionStorage (keyed by ``current_run_id``):
 *    the operator can't fix the current run, but a *new* run = new key = the
 *    reminder returns. Independent of the pre-flight localStorage dismiss.
 *  * Copy is honest about "future runs" — setting the key now can't help the
 *    in-flight unkeyed sweep, only the next (post-restart) run.
 */

import { useEffect, useState } from "react";

import {
  fetchBootstrapStatus,
  type BootstrapStatusResponse,
} from "@/api/bootstrap";

const PREFLIGHT_DISMISS_KEY = "openfigiKeyNudgeDismissed";
const DRIFTHEAL_DISMISS_PREFIX = "openfigiKeyDriftHealDismissed";
const OPENFIGI_KEY_URL = "https://www.openfigi.com/api";
// S13 — the OpenFIGI CUSIP post-bulk sweep stage (the key only speeds
// this one stage). Mirrors app/services/bootstrap_orchestrator.py
// JOB_CUSIP_RESOLVER_POST_BULK_SWEEP.
const S13_STAGE_KEY = "cusip_resolver_post_bulk_sweep";
// Drift-heal threshold: S13 running this long without a key → re-surface.
const DRIFTHEAL_THRESHOLD_MS = 2 * 60 * 1000;
// Poll cadence. 30 s (vs the sibling's 60 s) so the drift-heal nudge
// surfaces within ≈one interval of crossing the 2-min threshold. Bootstrap
// is a rare, bounded operator event, so a status read every 30 s is cheap.
const POLL_MS = 30_000;

type NudgeMode = "preflight" | "driftheal";

/** Run-scoped sessionStorage key for the drift-heal dismiss. */
function driftHealDismissKey(runId: number | null): string {
  return `${DRIFTHEAL_DISMISS_PREFIX}:${runId ?? "none"}`;
}

/** Which nudge (if any) applies for a single status snapshot.
 *  ``nowMs`` is the moment the snapshot was *fetched* (not render time):
 *  pinning the clock to fetch-time keeps the drift-heal threshold a pure
 *  function of one consistent snapshot, so a later re-render can never
 *  surface the nudge from stale data (Codex ckpt-2). Mutually exclusive by
 *  top-level status, so at most one mode is returned. */
function nudgeMode(
  data: BootstrapStatusResponse,
  nowMs: number,
): NudgeMode | null {
  if (data.openfigi_key_present) return null;

  // Pre-flight: a (re-)run may still execute S13 without a key.
  if (data.status === "pending") return "preflight";
  if (data.status === "partial_error") {
    // Hide if S13 already terminally succeeded — retry won't rerun it,
    // so the key can no longer help.
    const s13 = data.stages.find((s) => s.stage_key === S13_STAGE_KEY);
    const s13Done = s13?.status === "success" || s13?.status === "skipped";
    return s13Done ? null : "preflight";
  }

  // Drift-heal: S13 crawling unkeyed mid-run.
  if (data.status === "running") {
    const s13 = data.stages.find((s) => s.stage_key === S13_STAGE_KEY);
    if (s13?.status !== "running" || !s13.started_at) return null;
    const startedMs = new Date(s13.started_at).getTime();
    if (Number.isNaN(startedMs)) return null;
    if (nowMs - startedMs > DRIFTHEAL_THRESHOLD_MS) return "driftheal";
  }

  return null;
}

export function OpenFigiKeyNudgeBanner() {
  // Snapshot + the clock reading at fetch time, set together so the
  // drift-heal threshold is computed against the moment the data was true.
  const [snapshot, setSnapshot] = useState<{
    data: BootstrapStatusResponse;
    fetchedAtMs: number;
  } | null>(null);
  const [preflightDismissed, setPreflightDismissed] = useState<boolean>(() => {
    try {
      return window.localStorage.getItem(PREFLIGHT_DISMISS_KEY) === "1";
    } catch {
      return false;
    }
  });
  // In-memory run-scoped drift-heal dismiss. sessionStorage carries it across
  // remounts within the session; this state guarantees the dismiss still hides
  // the banner this mount even when sessionStorage.setItem throws (Codex
  // ckpt-2). `undefined` = nothing dismissed yet this mount.
  const [driftHealDismissedRun, setDriftHealDismissedRun] = useState<
    number | null | undefined
  >(undefined);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const result = await fetchBootstrapStatus();
        if (!cancelled) setSnapshot({ data: result, fetchedAtMs: Date.now() });
      } catch {
        // Best-effort banner — a fetch failure (no session, network
        // hiccup) just leaves it hidden, no user-facing error.
      }
    };
    void load();
    const id = window.setInterval(() => void load(), POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  if (snapshot === null) return null;
  const { data, fetchedAtMs } = snapshot;

  const mode = nudgeMode(data, fetchedAtMs);
  if (mode === null) return null;
  if (mode === "preflight" && preflightDismissed) return null;

  const isDriftHeal = mode === "driftheal";
  const runId = data.current_run_id;
  if (isDriftHeal) {
    // Hidden if dismissed in-memory this mount (covers sessionStorage write
    // failures) OR persisted in run-scoped sessionStorage (survives remount).
    if (driftHealDismissedRun === runId) return null;
    let persisted = false;
    try {
      persisted =
        window.sessionStorage.getItem(driftHealDismissKey(runId)) === "1";
    } catch {
      persisted = false;
    }
    if (persisted) return null;
  }

  const handleDismiss = () => {
    if (isDriftHeal) {
      try {
        window.sessionStorage.setItem(driftHealDismissKey(runId), "1");
      } catch {
        // sessionStorage unavailable — the in-memory state below still hides
        // it for this mount (just not across a remount).
      }
      setDriftHealDismissedRun(runId);
      return;
    }
    try {
      window.localStorage.setItem(PREFLIGHT_DISMISS_KEY, "1");
    } catch {
      // localStorage unavailable — fall through to in-state dismiss only.
    }
    setPreflightDismissed(true);
  };

  return (
    <div
      role="status"
      className="flex flex-wrap items-center justify-between gap-3 border-b border-indigo-200 dark:border-indigo-800 bg-indigo-50 dark:bg-indigo-950/40 px-6 py-2 text-sm text-indigo-900 dark:text-indigo-100"
    >
      <span>
        {isDriftHeal ? (
          <>
            ⏳ CUSIP resolution (S13) has been running over 2 minutes without{" "}
            <code className="font-mono">OPENFIGI_API_KEY</code>. Setting a
            (free) key and restarting the API speeds future runs ~10×.
          </>
        ) : (
          <>
            💡 Recommended: set{" "}
            <code className="font-mono">OPENFIGI_API_KEY</code> and restart the
            API before bootstrap — CUSIP resolution runs ~10× faster with a
            (free) key.
          </>
        )}
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
