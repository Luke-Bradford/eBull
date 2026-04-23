/**
 * Shared Sync-now trigger hook.
 *
 * Before #323, `SyncDashboard` owned its own local `triggerState` and
 * the Sync-now button's disabled logic. With the AdminPage triage
 * rewrite, we want TWO buttons — a top-level one that stays visible
 * even when `SyncDashboard` is collapsed, and the existing one inside
 * `SyncDashboard` when expanded — without ever firing two concurrent
 * POSTs at the backend.
 *
 * This hook is the single source of truth:
 *   - `AdminPage` calls `useSyncTrigger(onTriggered)` once.
 *   - Passes the returned object to every button (top-level + `SyncDashboard`).
 *   - Every button shares the same `kind`, and `kind === "running"` OR
 *     `kind === "queued"` disables all of them.
 *   - A second click is a no-op until the hook transitions back to `idle`.
 *
 * The hook also owns the kind reset: when the caller observes that a
 * sync is running server-side (e.g. `status.is_running === true`), it
 * calls `clearQueued(isRunning)` so the "Queued" badge resolves into
 * the plain "Running" disabled state the server now owns.
 */
import { useCallback, useEffect, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import { triggerSync } from "@/api/sync";
import type { SyncTriggerRequest } from "@/api/sync";

export type SyncTriggerKind =
  | "idle"
  | "running"
  | "queued"
  | "conflict"
  | "error";

export interface SyncTriggerState {
  readonly kind: SyncTriggerKind;
  readonly queuedRunId: number | null;
  readonly message: string | null;
  /** Fire the trigger. No-op when already running or queued. */
  readonly trigger: () => Promise<void>;
  /** Clear an `error` back to `idle`; reset a `queued` when the server
   *  confirms a sync is now running. Called by the caller once the
   *  next `/sync/status` tick arrives. */
  readonly clearQueued: (isRunning: boolean) => void;
}

interface InternalState {
  kind: SyncTriggerKind;
  queuedRunId: number | null;
  message: string | null;
}

export function useSyncTrigger(
  onTriggered: () => void,
): SyncTriggerState {
  const [state, setState] = useState<InternalState>({
    kind: "idle",
    queuedRunId: null,
    message: null,
  });
  // Synchronous in-flight guard. setState updates are batched and
  // asynchronous, so checking `state.kind` here cannot gate two
  // concurrent trigger() calls that land in the same microtask. A
  // ref gives us a "has the POST been dispatched yet?" signal that
  // updates synchronously.
  const inFlightRef = useRef<boolean>(false);

  const trigger = useCallback(async () => {
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    setState({ kind: "running", queuedRunId: null, message: null });
    const body: SyncTriggerRequest = { scope: "behind" };
    try {
      const result = await triggerSync(body);
      setState({
        kind: "queued",
        queuedRunId: result.sync_run_id,
        message: null,
      });
      onTriggered();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        // 409 means an orchestrator sync is already running. The
        // operator's click was not a failure — the system is already
        // doing the work. Render as an amber informational pill
        // (handled by consumers) instead of a red error, and fire the
        // caller's status poll immediately so the "conflict" amber
        // resolves into the grey "Running" disabled state within one
        // poll cycle instead of up to the next idle-cadence tick
        // (currently 60s).
        setState({
          kind: "conflict",
          queuedRunId: null,
          message: "Another sync is already running",
        });
        onTriggered();
        return;
      }
      const message =
        err instanceof ApiError
          ? err.status === 503
            ? "Sync orchestrator disabled"
            : `Failed (HTTP ${err.status})`
          : "Failed";
      setState({ kind: "error", queuedRunId: null, message });
    }
  }, [onTriggered]);

  const clearQueued = useCallback((isRunning: boolean) => {
    setState((prev) => {
      // The `queued` badge survives only until the server confirms
      // the sync actually started. Once isRunning flips to true,
      // drop the badge; the button stays disabled by the caller
      // because the caller also OR's with `isRunning` when wiring
      // `disabled`.
      if (prev.kind === "queued" && isRunning) {
        return { kind: "idle", queuedRunId: null, message: null };
      }
      // `conflict` collapses to `idle` as soon as the caller's status
      // poll confirms a sync is running — the amber "Another sync is
      // already running" pill has done its job; from that point the
      // normal grey "Running" state (driven by `isRunning`) carries
      // the UX. Otherwise the pill would persist past the end of the
      // server-side sync and mislead the operator.
      if (prev.kind === "conflict" && isRunning) {
        return { kind: "idle", queuedRunId: null, message: null };
      }
      // `error` has no auto-reset — caller must click again.
      return prev;
    });
  }, []);

  // Single source of truth for inFlightRef: it's `true` iff the hook
  // is in a running or queued state. Any transition to idle or error
  // resets the ref. Avoids the previous manual-reset paths that could
  // leave the ref out of sync with `kind` (e.g. fast-run fallback
  // timer, caller-initiated reset, unexpected state path).
  useEffect(() => {
    // `conflict` is a terminal state (the POST failed with 409) — the
    // guard must release so a retry click after the server-side sync
    // finishes can dispatch a new POST. Same logic as `error` and
    // `idle`.
    inFlightRef.current =
      state.kind === "running" || state.kind === "queued";
  }, [state.kind]);

  return {
    kind: state.kind,
    queuedRunId: state.queuedRunId,
    message: state.message,
    trigger,
    clearQueued,
  };
}
