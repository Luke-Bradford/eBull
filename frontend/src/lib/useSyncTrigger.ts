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
import { useCallback, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import { triggerSync } from "@/api/sync";
import type { SyncTriggerRequest } from "@/api/sync";

export type SyncTriggerKind = "idle" | "running" | "queued" | "error";

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
    const body: SyncTriggerRequest = { scope: "full" };
    try {
      const result = await triggerSync(body);
      setState({
        kind: "queued",
        queuedRunId: result.sync_run_id,
        message: null,
      });
      onTriggered();
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.status === 409
            ? "Sync already running"
            : err.status === 503
              ? "Sync orchestrator disabled"
              : `Failed (HTTP ${err.status})`
          : "Failed";
      setState({ kind: "error", queuedRunId: null, message });
      // Release the guard on the error branch so the operator can
      // click Retry. The button's disabled state would also block a
      // second click on the success branch, so no defensive early
      // reset is needed there.
      inFlightRef.current = false;
    }
    // Deliberately no finally: when the POST succeeds, inFlightRef
    // stays true through the running → queued window until
    // clearQueued() transitions us back to idle. A second click in
    // that window would otherwise slip past the guard while `kind`
    // is still `queued`.
  }, [onTriggered]);

  const clearQueued = useCallback((isRunning: boolean) => {
    setState((prev) => {
      // The `queued` badge survives only until the server confirms
      // the sync actually started. Once isRunning flips to true,
      // drop the badge; the button stays disabled by the caller
      // because the caller also OR's with `isRunning` when wiring
      // `disabled`.
      if (prev.kind === "queued" && isRunning) {
        inFlightRef.current = false;
        return { kind: "idle", queuedRunId: null, message: null };
      }
      // `error` has no auto-reset — caller must click again.
      return prev;
    });
  }, []);

  return {
    kind: state.kind,
    queuedRunId: state.queuedRunId,
    message: state.message,
    trigger,
    clearQueued,
  };
}
