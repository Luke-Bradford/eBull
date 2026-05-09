/**
 * ProcessesTable — admin control hub processes view (#1076 / #1064).
 *
 * Owns:
 *   - lane-chip filter state
 *   - sort (failed/stale first, then status priority, then next_fire ASC)
 *   - per-row trigger / cancel handlers + per-row error stash
 *   - confirm-modal state for full-wash (typed-name) and cancel
 *
 * Polling lives in the parent (`useProcesses`); ProcessesTable is a
 * pure render of the snapshot plus the operator-action plumbing.
 *
 * Spec §"Failure-mode invariants": when the envelope's `partial` flag
 * is set, render a small inline banner above the table — never a
 * top-of-page error banner. One adapter failing is not "all sources
 * failed" (loading-error-empty-states.md rule).
 */

import { useCallback, useMemo, useState } from "react";

import { ApiError } from "@/api/client";
import { cancelProcess, triggerProcess } from "@/api/processes";
import type {
  ProcessLane,
  ProcessListResponse,
  ProcessRowResponse,
} from "@/api/types";
import { Modal } from "@/components/ui/Modal";

import { LaneFilter } from "@/components/admin/LaneFilter";
import { ProcessRow } from "@/components/admin/ProcessRow";
import { StaleBanner } from "@/components/admin/StaleBanner";
import {
  STATUS_SORT_PRIORITY,
  reasonTooltip,
} from "@/components/admin/processStatus";

export interface ProcessesTableProps {
  readonly snapshot: ProcessListResponse;
  readonly onMutationSuccess: () => void;
}

interface RowErrorState {
  readonly trigger?: unknown;
  readonly cancel?: unknown;
}

export function ProcessesTable({
  snapshot,
  onMutationSuccess,
}: ProcessesTableProps) {
  const [selectedLane, setSelectedLane] = useState<ProcessLane | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [rowErrors, setRowErrors] = useState<Record<string, RowErrorState>>(
    {},
  );
  const [fullWashTarget, setFullWashTarget] =
    useState<ProcessRowResponse | null>(null);
  const [cancelTarget, setCancelTarget] = useState<ProcessRowResponse | null>(
    null,
  );

  const counts = useMemo(() => {
    const out: Partial<Record<ProcessLane, number>> = {};
    for (const r of snapshot.rows) {
      out[r.lane] = (out[r.lane] ?? 0) + 1;
    }
    return out;
  }, [snapshot.rows]);

  const visibleRows = useMemo(() => {
    const rows =
      selectedLane === null
        ? snapshot.rows
        : snapshot.rows.filter((r) => r.lane === selectedLane);
    return [...rows].sort(compareRows);
  }, [snapshot.rows, selectedLane]);

  const setRowError = useCallback(
    (processId: string, patch: RowErrorState) => {
      setRowErrors((prev) => ({
        ...prev,
        [processId]: { ...prev[processId], ...patch },
      }));
    },
    [],
  );

  const clearRowError = useCallback(
    (processId: string, key: keyof RowErrorState) => {
      setRowErrors((prev) => {
        const current = prev[processId];
        if (!current || current[key] === undefined) return prev;
        const next = { ...current, [key]: undefined };
        return { ...prev, [processId]: next };
      });
    },
    [],
  );

  const handleIterate = useCallback(
    async (row: ProcessRowResponse) => {
      clearRowError(row.process_id, "trigger");
      setBusyId(row.process_id);
      try {
        await triggerProcess(row.process_id, { mode: "iterate" });
        onMutationSuccess();
      } catch (err) {
        setRowError(row.process_id, { trigger: err });
        if (!(err instanceof ApiError)) {
          // Unexpected (network etc.). Surface in console; fixed phrase
          // already shown in the row.
          console.error("triggerProcess(iterate) failed", err);
        }
      } finally {
        setBusyId(null);
      }
    },
    [clearRowError, onMutationSuccess, setRowError],
  );

  const handleFullWashConfirmed = useCallback(
    async (row: ProcessRowResponse) => {
      clearRowError(row.process_id, "trigger");
      setBusyId(row.process_id);
      try {
        await triggerProcess(row.process_id, { mode: "full_wash" });
        setFullWashTarget(null);
        onMutationSuccess();
      } catch (err) {
        setRowError(row.process_id, { trigger: err });
        setFullWashTarget(null);
        if (!(err instanceof ApiError)) {
          console.error("triggerProcess(full_wash) failed", err);
        }
      } finally {
        setBusyId(null);
      }
    },
    [clearRowError, onMutationSuccess, setRowError],
  );

  const handleCancelConfirmed = useCallback(
    async (row: ProcessRowResponse, mode: "cooperative" | "terminate") => {
      clearRowError(row.process_id, "cancel");
      setBusyId(row.process_id);
      try {
        await cancelProcess(row.process_id, { mode });
        setCancelTarget(null);
        onMutationSuccess();
      } catch (err) {
        setRowError(row.process_id, { cancel: err });
        setCancelTarget(null);
        if (!(err instanceof ApiError)) {
          console.error("cancelProcess failed", err);
        }
      } finally {
        setBusyId(null);
      }
    },
    [clearRowError, onMutationSuccess, setRowError],
  );

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <LaneFilter
          selected={selectedLane}
          counts={counts}
          onSelect={setSelectedLane}
        />
        <div className="text-xs text-slate-500 dark:text-slate-400">
          {visibleRows.length} of {snapshot.rows.length} processes
        </div>
      </div>

      <StaleBanner rows={snapshot.rows} />

      {snapshot.partial ? (
        <div
          role="status"
          className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200"
        >
          One adapter is unavailable — some lanes are omitted from this
          snapshot. The other lanes still reflect live state.
        </div>
      ) : null}

      {visibleRows.length === 0 ? (
        <p className="text-sm text-slate-500 dark:text-slate-400">
          No processes match the current lane filter.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
              <tr>
                <th className="px-2 py-2">Process</th>
                <th className="px-2 py-2">Lane</th>
                <th className="px-2 py-2">Status</th>
                <th className="px-2 py-2">Last run</th>
                <th className="px-2 py-2">Cadence</th>
                <th className="px-2 py-2 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
              {visibleRows.map((row) => (
                <ProcessRow
                  key={row.process_id}
                  row={row}
                  triggerError={rowErrors[row.process_id]?.trigger}
                  cancelError={rowErrors[row.process_id]?.cancel}
                  busy={busyId === row.process_id}
                  onIterate={handleIterate}
                  onFullWash={(r) => setFullWashTarget(r)}
                  onCancel={(r) => setCancelTarget(r)}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}

      {fullWashTarget ? (
        <FullWashConfirmDialog
          row={fullWashTarget}
          busy={busyId === fullWashTarget.process_id}
          onCancel={() => setFullWashTarget(null)}
          onConfirm={() => handleFullWashConfirmed(fullWashTarget)}
        />
      ) : null}

      {cancelTarget ? (
        <CancelConfirmDialog
          row={cancelTarget}
          busy={busyId === cancelTarget.process_id}
          onCancel={() => setCancelTarget(null)}
          onConfirm={(mode) => handleCancelConfirmed(cancelTarget, mode)}
        />
      ) : null}
    </div>
  );
}

function compareRows(a: ProcessRowResponse, b: ProcessRowResponse): number {
  const sa = STATUS_SORT_PRIORITY[a.status] ?? 99;
  const sb = STATUS_SORT_PRIORITY[b.status] ?? 99;
  if (sa !== sb) return sa - sb;
  // Failed jobs without a next-fire (one-shot) come after the ones
  // that will actually retry — operator action vs auto-recovery.
  const ta = a.next_fire_at ? Date.parse(a.next_fire_at) : Infinity;
  const tb = b.next_fire_at ? Date.parse(b.next_fire_at) : Infinity;
  if (ta !== tb) return ta - tb;
  return a.display_name.localeCompare(b.display_name);
}

// ---------------------------------------------------------------------------
// Confirm dialogs
// ---------------------------------------------------------------------------

function FullWashConfirmDialog({
  row,
  busy,
  onCancel,
  onConfirm,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const [typed, setTyped] = useState("");
  const matches = typed === row.display_name;
  return (
    <Modal
      isOpen={true}
      onRequestClose={onCancel}
      labelledBy="full-wash-confirm-title"
    >
      <h2
        id="full-wash-confirm-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        Confirm full-wash
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        Full-wash resets the watermark for{" "}
        <span className="font-medium">{row.display_name}</span> and re-fetches
        from epoch. ON CONFLICT idempotency prevents row duplication; the
        cost is bandwidth and rate-budget.
      </p>
      <p className="mt-2 text-xs text-slate-500 dark:text-slate-400">
        Type the process name exactly to enable the confirm button.
      </p>
      <label className="mt-3 block text-xs font-medium text-slate-700 dark:text-slate-200">
        Process name
        <input
          type="text"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          autoFocus
          aria-label="Process name confirmation"
          placeholder={row.display_name}
          className="mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1 text-sm font-mono text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
        />
      </label>
      <div className="mt-4 flex justify-end gap-2">
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={onConfirm}
          disabled={!matches || busy}
          className="rounded border border-red-400 bg-red-600 px-3 py-1 text-xs font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-700 dark:bg-red-700 dark:hover:bg-red-800"
        >
          {busy ? "Triggering…" : "Full-wash"}
        </button>
      </div>
    </Modal>
  );
}

function CancelConfirmDialog({
  row,
  busy,
  onCancel,
  onConfirm,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  onCancel: () => void;
  onConfirm: (mode: "cooperative" | "terminate") => void;
}) {
  // Codex pre-push BLOCKING: a closed `<details>` keeps the terminate
  // `<button>` tabbable in the DOM. The Modal focus trap walks the
  // dialog subtree on open and lands on the first tabbable, which
  // would be the hidden destructive button. Render terminate only
  // when the operator has explicitly opened the More disclosure.
  const [moreOpen, setMoreOpen] = useState(false);
  return (
    <Modal
      isOpen={true}
      onRequestClose={onCancel}
      labelledBy="cancel-confirm-title"
    >
      <h2
        id="cancel-confirm-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        Cancel {row.display_name}?
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        Cooperative cancel signals the worker to stop at its next checkpoint.
        The active checkpoint completes (writes are idempotent); the run
        transitions to <span className="font-mono">cancelled</span> once the
        worker observes the flag.
      </p>
      <div className="mt-3 text-xs text-slate-600 dark:text-slate-300">
        <button
          type="button"
          onClick={() => setMoreOpen((v) => !v)}
          aria-expanded={moreOpen}
          className="text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
        >
          {moreOpen ? "▾" : "▸"} More — terminate (escape hatch)
        </button>
        {moreOpen ? (
          <div className="mt-2">
            <p className="leading-relaxed">
              Terminate marks for cleanup. Active SEC fetches continue. To
              force a stop, use cooperative cancel and wait, or restart the
              jobs process.
            </p>
            <button
              type="button"
              onClick={() => onConfirm("terminate")}
              disabled={busy}
              className="mt-2 rounded border border-red-300 bg-red-50 px-2 py-1 text-xs font-medium text-red-700 hover:bg-red-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300 dark:hover:bg-red-950/60"
            >
              Terminate (mark for cleanup)
            </button>
          </div>
        ) : null}
      </div>
      <div className="mt-4 flex justify-end gap-2">
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Keep running
        </button>
        <button
          type="button"
          onClick={() => onConfirm("cooperative")}
          disabled={busy}
          className="rounded border border-amber-400 bg-amber-500 px-3 py-1 text-xs font-medium text-white hover:bg-amber-600 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-700 dark:bg-amber-700 dark:hover:bg-amber-800"
        >
          {busy ? "Cancelling…" : "Cancel cooperatively"}
        </button>
      </div>
    </Modal>
  );
}

// Re-export so tests can assert against the canonical reasonTooltip
// without re-importing from another module.
export { reasonTooltip };
