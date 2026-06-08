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

import { useCallback, useEffect, useMemo, useState } from "react";

import { ApiError } from "@/api/client";
import { cancelProcess, triggerProcess } from "@/api/processes";
import type {
  ProcessLane,
  ProcessListResponse,
  ProcessRowResponse,
} from "@/api/types";
import { Modal } from "@/components/ui/Modal";

import { LaneFilter } from "@/components/admin/LaneFilter";
import { ProcessRow, processRowSignature } from "@/components/admin/ProcessRow";
import { StaleBanner } from "@/components/admin/StaleBanner";
import {
  VERDICT_SORT_PRIORITY,
  reasonTooltip,
} from "@/components/admin/processStatus";

export interface ProcessesTableProps {
  readonly snapshot: ProcessListResponse;
  readonly onMutationSuccess: () => void;
  // PR3a #1064 — bootstrap-incomplete render mode. When the
  // operator's first-install bootstrap is not yet ``complete`` the
  // table hides every non-bootstrap category so the only path
  // forward is the bootstrap row's "Re-run failed" / "Re-run all"
  // buttons. Pass ``null`` to render every row regardless (e.g. when
  // the bootstrap-status fetch is pending or errored — fail-open).
  // See ``.claude/skills/data-engineer/SKILL.md`` §7.1 for the
  // operator design intent.
  readonly bootstrapStatus?:
    | "pending"
    | "running"
    | "complete"
    | "partial_error"
    | null;
  // #1513 — client-side completion time of the last successful poll,
  // rendered as the header's "checked HH:MM" freshness anchor. Optional;
  // omitted (null) before the first poll lands.
  readonly checkedAt?: Date | null;
  // #1508 / C4 — the scheduler/worker process is not running
  // (`/system/status` `engine_down`). Threaded straight to the StaleBanner,
  // which raises a hard-red "Jobs engine not running" banner that wins over
  // every per-row verdict (nothing is updating, so the per-row summary lies).
  readonly engineDown?: boolean;
}

interface RowErrorState {
  readonly trigger?: unknown;
  readonly cancel?: unknown;
}

export function ProcessesTable({
  snapshot,
  onMutationSuccess,
  bootstrapStatus = null,
  checkedAt = null,
  engineDown = false,
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

  // PR3a #1064 — bootstrap-only mode. When bootstrap is not complete
  // every other lane / mechanism is hidden so the operator sees only
  // the bootstrap row and its child stages. The lane filter is also
  // hidden in this mode (no point filtering a one-row list). The
  // ``null`` fallback (status fetch pending or errored) is fail-open:
  // render the full table so a bootstrap-status hiccup doesn't lock
  // the operator out of every other category.
  const bootstrapOnly =
    bootstrapStatus !== null && bootstrapStatus !== "complete";

  const baseRows = useMemo(() => {
    if (bootstrapOnly) {
      return snapshot.rows.filter((r) => r.mechanism === "bootstrap");
    }
    // #1508 — once bootstrap is ``complete`` the first-install row is done
    // and does not belong in the steady-state ops list (operator: "there
    // are still bootstrap jobs showing"). Fail-open: when the status fetch
    // returned null (pending/errored) keep the row rather than hide
    // information on uncertainty.
    if (bootstrapStatus === "complete") {
      return snapshot.rows.filter((r) => r.mechanism !== "bootstrap");
    }
    return snapshot.rows;
  }, [snapshot.rows, bootstrapOnly, bootstrapStatus]);

  const counts = useMemo(() => {
    const out: Partial<Record<ProcessLane, number>> = {};
    for (const r of baseRows) {
      out[r.lane] = (out[r.lane] ?? 0) + 1;
    }
    return out;
  }, [baseRows]);

  const visibleRows = useMemo(() => {
    const rows =
      selectedLane === null
        ? baseRows
        : baseRows.filter((r) => r.lane === selectedLane);
    return [...rows].sort(compareRows);
  }, [baseRows, selectedLane]);

  // #1508 C3 — two-state page. ONLY `attention` pins (the operator must
  // act). The three calm verdicts — `current` (steady, fresh), `working`
  // (a live run — system working as designed) and `self_healing`
  // (auto-recovering) — all fold behind ONE inline disclosure. A live run
  // does not lose its Cancel affordance: the Cancel button is gated on
  // `can_cancel` inside ProcessRow, so it is still reachable once the
  // operator expands the disclosure — it just no longer screams for
  // attention when nothing is wrong. compareRows floats attention to the
  // top, so the split preserves order within each group. Disabled in
  // bootstrap-only mode: there the single bootstrap row IS the primary
  // action surface and must never be tucked behind a disclosure.
  const pinnedRows = useMemo(
    () =>
      bootstrapOnly
        ? visibleRows
        : visibleRows.filter((r) => !isCollapsible(r.health_verdict)),
    [visibleRows, bootstrapOnly],
  );
  const collapsedRows = useMemo(
    () =>
      bootstrapOnly
        ? []
        : visibleRows.filter((r) => isCollapsible(r.health_verdict)),
    [visibleRows, bootstrapOnly],
  );
  const collapsedSelfHealing = collapsedRows.filter(
    (r) => r.health_verdict === "self_healing",
  ).length;
  const collapsedWorking = collapsedRows.filter(
    (r) => r.health_verdict === "working",
  ).length;
  const collapsedCurrent =
    collapsedRows.length - collapsedSelfHealing - collapsedWorking;
  const [showCollapsed, setShowCollapsed] = useState(false);
  // Reset to the default-collapsed state whenever the lane filter changes,
  // so switching lanes never carries another lane's expanded state over
  // (Codex ckpt-2) — each lane re-collapses its own quiet rows.
  useEffect(() => {
    setShowCollapsed(false);
  }, [selectedLane]);

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

  // Shared row renderer so the attention group and the (collapsible)
  // non-actionable group emit identical ProcessRow markup. The signature
  // prop keeps React.memo skipping unchanged rows on every poll (#1480).
  const renderRow = (row: ProcessRowResponse) => (
    <ProcessRow
      key={row.process_id}
      row={row}
      signature={processRowSignature(row)}
      triggerError={rowErrors[row.process_id]?.trigger}
      cancelError={rowErrors[row.process_id]?.cancel}
      busy={busyId === row.process_id}
      onIterate={handleIterate}
      // Stable setters passed directly so memo's reference compare holds.
      onFullWash={setFullWashTarget}
      onCancel={setCancelTarget}
    />
  );

  return (
    <div className="space-y-3">
      {/* In bootstrap-only mode the lane chips would offer one option
          (whichever lane the bootstrap row sits in) — hide them and use
          the freed space for the explanatory banner. */}
      {bootstrapOnly ? (
        <div
          role="status"
          aria-label="Bootstrap incomplete — only the bootstrap row is shown"
          className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-xs text-sky-800 dark:border-sky-900 dark:bg-sky-950/40 dark:text-sky-200"
        >
          First-install bootstrap is{" "}
          <span className="font-semibold">{bootstrapStatus}</span>. Other
          categories are gated until bootstrap reaches{" "}
          <span className="font-semibold">complete</span> —{" "}
          {/*
            Deliberately keyed on `bootstrapStatus === "pending"` (the
            bootstrap-status-endpoint vocabulary: pending|running|complete|
            partial_error, wired from AdminPage `bootstrap.data?.status`),
            NOT on the row-level `status === "pending_first_run"`
            (ProcessStatus vocabulary) used by ProcessRow / ProcessDetailPage.
            They are two enums over the SAME underlying `bootstrap_state`:
            state 'pending' → endpoint "pending" AND row "pending_first_run",
            so on a real first run both predicates fire together. Do NOT
            "align" the literals — they intentionally differ because they
            read different (but co-derived) status fields. (#1452)
          */}
          {bootstrapStatus === "pending"
            ? "run the bootstrap from the bootstrap row."
            : "re-run failed stages or re-run all from the bootstrap row."}
        </div>
      ) : (
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
      )}

      <StaleBanner
        rows={baseRows}
        checkedAt={checkedAt}
        engineDown={engineDown}
      />

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
              {pinnedRows.map(renderRow)}
              {collapsedRows.length > 0 ? (
                <tr data-testid="collapsed-disclosure">
                  <td colSpan={6} className="px-2 py-2">
                    <button
                      type="button"
                      onClick={() => setShowCollapsed((v) => !v)}
                      aria-expanded={showCollapsed}
                      className="text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
                    >
                      <span aria-hidden="true">
                        {showCollapsed ? "▾ " : "▸ "}
                      </span>
                      {collapsedLabel(
                        collapsedCurrent,
                        collapsedWorking,
                        collapsedSelfHealing,
                      )}
                      {" — "}
                      {showCollapsed ? "hide" : "show"}
                    </button>
                  </td>
                </tr>
              ) : null}
              {showCollapsed ? collapsedRows.map(renderRow) : null}
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

/** #1508 C3 — the three calm verdicts fold into the disclosure: steady-fresh
 *  `current`, in-flight `working`, and auto-recovering `self_healing`. Only
 *  `attention` (the operator must act) stays pinned. */
function isCollapsible(verdict: ProcessRowResponse["health_verdict"]): boolean {
  return verdict !== "attention";
}

/** Disclosure label for the collapsed rows (#1508 C3), e.g.
 *  "12 current · 2 working · 3 self-healing". Each arg is the count of
 *  collapsed rows of that verdict. */
function collapsedLabel(
  current: number,
  working: number,
  selfHealing: number,
): string {
  const parts: string[] = [];
  if (current > 0) parts.push(`${current} current`);
  if (working > 0) parts.push(`${working} working`);
  if (selfHealing > 0) parts.push(`${selfHealing} self-healing`);
  return parts.join(" · ");
}

function compareRows(a: ProcessRowResponse, b: ProcessRowResponse): number {
  // #1508 C3: two-state sort — only `attention` floats to the pinned
  // region; the three calm verdicts (current/working/self_healing) share
  // one rank and stay in their original relative order. This collapses the
  // prior status-based priority + synthetic `stale` rank into one verdict,
  // so an `ok`/`idle` row that goes overdue (now verdict=attention)
  // surfaces at the top without a separate stale-tuple consult.
  const sa = VERDICT_SORT_PRIORITY[a.health_verdict] ?? 99;
  const sb = VERDICT_SORT_PRIORITY[b.health_verdict] ?? 99;
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
  // PR3a #1064 — bootstrap mechanism uses different verbs per
  // data-engineer skill §7.3. Modal heading + body + confirm-button
  // copy follow the same swap; the underlying POST stays mode='full_wash'.
  //
  // Operator 2026-05-22 (#1264): the prior type-to-confirm gate
  // ('type the process name exactly') was friction without safety —
  // process names are internal identifiers the operator does not need
  // to know. Replaced with a single click-through confirm. The verb
  // button itself stays red so the destructive intent is still clear;
  // the Cancel button remains as the obvious bail-out.
  const isBootstrap = row.mechanism === "bootstrap";
  // First install (#1432): nothing has run yet, so this is a *start*, not
  // a destructive re-run. Non-destructive copy + a primary (blue) confirm
  // button rather than the red "wipe everything" framing the re-run states
  // use. The underlying POST is still mode='full_wash' (resets stages to
  // pending), but on a never-run row there is nothing to wipe.
  const isFirstRun = isBootstrap && row.status === "pending_first_run";
  const heading = isFirstRun
    ? "Start bootstrap"
    : isBootstrap
      ? "Confirm Re-run all"
      : "Confirm full-wash";
  const verb = isFirstRun
    ? "Run bootstrap"
    : isBootstrap
      ? "Re-run all"
      : "Full-wash";
  const description = isFirstRun
    ? `Start the first-install bootstrap for ${row.display_name}. Walks the init → eToro → SEC stage sequence to populate the tradable universe and filings. Safe to run — this is the first run, nothing is overwritten.`
    : isBootstrap
      ? `Re-run all resets every stage of ${row.display_name} to pending and replays the full first-install bootstrap. Stages re-run from scratch; ingested rows are deduped at the destination by ON CONFLICT.`
      : `Full-wash resets the watermark for ${row.display_name} and re-fetches from epoch. ON CONFLICT idempotency prevents row duplication; the cost is bandwidth and rate-budget.`;
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
        {heading}
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        {description}
      </p>
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
          disabled={busy}
          autoFocus
          className={
            isFirstRun
              ? "rounded border border-blue-600 bg-blue-600 px-3 py-1 text-xs font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-blue-500 dark:bg-blue-600 dark:hover:bg-blue-700"
              : "rounded border border-red-400 bg-red-600 px-3 py-1 text-xs font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-700 dark:bg-red-700 dark:hover:bg-red-800"
          }
        >
          {busy ? "Triggering…" : verb}
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
