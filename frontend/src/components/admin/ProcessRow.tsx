/**
 * ProcessRow — one row of the admin ProcessesTable (#1076 / #1064).
 *
 * Renders the unified `ProcessRowResponse` envelope. Trigger buttons
 * delegate to the parent (which owns the confirm-modal state); 409s
 * raised by the parent's POST handler are surfaced via
 * `triggerError` and rendered through `reasonTooltip`.
 *
 * Visible-motion (spec §"Visible-motion rules"): a pulsing left border
 * marks `running` and `stale` rows. Pure CSS, no progress data needed.
 * `motion-reduce:` Tailwind variant respects the operator's
 * `prefers-reduced-motion` setting.
 */

import { memo, useEffect, useId, useRef, useState } from "react";
import { Link } from "react-router-dom";

import type { ProcessRowResponse } from "@/api/types";
import { formatDateTime } from "@/lib/format";

import {
  NEXT_RUN_EXPECTED_TOOLTIP,
  REASON_TOOLTIP,
  VERDICT_VISUAL,
  reasonTooltip,
} from "@/components/admin/processStatus";

export interface ProcessRowProps {
  readonly row: ProcessRowResponse;
  // Content signature computed by the parent (`processRowSignature`).
  // Drives the `React.memo` skip decision (#1480): each poll hands the
  // table a fresh JSON snapshot, so every `row` object has a new
  // identity even when nothing changed. A reference compare would
  // repaint all 40 rows every tick (visible flicker + reflow over
  // RDP). The signature lets memo repaint only the rows whose rendered
  // content actually changed. It MUST be a prop (computed at the
  // parent's render time and stored by React), not recomputed inside
  // the comparator — the comparator sees `prev`/`next` at the same
  // instant, so any `Date.now()`-derived term computed there cancels
  // out and the stuck-process elapsed chip would never advance.
  readonly signature: string;
  readonly triggerError: unknown;
  readonly cancelError: unknown;
  readonly busy: boolean;
  readonly onIterate: (row: ProcessRowResponse) => void;
  readonly onFullWash: (row: ProcessRowResponse) => void;
  readonly onCancel: (row: ProcessRowResponse) => void;
}

/**
 * Stable content signature for a process row (#1480).
 *
 * Conservative by design: serialise the whole envelope so any field
 * change forces a repaint, plus the live elapsed-since-heartbeat label
 * for stuck rows so the `no progress Nm` chip keeps advancing (it is
 * the operator's wedge signal — #1474 / #1478) while every quiescent
 * row stays frozen between polls. Cheap for ~40 small objects per tick.
 */
export function processRowSignature(row: ProcessRowResponse): string {
  const heartbeatBase =
    row.active_run?.last_progress_at ?? row.active_run?.started_at ?? null;
  const elapsed =
    row.stale_reasons.includes("mid_flight_stuck") && heartbeatBase !== null
      ? formatElapsedSince(heartbeatBase)
      : "";
  return `${JSON.stringify(row)}|${elapsed}`;
}

function arePropsEqual(prev: ProcessRowProps, next: ProcessRowProps): boolean {
  // `onIterate` / `onFullWash` / `onCancel` are stable refs from the
  // parent (useCallback / useState setters) — compared by reference so
  // a future regression that passes an inline arrow re-renders rather
  // than silently going stale. `triggerError` / `cancelError` are
  // mutation-only (unchanged across polls) — reference compare is right.
  return (
    prev.signature === next.signature &&
    prev.busy === next.busy &&
    prev.triggerError === next.triggerError &&
    prev.cancelError === next.cancelError &&
    prev.onIterate === next.onIterate &&
    prev.onFullWash === next.onFullWash &&
    prev.onCancel === next.onCancel
  );
}

const PENDING_RETRY_TOOLTIP =
  "hiding prior errors during retry — re-shown if retry also fails or fails to reattempt failed subjects.";

function ProcessRowImpl({
  row,
  triggerError,
  cancelError,
  busy,
  onIterate,
  onFullWash,
  onCancel,
}: ProcessRowProps) {
  // Pulse precedence keyed off the single verdict (#1512): self-healing
  // and attention pulse amber/red (something is recovering or wrong);
  // working pulses sky (a run is in flight); current is static.
  // `motion-reduce:animate-none` keeps the colour but stops the
  // animation for operators with prefers-reduced-motion (PR8 carve-out).
  const verdict = row.health_verdict;
  const pulseBorder =
    verdict === "self_healing"
      ? "border-l-4 border-l-amber-500 animate-pulse motion-reduce:animate-none"
      : verdict === "attention"
        ? "border-l-4 border-l-red-500"
        : verdict === "working"
          ? "border-l-4 border-l-sky-500 animate-pulse motion-reduce:animate-none"
          : "border-l-4 border-l-transparent";

  const lastRunLabel = row.last_run
    ? `${formatDateTime(row.last_run.finished_at)} · ${formatDuration(row.last_run.duration_seconds)} · ${
        row.last_run.status
      }`
    : "never";

  const watermarkTooltip = row.watermark?.human ?? "no resume cursor";

  // Per data-engineer skill §7.3 — bootstrap mechanism uses different
  // verbs because iterate / full_wash semantics map to "resume incomplete
  // stages" vs "reset every stage to pending" rather than the
  // watermark-aware fetch on scheduled jobs. Underlying mechanics
  // (iterate / full_wash modes on the trigger endpoint) unchanged; only
  // the operator-visible label changes per mechanism.
  const isBootstrap = row.mechanism === "bootstrap";
  // First install: nothing has ever run, so the only meaningful action is
  // *starting* the bootstrap. "Re-run failed" / "Cancel" cannot apply
  // (no failed stages, no active run) and "Re-run all" is the wrong verb —
  // it's a first run, not a re-run, and there is nothing to wipe. Collapse
  // to a single non-destructive "Run bootstrap" button (#1264). All other
  // bootstrap states keep the full re-run / cancel vocabulary.
  const isFirstRun = isBootstrap && row.status === "pending_first_run";
  // Clean-complete: the bootstrap finished with every stage successful
  // (status='ok'). "Re-run all" is still a legal action (full re-bootstrap)
  // but it is NOT the expected next step — nothing failed — so it must not
  // wear the red destructive styling that signals "fix the failure here".
  // Keep it enabled but de-emphasised to a neutral tone (#1432). The
  // confirm dialog still guards the wipe.
  const isCleanComplete = isBootstrap && row.status === "ok";
  const iterateLabel = isBootstrap ? "Re-run failed" : "Iterate";
  const fullWashLabel = isFirstRun
    ? "Run bootstrap"
    : isBootstrap
      ? "Re-run all"
      : "Full-wash";

  const iterateDisabled = !row.can_iterate || busy;
  const iterateTooltip = row.can_iterate
    ? isBootstrap
      ? "Resume incomplete + failed stages from where they stopped."
      : watermarkTooltip
    : `${iterateLabel} is not available right now — open the process detail page for the precondition that is blocking it.`;

  const fullWashDisabled = !row.can_full_wash || busy;
  const fullWashTooltip = isFirstRun
    ? "Start the first-install bootstrap — populates the universe + filings. Asks for confirmation first."
    : row.can_full_wash
      ? isCleanComplete
        ? "Last bootstrap completed cleanly — this wipes every stage and replays the full install from scratch. Only needed to fully re-bootstrap (confirm required)."
        : isBootstrap
          ? "Reset every stage to pending; full first-install replay (confirm required)."
          : "Reset watermark and re-fetch from epoch (confirm required)."
      : `${fullWashLabel} is not available right now.`;

  const cancelDisabled = !row.can_cancel || busy;
  const cancelTooltip = row.can_cancel
    ? "Cooperative cancel — the worker stops at its next checkpoint."
    : "No active run to cancel.";

  return (
    <tr
      data-process-id={row.process_id}
      data-status={row.status}
      className={`align-top text-sm ${pulseBorder}`}
    >
      <td className="px-2 py-2">
        <div className="flex items-center gap-1">
          <Link
            to={`/admin/processes/${encodeURIComponent(row.process_id)}`}
            className="font-medium text-blue-700 hover:underline dark:text-blue-300"
          >
            {row.display_name}
          </Link>
          {row.description ? (
            <DescriptionTooltip description={row.description} />
          ) : null}
        </div>
        <div className="text-xs text-slate-500 dark:text-slate-400">
          {row.process_id} · {row.mechanism}
        </div>
        {row.status === "failed" && row.last_n_errors.length > 0 ? (
          <ErrorPreview errors={row.last_n_errors} />
        ) : null}
      </td>
      <td className="px-2 py-2">
        <span
          data-testid="lane-chip"
          aria-label={`Lane: ${row.lane}`}
          className="inline-flex rounded-full border bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
        >
          {row.lane}
        </span>
      </td>
      <td className="px-2 py-2">
        <StatusPill row={row} />
        <VerdictReason row={row} />
      </td>
      <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-400">
        {lastRunLabel}
      </td>
      <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-400">
        {isBootstrap ? (
          // Bootstrap stages run as a fixed sequence, not on a cadence
          // (data-engineer skill §7.2). Show a placeholder rather than
          // a phantom schedule.
          <span className="text-slate-400 dark:text-slate-500">—</span>
        ) : (
          <>
            <div>{row.cadence_human}</div>
            {row.next_fire_at ? (
              <div
                className="text-slate-500 dark:text-slate-500"
                title={NEXT_RUN_EXPECTED_TOOLTIP}
              >
                next (expected): {formatDateTime(row.next_fire_at)}
              </div>
            ) : null}
          </>
        )}
      </td>
      <td className="px-2 py-2 text-right">
        <div className="flex flex-wrap items-center justify-end gap-1">
          {isFirstRun ? null : (
            <ActionButton
              label={iterateLabel}
              tooltip={iterateTooltip}
              disabled={iterateDisabled}
              onClick={() => onIterate(row)}
            />
          )}
          <ActionButton
            label={fullWashLabel}
            tooltip={fullWashTooltip}
            disabled={fullWashDisabled}
            onClick={() => onFullWash(row)}
            tone={isFirstRun ? "primary" : isCleanComplete ? "default" : "danger"}
          />
          {isFirstRun ? null : (
            <ActionButton
              label="Cancel"
              tooltip={cancelTooltip}
              disabled={cancelDisabled}
              onClick={() => onCancel(row)}
            />
          )}
        </div>
        {triggerError ? (
          <div
            role="status"
            className="mt-1 text-right text-[11px] text-red-700 dark:text-red-300"
            title={reasonTooltip(triggerError)}
          >
            trigger rejected
          </div>
        ) : null}
        {cancelError ? (
          <div
            role="status"
            className="mt-1 text-right text-[11px] text-red-700 dark:text-red-300"
            title={reasonTooltip(cancelError)}
          >
            cancel rejected
          </div>
        ) : null}
      </td>
    </tr>
  );
}

/**
 * Memoised row. Repaints only when `arePropsEqual` returns false —
 * i.e. when the content signature, busy flag, or an error ref changes.
 * Polls that return an unchanged snapshot are a no-op at the DOM layer,
 * which is what kills the per-tick flicker + reflow (#1480).
 */
export const ProcessRow = memo(ProcessRowImpl, arePropsEqual);

function VerdictReason({ row }: { row: ProcessRowResponse }) {
  // #1512 — one inline reason line (folds #1230: visible, not hover-only).
  // For a wedged run we append the live elapsed-since-heartbeat ("running
  // but no progress 7m") — the operator's wedge signal (#1474 / #1478),
  // computed client-side from active_run so it keeps advancing between
  // polls. Empty reason (current / plain working) renders nothing.
  if (!row.verdict_reason) return null;
  const heartbeatBase =
    row.active_run?.last_progress_at ?? row.active_run?.started_at ?? null;
  const elapsed =
    row.stale_reasons.includes("mid_flight_stuck") && heartbeatBase !== null
      ? ` ${formatElapsedSince(heartbeatBase)}`
      : "";
  return (
    <div
      data-testid="verdict-reason"
      className="mt-1 text-[11px] text-slate-600 dark:text-slate-400"
    >
      {row.verdict_reason}
      {elapsed}
    </div>
  );
}

function StatusPill({ row }: { row: ProcessRowResponse }) {
  // #1512 — render the single computed verdict, not the raw status.
  const visual = VERDICT_VISUAL[row.health_verdict];
  const tooltip = row.self_healing ? PENDING_RETRY_TOOLTIP : undefined;
  return (
    <span
      data-testid="status-pill"
      data-verdict={row.health_verdict}
      title={tooltip}
      aria-label={`Health: ${visual.label}`}
      className={`inline-flex items-center rounded-full border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${visual.toneClass}`}
    >
      {visual.label}
    </span>
  );
}

function ActionButton({
  label,
  tooltip,
  disabled,
  onClick,
  tone = "default",
}: {
  label: string;
  tooltip: string;
  disabled: boolean;
  onClick: () => void;
  tone?: "default" | "danger" | "primary";
}) {
  const toneClass =
    tone === "danger"
      ? "border-red-300 bg-white text-red-700 hover:bg-red-50 dark:border-red-900 dark:bg-slate-900 dark:text-red-300 dark:hover:bg-red-950/40"
      : tone === "primary"
        ? "border-blue-600 bg-blue-600 text-white hover:bg-blue-700 dark:border-blue-500 dark:bg-blue-600 dark:text-white dark:hover:bg-blue-700"
        : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40";
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={tooltip}
      className={`rounded border px-2 py-0.5 text-xs font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${toneClass}`}
    >
      {label}
    </button>
  );
}

function ErrorPreview({
  errors,
}: {
  errors: ProcessRowResponse["last_n_errors"];
}) {
  // Spec §"Error display rules" — inline preview is always visible
  // (no click-to-reveal); drill-in shows the full list on the Errors
  // tab. Show up to two error classes inline; truncate the rest with
  // a `+N more` link to the drill-in.
  const head = errors.slice(0, 2);
  const remainder = errors.length - head.length;
  return (
    <ul className="mt-1 space-y-0.5 text-xs text-red-700 dark:text-red-300">
      {head.map((e) => (
        <li key={e.error_class} className="truncate" title={e.sample_message}>
          <span className="font-medium">{e.error_class}</span>{" "}
          <span className="text-slate-500 dark:text-slate-400">
            (×{e.count})
          </span>
          {e.sample_subject ? (
            <span className="ml-1 text-slate-500 dark:text-slate-400">
              · {e.sample_subject}
            </span>
          ) : null}
        </li>
      ))}
      {remainder > 0 ? (
        <li className="text-[11px] text-slate-500 dark:text-slate-400">
          +{remainder} more
        </li>
      ) : null}
    </ul>
  );
}

function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds - m * 60);
  return `${m}m ${s.toString().padStart(2, "0")}s`;
}

/**
 * Coarse "Nm" formatter for the mid_flight_stuck chip suffix (PR8).
 * The polling cadence is 5s when any row is running, so sub-minute
 * precision is theatre — round to whole minutes.
 */
function formatElapsedSince(iso: string): string {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "";
  const sec = Math.max(0, Math.round((Date.now() - t) / 1000));
  if (sec < 60) return `${sec}s`;
  return `${Math.round(sec / 60)}m`;
}

/**
 * ⓘ tooltip showing operator-facing description (PR4 #1082).
 *
 * Hover-or-click popover. Native ``title`` was the original
 * implementation but operator feedback flagged two problems:
 *   - ~1.5s browser-default delay before the title surfaces
 *   - Clicking the icon (a natural expectation) hid the title
 *     instead of pinning it
 *
 * The replacement is a small CSS-driven popover triggered by
 * ``onPointerEnter`` / ``onPointerLeave`` (no delay) and a click
 * toggle that pins it open until clicked again or focus leaves the
 * row. ``aria-label`` still carries the description for screen
 * readers; the popover is also reachable by Tab so keyboard-only
 * operators can press Enter/Space to pin it.
 */
function DescriptionTooltip({ description }: { description: string }) {
  const [hovered, setHovered] = useState(false);
  const [pinned, setPinned] = useState(false);
  const containerRef = useRef<HTMLSpanElement>(null);
  const tooltipId = useId();

  // Click outside closes the pinned popover. Idle when not pinned so
  // the listener doesn't run for every row in the table.
  useEffect(() => {
    if (!pinned) return;
    function handlePointerDown(e: PointerEvent) {
      const target = e.target as Node | null;
      if (target && containerRef.current && !containerRef.current.contains(target)) {
        setPinned(false);
      }
    }
    document.addEventListener("pointerdown", handlePointerDown);
    return () =>
      document.removeEventListener("pointerdown", handlePointerDown);
  }, [pinned]);

  const visible = hovered || pinned;
  return (
    <span
      ref={containerRef}
      className="relative inline-flex"
      onPointerEnter={() => setHovered(true)}
      onPointerLeave={() => setHovered(false)}
    >
      <button
        type="button"
        aria-label={description}
        aria-expanded={visible}
        // PR4 round 3 a11y fix — link the trigger to the rendered
        // tooltip via aria-describedby so AT announces the popover
        // when it surfaces. Always reference the id (tooltip span is
        // rendered conditionally; AT just sees no descriptor while
        // the span is unmounted, which is the correct quiet state).
        aria-describedby={visible ? tooltipId : undefined}
        data-testid="process-description-tooltip"
        onClick={() => setPinned((p) => !p)}
        className="inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-slate-400 text-[10px] font-bold text-slate-500 hover:border-slate-600 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-blue-500 dark:border-slate-500 dark:text-slate-400 dark:hover:border-slate-300 dark:hover:text-slate-200"
      >
        i
      </button>
      {visible ? (
        <span
          id={tooltipId}
          role="tooltip"
          className="absolute left-5 top-0 z-10 w-64 rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-normal normal-case tracking-normal text-slate-700 shadow-lg dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200"
        >
          {description}
        </span>
      ) : null}
    </span>
  );
}

// Re-export so test files can assert on the canonical mapping rather
// than duplicating it.
export { REASON_TOOLTIP };
