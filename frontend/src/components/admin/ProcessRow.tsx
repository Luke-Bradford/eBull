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

import { Link } from "react-router-dom";

import type { ProcessRowResponse, StaleReason } from "@/api/types";
import { formatDateTime } from "@/lib/format";

import {
  REASON_TOOLTIP,
  STALE_REASON_LABEL,
  STATUS_VISUAL,
  reasonTooltip,
} from "@/components/admin/processStatus";

export interface ProcessRowProps {
  readonly row: ProcessRowResponse;
  readonly triggerError: unknown;
  readonly cancelError: unknown;
  readonly busy: boolean;
  readonly onIterate: (row: ProcessRowResponse) => void;
  readonly onFullWash: (row: ProcessRowResponse) => void;
  readonly onCancel: (row: ProcessRowResponse) => void;
}

const PENDING_RETRY_TOOLTIP =
  "hiding prior errors during retry — re-shown if retry also fails or fails to reattempt failed subjects.";

export function ProcessRow({
  row,
  triggerError,
  cancelError,
  busy,
  onIterate,
  onFullWash,
  onCancel,
}: ProcessRowProps) {
  const visual = STATUS_VISUAL[row.status];
  // Pulse precedence (spec §"Visible-motion rules"): stale rows pulse
  // amber even while running, because mid_flight_stuck overlaps
  // status="running" by definition. `motion-reduce:animate-none`
  // keeps the colour but stops the animation for operators with
  // prefers-reduced-motion (PR8 carve-out from PR9 a11y sweep — avoid
  // a regression window).
  const isStale = row.stale_reasons.length > 0;
  const pulseBorder = isStale
    ? "border-l-4 border-l-amber-500 animate-pulse motion-reduce:animate-none"
    : visual.pulse
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
  const iterateLabel = isBootstrap ? "Re-run failed" : "Iterate";
  const fullWashLabel = isBootstrap ? "Re-run all" : "Full-wash";

  const iterateDisabled = !row.can_iterate || busy;
  const iterateTooltip = row.can_iterate
    ? isBootstrap
      ? "Resume incomplete + failed stages from where they stopped."
      : watermarkTooltip
    : `${iterateLabel} is not available right now — open the process detail page for the precondition that is blocking it.`;

  const fullWashDisabled = !row.can_full_wash || busy;
  const fullWashTooltip = row.can_full_wash
    ? isBootstrap
      ? "Reset every stage to pending; full first-install replay (typed-name confirm required)."
      : "Reset watermark and re-fetch from epoch (typed-name confirm required)."
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
        <Link
          to={`/admin/processes/${encodeURIComponent(row.process_id)}`}
          className="font-medium text-blue-700 hover:underline dark:text-blue-300"
        >
          {row.display_name}
        </Link>
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
        {isStale ? <StaleChips row={row} /> : null}
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
              <div className="text-slate-500 dark:text-slate-500">
                next: {formatDateTime(row.next_fire_at)}
              </div>
            ) : null}
          </>
        )}
      </td>
      <td className="px-2 py-2 text-right">
        <div className="flex flex-wrap items-center justify-end gap-1">
          <ActionButton
            label={iterateLabel}
            tooltip={iterateTooltip}
            disabled={iterateDisabled}
            onClick={() => onIterate(row)}
          />
          <ActionButton
            label={fullWashLabel}
            tooltip={fullWashTooltip}
            disabled={fullWashDisabled}
            onClick={() => onFullWash(row)}
            tone="danger"
          />
          <ActionButton
            label="Cancel"
            tooltip={cancelTooltip}
            disabled={cancelDisabled}
            onClick={() => onCancel(row)}
          />
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

function StaleChips({ row }: { row: ProcessRowResponse }) {
  // Stale-reason chips (PR8 / #1083). One subtle pill per reason;
  // mid_flight_stuck includes the elapsed-since-heartbeat suffix
  // computed from `active_run.last_progress_at` (or `started_at` as
  // fallback) so the operator sees "no progress 7m" rather than just
  // "no progress".
  const heartbeatBase =
    row.active_run?.last_progress_at ?? row.active_run?.started_at ?? null;
  return (
    <div
      className="mt-1 flex flex-wrap gap-1"
      data-testid="stale-chips"
    >
      {row.stale_reasons.map((reason) => (
        <StaleChip
          key={reason}
          reason={reason}
          heartbeatBase={heartbeatBase}
        />
      ))}
    </div>
  );
}

function StaleChip({
  reason,
  heartbeatBase,
}: {
  reason: StaleReason;
  heartbeatBase: string | null;
}) {
  const label = STALE_REASON_LABEL[reason];
  const text =
    reason === "mid_flight_stuck" && heartbeatBase !== null
      ? `${label} ${formatElapsedSince(heartbeatBase)}`
      : label;
  return (
    <span
      data-stale-reason={reason}
      aria-label={`Stale reason: ${text}`}
      className="inline-flex items-center rounded-full border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-amber-800 dark:border-amber-800 dark:bg-amber-950/60 dark:text-amber-200"
    >
      {text}
    </span>
  );
}

function StatusPill({ row }: { row: ProcessRowResponse }) {
  const visual = STATUS_VISUAL[row.status];
  const tooltip =
    row.status === "pending_retry" ? PENDING_RETRY_TOOLTIP : undefined;
  return (
    <span
      data-testid="status-pill"
      title={tooltip}
      aria-label={`Status: ${visual.label}`}
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
  tone?: "default" | "danger";
}) {
  const toneClass =
    tone === "danger"
      ? "border-red-300 bg-white text-red-700 hover:bg-red-50 dark:border-red-900 dark:bg-slate-900 dark:text-red-300 dark:hover:bg-red-950/40"
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

// Re-export so test files can assert on the canonical mapping rather
// than duplicating it.
export { REASON_TOOLTIP };
