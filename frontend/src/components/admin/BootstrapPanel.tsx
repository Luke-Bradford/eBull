/**
 * First-install bootstrap admin panel (#997).
 *
 * Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.
 *
 * Replaces the static SeedProgressPanel placement at the top of the admin
 * page for fresh installs. Renders:
 *
 *   * Header — status pill + action button(s) keyed by status:
 *       pending        → "Run bootstrap"
 *       running        → disabled "Running…" (no cancel — see spec §Cancel)
 *       complete       → "Re-run bootstrap" (secondary tone)
 *       partial_error  → primary "Retry failed (N)" + secondary
 *                        "Re-run all" + secondary "Mark complete"
 *   * Per-stage list — 17 rows grouped by phase/lane (1 init, 1 eToro,
 *     15 SEC). Each row shows stage_key + job_name caption, lane badge,
 *     status, progress (units_done / expected_units when known), elapsed +
 *     ETA, and a truncated last_error that expands on click.
 *
 * Polls GET /system/bootstrap/status at 5s while running, 60s otherwise
 * (mirrors AdminPage's running-vs-idle cadence pattern).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  fetchBootstrapStatus,
  markBootstrapComplete,
  retryFailedBootstrap,
  runBootstrap,
  type BootstrapStageResponse,
  type BootstrapStatus,
  type BootstrapStatusResponse,
} from "@/api/bootstrap";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

type ActionState =
  | { kind: "idle" }
  | { kind: "submitting" }
  | { kind: "error"; message: string };

const STATUS_TONE: Record<BootstrapStatus, string> = {
  pending: "bg-slate-200 dark:bg-slate-700 text-slate-700 dark:text-slate-200",
  running: "bg-amber-100 dark:bg-amber-900/40 text-amber-800 dark:text-amber-200",
  complete: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-800 dark:text-emerald-200",
  partial_error: "bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-200",
};

const STATUS_LABEL: Record<BootstrapStatus, string> = {
  pending: "Pending",
  running: "Running",
  complete: "Complete",
  partial_error: "Partial — errors",
};

const LANE_BADGE: Record<string, string> = {
  init: "bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-200",
  etoro: "bg-sky-100 dark:bg-sky-900/40 text-sky-800 dark:text-sky-200",
  sec: "bg-violet-100 dark:bg-violet-900/40 text-violet-800 dark:text-violet-200",
  sec_rate: "bg-violet-100 dark:bg-violet-900/40 text-violet-800 dark:text-violet-200",
  sec_bulk_download: "bg-purple-100 dark:bg-purple-900/40 text-purple-800 dark:text-purple-200",
  db: "bg-teal-100 dark:bg-teal-900/40 text-teal-800 dark:text-teal-200",
};

const STAGE_STATUS_TONE: Record<string, string> = {
  pending: "text-slate-400",
  running: "text-amber-700",
  success: "text-emerald-700",
  error: "text-red-700",
  skipped: "text-slate-400",
  // ``blocked`` = upstream-failure propagation (#1020). Same red tone
  // as ``error`` but the sublabel below distinguishes it.
  blocked: "text-red-700",
};

const STAGE_STATUS_SUBLABEL: Record<string, string> = {
  blocked: "Skipped — upstream failure",
};

function formatElapsed(startedAt: string | null, completedAt: string | null): string {
  if (startedAt === null) return "—";
  const start = new Date(startedAt).getTime();
  const end = completedAt !== null ? new Date(completedAt).getTime() : Date.now();
  const ms = end - start;
  if (Number.isNaN(ms) || ms < 0) return "—";
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  const mins = Math.floor(ms / 60_000);
  const secs = Math.round((ms - mins * 60_000) / 1000);
  return `${mins}m ${secs}s`;
}

function formatProgress(stage: BootstrapStageResponse): string {
  if (stage.expected_units !== null && stage.units_done !== null) {
    const pct = stage.expected_units > 0
      ? `${Math.round((stage.units_done / stage.expected_units) * 100)}%`
      : "—";
    return `${stage.units_done.toLocaleString()} / ${stage.expected_units.toLocaleString()} (${pct})`;
  }
  if (stage.rows_processed !== null) {
    return `${stage.rows_processed.toLocaleString()} rows`;
  }
  return "—";
}

export function BootstrapPanel() {
  // #1016 — preserveOnRefetch keeps the prior payload visible during the
  // 5s poll tick so the operator's scroll position is preserved and the
  // table doesn't flicker on every refresh. Skeleton still renders on
  // the initial load (before the first successful fetch).
  const state = useAsync(fetchBootstrapStatus, [], { preserveOnRefetch: true });
  const refetch = state.refetch;

  // Poll cadence: 5s while running, 60s otherwise. See AdminPage.tsx
  // for the same pattern — keep refetch in a ref so the interval
  // does not re-arm on every render.
  const isRunning = state.data?.status === "running";
  const pollInterval = isRunning ? 5_000 : 60_000;
  const refetchRef = useRef(refetch);
  useEffect(() => {
    refetchRef.current = refetch;
  }, [refetch]);
  useEffect(() => {
    const id = window.setInterval(() => refetchRef.current(), pollInterval);
    return () => window.clearInterval(id);
  }, [pollInterval]);

  const [actionState, setActionState] = useState<ActionState>({ kind: "idle" });
  const [expandedErrors, setExpandedErrors] = useState<ReadonlySet<string>>(new Set());

  const performAction = useCallback(
    async (action: () => Promise<unknown>) => {
      setActionState({ kind: "submitting" });
      try {
        await action();
        setActionState({ kind: "idle" });
        refetch();
      } catch (err) {
        const message = err instanceof Error ? err.message : "Action failed";
        setActionState({ kind: "error", message });
      }
    },
    [refetch],
  );

  const onRun = useCallback(() => void performAction(runBootstrap), [performAction]);
  const onRetryFailed = useCallback(
    () => void performAction(retryFailedBootstrap),
    [performAction],
  );
  const onMarkComplete = useCallback(
    () => void performAction(markBootstrapComplete),
    [performAction],
  );

  const failedCount = useMemo(() => {
    if (state.data === null) return 0;
    // Count both `error` and `blocked` — both are unsuccessful
    // outcomes that retry-failed needs to reset (#1020).
    return state.data.stages.filter(
      (s) => s.status === "error" || s.status === "blocked",
    ).length;
  }, [state.data]);

  return (
    <Section title="First-install bootstrap">
      {state.loading ? (
        <SectionSkeleton rows={4} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data ? (
        <BootstrapPanelBody
          data={state.data}
          actionState={actionState}
          failedCount={failedCount}
          expandedErrors={expandedErrors}
          setExpandedErrors={setExpandedErrors}
          onRun={onRun}
          onRetryFailed={onRetryFailed}
          onMarkComplete={onMarkComplete}
        />
      ) : null}
    </Section>
  );
}

function BootstrapPanelBody({
  data,
  actionState,
  failedCount,
  expandedErrors,
  setExpandedErrors,
  onRun,
  onRetryFailed,
  onMarkComplete,
}: {
  data: BootstrapStatusResponse;
  actionState: ActionState;
  failedCount: number;
  expandedErrors: ReadonlySet<string>;
  setExpandedErrors: (next: ReadonlySet<string>) => void;
  onRun: () => void;
  onRetryFailed: () => void;
  onMarkComplete: () => void;
}) {
  const submitting = actionState.kind === "submitting";

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <span
            className={`rounded-full px-3 py-1 text-xs font-medium ${
              STATUS_TONE[data.status]
            }`}
          >
            {STATUS_LABEL[data.status]}
            {data.status === "partial_error" && failedCount > 0
              ? ` — ${failedCount}`
              : ""}
          </span>
          {data.current_run_id !== null ? (
            <span className="text-xs text-slate-500">run #{data.current_run_id}</span>
          ) : null}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {data.status === "pending" ? (
            <PrimaryButton onClick={onRun} disabled={submitting}>
              Run bootstrap
            </PrimaryButton>
          ) : null}
          {data.status === "running" ? (
            <PrimaryButton onClick={onRun} disabled>
              Running…
            </PrimaryButton>
          ) : null}
          {data.status === "complete" ? (
            <SecondaryButton onClick={onRun} disabled={submitting}>
              Re-run bootstrap
            </SecondaryButton>
          ) : null}
          {data.status === "partial_error" ? (
            <>
              <PrimaryButton onClick={onRetryFailed} disabled={submitting || failedCount === 0}>
                Retry failed ({failedCount})
              </PrimaryButton>
              <SecondaryButton onClick={onRun} disabled={submitting}>
                Re-run all
              </SecondaryButton>
              <SecondaryButton onClick={onMarkComplete} disabled={submitting}>
                Mark complete
              </SecondaryButton>
            </>
          ) : null}
        </div>
      </div>

      {actionState.kind === "error" ? (
        <div
          role="alert"
          className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950/40 px-3 py-2 text-xs text-red-800 dark:text-red-200"
        >
          {actionState.message}
        </div>
      ) : null}

      <StagesTable
        stages={data.stages}
        expandedErrors={expandedErrors}
        setExpandedErrors={setExpandedErrors}
      />
    </div>
  );
}

function StagesTable({
  stages,
  expandedErrors,
  setExpandedErrors,
}: {
  stages: readonly BootstrapStageResponse[];
  expandedErrors: ReadonlySet<string>;
  setExpandedErrors: (next: ReadonlySet<string>) => void;
}) {
  if (stages.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        No bootstrap run on record yet. Click "Run bootstrap" to begin.
      </p>
    );
  }

  const toggle = (key: string) => {
    const next = new Set(expandedErrors);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    setExpandedErrors(next);
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Stage</th>
            <th className="py-2 pr-4">Lane</th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4">Progress</th>
            <th className="py-2 pr-4">Elapsed</th>
            <th className="py-2 pr-4">Last error</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {stages.map((stage) => {
            const expanded = expandedErrors.has(stage.stage_key);
            const errorText = stage.last_error ?? "";
            const truncated =
              errorText.length > 80 ? `${errorText.slice(0, 80)}…` : errorText;
            return (
              <tr key={stage.stage_key} className="align-top">
                <td className="py-2 pr-4">
                  <div className="font-medium text-slate-700">{stage.stage_key}</div>
                  <div className="text-xs text-slate-500">{stage.job_name}</div>
                </td>
                <td className="py-2 pr-4">
                  <span
                    className={`rounded px-2 py-0.5 text-xs font-medium ${
                      LANE_BADGE[stage.lane]
                    }`}
                  >
                    {stage.lane}
                  </span>
                </td>
                <td className={`py-2 pr-4 text-xs ${STAGE_STATUS_TONE[stage.status] ?? ""}`}>
                  <div>
                    {stage.status}
                    {stage.attempt_count > 1 ? ` (×${stage.attempt_count})` : ""}
                  </div>
                  {STAGE_STATUS_SUBLABEL[stage.status] !== undefined ? (
                    <div className="text-[10px] text-slate-500">
                      {STAGE_STATUS_SUBLABEL[stage.status]}
                    </div>
                  ) : null}
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {formatProgress(stage)}
                </td>
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {formatElapsed(stage.started_at, stage.completed_at)}
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {errorText !== "" ? (
                    <button
                      type="button"
                      onClick={() => toggle(stage.stage_key)}
                      className="text-left text-red-700 hover:underline"
                    >
                      {expanded ? errorText : truncated}
                    </button>
                  ) : (
                    <span className="text-slate-400">—</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function PrimaryButton({
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  // ``...rest`` first, then ``type="button"`` so the default cannot
  // accidentally promote a caller's submit-button to a form submit.
  // Pre-PR1003-NITPICK we had the order reversed.
  return (
    <button
      {...rest}
      type="button"
      className="rounded bg-sky-600 px-3 py-1 text-sm font-medium text-white hover:bg-sky-700 disabled:bg-slate-300"
    >
      {children}
    </button>
  );
}

function SecondaryButton({
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      {...rest}
      type="button"
      className="rounded border border-slate-300 bg-white px-3 py-1 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
    >
      {children}
    </button>
  );
}
