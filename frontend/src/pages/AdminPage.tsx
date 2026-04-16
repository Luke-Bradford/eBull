/**
 * Admin page (issue #260 Phase 5).
 *
 * Two sections:
 *   1. Sync dashboard — 15-layer freshness grid + recent sync runs +
 *      "Sync now" button. Owned by the orchestrator.
 *   2. Background tasks — the 5 scheduled jobs that live outside the
 *      orchestrator DAG (execute_approved_orders, monitor_positions,
 *      retry_deferred_recommendations, weekly_coverage_review,
 *      attribution_summary). Retained because the operator still needs
 *      Run-Now for them and they are not part of the data-sync flow.
 *
 * The prior "Recent runs" table is removed — the Sync dashboard's
 * recent-sync-runs table is the authoritative per-run view now, and
 * individual job runs for the background tasks can be retrieved via
 * the /jobs/runs API if needed (operator CLI, not UI).
 */

import { useCallback, useState } from "react";

import { fetchJobsOverview, runJob } from "@/api/jobs";
import type { JobOverviewResponse } from "@/api/types";
import { ApiError } from "@/api/client";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";
import { formatDateTime } from "@/lib/format";
import { SyncDashboard } from "@/pages/SyncDashboard";

type RowState =
  | { kind: "idle" }
  | { kind: "running" }
  | { kind: "error"; message: string }
  | { kind: "queued" };

const STATUS_TONE: Record<string, string> = {
  success: "text-emerald-600",
  failure: "text-red-600",
  running: "text-amber-600",
  skipped: "text-slate-400",
};

// Orchestrator-owned scheduled jobs — surfaced by the Sync dashboard
// above; not duplicated here. Every other SCHEDULED_JOBS entry is a
// "background task" that remains outside the orchestrator DAG.
const ORCHESTRATOR_OWNED = new Set([
  "orchestrator_full_sync",
  "orchestrator_high_frequency_sync",
]);

export function AdminPage() {
  const jobs = useAsync(fetchJobsOverview, []);

  const [rowState, setRowState] = useState<Record<string, RowState>>({});

  const refetchJobs = jobs.refetch;
  const handleRun = useCallback(
    async (name: string) => {
      setRowState((prev) => ({ ...prev, [name]: { kind: "running" } }));
      try {
        await runJob(name);
        setRowState((prev) => ({ ...prev, [name]: { kind: "queued" } }));
        refetchJobs();
      } catch (err) {
        const message =
          err instanceof ApiError
            ? err.status === 409
              ? "Already running"
              : err.status === 404
                ? "Unknown job"
                : `Failed (HTTP ${err.status})`
            : "Failed";
        setRowState((prev) => ({
          ...prev,
          [name]: { kind: "error", message },
        }));
      }
    },
    [refetchJobs],
  );

  const backgroundJobs = (jobs.data?.jobs ?? []).filter(
    (j) => !ORCHESTRATOR_OWNED.has(j.name),
  );

  return (
    <div className="space-y-8">
      <SyncDashboard />

      <Section title="Background tasks">
        {jobs.loading ? (
          <SectionSkeleton rows={5} />
        ) : jobs.error !== null ? (
          <SectionError onRetry={jobs.refetch} />
        ) : (
          <>
            <p className="mb-3 text-xs text-slate-500">
              Scheduled jobs that live outside the orchestrator DAG —
              transaction execution, position monitoring, deferred-rec
              retries, and periodic governance. Data-pipeline jobs
              (candles, theses, scoring, reports, etc.) are driven by
              the orchestrator above.
            </p>
            <JobsTable
              items={backgroundJobs}
              rowState={rowState}
              onRun={handleRun}
            />
          </>
        )}
      </Section>
    </div>
  );
}

function JobsTable({
  items,
  rowState,
  onRun,
}: {
  items: JobOverviewResponse[];
  rowState: Record<string, RowState>;
  onRun: (name: string) => void;
}) {
  if (items.length === 0) {
    return <p className="text-sm text-slate-500">No background jobs registered.</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Job</th>
            <th className="py-2 pr-4">Cadence</th>
            <th className="py-2 pr-4">Next run (declared)</th>
            <th className="py-2 pr-4">Last result</th>
            <th className="py-2 pr-4">Last finished</th>
            <th className="py-2 pr-4 text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((job) => {
            const state = rowState[job.name] ?? { kind: "idle" };
            return (
              <tr key={job.name} className="align-top">
                <td className="py-2 pr-4">
                  <div className="font-medium text-slate-700">{job.name}</div>
                  <div className="text-xs text-slate-500">
                    {job.description}
                  </div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {job.cadence}
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {formatDateTime(job.next_run_time)}
                </td>
                <td className="py-2 pr-4 text-xs">
                  <span
                    className={
                      STATUS_TONE[job.last_status ?? ""] ?? "text-slate-400"
                    }
                  >
                    {job.last_status ?? "never run"}
                  </span>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {formatDateTime(job.last_finished_at)}
                </td>
                <td className="py-2 pr-0 text-right">
                  <RunButton
                    name={job.name}
                    state={state}
                    onClick={() => onRun(job.name)}
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function RunButton({
  name,
  state,
  onClick,
}: {
  name: string;
  state: RowState;
  onClick: () => void;
}) {
  const disabled = state.kind === "running";
  const label =
    state.kind === "running"
      ? "Triggering…"
      : state.kind === "queued"
        ? "Queued ✓"
        : state.kind === "error"
          ? state.message
          : "Run now";
  const tone =
    state.kind === "error"
      ? "border-red-300 bg-red-50 text-red-700 hover:bg-red-100"
      : state.kind === "queued"
        ? "border-emerald-300 bg-emerald-50 text-emerald-700"
        : "border-slate-200 bg-white text-slate-700 hover:bg-slate-50";
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={`Run ${name} now`}
      className={`rounded border px-2 py-1 text-xs font-medium disabled:opacity-50 ${tone}`}
    >
      {label}
    </button>
  );
}
