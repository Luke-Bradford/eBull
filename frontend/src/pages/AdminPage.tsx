/**
 * Admin page — scheduled jobs (#13 PR B).
 *
 * Two panels, each owning its own request lifecycle so a slow or
 * failing endpoint cannot blank the other:
 *
 *   1. Jobs table — declared schedule + computed next-run + last-run
 *      summary, sourced from GET /system/jobs. Each row carries a
 *      "Run now" button that POSTs /jobs/{name}/run; the per-row
 *      transient state (running / error / success) lives in this page,
 *      not in a global store.
 *   2. Recent runs — newest-first slice of job_runs from GET /jobs/runs.
 *      Refetched whenever a manual trigger is fired so the operator
 *      sees their action take effect without a manual refresh.
 *
 * The system-status / kill-switch / data-layer surface is intentionally
 * NOT duplicated here — it lives on the dashboard's SystemStatusPanel
 * already and PR B is scoped to the jobs surface only.
 */

import { useCallback, useState } from "react";

import { fetchJobRuns, fetchJobsOverview, runJob } from "@/api/jobs";
import type {
  JobOverviewResponse,
  JobRunResponse,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
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

export function AdminPage() {
  const jobs = useAsync(fetchJobsOverview, []);
  const runs = useAsync(() => fetchJobRuns(null, 50), []);

  // Per-row transient state. Keyed on job name; cleared automatically
  // when a fresh fetch resolves so a stale "queued" badge does not
  // outlive the next refresh.
  const [rowState, setRowState] = useState<Record<string, RowState>>({});

  // Depend on the stable refetch references rather than the full
  // useAsync state objects -- ``jobs`` and ``runs`` are new objects on
  // every render (loading/data/error transitions), but their
  // ``refetch`` callbacks are memoised inside ``useAsync``. Closing
  // over the state objects would recreate ``handleRun`` (and force
  // ``JobsTable`` to re-render every row) on every async transition.
  // Round 1 review WARNING 3.
  const refetchJobs = jobs.refetch;
  const refetchRuns = runs.refetch;
  const handleRun = useCallback(
    async (name: string) => {
      setRowState((prev) => ({ ...prev, [name]: { kind: "running" } }));
      try {
        await runJob(name);
        setRowState((prev) => ({ ...prev, [name]: { kind: "queued" } }));
        // Refresh both panels so the operator's action is reflected
        // without a manual refresh. A single refresh is enough; the
        // operator can hit the button again or reload the page if
        // they want to see further updates.
        refetchRuns();
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
        setRowState((prev) => ({ ...prev, [name]: { kind: "error", message } }));
      }
    },
    [refetchJobs, refetchRuns],
  );

  return (
    <div className="space-y-8">
      {/* Phase 3: sync orchestrator dashboard (issue #260). Sits above the
          legacy jobs table — the jobs table remains until Phase 5 removes
          it, so operators can cross-check old behaviour vs orchestrator
          behaviour during the cutover window. */}
      <SyncDashboard />

      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">
          Legacy scheduled jobs
        </h1>
      </div>

      <Section title="Jobs">
        {jobs.loading ? (
          <SectionSkeleton rows={6} />
        ) : jobs.error !== null ? (
          <SectionError onRetry={jobs.refetch} />
        ) : (
          <JobsTable
            items={jobs.data?.jobs ?? []}
            rowState={rowState}
            onRun={handleRun}
          />
        )}
      </Section>

      <Section
        title="Recent runs"
        action={
          <button
            type="button"
            onClick={runs.refetch}
            className="rounded border border-slate-200 bg-white px-2 py-0.5 text-[10px] font-medium text-slate-600 hover:bg-slate-50"
          >
            Refresh
          </button>
        }
      >
        {runs.loading ? (
          <SectionSkeleton rows={5} />
        ) : runs.error !== null ? (
          <SectionError onRetry={runs.refetch} />
        ) : (
          <RecentRunsTable items={runs.data?.items ?? []} />
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
    return <p className="text-sm text-slate-500">No jobs registered.</p>;
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
                  <div className="text-xs text-slate-500">{job.description}</div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">{job.cadence}</td>
                <td className="py-2 pr-4 text-xs text-slate-600">
                  {formatDateTime(job.next_run_time)}
                </td>
                <td className="py-2 pr-4 text-xs">
                  <span
                    className={STATUS_TONE[job.last_status ?? ""] ?? "text-slate-400"}
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

function RecentRunsTable({ items }: { items: JobRunResponse[] }) {
  if (items.length === 0) {
    return <p className="text-sm text-slate-500">No runs recorded yet.</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Job</th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4">Started</th>
            <th className="py-2 pr-4">Finished</th>
            <th className="py-2 pr-4">Rows</th>
            <th className="py-2 pr-4">Error</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((run) => (
            <tr key={run.run_id}>
              <td className="py-2 pr-4 font-medium text-slate-700">{run.job_name}</td>
              <td className="py-2 pr-4 text-xs">
                <span className={STATUS_TONE[run.status] ?? "text-slate-500"}>
                  {run.status}
                </span>
              </td>
              <td className="py-2 pr-4 text-xs text-slate-600">
                {formatDateTime(run.started_at)}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-600">
                {formatDateTime(run.finished_at)}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-600">
                {run.row_count ?? "—"}
              </td>
              <td
                className="max-w-xs truncate py-2 pr-4 text-xs text-red-600"
                title={run.error_msg ?? undefined}
              >
                {run.error_msg ?? ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
