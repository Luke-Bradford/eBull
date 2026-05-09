/**
 * Admin page — control-hub rewrite (#1064).
 *
 * Three-section composition:
 *   1. Problems panel — failing layers + failing jobs + coverage
 *      anomalies (null rows). Hidden when all sources resolved and
 *      combined problem list is empty.
 *   2. Fund data row — four live cells + three pending placeholders
 *      for summaries we don't yet have endpoints for.
 *   3. Processes table — unified view of bootstrap + scheduled jobs +
 *      ingest sweeps. Drill-in lives at /admin/processes/{id}.
 *      Background-tasks table + filings coverage stay during PR6/PR7
 *      migration; PR9 decommissions the rest.
 *
 * #1078 — admin control hub PR6. The legacy SyncDashboard (Sync-now
 * button + 15-layer grid + recent-runs feed) and LayerHealthList have
 * been decommissioned: the orchestrator surfaces as one row in the
 * Processes table and its DAG drill-in lives on
 * /admin/processes/orchestrator_full_sync. SeedProgressPanel +
 * BootstrapPanel + Background-tasks table stay until PR7 / PR9.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { fetchCoverageSummary } from "@/api/coverage";
import { fetchJobsOverview, runJob } from "@/api/jobs";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchSystemStatus } from "@/api/system";
import { fetchSyncLayersV2, fetchSyncStatus } from "@/api/sync";
import { ApiError } from "@/api/client";
import type {
  CoverageSummaryResponse,
  JobOverviewResponse,
} from "@/api/types";
import { BootstrapPanel } from "@/components/admin/BootstrapPanel";
import { CollapsibleSection } from "@/components/admin/CollapsibleSection";
import { FundDataRow } from "@/components/admin/FundDataRow";
import { ProblemsPanel } from "@/components/admin/ProblemsPanel";
import { ProcessesTable } from "@/components/admin/ProcessesTable";
import { SeedProgressPanel } from "@/components/admin/SeedProgressPanel";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";
import { formatDateTime } from "@/lib/format";
import { useProcesses } from "@/lib/useProcesses";

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

const ORCHESTRATOR_OWNED = new Set([
  "orchestrator_full_sync",
  "orchestrator_high_frequency_sync",
]);

export function AdminPage() {
  const v2 = useAsync(fetchSyncLayersV2, []);
  const status = useAsync(fetchSyncStatus, []);
  const coverage = useAsync(fetchCoverageSummary, []);
  const jobs = useAsync(fetchJobsOverview, []);
  // /system/status carries the operator credential health summary used
  // by the Problems banner (#979 / #974/E). Fetched alongside the
  // existing admin sources; the same auto-refresh loop polls it.
  const systemStatus = useAsync(fetchSystemStatus, []);
  const recs = useAsync(
    () =>
      fetchRecommendations(
        { action: null, status: null, instrument_id: null },
        0,
        1,
      ),
    [],
  );

  // Admin control hub processes view (#1076 / #1064). Self-polls via
  // its own cadence-flip interval (5s while running, 30s otherwise).
  // PR6 decommissioned the legacy SyncDashboard + LayerHealthList —
  // the orchestrator surfaces here as one row + DAG drill-in.
  const processes = useProcesses();

  // Extract the refetch refs as local const bindings so ESLint can
  // see their identity and verify the dep array without the suppression
  // that previously papered over the stability contract. `useAsync`
  // wraps refetch in `useCallback([], [])` — see useAsync.test.ts
  // which pins that invariant.
  const refetchV2 = v2.refetch;
  const refetchStatus = status.refetch;
  const refetchCoverage = coverage.refetch;
  const refetchJobs = jobs.refetch;
  const refetchRecs = recs.refetch;

  const refetchAll = useCallback(() => {
    refetchV2();
    refetchStatus();
    refetchCoverage();
    refetchJobs();
    refetchRecs();
  }, [refetchV2, refetchStatus, refetchCoverage, refetchJobs, refetchRecs]);

  const isRunning = status.data?.is_running ?? false;
  const refreshInterval = isRunning ? 10_000 : 60_000;
  // Keep `refetchAll` in a ref so the interval does not re-arm on
  // every render — only when the cadence itself changes (running
  // ↔ idle transition). Without this split, a cadence flip at
  // second 59 of the 60s idle cycle would drop the elapsed window
  // and fire an immediate refetch; this keeps the natural tick.
  const refetchAllRef = useRef(refetchAll);
  useEffect(() => {
    refetchAllRef.current = refetchAll;
  }, [refetchAll]);
  useEffect(() => {
    const id = window.setInterval(() => refetchAllRef.current(), refreshInterval);
    return () => window.clearInterval(id);
  }, [refreshInterval]);

  const [processesOpen, setProcessesOpen] = useState(true);
  const [rowState, setRowState] = useState<Record<string, RowState>>({});

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
        setRowState((prev) => ({ ...prev, [name]: { kind: "error", message } }));
      }
    },
    [refetchJobs],
  );

  const backgroundJobs = (jobs.data?.jobs ?? []).filter(
    (j) => !ORCHESTRATOR_OWNED.has(j.name),
  );

  // ProblemsPanel surfaces failing layers with a drill-through; PR6
  // moves the orchestrator detail behind /admin/processes/orchestrator_full_sync
  // so the click navigates there with the DAG tab pre-selected via
  // the route's hash.
  const navigate = useNavigate();
  const openOrchestratorFor = useCallback(
    (_layerName: string) => {
      navigate("/admin/processes/orchestrator_full_sync");
    },
    [navigate],
  );

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">Admin</h1>
      </div>

      <ProblemsPanel
        v2={v2.data}
        jobs={jobs.data}
        coverage={coverage.data}
        credentialHealth={systemStatus.data?.credential_health ?? null}
        v2Error={v2.error !== null}
        jobsError={jobs.error !== null}
        coverageError={coverage.error !== null}
        onOpenOrchestrator={openOrchestratorFor}
      />

      <BootstrapPanel />

      <CollapsibleSection
        title="Processes"
        summary="control hub"
        open={processesOpen}
        onOpenChange={setProcessesOpen}
      >
        {processes.loading ? (
          <SectionSkeleton rows={5} />
        ) : processes.error !== null ? (
          <SectionError onRetry={processes.refetch} />
        ) : processes.data ? (
          <ProcessesTable
            snapshot={processes.data}
            onMutationSuccess={processes.refetch}
          />
        ) : null}
      </CollapsibleSection>

      <FundDataRow
        coverage={coverage.data}
        coverageError={coverage.error !== null}
        recommendations={recs.data}
        recommendationsError={recs.error !== null}
      />

      <SeedProgressPanel />

      <CollapsibleSection
        title="Background tasks"
        summary={`${backgroundJobs.length} scheduled`}
      >
        {jobs.loading ? (
          <SectionSkeleton rows={5} />
        ) : jobs.error !== null ? (
          <SectionError onRetry={jobs.refetch} />
        ) : (
          <>
            <p className="mb-3 text-xs text-slate-500">
              Scheduled jobs that live outside the orchestrator DAG —
              transaction execution, position monitoring, deferred-rec
              retries, and periodic governance.
            </p>
            <JobsTable items={backgroundJobs} rowState={rowState} onRun={handleRun} />
          </>
        )}
      </CollapsibleSection>

      <CollapsibleSection
        title="Filings coverage"
        summary={
          coverage.data
            ? `${coverage.data.analysable} / ${coverage.data.total_tradable} analysable`
            : undefined
        }
      >
        {coverage.loading ? (
          <SectionSkeleton rows={2} />
        ) : coverage.error !== null ? (
          <SectionError onRetry={coverage.refetch} />
        ) : coverage.data ? (
          <CoverageSummaryCard summary={coverage.data} />
        ) : null}
      </CollapsibleSection>
    </div>
  );
}

function CoverageSummaryCard({
  summary,
}: {
  summary: CoverageSummaryResponse;
}) {
  const stuckTotal = summary.insufficient + summary.structurally_young;
  const cells: Array<{ label: string; value: number; tone: string }> = [
    { label: "Analysable", value: summary.analysable, tone: "text-emerald-700" },
    { label: "Insufficient", value: summary.insufficient, tone: "text-amber-700" },
    {
      label: "Structurally young",
      value: summary.structurally_young,
      tone: "text-amber-700",
    },
    { label: "FPI", value: summary.fpi, tone: "text-slate-700" },
    {
      label: "No primary SEC CIK",
      value: summary.no_primary_sec_cik,
      tone: "text-slate-500",
    },
    { label: "Unknown", value: summary.unknown, tone: "text-slate-500" },
    {
      label: "Null (pre-audit)",
      value: summary.null_rows,
      tone: summary.null_rows === 0 ? "text-slate-400" : "text-red-600",
    },
    { label: "Total tradable", value: summary.total_tradable, tone: "text-slate-600" },
  ];
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {cells.map((c) => (
          <div key={c.label} className="rounded border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/40 p-3">
            <div className="text-xs uppercase tracking-wide text-slate-500">
              {c.label}
            </div>
            <div className={`mt-1 text-2xl font-semibold ${c.tone}`}>
              {c.value}
            </div>
          </div>
        ))}
      </div>
      {stuckTotal > 0 ? (
        <p className="text-xs text-slate-600">
          <Link
            to="/admin/coverage/insufficient"
            className="font-medium text-blue-700 hover:underline"
          >
            Review {stuckTotal} stuck instrument{stuckTotal === 1 ? "" : "s"} →
          </Link>
        </p>
      ) : (
        <p className="text-xs text-slate-500">
          No instruments currently stuck below the analysable bar.
        </p>
      )}
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
                  <div className="text-xs text-slate-500">{job.description}</div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">{job.cadence}</td>
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
        : "border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 text-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800/40";
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
