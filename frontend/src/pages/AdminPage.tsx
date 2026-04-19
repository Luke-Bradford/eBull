/**
 * Admin page — triage-first rewrite (issue #323).
 *
 * Three-section composition:
 *   1. Problems panel — failing layers + failing jobs + coverage
 *      anomalies (null rows). Hidden when all sources resolved and
 *      combined problem list is empty.
 *   2. Fund data row — four live cells + three pending placeholders
 *      for summaries we don't yet have endpoints for.
 *   3. Collapsed-by-default details: orchestrator (15-layer grid +
 *      recent runs), background jobs, filings coverage.
 *
 * AdminPage owns the auto-refresh loop (10s when a sync is running,
 * 60s otherwise) for its five top-level fetches. It also instantiates
 * the shared `useSyncTrigger` hook and passes it to both the
 * top-level Sync-now button and the inner `SyncDashboard`, so both
 * buttons reflect one piece of state and a second click never fires
 * a second POST (spec §11).
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";

import { fetchCoverageSummary } from "@/api/coverage";
import { fetchJobsOverview, runJob } from "@/api/jobs";
import { fetchRecommendations } from "@/api/recommendations";
import { fetchSyncLayers, fetchSyncStatus } from "@/api/sync";
import { ApiError } from "@/api/client";
import type {
  CoverageSummaryResponse,
  JobOverviewResponse,
} from "@/api/types";
import { CollapsibleSection } from "@/components/admin/CollapsibleSection";
import { FundDataRow } from "@/components/admin/FundDataRow";
import { ProblemsPanel } from "@/components/admin/ProblemsPanel";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";
import { formatDateTime } from "@/lib/format";
import { useSyncTrigger } from "@/lib/useSyncTrigger";
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

const ORCHESTRATOR_OWNED = new Set([
  "orchestrator_full_sync",
  "orchestrator_high_frequency_sync",
]);

export function AdminPage() {
  const layers = useAsync(fetchSyncLayers, []);
  const status = useAsync(fetchSyncStatus, []);
  const coverage = useAsync(fetchCoverageSummary, []);
  const jobs = useAsync(fetchJobsOverview, []);
  const recs = useAsync(
    () =>
      fetchRecommendations(
        { action: null, status: null, instrument_id: null },
        0,
        1,
      ),
    [],
  );

  // Extract the refetch refs as local const bindings so ESLint can
  // see their identity and verify the dep array without the suppression
  // that previously papered over the stability contract. `useAsync`
  // wraps refetch in `useCallback([], [])` — see useAsync.test.ts
  // which pins that invariant.
  const refetchLayers = layers.refetch;
  const refetchStatus = status.refetch;
  const refetchCoverage = coverage.refetch;
  const refetchJobs = jobs.refetch;
  const refetchRecs = recs.refetch;

  const refetchAll = useCallback(() => {
    refetchLayers();
    refetchStatus();
    refetchCoverage();
    refetchJobs();
    refetchRecs();
  }, [refetchLayers, refetchStatus, refetchCoverage, refetchJobs, refetchRecs]);

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

  // Shared sync-trigger — single source of truth for both the
  // top-level button here and the inner button inside SyncDashboard.
  const syncTrigger = useSyncTrigger(refetchAll);
  // Destructure once — `useSyncTrigger` returns a plain object
  // literal, so the outer reference changes every render. Using
  // the stable member refs in hook deps below prevents effects
  // from re-arming on every render (most visibly: the fallback
  // timer below, which would otherwise reset every render and
  // could never actually fire).
  const {
    kind: triggerKind,
    message: triggerMessage,
    trigger: triggerSync_,
    clearQueued: triggerClearQueued,
  } = syncTrigger;

  // Drive the `queued → idle` transition off the top-level status
  // poll. Previously only SyncDashboard called clearQueued, which
  // meant a click on the top button with the orchestrator section
  // collapsed left the hook stuck in `queued` forever (button stays
  // disabled until reload). With the top-level status fetch here we
  // can advance the trigger state independently. Also clears via a
  // one-off timer for the fast-run case where a sync finishes
  // before the next /sync/status tick ever observes is_running=true.
  useEffect(() => {
    triggerClearQueued(isRunning);
  }, [triggerClearQueued, isRunning]);

  useEffect(() => {
    if (triggerKind !== "queued") return;
    // Fallback: if the server-side sync finishes before we ever see
    // is_running=true, recover the idle state after the next refresh
    // tick. Otherwise the button would stay disabled indefinitely.
    const id = window.setTimeout(
      () => triggerClearQueued(true),
      refreshInterval + 2_000,
    );
    return () => window.clearTimeout(id);
  }, [triggerKind, triggerClearQueued, refreshInterval]);

  const [orchestratorOpen, setOrchestratorOpen] = useState(false);
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

  // Stable callback so ProblemsPanel's useEffect on `layers` does
  // not re-run (and re-compute problems) every AdminPage render.
  const openOrchestrator = useCallback(() => {
    setOrchestratorOpen(true);
    // Let the section mount before we scroll, so the target exists.
    // scrollIntoView is undefined in jsdom so we guard defensively —
    // the browser behaviour is unchanged.
    requestAnimationFrame(() => {
      const el = document.getElementById("admin-orchestrator-details");
      if (el && typeof el.scrollIntoView === "function") {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }, []);

  const backgroundJobs = (jobs.data?.jobs ?? []).filter(
    (j) => !ORCHESTRATOR_OWNED.has(j.name),
  );

  const layerProblemCount =
    (layers.data?.layers ?? []).filter(
      (l) =>
        (l.consecutive_failures >= 1 && l.last_error_category !== null) ||
        (!l.is_fresh && l.is_blocking),
    ).length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800">Admin</h1>
        <SyncNowButton
          triggerKind={triggerKind}
          message={triggerMessage}
          isRunning={isRunning}
          onClick={() => void triggerSync_()}
        />
      </div>

      <ProblemsPanel
        layers={layers.data}
        jobs={jobs.data}
        coverage={coverage.data}
        layersError={layers.error !== null}
        jobsError={jobs.error !== null}
        coverageError={coverage.error !== null}
        onOpenOrchestrator={openOrchestrator}
      />

      <FundDataRow
        coverage={coverage.data}
        coverageError={coverage.error !== null}
        recommendations={recs.data}
        recommendationsError={recs.error !== null}
      />

      <CollapsibleSection
        title="Orchestrator details"
        summary={
          layerProblemCount > 0
            ? `${layerProblemCount} layer${layerProblemCount === 1 ? "" : "s"} need attention`
            : "all layers healthy"
        }
        open={orchestratorOpen}
        onOpenChange={setOrchestratorOpen}
        sectionId="admin-orchestrator-details"
      >
        <SyncDashboard syncTrigger={syncTrigger} />
      </CollapsibleSection>

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

function SyncNowButton({
  triggerKind,
  message,
  isRunning,
  onClick,
}: {
  triggerKind: "idle" | "running" | "queued" | "error";
  message: string | null;
  isRunning: boolean;
  onClick: () => void;
}) {
  const disabled =
    triggerKind === "running" || triggerKind === "queued" || isRunning;
  const label =
    triggerKind === "running"
      ? "Triggering…"
      : triggerKind === "queued"
        ? "Queued"
        : isRunning
          ? "Running"
          : "Sync now";
  return (
    <div className="flex items-center gap-2">
      {triggerKind === "error" && message !== null ? (
        <span className="text-xs text-red-600">{message}</span>
      ) : null}
      <button
        type="button"
        onClick={onClick}
        disabled={disabled}
        className="rounded bg-sky-600 px-3 py-1 text-sm font-medium text-white hover:bg-sky-700 disabled:bg-slate-300"
      >
        {label}
      </button>
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
          <div key={c.label} className="rounded border border-slate-200 bg-slate-50 p-3">
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
