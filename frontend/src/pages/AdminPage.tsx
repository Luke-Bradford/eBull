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
 *      ingest sweeps. Drill-in lives at /admin/processes/{id}. SEC
 *      ingest seed progress + per-CIK timing now surface as the SEC
 *      ingest process rows; the legacy SeedProgressPanel was
 *      decommissioned in PR9 (#1085). Background-tasks table +
 *      filings coverage remain.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { fetchBootstrapStatus } from "@/api/bootstrap";
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
import { CollapsibleSection } from "@/components/admin/CollapsibleSection";
import { FundDataRow } from "@/components/admin/FundDataRow";
import { KillSwitchSection } from "@/components/admin/KillSwitchSection";
import { ProblemsPanel } from "@/components/admin/ProblemsPanel";
import { ProcessesTable } from "@/components/admin/ProcessesTable";
import { NEXT_RUN_EXPECTED_TOOLTIP, VERDICT_VISUAL } from "@/components/admin/processStatus";
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
  // PR3a #1064 — bootstrap-only render mode. When bootstrap_state.status
  // is anything other than 'complete' the ProcessesTable hides every
  // non-bootstrap category. Single read on mount; if a re-run flips
  // status to 'complete' the operator's next refetchAll catches it.
  const bootstrap = useAsync(fetchBootstrapStatus, []);

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
  // PR3a #1064 — re-poll bootstrap status on every refresh so a
  // 'partial_error' → 'complete' transition surfaces without a page
  // reload. Without this the operator stays in bootstrap-only render
  // mode after a successful Re-run all (Codex pre-push round 1).
  const refetchBootstrap = bootstrap.refetch;

  const refetchAll = useCallback(() => {
    refetchV2();
    refetchStatus();
    refetchCoverage();
    refetchJobs();
    refetchRecs();
    refetchBootstrap();
  }, [
    refetchV2,
    refetchStatus,
    refetchCoverage,
    refetchJobs,
    refetchRecs,
    refetchBootstrap,
  ]);

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

      <KillSwitchSection />

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

      <FundDataRow
        coverage={coverage.data}
        coverageError={coverage.error !== null}
        recommendations={recs.data}
        recommendationsError={recs.error !== null}
      />

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
            onMutationSuccess={() => {
              // After a trigger / cancel re-poll BOTH the processes
              // snapshot AND bootstrap status — a successful Re-run
              // all on the bootstrap row may flip status to
              // 'complete', which lifts the bootstrap-only render
              // gate. Refetching only `processes` would leave the
              // table in bootstrap-only mode until the next cadence
              // tick (Codex pre-push round 1).
              processes.refetch();
              refetchBootstrap();
            }}
            bootstrapStatus={bootstrap.data?.status ?? null}
            checkedAt={processes.checkedAt}
            // #1508 / C4 — fold the dead-engine signal from /system/status
            // (already fetched above for the credential-health banner) into
            // the Processes header. When the jobs process is not running every
            // per-row verdict is stale, so the table raises a hard-red banner.
            // Fail-open: a pending/errored /system/status read leaves this
            // false (no false alarm).
            engineDown={systemStatus.data?.engine_down ?? false}
          />
        ) : null}
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
      {/* #1305 — operator awareness: bulk bootstrap seeds a rolling-window
          DEPTH FLOOR, not full history. Source of truth for the depths:
          app/services/sec_bulk_download.py:241-243 (n_quarters_13f=4,
          n_quarters_insider=8, n_quarters_nport=4). Static note by design —
          v1 has no API exposing these params (issue #1305: "no code change
          for v1" beyond this note). If the defaults change, update this copy. */}
      <p className="text-xs text-slate-500">
        Bulk bootstrap covers a rolling-window depth floor: 13F ≈ 12 months,
        N-PORT 1 year, insider (Form 3/4/5) 2 years. Deeper history requires a
        re-bootstrap with widened depth params.
      </p>
    </div>
  );
}

// #1689 — render the single computed verdict pill (the same `VERDICT_VISUAL`
// the Processes Hub uses) instead of the raw `last_status` tone, plus an
// "attempt N · <reason>" detail line for a retrying (self_healing) row. So a
// transient / retrying / restart-reaped run is never painted red here.
function JobStatusCell({ job }: { job: JobOverviewResponse }) {
  const visual = VERDICT_VISUAL[job.health_verdict];
  const detail =
    job.self_healing && job.attempt != null
      ? `attempt ${job.attempt}${job.verdict_reason ? ` · ${job.verdict_reason}` : ""}`
      : job.verdict_reason;
  return (
    <div className="flex flex-col gap-0.5">
      <span
        data-testid="job-verdict-pill"
        data-verdict={job.health_verdict}
        className={`inline-flex w-fit items-center rounded-full border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${visual.toneClass}`}
      >
        {visual.label}
      </span>
      {detail ? <span className="text-[10px] text-slate-500">{detail}</span> : null}
    </div>
  );
}

function JobsSubTable({
  items,
  rowState,
  onRun,
}: {
  items: JobOverviewResponse[];
  rowState: Record<string, RowState>;
  onRun: (name: string) => void;
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Job</th>
            <th className="py-2 pr-4">Cadence</th>
            <th className="py-2 pr-4" title={NEXT_RUN_EXPECTED_TOOLTIP}>
              Next run (expected)
            </th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4">Last finished</th>
            <th className="py-2 pr-4 text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {items.map((job) => {
            const state = rowState[job.name] ?? { kind: "idle" };
            const label = job.display_name ?? job.name;
            return (
              <tr key={job.name} className="align-top" data-job-row={job.name}>
                <td className="py-2 pr-4">
                  <div className="font-medium text-slate-700">{label}</div>
                  <div className="text-xs text-slate-500">{job.description}</div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-600">{job.cadence}</td>
                <td
                  className="py-2 pr-4 text-xs text-slate-600"
                  title={NEXT_RUN_EXPECTED_TOOLTIP}
                >
                  {formatDateTime(job.next_run_time)}
                </td>
                <td className="py-2 pr-4 text-xs">
                  <JobStatusCell job={job} />
                </td>
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {formatDateTime(job.last_finished_at)}
                </td>
                <td className="py-2 pr-0 text-right">
                  <RunButton
                    name={job.name}
                    label={label}
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

// #1530 / #1689 — steady-state keepers render inline; one-shot bootstrap /
// backfill jobs (which legitimately sit idle/failed between runs) move into a
// collapsed section so an aged one-shot never reads as a steady-state alarm.
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
  const steady = items.filter((j) => j.role === "steady_state");
  const manual = items.filter((j) => j.role !== "steady_state");
  return (
    <>
      {steady.length > 0 ? (
        <JobsSubTable items={steady} rowState={rowState} onRun={onRun} />
      ) : (
        <p className="text-sm text-slate-500">No steady-state jobs.</p>
      )}
      {manual.length > 0 ? (
        <details className="mt-3">
          <summary className="cursor-pointer text-xs font-medium text-slate-500">
            Manual &amp; backfill ({manual.length}) — one-shot installs and historical
            catch-ups
          </summary>
          <div className="mt-2">
            <JobsSubTable items={manual} rowState={rowState} onRun={onRun} />
          </div>
        </details>
      ) : null}
    </>
  );
}

function RunButton({
  name,
  label,
  state,
  onClick,
}: {
  name: string;
  label: string;
  state: RowState;
  onClick: () => void;
}) {
  const disabled = state.kind === "running";
  const buttonLabel =
    state.kind === "running"
      ? "Triggering…"
      : state.kind === "queued"
        ? "Queued ✓"
        : state.kind === "error"
          ? state.message
          : "Run now";
  const tone =
    state.kind === "error"
      ? "border-red-300 dark:border-red-700 bg-red-50 dark:bg-red-950/40 text-red-700 dark:text-red-300 hover:bg-red-100"
      : state.kind === "queued"
        ? "border-emerald-300 dark:border-emerald-700 bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300"
        : "border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 text-slate-700 hover:bg-slate-50 dark:hover:bg-slate-800/40";
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={`Run ${label} now`}
      data-job-name={name}
      className={`rounded border px-2 py-1 text-xs font-medium disabled:opacity-50 ${tone}`}
    >
      {buttonLabel}
    </button>
  );
}
