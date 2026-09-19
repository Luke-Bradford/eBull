/**
 * ProcessDetailPage — drill-in route /admin/processes/:id (#1076 / #1064).
 *
 * Three tabs per spec §"Information architecture":
 *   - Overview: current row state, cadence, watermark, action buttons.
 *   - History: last 7 days of runs (`GET /runs?days=7`).
 *   - Errors:  grouped error classes of the latest terminal run.
 *
 * Trigger / cancel buttons reuse the same envelope contracts as the
 * ProcessesTable; modal state is owned here so a 409 from the detail
 * page surfaces inline rather than escaping back to the table view.
 */

import type { ReactNode } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { ApiError } from "@/api/client";
import { runJob } from "@/api/jobs";
import {
  cancelProcess,
  fetchBootstrapTimeline,
  fetchOrchestratorDag,
  fetchProcess,
  fetchProcessRuns,
  triggerProcess,
} from "@/api/processes";
import type {
  ActiveRunSummaryResponse,
  BootstrapTimelineArchiveResponse,
  BootstrapTimelineResponse,
  BootstrapTimelineStageResponse,
  CancelMode,
  OrchestratorDagLayerResponse,
  OrchestratorDagResponse,
  OrchestratorDagSyncRunResponse,
  ProcessRowResponse,
  ProcessRunSummaryResponse,
} from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { Badge, type BadgeTone } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { AdvancedParamsForm } from "@/components/admin/AdvancedParamsForm";
import {
  REASON_TOOLTIP,
  RUN_STATUS_VISUAL,
  STALE_REASON_LABEL,
  STATUS_VISUAL,
  meansActiveRunIsStale,
  reasonTooltip,
} from "@/components/admin/processStatus";
import { VerdictPill } from "@/components/admin/VerdictPill";
import { useAsync } from "@/lib/useAsync";
import {
  formatDateTime,
  formatEta,
  formatHeartbeatAge,
  formatRate,
} from "@/lib/format";

type TabKey = "overview" | "history" | "errors" | "dag" | "timeline" | "advanced";

// #1267 — valid `?tab=` deep-link targets. Runtime mirror of TabKey so a
// link like /admin/processes/bootstrap?tab=timeline can preselect a tab;
// anything unknown falls back to overview, and the existing per-process
// guards below still reset tabs the process doesn't own.
const TAB_KEYS: readonly TabKey[] = [
  "overview",
  "history",
  "errors",
  "dag",
  "timeline",
  "advanced",
];

const ORCHESTRATOR_FULL_SYNC_ID = "orchestrator_full_sync";
const BOOTSTRAP_PROCESS_ID = "bootstrap";

// #1409 P5 — timeline auto-refresh cadence + the number of extra polls
// to run after a run reaches a terminal state (the run row can flip
// terminal a beat before the final stage rows commit).
const TIMELINE_POLL_MS = 5000;
const TIMELINE_POST_TERMINAL_POLLS = 3;

// Lane order for the bootstrap timeline tab. Stages within a lane are
// sorted by stage_order ASC; lanes themselves render in the order
// declared by the orchestrator (init runs before etoro / sec lanes;
// db lane is the cross-source ingestion sink). Unknown lanes from
// legacy runs sort last so the operator still sees them.
const BOOTSTRAP_LANE_ORDER = [
  "init",
  "etoro",
  "sec_rate",
  "sec_bulk_download",
  "db",
  // #1141 — Phase C bulk-ingest family lanes. Render adjacent to the
  // generic `db` lane so the operator sees the parallel family bucket
  // grouped with the other DB-bound stages.
  "db_filings",
  "db_fundamentals_raw",
  "db_ownership_inst",
  "db_ownership_insider",
  "db_ownership_funds",
  "sec",
] as const;

export function ProcessDetailPage() {
  const params = useParams<{ id: string }>();
  const id = params.id ?? "";
  // #1267 — initial tab honours a `?tab=` deep link (read once at mount;
  // tab switches stay local state, the URL is not kept in sync).
  const [searchParams] = useSearchParams();
  const [tab, setTab] = useState<TabKey>(() => {
    const requested = searchParams.get("tab");
    return requested !== null &&
      (TAB_KEYS as readonly string[]).includes(requested)
      ? (requested as TabKey)
      : "overview";
  });
  const [busy, setBusy] = useState(false);
  const [triggerError, setTriggerError] = useState<unknown>(null);
  const [cancelError, setCancelError] = useState<unknown>(null);
  const [showFullWash, setShowFullWash] = useState(false);
  const [showCancel, setShowCancel] = useState(false);
  const [advancedError, setAdvancedError] = useState<unknown>(null);
  const [advancedRequestId, setAdvancedRequestId] = useState<number | null>(null);

  // useAsync captures fn via a ref — fresh arrow per render is fine.
  const detail = useAsync(() => fetchProcess(id), [id]);
  const runs = useAsync(() => fetchProcessRuns(id, 7), [id]);
  // DAG drill-in fetcher (#1078). Gated on BOTH (process_id is the
  // orchestrator) AND (active tab is DAG) so non-orchestrator detail
  // pages never call /dag and the orchestrator page does not fetch
  // the DAG until the operator opens the tab. `preserveOnRefetch`
  // keeps the rendered grid stable while a tick re-fetches (the
  // useAsync default is false — Codex pre-impl review M2).
  const isOrchestrator = id === ORCHESTRATOR_FULL_SYNC_ID;
  const isBootstrap = id === BOOTSTRAP_PROCESS_ID;
  const dag = useAsync<OrchestratorDagResponse | null>(
    () =>
      tab === "dag" && isOrchestrator
        ? fetchOrchestratorDag(id)
        : Promise.resolve(null),
    [id, tab, isOrchestrator],
    { preserveOnRefetch: true },
  );
  // Bootstrap timeline drill-in fetcher (#1080). Mirrors the DAG fetcher's
  // gating contract: BOTH (process_id is bootstrap) AND (active tab is
  // timeline). Restricted endpoint — non-bootstrap ids 404. URL-derived
  // `id` (not `detail.data?.process_id`) drives the gate so the fetch
  // can fire without waiting for the row read to land.
  const timeline = useAsync<BootstrapTimelineResponse | null>(
    () =>
      tab === "timeline" && isBootstrap
        ? fetchBootstrapTimeline(id)
        : Promise.resolve(null),
    [id, tab, isBootstrap],
    { preserveOnRefetch: true },
  );

  // PR2 #1064 — Advanced tab visibility. Only shown for scheduled
  // jobs that declare params_metadata. Bootstrap + ingest_sweep
  // mechanisms own no operator-exposable params; scheduled jobs with
  // an empty metadata tuple (the majority today) also hide the tab.
  const showAdvanced =
    detail.data?.mechanism === "scheduled_job" &&
    (detail.data?.params_metadata?.length ?? 0) > 0;

  // Reset tab to overview if currently on a process-specific tab and the
  // route param changes to an id that no longer owns that tab (operator
  // clicked a different process row from the table). Codex pre-impl
  // review M-r2-2 originally pinned the DAG variant; PR7 extends to
  // timeline. PR2 extends to advanced — when the operator pivots from a
  // scheduled job with metadata to a process that has none, fall back.
  useEffect(() => {
    if (tab === "dag" && !isOrchestrator) {
      setTab("overview");
    } else if (tab === "timeline" && !isBootstrap) {
      setTab("overview");
    } else if (tab === "advanced" && !showAdvanced) {
      setTab("overview");
    }
  }, [tab, isOrchestrator, isBootstrap, showAdvanced]);

  // #1271 / #1409 P5 — Auto-refresh the timeline while the bootstrap run
  // is in flight so the operator sees stage transitions + processed_count
  // growth without navigating away and back. Polls every 5s while
  // running. #1409 §5.5 adds a short grace window AFTER the run reaches a
  // terminal state: the run row can flip terminal a beat before the final
  // stage rows commit, so we poll a few more times to catch the settled
  // end-state, then stop (we don't hammer the API on a long-finished run).
  // Starts at 0 so a fresh mount on a no-run / long-terminal run polls
  // zero times — the grace budget is granted only after we observe a
  // running run (the running branch below) so it spends on the run we
  // watched finish (Codex ckpt-2).
  const postTerminalRef = useRef(0);
  const timelineRefetch = timeline.refetch;
  const timelineRunStatus = timeline.data?.run?.status ?? null;
  useEffect(() => {
    if (tab !== "timeline" || !isBootstrap) return;
    if (timelineRunStatus === "running") {
      // Reset the grace budget while live so it's full when the run ends.
      postTerminalRef.current = TIMELINE_POST_TERMINAL_POLLS;
      const id = window.setInterval(() => timelineRefetch(), TIMELINE_POLL_MS);
      return () => window.clearInterval(id);
    }
    // Terminal (or no run yet). Only poll if we still owe grace polls
    // from a run we watched finish — a fresh mount on a long-done run
    // (budget already 0) polls zero times.
    if (postTerminalRef.current <= 0) return;
    const id = window.setInterval(() => {
      postTerminalRef.current -= 1;
      timelineRefetch();
      if (postTerminalRef.current <= 0) window.clearInterval(id);
    }, TIMELINE_POLL_MS);
    return () => window.clearInterval(id);
  }, [tab, isBootstrap, timelineRunStatus, timelineRefetch]);

  // #1409 §5.5 — "last refreshed" stamp. Updated on every timeline
  // payload the fetch layer hands back so the operator can tell live
  // polling from a frozen view. A plain timestamp (not an accumulator)
  // — no stale-closure hazard.
  const [timelineRefreshedAt, setTimelineRefreshedAt] = useState<string | null>(
    null,
  );
  const timelineData = timeline.data;
  useEffect(() => {
    if (timelineData) setTimelineRefreshedAt(new Date().toISOString());
  }, [timelineData]);

  // Extract the refetch refs as local const bindings so ESLint can
  // see their identity and verify the dep array — `useAsync` wraps
  // refetch in `useCallback([], [])` (see useAsync.test.ts which
  // pins that invariant) so these references are stable across
  // renders. Listing the full `detail` / `runs` / `dag` hook-return
  // objects in deps would re-derive `refetchAll` every render and
  // propagate the identity churn through every `handleX` below —
  // PR #1077 review WARNING / PREVENTION-log #1209.
  const refetchDetail = detail.refetch;
  const refetchRuns = runs.refetch;
  const refetchDag = dag.refetch;
  const refetchTimeline = timeline.refetch;

  const refetchAll = useCallback(() => {
    refetchDetail();
    refetchRuns();
    refetchDag();
    refetchTimeline();
  }, [refetchDetail, refetchRuns, refetchDag, refetchTimeline]);

  const handleIterate = useCallback(async () => {
    setTriggerError(null);
    setBusy(true);
    try {
      await triggerProcess(id, { mode: "iterate" });
      refetchAll();
    } catch (err) {
      setTriggerError(err);
      if (!(err instanceof ApiError))
        console.error("triggerProcess(iterate) failed", err);
    } finally {
      setBusy(false);
    }
  }, [id, refetchAll]);

  const handleFullWashConfirmed = useCallback(async () => {
    setTriggerError(null);
    setBusy(true);
    try {
      await triggerProcess(id, { mode: "full_wash" });
      setShowFullWash(false);
      refetchAll();
    } catch (err) {
      // On error, dismiss the modal and surface the structured 409
      // reason in the ActionBar — keeping the modal up alongside an
      // out-of-context error pill would be confusing. Operator
      // remediation lives on the row (see ApiError.detail.reason →
      // reasonTooltip mapping in ActionBar).
      setTriggerError(err);
      setShowFullWash(false);
      if (!(err instanceof ApiError))
        console.error("triggerProcess(full_wash) failed", err);
    } finally {
      setBusy(false);
    }
  }, [id, refetchAll]);

  const handleAdvancedSubmit = useCallback(
    async (params: Record<string, unknown>) => {
      setAdvancedError(null);
      setAdvancedRequestId(null);
      setBusy(true);
      try {
        // For scheduled_job mechanism, process_id is the job_name verbatim
        // (see app/api/processes.py::trigger_process: target_job_name = process_id).
        const result = await runJob(id, { params });
        setAdvancedRequestId(result?.request_id ?? null);
        refetchAll();
      } catch (err) {
        setAdvancedError(err);
        if (!(err instanceof ApiError))
          // eslint-disable-next-line no-console
          console.error("runJob (advanced) failed", err);
      } finally {
        setBusy(false);
      }
    },
    [id, refetchAll],
  );

  // #2274 — the run THIS render is showing. Read outside the callback and
  // listed in its deps so the request pins what the operator can actually see,
  // rather than whatever a stale closure captured.
  const activeRunId = detail.data?.active_run?.run_id;

  const handleCancelConfirmed = useCallback(
    async (mode: CancelMode) => {
      setCancelError(null);
      setBusy(true);
      try {
        // #2274 — pin the run the operator is looking at. Without it the
        // server takes whatever is running when the POST lands, which on the
        // sync side can be a different scope entirely.
        await cancelProcess(id, { mode, target_run_id: activeRunId });
        setShowCancel(false);
        refetchAll();
      } catch (err) {
        // Same pattern as full-wash above — dismiss modal, surface
        // the reason in the ActionBar tooltip via reasonTooltip.
        setCancelError(err);
        setShowCancel(false);
        // #2274 — the rejection says the run on screen is gone, either
        // replaced (`run_changed`) or simply finished (`no_active_run`). This
        // page does NOT poll its process envelope, so without an explicit
        // refetch the operator resubmits the same dead pin and gets the same
        // 409 forever.
        if (meansActiveRunIsStale(err)) refetchAll();
        if (!(err instanceof ApiError))
          console.error("cancelProcess failed", err);
      } finally {
        setBusy(false);
      }
    },
    [activeRunId, id, refetchAll],
  );

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <div>
          <Link
            to="/admin"
            className="text-xs text-blue-700 hover:underline dark:text-blue-300"
          >
            ← Admin
          </Link>
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
              {detail.data?.display_name ?? id}
            </h1>
            {/* #2274 — the health claim lives in the HEADER, not in the
                Overview tab, so it survives a switch to History / Errors /
                DAG. Before this the drill-in rendered STATUS_VISUAL[status]
                inside Overview only, which both disagreed with the table's
                verdict and vanished on any other tab. */}
            {detail.data ? <VerdictPill row={detail.data} /> : null}
          </div>
          {detail.data ? (
            <p className="text-xs text-slate-500 dark:text-slate-400">
              {detail.data.process_id} · {detail.data.mechanism} ·{" "}
              {detail.data.lane}
            </p>
          ) : null}
          {detail.data ? <HealthDetail row={detail.data} /> : null}
          {/* PR4 #1082 — operator-facing description rendered inline
              on the drill-in (vs the ⓘ tooltip on the table row). The
              drill-in has the screen real estate; the table row uses
              the icon to keep the column compact. */}
          {detail.data?.description ? (
            <p className="mt-1 max-w-3xl text-xs text-slate-600 dark:text-slate-300">
              {detail.data.description}
            </p>
          ) : null}
        </div>
        {detail.data ? (
          <ActionBar
            row={detail.data}
            busy={busy}
            triggerError={triggerError}
            cancelError={cancelError}
            onIterate={handleIterate}
            onFullWash={() => setShowFullWash(true)}
            onCancel={() => setShowCancel(true)}
          />
        ) : null}
      </div>

      <TabBar
        tab={tab}
        setTab={setTab}
        showDag={isOrchestrator}
        showTimeline={isBootstrap}
        showAdvanced={showAdvanced}
      />

      <Section title={tabTitle(tab)}>
        {tab === "overview" ? (
          <OverviewTab
            row={detail.data}
            loading={detail.loading}
            error={detail.error}
            onRetry={detail.refetch}
          />
        ) : tab === "history" ? (
          <HistoryTab
            runs={runs.data}
            loading={runs.loading}
            error={runs.error}
            onRetry={runs.refetch}
          />
        ) : tab === "errors" ? (
          <ErrorsTab
            row={detail.data}
            loading={detail.loading}
            error={detail.error}
            onRetry={detail.refetch}
          />
        ) : tab === "dag" && isOrchestrator ? (
          <DagTab
            payload={dag.data}
            loading={dag.loading}
            error={dag.error}
            onRetry={dag.refetch}
          />
        ) : tab === "timeline" && isBootstrap ? (
          <TimelineTab
            payload={timeline.data}
            loading={timeline.loading}
            error={timeline.error}
            onRetry={timeline.refetch}
            refreshedAt={timelineRefreshedAt}
          />
        ) : tab === "advanced" && showAdvanced && detail.data ? (
          <AdvancedTab
            row={detail.data}
            busy={busy}
            error={advancedError}
            requestId={advancedRequestId}
            onSubmit={handleAdvancedSubmit}
          />
        ) : null}
      </Section>

      {showFullWash && detail.data ? (
        <FullWashConfirmDialog
          row={detail.data}
          busy={busy}
          onCancel={() => setShowFullWash(false)}
          onConfirm={handleFullWashConfirmed}
        />
      ) : null}

      {showCancel && detail.data ? (
        <CancelConfirmDialog
          row={detail.data}
          busy={busy}
          onCancel={() => setShowCancel(false)}
          onConfirm={handleCancelConfirmed}
        />
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Action bar (Iterate / Full-wash / Cancel)
// ---------------------------------------------------------------------------

function ActionBar({
  row,
  busy,
  triggerError,
  cancelError,
  onIterate,
  onFullWash,
  onCancel,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  triggerError: unknown;
  cancelError: unknown;
  onIterate: () => void;
  onFullWash: () => void;
  onCancel: () => void;
}) {
  const watermarkTooltip = row.watermark?.human ?? "no resume cursor";
  // Mechanism-specific labels — bootstrap maps iterate/full_wash to
  // "Re-run failed" / "Re-run all" per data-engineer skill §7.3.
  const isBootstrap = row.mechanism === "bootstrap";
  // First install (#1264): collapse to a single non-destructive "Run
  // bootstrap" button — mirrors the table-row affordance. "Re-run failed"
  // / "Cancel" cannot apply on a never-run row.
  const isFirstRun = isBootstrap && row.status === "pending_first_run";
  // Clean-complete (#1432): bootstrap finished with every stage successful
  // — "Re-run all" stays available but loses its red destructive tone, since
  // nothing failed. Mirrors the table-row affordance.
  const isCleanComplete = isBootstrap && row.status === "ok";
  const iterateLabel = isBootstrap ? "Re-run failed" : "Iterate";
  const fullWashLabel = isFirstRun
    ? "Run bootstrap"
    : isBootstrap
      ? "Re-run all"
      : "Full-wash";
  const iterateTooltip = row.can_iterate
    ? isBootstrap
      ? "Resume incomplete + failed stages from where they stopped."
      : watermarkTooltip
    : `${iterateLabel} is not available right now.`;
  const fullWashTooltip = isFirstRun
    ? "Start the first-install bootstrap — populates the universe + filings. Asks for confirmation first."
    : row.can_full_wash
      ? isCleanComplete
        ? "Last bootstrap completed cleanly — this wipes every stage and replays the full install from scratch. Only needed to fully re-bootstrap (confirm required)."
        : isBootstrap
          ? "Reset every stage to pending; full first-install replay (confirm required)."
          : "Reset watermark and re-fetch from epoch (confirm required)."
      : `${fullWashLabel} is not available right now.`;
  return (
    <div className="flex flex-col items-end gap-1">
      <div className="flex items-center gap-2">
        {isFirstRun ? null : (
          <button
            type="button"
            onClick={onIterate}
            disabled={!row.can_iterate || busy}
            title={iterateTooltip}
            className="rounded border border-slate-300 bg-white px-3 py-1 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
          >
            {iterateLabel}
          </button>
        )}
        <button
          type="button"
          onClick={onFullWash}
          disabled={!row.can_full_wash || busy}
          title={fullWashTooltip}
          className={
            isFirstRun
              ? "rounded border border-blue-600 bg-blue-600 px-3 py-1 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50 dark:border-blue-500 dark:bg-blue-600 dark:hover:bg-blue-700"
              : isCleanComplete
                ? "rounded border border-slate-300 bg-white px-3 py-1 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
                : "rounded border border-red-300 bg-white px-3 py-1 text-sm font-medium text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-900 dark:bg-slate-900 dark:text-red-300 dark:hover:bg-red-950/40"
          }
        >
          {fullWashLabel}
        </button>
        {isFirstRun ? null : (
          <button
            type="button"
            onClick={onCancel}
            disabled={!row.can_cancel || busy}
            title={row.can_cancel ? "Cooperative cancel — the worker stops at its next checkpoint." : "No active run to cancel."}
            className="rounded border border-amber-300 bg-white px-3 py-1 text-sm font-medium text-amber-700 hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-900 dark:bg-slate-900 dark:text-amber-300 dark:hover:bg-amber-950/40"
          >
            Cancel
          </button>
        )}
      </div>
      {triggerError ? (
        <div
          role="status"
          className="text-xs text-red-700 dark:text-red-300"
          title={reasonTooltip(triggerError)}
        >
          trigger rejected
        </div>
      ) : null}
      {cancelError ? (
        <div
          role="status"
          className="text-xs text-red-700 dark:text-red-300"
          title={reasonTooltip(cancelError)}
        >
          cancel rejected
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tabs
// ---------------------------------------------------------------------------

function TabBar({
  tab,
  setTab,
  showDag,
  showTimeline,
  showAdvanced,
}: {
  tab: TabKey;
  setTab: (t: TabKey) => void;
  showDag: boolean;
  showTimeline: boolean;
  showAdvanced: boolean;
}) {
  const tabs: { key: TabKey; label: string }[] = [
    { key: "overview", label: "Overview" },
    { key: "history", label: "History" },
    { key: "errors", label: "Errors" },
    ...(showDag ? [{ key: "dag" as TabKey, label: "DAG" }] : []),
    ...(showTimeline ? [{ key: "timeline" as TabKey, label: "Timeline" }] : []),
    ...(showAdvanced
      ? [{ key: "advanced" as TabKey, label: "Advanced" }]
      : []),
  ];
  return (
    <div role="tablist" className="flex gap-1 border-b border-slate-200 dark:border-slate-800">
      {tabs.map((t) => {
        const active = tab === t.key;
        return (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => setTab(t.key)}
            className={`-mb-px rounded-t border border-b-0 px-3 py-1 text-sm font-medium ${
              active
                ? "border-slate-300 bg-white text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                : "border-transparent bg-transparent text-slate-500 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-800/40"
            }`}
          >
            {t.label}
          </button>
        );
      })}
    </div>
  );
}

function tabTitle(tab: TabKey): string {
  switch (tab) {
    case "overview":
      return "Overview";
    case "history":
      return "Run history (last 7 days)";
    case "errors":
      return "Errors (latest terminal run)";
    case "dag":
      return "DAG (latest sync run)";
    case "timeline":
      return "Bootstrap timeline (latest run)";
    case "advanced":
      return "Advanced — re-fetch with custom params";
  }
}

/**
 * #2274 — the verdict's reason, plus every stale reason the backend reported.
 *
 * `verdict_reason` is a HEADLINE: several reasons can fire and
 * `compute_verdict` picks one (`_REASON_ORDER`, or `_WEDGE_HEADLINE_ORDER`
 * inside its `status == "disabled"` branch). The drill-in is where the full
 * list belongs — which is why `stale_reasons` is on the payload at all
 * (`api/types.ts`: "`status` + `stale_reasons` stay on the payload for the
 * drill-in"). Until now nothing rendered it: `STALE_REASON_LABEL` was
 * exported, unit-tested and dead.
 *
 * ⚠⚠ The chips carry NO `Badge` tone, and that is load-bearing rather than a
 * style choice. A reason surviving in the payload does not mean the backend
 * thinks it is an alarm: `compute_verdict` returns neutral `paused` for a
 * halted row still carrying `schedule_missed` / `watermark_gap`, and
 * `self_healing` calms a reason whose retry is already in flight. Painting
 * every reason amber would undo that suppression and put two disagreeing
 * signals on one page — the exact defect this change removes. The verdict pill
 * stays the only toned health claim here; these are the inputs it was computed
 * from, stated as such.
 *
 * ⚠ Order is the payload's own array order, NOT a re-derived precedence — the
 * backend's ordering constants govern which reason wins the headline, and
 * mirroring them here would be a second copy of a rule that can drift.
 *
 * ⚠ No elapsed-since-heartbeat suffix on any chip. `ProcessRow` appends one to
 * its reason line, but that advances only because the TABLE polls and folds
 * the elapsed string into `processRowSignature`; `formatElapsedSince` has no
 * timer of its own. This page's envelope (`useAsync(() => fetchProcess(id),
 * [id])`) has no interval — it refetches on actions and route change only — so
 * a duration rendered here would freeze at fetch time while reading as live.
 */
function HealthDetail({ row }: { row: ProcessRowResponse }) {
  if (!row.verdict_reason && row.stale_reasons.length === 0) return null;
  return (
    <div className="mt-1 space-y-1">
      {row.verdict_reason ? (
        <p
          data-testid="verdict-reason"
          className="text-xs text-slate-600 dark:text-slate-300"
        >
          {row.verdict_reason}
        </p>
      ) : null}
      {row.stale_reasons.length > 0 ? (
        <div
          data-testid="stale-reasons"
          className="flex flex-wrap items-center gap-1 text-[11px] text-slate-500 dark:text-slate-400"
        >
          <span className="uppercase tracking-wide">reported</span>
          {row.stale_reasons.map((reason) => (
            <span
              key={reason}
              data-testid="stale-reason-chip"
              data-reason={reason}
              className="rounded border border-slate-300 px-1.5 py-0.5 dark:border-slate-700"
            >
              {STALE_REASON_LABEL[reason]}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function OverviewTab({
  row,
  loading,
  error,
  onRetry,
}: {
  row: ProcessRowResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={4} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!row) return <p className="text-sm text-slate-500">No detail available.</p>;
  return (
    <div className="space-y-3 text-sm">
      {/* #2274 — DEMOTED from a toned pill to plain text, and the health claim
          moved to the page header.
          `status` is kept because it is real and distinct information: the
          adapter-normalised process state the verdict was computed FROM,
          including the kill switch's masking to `disabled`. It is not a stored
          run status. It loses its tone because a second toned pill beside the
          verdict is the two-cells-that-disagree defect `ProcessRow::RecentReaps`
          already names. The label still comes from `STATUS_VISUAL`, so the two
          surfaces keep sharing their wording for this field. */}
      <KeyValueRow
        label="Process state"
        value={STATUS_VISUAL[row.status].label}
      />
      <KeyValueRow label="Cadence" value={row.cadence_human} />
      <KeyValueRow
        label="Next fire"
        value={row.next_fire_at ? formatDateTime(row.next_fire_at) : "—"}
      />
      <KeyValueRow
        label="Watermark"
        value={row.watermark ? row.watermark.human : "no resume cursor"}
      />
      {row.last_run ? (
        <KeyValueRow
          label="Last run"
          value={`${formatDateTime(row.last_run.finished_at)} · ${row.last_run.status} · ${row.last_run.rows_processed ?? "?"} rows`}
        />
      ) : (
        <KeyValueRow label="Last run" value="never" />
      )}
      {row.active_run ? (
        <KeyValueRow
          label="Active run"
          value={`${runKindLabel(row.active_run.run_kind)}#${row.active_run.run_id} · started ${formatDateTime(row.active_run.started_at)}`}
        />
      ) : null}
    </div>
  );
}

/**
 * #2274 — name the table the run id came from.
 *
 * The orchestrator wrapper rows publish a `sync_runs.sync_run_id` in the slot
 * every other scheduled row uses for a `job_runs.run_id`, and the two id spaces
 * overlap freely. An operator reading "run #418" off the full sync and then
 * grepping `job_runs` for 418 finds an unrelated job's run.
 */
function runKindLabel(kind: ActiveRunSummaryResponse["run_kind"]): string {
  switch (kind) {
    case "sync_run":
      return "sync run ";
    case "bootstrap_run":
      return "bootstrap run ";
    default:
      return "run ";
  }
}

function KeyValueRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="w-28 text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        {label}
      </span>
      <span className="text-sm text-slate-700 dark:text-slate-200">{value}</span>
    </div>
  );
}

/**
 * Positive JobProgress error buckets for a run, in stable name order.
 *
 * The `n > 0` filter deliberately lives in ONE place — the adapter
 * (`scheduled_adapter._progress_error_buckets`), which mirrors
 * `job_progress.degradation_reason`'s predicate so the Errored cell and the
 * Status cell cannot disagree about which buckets fired. Re-filtering here
 * would fork that rule. This sorts only, matching `degradation_reason`'s own
 * sorted reason string so the cell and `error_msg` read in the same order.
 *
 * `null` (job reports no JobProgress) and `{}` (reported, none positive) both
 * yield `[]` for rendering — the API keeps them distinct, the cell does not
 * need to.
 */
function progressErrorEntries(run: ProcessRunSummaryResponse): [string, number][] {
  if (!run.progress_errors) return [];
  return Object.entries(run.progress_errors).sort(([a], [b]) => a.localeCompare(b));
}

function HistoryTab({
  runs,
  loading,
  error,
  onRetry,
}: {
  runs: ProcessRunSummaryResponse[] | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={5} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!runs || runs.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No runs in the last 7 days.
      </p>
    );
  }
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <tr>
          <th className="px-2 py-2">Started</th>
          <th className="px-2 py-2">Finished</th>
          <th className="px-2 py-2">Duration</th>
          <th className="px-2 py-2">Rows</th>
          <th className="px-2 py-2">Errored</th>
          <th className="px-2 py-2">Status</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
        {runs.map((r) => {
          const visual = RUN_STATUS_VISUAL[r.status];
          return (
            <tr key={r.run_id} className="text-sm">
              <td className="px-2 py-2 text-slate-700 dark:text-slate-200">
                {formatDateTime(r.started_at)}
              </td>
              <td className="px-2 py-2 text-slate-600 dark:text-slate-400">
                {formatDateTime(r.finished_at)}
              </td>
              <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
                {r.duration_seconds.toFixed(1)}s
              </td>
              <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
                {r.rows_processed ?? "—"}
              </td>
              {/*
                #3111 — `rows_processed` COUNTS THE FAILURES. That is the defect
                this ticket was filed about (`scheduler.py:7860` assigned
                `parsed + tombstoned + failed` to row_count), so "5 rows" on a
                tick where all five threw read exactly like five extractions.
                Showing the errored count beside it is what makes the two
                distinguishable without opening the Errors tab.

                Zero renders as an em-dash rather than "0": a column of zeroes
                trains the eye to skip it, which is how the one non-zero gets
                missed.

                ⚠⚠ TWO DISJOINT ERROR COUNTERS REACH THIS CELL AND THEY ARE NOT
                SUMMABLE. `job_runs.rows_errored` is one;
                `JobProgress.errors` (`progress_json`) is the other, and it is
                what fired on the only `degraded` run in the corpus
                (`daily_candle_refresh` 133470, `rows_errored=0`,
                `progress_json.errors={"failed":1}`) — which slice 4 rendered
                as `—`, the defect slice 5 fixes. A sum would double-count:
                `sec_manifest_worker.py:760` pins
                `agg.rows_errored == failed + dispatch_errors` while `:367`
                puts the same two counters into its JobProgress.

                BUCKETS FIRST, then the scalar. Not a claim about authority —
                a producer census. `rows_errored` has exactly one producer
                (`JobTelemetryAggregator`, instantiated only at
                `scheduler.py:8093`), that same job also writes the buckets,
                and there the scalar is their sum by construction. So showing
                the buckets loses nothing recoverable and gains the split that
                matters operationally: `failed` means the row returns on a
                retry stamp, `dispatch_errors` means the transition itself
                failed. Every OTHER bucket producer writes no scalar at all.

                ⚠ Trigger to revisit: a producer emitting a positive bucket
                that is NOT part of its `rows_errored` total. Then the cell
                needs two labelled segments rather than a precedence. Guarded
                by a test pinning WorkerStats' buckets against the aggregator.
              */}
              <td
                className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400"
                data-testid="run-rows-errored"
                title={
                  progressErrorEntries(r).length > 0
                    ? // ⚠ The scalar is STATED, not assumed. An earlier draft said
                      // "rows_errored is 0 or absent" here — false for the one job
                      // that writes both counters, where it is their sum, and the
                      // cell hides it. Codex ckpt-2 caught it against this file's
                      // own test fixture.
                      `job_runs.progress_json.errors — the buckets this run reported as errors, shown unsummed. job_runs.rows_errored on this run is ${r.rows_errored}; the two counters are disjointly written and never added.`
                    : r.rows_errored > 0
                      ? "job_runs.rows_errored — rows this run recorded as errored. This run reported no positive JobProgress error buckets."
                      : "No errors on either counter (job_runs.rows_errored and progress_json.errors). A run can still be degraded for making no terminal progress, which is not an error count."
                }
              >
                {(() => {
                  const buckets = progressErrorEntries(r);
                  if (buckets.length > 0) {
                    return (
                      <span className="font-medium text-amber-700 dark:text-amber-300">
                        {buckets.map(([name, n]) => `${name} ${n}`).join(" · ")}
                      </span>
                    );
                  }
                  if (r.rows_errored > 0) {
                    return (
                      <span className="font-medium text-amber-700 dark:text-amber-300">
                        {r.rows_errored}
                      </span>
                    );
                  }
                  return "—";
                })()}
              </td>
              <td className="px-2 py-2 text-xs">
                <Badge
                  tone={visual.tone}
                  className={visual.extraClass}
                  data-testid="run-status"
                >
                  {visual.label}
                </Badge>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ErrorsTab({
  row,
  loading,
  error,
  onRetry,
}: {
  row: ProcessRowResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={3} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!row) return <p className="text-sm text-slate-500">No detail available.</p>;
  // #3111 slice 5 — this tab reads ONLY `job_runs.error_classes`, so "no errors
  // on the latest terminal run" was a universal denial it cannot support. It
  // would now sit beside a History row reading `failed 1`, because a run can
  // report positive `JobProgress.errors` while writing no `error_classes` at
  // all (they are disjointly written counters). The copy states what it reads.
  // Copy only — no new read path here.
  if (row.last_n_errors.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No per-class error detail (<code>job_runs.error_classes</code>) on the
        latest terminal run. A run can still report errors on the other counter
        — check the Errored column in History.
      </p>
    );
  }
  return (
    <ul className="space-y-2">
      {row.last_n_errors.map((e) => (
        <li
          key={e.error_class}
          className="rounded border border-red-200 bg-red-50 p-2 text-sm dark:border-red-900 dark:bg-red-950/40"
        >
          <div className="flex items-baseline justify-between">
            <span className="font-medium text-red-800 dark:text-red-200">
              {e.error_class}
            </span>
            <span className="text-xs text-red-700 dark:text-red-300">
              ×{e.count} · last seen {formatDateTime(e.last_seen_at)}
            </span>
          </div>
          {e.sample_subject ? (
            <div className="mt-1 text-xs text-red-700 dark:text-red-300">
              {e.sample_subject}
            </div>
          ) : null}
          <pre className="mt-1 whitespace-pre-wrap break-words text-xs text-red-900 dark:text-red-100">
            {e.sample_message}
          </pre>
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------
// DAG drill-in tab (#1078 — orchestrator_full_sync only)
// ---------------------------------------------------------------------------

/** Layer status → Badge tone (#2148). Semantic values only; no colour here. */
const LAYER_STATUS_TONE: Record<string, { tone: BadgeTone; extraClass?: string }> = {
  pending: { tone: "neutral" },
  // `running` was sky; folded onto the colour table's `info` slot with the
  // other in-progress states (#2148).
  running: { tone: "info" },
  complete: { tone: "ok" },
  failed: { tone: "risk" },
  skipped: { tone: "neutral" },
  partial: { tone: "warn" },
  cancelled: { tone: "neutral", extraClass: "line-through" },
};

function AdvancedTab({
  row,
  busy,
  error,
  requestId,
  onSubmit,
}: {
  row: ProcessRowResponse;
  busy: boolean;
  error: unknown;
  requestId: number | null;
  onSubmit: (params: Record<string, unknown>) => Promise<void>;
}) {
  return (
    <div className="space-y-4">
      <p className="text-xs text-slate-500 dark:text-slate-400">
        Submits to <code>POST /jobs/{row.process_id}/run</code> with the
        validated params envelope. The job is queued; track outcome on
        the requests panel.
      </p>
      <AdvancedParamsForm
        metadata={row.params_metadata}
        busy={busy}
        onSubmit={onSubmit}
      />
      {requestId !== null ? (
        <p
          role="status"
          className="text-xs text-emerald-700 dark:text-emerald-300"
        >
          queued as request #{requestId}
        </p>
      ) : null}
      {error ? (
        <p
          role="alert"
          className="text-xs text-red-700 dark:text-red-300"
          title={reasonTooltip(error)}
        >
          trigger rejected
          {error instanceof ApiError && typeof error.detail === "string"
            ? `: ${error.detail}`
            : null}
        </p>
      ) : null}
    </div>
  );
}

function DagTab({
  payload,
  loading,
  error,
  onRetry,
}: {
  payload: OrchestratorDagResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
}) {
  if (loading) return <SectionSkeleton rows={6} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!payload || payload.sync_run === null) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No recent sync run. Trigger a sync from the action bar above to populate the DAG.
      </p>
    );
  }
  return (
    <div className="space-y-4">
      <DagRunSummary run={payload.sync_run} />
      <DagLayerTable layers={payload.layers} />
    </div>
  );
}

function DagRunSummary({ run }: { run: OrchestratorDagSyncRunResponse }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <Cell label="Run id" value={`#${run.sync_run_id}`} />
      <Cell
        label="Scope"
        value={`${run.scope}${run.scope_detail ? ` · ${run.scope_detail}` : ""}`}
      />
      <Cell label="Trigger" value={run.trigger} />
      <Cell label="Status" value={run.status} />
      <Cell label="Started" value={formatDateTime(run.started_at)} />
      <Cell
        label="Finished"
        value={run.finished_at ? formatDateTime(run.finished_at) : "—"}
      />
      <Cell
        label="Layers"
        value={`${run.layers_done}/${run.layers_planned} done · ${run.layers_failed} failed · ${run.layers_skipped} skipped`}
      />
      <Cell
        label="Cancel signal"
        value={run.cancel_requested_at ? formatDateTime(run.cancel_requested_at) : "—"}
      />
    </div>
  );
}

function Cell({
  label,
  value,
}: {
  label: string;
  value: string | ReactNode;
}) {
  // `title` is set only when value is a plain string — ReactNode
  // values (#1140 Task C run-warning chip) render their own tooltip.
  const title = typeof value === "string" ? value : undefined;
  return (
    <div className="rounded border border-slate-200 bg-slate-50 p-2 text-sm dark:border-slate-800 dark:bg-slate-900/40">
      <div className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        {label}
      </div>
      <div className="mt-0.5 truncate text-slate-700 dark:text-slate-200" title={title}>
        {value}
      </div>
    </div>
  );
}

function DagLayerTable({ layers }: { layers: OrchestratorDagLayerResponse[] }) {
  if (layers.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No layer rows recorded for this run.
      </p>
    );
  }
  // Group by tier so the operator sees source / raw / computed /
  // decisions ordering. ``null`` tier (defensive, registry drift)
  // sinks to the end.
  const sorted = [...layers].sort((a, b) => {
    const at = a.tier ?? 99;
    const bt = b.tier ?? 99;
    if (at !== bt) return at - bt;
    return a.display_name.localeCompare(b.display_name);
  });
  return (
    <table className="w-full text-left text-sm">
      <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <tr>
          <th className="px-2 py-2">Layer</th>
          <th className="px-2 py-2">Tier</th>
          <th className="px-2 py-2">Status</th>
          <th className="px-2 py-2">Items</th>
          <th className="px-2 py-2">Finished</th>
          <th className="px-2 py-2">Detail</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
        {sorted.map((layer) => {
          // An unmapped status degrades to a visible neutral badge, never to a
          // blank (design-system.md, #1808 class).
          const visual = LAYER_STATUS_TONE[layer.status] ?? { tone: "neutral" as const };
          const items =
            layer.items_total === null && layer.items_done === null
              ? "—"
              : `${layer.items_done ?? 0}/${layer.items_total ?? "?"}`;
          const detail =
            layer.error_message ??
            layer.skip_reason ??
            (layer.error_category ? `category: ${layer.error_category}` : "—");
          return (
            <tr key={layer.name} className="text-sm align-top">
              <td className="px-2 py-2 text-slate-700 dark:text-slate-200">
                <div className="font-medium">{layer.display_name}</div>
                <div className="text-xs text-slate-500 dark:text-slate-400">
                  {layer.name}
                </div>
              </td>
              <td className="px-2 py-2 text-slate-600 dark:text-slate-400">
                {layer.tier === null ? "—" : `T${layer.tier}`}
              </td>
              <td className="px-2 py-2">
                <Badge tone={visual.tone} uppercase className={visual.extraClass}>
                  {layer.status}
                </Badge>
              </td>
              <td className="px-2 py-2 tabular-nums text-slate-600 dark:text-slate-400">
                {items}
              </td>
              <td className="px-2 py-2 text-xs text-slate-500 dark:text-slate-400">
                {layer.finished_at ? formatDateTime(layer.finished_at) : "—"}
              </td>
              <td className="px-2 py-2 text-xs text-slate-600 dark:text-slate-400">
                <span className="break-words" title={detail}>
                  {detail}
                </span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------------------
// Bootstrap timeline drill-in tab (#1080 — bootstrap process_id only)
// ---------------------------------------------------------------------------

/** Stage status → Badge tone (#2148). Semantic values only; no colour here. */
const STAGE_STATUS_TONE: Record<string, { tone: BadgeTone; extraClass?: string }> = {
  pending: { tone: "neutral" },
  // `running` was sky; folded onto the colour table's `info` slot (#2148).
  running: { tone: "info" },
  success: { tone: "ok" },
  error: { tone: "risk" },
  skipped: { tone: "neutral" },
  blocked: { tone: "warn" },
  // PR3c #1093: operator-cancelled stage. Neutral — visually distinct from red
  // (genuine failure) so the operator can tell at a glance which stages were
  // stopped by their own cancel vs which failed on their own. Italic keeps it
  // from looking interchangeable with not-yet-attempted `pending` / `skipped`,
  // which the old map did with a slightly darker grey (#2148: the tone table
  // has one neutral, so the distinction moves to the non-colour decoration).
  cancelled: { tone: "neutral", extraClass: "italic" },
};

// #1266 — `last_error` doubles as the skip/cancel REASON for deliberate
// non-error terminal states (`mark_stage_skipped` / `mark_stage_cancelled`
// write their reason into the same column). Red text on an intended
// bypass (e.g. the slow-connection fallback) reads as a failure to the
// operator, so the detail line tones by stage status: neutral for
// deliberate skips/cancels, amber for blocked (upstream failure forced
// the skip — matches its badge), red only for genuine errors.
const STAGE_DETAIL_TONE: Record<string, string> = {
  skipped: "text-slate-500 dark:text-slate-400",
  cancelled: "text-slate-500 dark:text-slate-400",
  blocked: "text-amber-700 dark:text-amber-300",
};

const ARCHIVE_TONE: Record<string, string> = {
  // Archive squares mirror their stage's status by default; once we have
  // a per-archive status surface (see #1064 follow-up) the tone can
  // diverge. For now: error stage → red square, success → emerald, else
  // sky.
  error: "bg-red-200 dark:bg-red-900/60 hover:bg-red-300 dark:hover:bg-red-900",
  success:
    "bg-emerald-200 dark:bg-emerald-900/60 hover:bg-emerald-300 dark:hover:bg-emerald-900",
};

function TimelineTab({
  payload,
  loading,
  error,
  onRetry,
  refreshedAt,
}: {
  payload: BootstrapTimelineResponse | null;
  loading: boolean;
  error: unknown;
  onRetry: () => void;
  refreshedAt: string | null;
}) {
  const [archive, setArchive] = useState<{
    stageDisplayName: string;
    archive: BootstrapTimelineArchiveResponse;
  } | null>(null);

  if (loading) return <SectionSkeleton rows={6} />;
  if (error) return <SectionError onRetry={onRetry} />;
  if (!payload || payload.run === null) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No bootstrap run yet. Trigger Run from the action bar above to populate
        the timeline.
      </p>
    );
  }

  const isRunning = payload.run.status === "running";
  return (
    <div className="space-y-4">
      <TimelineRunSummary run={payload.run} />
      {/* #1409 §5.5 — last-refreshed caption. A live (running) view
          auto-polls every 5s; the stamp lets the operator tell a moving
          view from a frozen one. */}
      {refreshedAt ? (
        <p
          className="text-[10px] text-slate-400 dark:text-slate-500"
          data-testid="timeline-refreshed-at"
        >
          {isRunning ? "Auto-refreshing · " : ""}last refreshed{" "}
          {formatDateTime(refreshedAt)}
        </p>
      ) : null}
      <TimelineLanesGrid
        stages={payload.stages}
        onArchiveClick={(stage, archiveRow) =>
          setArchive({
            stageDisplayName: stage.display_name,
            archive: archiveRow,
          })
        }
      />
      {archive ? (
        <ArchiveDetailModal
          stageDisplayName={archive.stageDisplayName}
          archive={archive.archive}
          onClose={() => setArchive(null)}
        />
      ) : null}
    </div>
  );
}

function TimelineRunSummary({
  run,
}: {
  run: NonNullable<BootstrapTimelineResponse["run"]>;
}) {
  // #1140 Task C — surface the run-level warning state when the run
  // is `complete` AND at least one stage carries a warning. Red runs
  // already shout; the amber dot is suppressed for them.
  const showWarningDot = run.has_warnings && run.status === "complete";
  const statusValue = showWarningDot ? (
    <span className="inline-flex items-center gap-1">
      <span>{run.status}</span>
      <span
        aria-label="Run completed with warnings"
        title="One or more stages succeeded with rows_processed=0 — open Timeline tab for detail."
        data-testid="run-warning-dot"
        className="inline-block h-2 w-2 rounded-full bg-amber-500"
      />
    </span>
  ) : (
    run.status
  );
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <Cell label="Run id" value={`#${run.run_id}`} />
      <Cell label="Status" value={statusValue} />
      <Cell label="Triggered" value={formatDateTime(run.triggered_at)} />
      <Cell
        label="Completed"
        value={run.completed_at ? formatDateTime(run.completed_at) : "—"}
      />
      <Cell
        label="Cancel signal"
        value={
          run.cancel_requested_at
            ? formatDateTime(run.cancel_requested_at)
            : "—"
        }
      />
    </div>
  );
}

function TimelineLanesGrid({
  stages,
  onArchiveClick,
}: {
  stages: BootstrapTimelineStageResponse[];
  onArchiveClick: (
    stage: BootstrapTimelineStageResponse,
    archive: BootstrapTimelineArchiveResponse,
  ) => void;
}) {
  if (stages.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        Run is in flight; no stage rows materialised yet. Reload in a few
        seconds.
      </p>
    );
  }
  // Group stages by lane; within each lane sort by stage_order ASC.
  const byLane = new Map<string, BootstrapTimelineStageResponse[]>();
  for (const stage of stages) {
    const list = byLane.get(stage.lane) ?? [];
    list.push(stage);
    byLane.set(stage.lane, list);
  }
  for (const list of byLane.values()) {
    list.sort((a, b) => a.stage_order - b.stage_order);
  }
  // Lane order: declared order first, then any trailing unknown lanes
  // sorted alphabetically so legacy rows still render.
  const orderedLanes: string[] = [];
  for (const lane of BOOTSTRAP_LANE_ORDER) {
    if (byLane.has(lane)) orderedLanes.push(lane);
  }
  const trailingLanes = [...byLane.keys()]
    .filter((lane) => !BOOTSTRAP_LANE_ORDER.includes(lane as never))
    .sort();
  orderedLanes.push(...trailingLanes);

  return (
    <div className="grid gap-3 lg:grid-cols-2">
      {orderedLanes.map((lane) => {
        const laneStages = byLane.get(lane) ?? [];
        return (
          <div
            key={lane}
            className="rounded border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-900/40"
          >
            <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
              {lane}{" "}
              <span className="ml-1 font-normal text-slate-400 dark:text-slate-500">
                ({laneStages.length})
              </span>
            </h3>
            <ul className="space-y-2">
              {laneStages.map((stage) => (
                <TimelineStageRow
                  key={stage.stage_key}
                  stage={stage}
                  onArchiveClick={(arch) => onArchiveClick(stage, arch)}
                />
              ))}
            </ul>
          </div>
        );
      })}
    </div>
  );
}

function TimelineStageRow({
  stage,
  onArchiveClick,
}: {
  stage: BootstrapTimelineStageResponse;
  onArchiveClick: (archive: BootstrapTimelineArchiveResponse) => void;
}) {
  const visual =
    STAGE_STATUS_TONE[stage.status] ?? STAGE_STATUS_TONE["pending"]!;
  const archiveTone =
    ARCHIVE_TONE[stage.status] ??
    "bg-sky-200 dark:bg-sky-900/60 hover:bg-sky-300 dark:hover:bg-sky-900";
  return (
    <li className="rounded border border-slate-200 p-2 text-sm dark:border-slate-800">
      <div className="flex items-start gap-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-baseline gap-2">
            <Badge tone={visual.tone} uppercase className={visual.extraClass}>
              {stage.status}
            </Badge>
            {stage.warning ? (
              <Badge
                tone="warn"
                uppercase
                title={stage.warning}
                aria-label={`Warning: ${stage.warning}`}
                data-testid="stage-warning-chip"
              >
                warning
              </Badge>
            ) : null}
            {/* #1409 P5 — stale chip. Server flags a running stage whose
                heartbeat exceeds the 1800s threshold; distinguishes a
                wedged stage from one that is simply slow. */}
            {stage.is_stale ? (
              <Badge
                tone="risk"
                uppercase
                title={`No progress for ${formatHeartbeatAge(stage.heartbeat_age_seconds).replace("updated ", "").replace(" ago", "")} — exceeds the 30-minute stall threshold`}
                aria-label="Stage appears stalled — no recent progress"
                data-testid="stage-stale-chip"
              >
                stalled
              </Badge>
            ) : null}
            <span
              className="truncate font-medium text-slate-700 dark:text-slate-200"
              title={stage.display_name}
            >
              {stage.display_name}
            </span>
          </div>
          <div className="text-xs text-slate-500 dark:text-slate-400">
            {stage.job_name || stage.stage_key}
          </div>
          {/* #1225 / #1271 — Live progress bar. Renders when we have
              either an explicit target (target_count > 0) OR a
              processed_count > 0 (stage instrumented but no upfront
              target). Stages that never write progress (current bulk
              ingesters per #1225) render no bar — operator sees the
              stage status badge only, which is the prior baseline.
              processed_count defensively coerced via ?? 0 — the DB
              schema is `INTEGER NOT NULL DEFAULT 0`, but null guard
              defends against future serializer drift (bot iter 1
              BLOCKING). */}
          {(() => {
            if (stage.status !== "running") return null;
            const processed = stage.processed_count ?? 0;
            const target = stage.target_count;
            const hasTarget = target !== null && target > 0;
            // #1409 P5 — server-computed live signals. Rendered for EVERY
            // running stage (even 0/- with no bar) so a slow-but-alive
            // stage is visibly distinct from a wedged one — the whole
            // point of #1409. `formatHeartbeatAge` consumes the server's
            // skew-free `heartbeat_age_seconds` rather than re-deriving
            // from the client clock.
            const rateLabel = formatRate(stage.rate);
            const etaLabel = formatEta(stage.eta_seconds);
            const meta: string[] = [];
            if (stage.rate !== null) meta.push(rateLabel);
            if (stage.eta_seconds !== null) meta.push(`ETA ${etaLabel}`);
            if (stage.heartbeat_age_seconds !== null)
              meta.push(formatHeartbeatAge(stage.heartbeat_age_seconds));
            // #1273 PR2 — cohort-definition fingerprint surfaces as a
            // native `title=` tooltip on the bar wrapper. `?? undefined`
            // suppresses the tooltip when the field is null.
            return (
              <div
                className="mt-1.5"
                title={stage.target_cohort_fingerprint ?? undefined}
              >
                {hasTarget ? (
                  <>
                    <div className="h-1 w-full overflow-hidden rounded bg-slate-200 dark:bg-slate-800">
                      <div
                        className="h-full bg-sky-500 dark:bg-sky-400 transition-[width] duration-500"
                        style={{
                          width: `${Math.min(
                            100,
                            (processed / (target as number)) * 100,
                          ).toFixed(1)}%`,
                        }}
                        aria-label={`${processed} of ${target}`}
                      />
                    </div>
                    <div className="mt-0.5 text-[10px] text-slate-500 dark:text-slate-400">
                      {processed.toLocaleString()} /{" "}
                      {(target as number).toLocaleString()}{" "}
                      ({((processed / (target as number)) * 100).toFixed(1)}%)
                    </div>
                  </>
                ) : processed > 0 ? (
                  <div className="text-[10px] text-slate-500 dark:text-slate-400">
                    {processed.toLocaleString()} processed (no target set)
                  </div>
                ) : null}
                {meta.length > 0 ? (
                  <div
                    className={`mt-0.5 text-[10px] ${stage.is_stale ? "text-red-600 dark:text-red-400" : "text-slate-500 dark:text-slate-400"}`}
                    data-testid="stage-live-meta"
                  >
                    {meta.join(" · ")}
                  </div>
                ) : null}
              </div>
            );
          })()}
          {stage.warning ? (
            <div
              className="mt-1 truncate text-xs text-amber-700 dark:text-amber-300"
              title={stage.warning}
            >
              {stage.warning}
            </div>
          ) : null}
          {stage.last_error ? (
            <div
              className={`mt-1 truncate text-xs ${
                STAGE_DETAIL_TONE[stage.status] ??
                "text-red-700 dark:text-red-300"
              }`}
              title={stage.last_error}
              data-testid="stage-detail-text"
            >
              {stage.last_error}
            </div>
          ) : null}
        </div>
        {stage.archives.length > 0 ? (
          <div
            className="flex flex-wrap gap-1"
            aria-label={`${stage.archives.length} archive results`}
          >
            {stage.archives.map((archive) => (
              <button
                key={archive.archive_name}
                type="button"
                onClick={() => onArchiveClick(archive)}
                title={archive.archive_name}
                aria-label={`Open archive detail: ${archive.archive_name}`}
                className={`h-4 w-4 rounded ${archiveTone}`}
              />
            ))}
          </div>
        ) : null}
      </div>
    </li>
  );
}

function ArchiveDetailModal({
  stageDisplayName,
  archive,
  onClose,
}: {
  stageDisplayName: string;
  archive: BootstrapTimelineArchiveResponse;
  onClose: () => void;
}) {
  const skipEntries = Object.entries(archive.rows_skipped_by_reason);
  return (
    <Modal isOpen={true} onRequestClose={onClose} labelledBy="archive-detail-title">
      <h2
        id="archive-detail-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        {archive.archive_name}
      </h2>
      <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
        {stageDisplayName}
      </p>
      <dl className="mt-3 grid grid-cols-2 gap-2 text-sm">
        <dt className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Rows written
        </dt>
        <dd className="tabular-nums text-slate-700 dark:text-slate-200">
          {archive.rows_written.toLocaleString()}
        </dd>
        <dt className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Completed
        </dt>
        <dd className="text-slate-700 dark:text-slate-200">
          {formatDateTime(archive.completed_at)}
        </dd>
      </dl>
      <div className="mt-3">
        <h3 className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Rows skipped by reason
        </h3>
        {skipEntries.length === 0 ? (
          <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
            None.
          </p>
        ) : (
          <ul className="mt-1 space-y-0.5 text-sm">
            {skipEntries.map(([reason, count]) => (
              <li
                key={reason}
                className="flex justify-between font-mono text-xs text-slate-700 dark:text-slate-200"
              >
                <span>{reason}</span>
                <span className="tabular-nums">{count.toLocaleString()}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="mt-4 flex justify-end">
        <button
          type="button"
          onClick={onClose}
          className="rounded border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800/40"
        >
          Close
        </button>
      </div>
    </Modal>
  );
}

// ---------------------------------------------------------------------------
// Confirm dialogs (mirror ProcessesTable's contracts)
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
  // PR3a #1064 — bootstrap mechanism uses different verbs.
  // Operator 2026-05-22 (#1264): type-to-confirm gate dropped —
  // process names are internal identifiers, double-confirm with a
  // single click on the red verb button is the operator-locked UX.
  const isBootstrap = row.mechanism === "bootstrap";
  // First install (#1432): non-destructive "Start bootstrap" framing +
  // blue confirm, mirroring the table-row modal.
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
  return (
    <Modal isOpen={true} onRequestClose={onCancel} labelledBy="detail-fw-title">
      <h2
        id="detail-fw-title"
        className="text-sm font-semibold text-slate-800 dark:text-slate-100"
      >
        {heading}
      </h2>
      <p className="mt-2 text-sm text-slate-700 dark:text-slate-300">
        {isFirstRun ? (
          <>
            Start the first-install bootstrap for{" "}
            <span className="font-medium">{row.display_name}</span>. Walks the
            init → eToro → SEC stage sequence to populate the tradable universe
            and filings. Safe to run — this is the first run, nothing is
            overwritten.
          </>
        ) : isBootstrap ? (
          <>
            Re-run all resets every stage of{" "}
            <span className="font-medium">{row.display_name}</span> to pending
            and replays the full first-install bootstrap. Stages re-run from
            scratch; ingested rows are deduped at the destination by ON
            CONFLICT.
          </>
        ) : (
          <>
            Full-wash resets the watermark for{" "}
            <span className="font-medium">{row.display_name}</span> and
            re-fetches from epoch.
          </>
        )}
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
  onConfirm: (mode: CancelMode) => void;
}) {
  // Codex pre-push BLOCKING: a closed `<details>` keeps the terminate
  // `<button>` tabbable in the DOM. The Modal focus trap walks the
  // dialog subtree on open and lands on the first tabbable, which
  // would be the hidden destructive button. Render terminate only
  // when the operator has explicitly opened the More disclosure.
  const [moreOpen, setMoreOpen] = useState(false);
  return (
    <Modal isOpen={true} onRequestClose={onCancel} labelledBy="detail-cancel-title">
      <h2
        id="detail-cancel-title"
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

// Re-export so tests reuse the canonical mapping.
export { REASON_TOOLTIP };
