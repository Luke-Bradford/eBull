/**
 * Problems triage panel (AdminPage #323, spec §5 + §8).
 *
 * Surfaces layer failures, failing jobs, and coverage anomalies so
 * operator sees "what's broken" without scrolling. Sources:
 *   - /sync/layers: blocking problems from consecutive_failures /
 *     last_error_category; stale-blocking / stale-non-blocking from
 *     is_fresh + is_blocking.
 *   - /system/jobs: `last_status === "failure"` only. Skipped jobs are
 *     noise filtered out (retry_deferred_recommendations and friends
 *     skip routinely with "no work to do").
 *   - /coverage/summary: null_rows > 0 surfaces a data-audit row.
 *
 * Cache contract (spec §8): per-source snapshots that only overwrite
 * on a non-null fresh value. A refetch-in-flight source keeps
 * rendering last-good problems. First mount shows a neutral
 * "Checking for problems…" banner until every source resolves at
 * least once.
 */
import { useEffect, useState } from "react";

import type {
  CoverageSummaryResponse,
  JobsListResponse,
} from "@/api/types";
import type { SyncLayer, SyncLayersResponse } from "@/api/sync";
import { formatDateTime } from "@/lib/format";

export type ProblemTone = "red" | "amber";

export interface Problem {
  readonly id: string;
  readonly tone: ProblemTone;
  readonly title: string;
  readonly detail: string | null;
  readonly action?: { readonly label: string; readonly onClick: () => void };
}

export interface ProblemsPanelProps {
  /** Live values. Null on first mount + while a refetch is in flight. */
  readonly layers: SyncLayersResponse | null;
  readonly jobs: JobsListResponse | null;
  readonly coverage: CoverageSummaryResponse | null;
  /** Per-source error flags — true when the latest fetch returned an error.
   *  A cached last-good snapshot still renders; we surface an amber
   *  "could not re-check" line at the top of the panel so the operator
   *  knows the displayed problems may be out of date. */
  readonly layersError: boolean;
  readonly jobsError: boolean;
  readonly coverageError: boolean;
  /** Click-through from a layer problem to the orchestrator collapsible. */
  readonly onOpenOrchestrator: () => void;
}

interface SourceCache {
  layers: Problem[] | null;
  jobs: Problem[] | null;
  coverage: Problem[] | null;
}

export function ProblemsPanel({
  layers,
  jobs,
  coverage,
  layersError,
  jobsError,
  coverageError,
  onOpenOrchestrator,
}: ProblemsPanelProps): JSX.Element | null {
  const [cache, setCache] = useState<SourceCache>({
    layers: null,
    jobs: null,
    coverage: null,
  });

  // Per-source cached snapshot: only overwrite when a non-null fresh
  // value arrives. A refetch-in-flight (value === null) leaves the
  // cached snapshot untouched — last-good rendering, never false-good
  // flash (spec §8).
  useEffect(() => {
    if (layers !== null) {
      setCache((prev) => ({ ...prev, layers: deriveLayerProblems(layers, onOpenOrchestrator) }));
    }
  }, [layers, onOpenOrchestrator]);

  useEffect(() => {
    if (jobs !== null) {
      setCache((prev) => ({ ...prev, jobs: deriveJobProblems(jobs) }));
    }
  }, [jobs]);

  useEffect(() => {
    if (coverage !== null) {
      setCache((prev) => ({ ...prev, coverage: deriveCoverageProblems(coverage) }));
    }
  }, [coverage]);

  // Combined problem list — only sources that have resolved at least
  // once contribute. A source still at `null` is excluded rather than
  // defaulting to empty; it will join the list once it resolves. We
  // ALSO show a "still checking N more source(s)" line when some
  // sources have not yet resolved — so a slow/hung /coverage cannot
  // mask live layer or job problems (spec §8).
  const problems: Problem[] = [
    ...(cache.layers ?? []),
    ...(cache.jobs ?? []),
    ...(cache.coverage ?? []),
  ];

  const pendingSources: string[] = [];
  if (cache.layers === null) pendingSources.push("layers");
  if (cache.jobs === null) pendingSources.push("jobs");
  if (cache.coverage === null) pendingSources.push("coverage");

  // Errored sources that DO have a cached snapshot from a previous
  // successful fetch. Surfaced as an amber header line so the
  // operator knows the cached problems may be stale.
  const erroredSources: string[] = [];
  if (layersError && cache.layers !== null) erroredSources.push("layers");
  if (jobsError && cache.jobs !== null) erroredSources.push("jobs");
  if (coverageError && cache.coverage !== null) erroredSources.push("coverage");

  const allPending = pendingSources.length === 3;

  if (allPending) {
    return (
      <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-600">
        Checking for problems…
      </div>
    );
  }

  if (
    problems.length === 0 &&
    pendingSources.length === 0 &&
    erroredSources.length === 0
  ) {
    // All sources resolved clean, no problems. Hidden state.
    return null;
  }

  // Otherwise: render resolved-source problems + secondary lines
  // for pending and errored sources so the operator knows the panel
  // is not fully live.
  return renderPanel(problems, pendingSources, erroredSources);
}

function renderPanel(
  problems: Problem[],
  pendingSources: string[],
  erroredSources: string[],
): JSX.Element {

  return (
    <section
      role="region"
      aria-label="Current problems"
      className="rounded-md border border-red-200 bg-red-50 shadow-sm"
    >
      <header className="flex items-center justify-between border-b border-red-200 px-4 py-2 text-sm font-semibold text-red-800">
        <span>
          {problems.length} problem{problems.length === 1 ? "" : "s"} need
          {problems.length === 1 ? "s" : ""} attention
        </span>
        <span className="flex items-center gap-3 text-xs font-normal">
          {pendingSources.length > 0 ? (
            <span className="text-slate-600">
              Checking {pendingSources.length} more source
              {pendingSources.length === 1 ? "" : "s"}…
            </span>
          ) : null}
          {erroredSources.length > 0 ? (
            <span className="text-amber-700" role="status">
              Could not re-check {erroredSources.join(", ")} — using last known state
            </span>
          ) : null}
        </span>
      </header>
      {problems.length === 0 ? (
        <p className="px-4 py-2 text-sm text-slate-600">
          No problems from resolved sources yet.
        </p>
      ) : (
        <ul className="divide-y divide-red-100">
          {problems.map((p) => (
            <li key={p.id} className="px-4 py-2 text-sm">
              <div className="flex items-start gap-2">
                <span
                  aria-hidden
                  className={`mt-1 inline-block h-2 w-2 rounded-full ${p.tone === "red" ? "bg-red-500" : "bg-amber-500"}`}
                />
                <div className="flex-1">
                  <div
                    className={`font-medium ${p.tone === "red" ? "text-red-800" : "text-amber-800"}`}
                  >
                    {p.title}
                  </div>
                  {p.detail !== null ? (
                    <div className="text-xs text-slate-600">{p.detail}</div>
                  ) : null}
                </div>
                {p.action ? (
                  <button
                    type="button"
                    onClick={p.action.onClick}
                    className="shrink-0 text-xs font-medium text-blue-700 hover:underline"
                  >
                    {p.action.label} →
                  </button>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function deriveLayerProblems(
  layers: SyncLayersResponse,
  onOpenOrchestrator: () => void,
): Problem[] {
  const out: Problem[] = [];
  for (const layer of layers.layers) {
    const p = classifyLayer(layer, onOpenOrchestrator);
    if (p !== null) out.push(p);
  }
  return out;
}

function classifyLayer(
  layer: SyncLayer,
  onOpenOrchestrator: () => void,
): Problem | null {
  const action = {
    label: "Open orchestrator details",
    onClick: onOpenOrchestrator,
  };
  // Red: explicit failure history (consecutive_failures >= 1 AND a
  // last_error_category we can show). Overrides stale classification.
  if (layer.consecutive_failures >= 1 && layer.last_error_category !== null) {
    return {
      id: `layer-fail-${layer.name}`,
      tone: "red",
      title: `${layer.display_name} — ${layer.consecutive_failures} consecutive failure${layer.consecutive_failures === 1 ? "" : "s"} (${layer.last_error_category})`,
      detail:
        layer.last_success_at !== null
          ? `Last success: ${formatDateTime(layer.last_success_at)}`
          : "Never succeeded",
      action,
    };
  }
  // Amber: stale blocking layer with no recorded failure category.
  if (!layer.is_fresh && layer.is_blocking) {
    return {
      id: `layer-stale-${layer.name}`,
      tone: "amber",
      title: `${layer.display_name} — stale`,
      detail: layer.freshness_detail,
      action,
    };
  }
  // Amber low-priority: stale non-blocking. Surfaces the signal but
  // the operator is not gated by it. Kept in the panel per spec §5;
  // layers that are permanently-a-bit-behind by design show up here
  // too — that's acceptable noise for a triage surface because the
  // alternative (silence) hides real regressions in the same class.
  if (!layer.is_fresh && !layer.is_blocking) {
    return {
      id: `layer-stale-nb-${layer.name}`,
      tone: "amber",
      title: `${layer.display_name} — stale (non-blocking)`,
      detail: layer.freshness_detail,
      action,
    };
  }
  return null;
}

function deriveJobProblems(jobs: JobsListResponse): Problem[] {
  const out: Problem[] = [];
  for (const job of jobs.jobs) {
    if (job.last_status !== "failure") continue;
    out.push({
      id: `job-fail-${job.name}`,
      tone: "red",
      title: `${job.name} — last run failed`,
      detail:
        job.last_finished_at !== null
          ? `Failed at ${formatDateTime(job.last_finished_at)}`
          : null,
    });
  }
  return out;
}

function deriveCoverageProblems(
  coverage: CoverageSummaryResponse,
): Problem[] {
  if (coverage.null_rows > 0) {
    return [
      {
        id: "coverage-null-rows",
        tone: "amber",
        title: `${coverage.null_rows} instrument${coverage.null_rows === 1 ? "" : "s"} have a NULL filings_status`,
        detail:
          "The filings-status audit job has not covered these instruments yet.",
      },
    ];
  }
  return [];
}
