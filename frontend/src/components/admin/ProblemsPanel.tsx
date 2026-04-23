/**
 * Problems triage panel — v2-backed (A.5 chunk 1).
 *
 * Consumes `/sync/layers/v2` for layer problems (action_needed +
 * secret_missing) while preserving the v1 jobs-failure and coverage
 * null-row carry-over from the original ProblemsPanel. Per-source
 * caches keep last-good snapshots across refetch-in-flight so a
 * transient null does not blank the red banner.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import type {
  ActionNeededItem,
  CoverageSummaryResponse,
  JobsListResponse,
  SecretMissingItem,
  SyncLayersV2Response,
} from "@/api/types";
import { formatDateTime } from "@/lib/format";


export interface ProblemsPanelProps {
  /** v2 payload. Null on first mount + while refetch is in flight. */
  readonly v2: SyncLayersV2Response | null;
  /** Jobs payload (unchanged from v1; failing jobs still surface here). */
  readonly jobs: JobsListResponse | null;
  /** Coverage payload (unchanged from v1; null_rows still surface here). */
  readonly coverage: CoverageSummaryResponse | null;
  readonly v2Error: boolean;
  readonly jobsError: boolean;
  readonly coverageError: boolean;
  /** Called with the root layer name when the operator clicks drill-through. */
  readonly onOpenOrchestrator: (layerName: string) => void;
}


interface SourceCache {
  v2: SyncLayersV2Response | null;
  jobs: JobsListResponse | null;
  coverage: CoverageSummaryResponse | null;
}


function mentionsSettings(text: string): boolean {
  // Match the canonical remedy phrasings emitted by the backend REMEDIES
  // table — e.g. "Set X in Settings → Providers", "Update the API key in
  // Settings". Narrow enough that a remedy mentioning settings/providers
  // in passing (e.g. "nothing to do with Settings — inspect the row
  // manually") stays plain text rather than becoming a misleading link.
  return /\bin\s+Settings\b/i.test(text) || /\bSettings\s*[→>]\s*Providers\b/i.test(text);
}


export function ProblemsPanel({
  v2,
  jobs,
  coverage,
  v2Error,
  jobsError,
  coverageError,
  onOpenOrchestrator,
}: ProblemsPanelProps): JSX.Element | null {
  const [cache, setCache] = useState<SourceCache>({ v2: null, jobs: null, coverage: null });

  useEffect(() => {
    if (v2 !== null) setCache((prev) => ({ ...prev, v2 }));
  }, [v2]);
  useEffect(() => {
    if (jobs !== null) setCache((prev) => ({ ...prev, jobs }));
  }, [jobs]);
  useEffect(() => {
    if (coverage !== null) setCache((prev) => ({ ...prev, coverage }));
  }, [coverage]);

  const pendingSources: string[] = [];
  if (cache.v2 === null) pendingSources.push("layers");
  if (cache.jobs === null) pendingSources.push("jobs");
  if (cache.coverage === null) pendingSources.push("coverage");

  const erroredSources: string[] = [];
  if (v2Error && cache.v2 !== null) erroredSources.push("layers");
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

  const actionNeeded = cache.v2?.action_needed ?? [];
  const secretMissing = cache.v2?.secret_missing ?? [];
  const failingJobs = (cache.jobs?.jobs ?? []).filter((j) => j.last_status === "failure");
  const coverageNullRows = cache.coverage?.null_rows ?? 0;

  const totalProblems = actionNeeded.length + secretMissing.length + failingJobs.length + (coverageNullRows > 0 ? 1 : 0);

  if (totalProblems === 0 && pendingSources.length === 0 && erroredSources.length === 0) {
    return null;
  }

  // `system_summary` from v2 only describes layer problems. If the total
  // problem count comes from carried-over jobs / coverage rows too, use
  // the combined count instead — otherwise a red panel with a failed
  // job shown underneath would be topped by "All layers healthy".
  const v2ProblemCount = actionNeeded.length + secretMissing.length;
  const headerText =
    totalProblems === v2ProblemCount && cache.v2?.system_summary !== undefined
      ? cache.v2.system_summary
      : totalProblems > 0
        ? `${totalProblems} problem(s) need attention`
        : "No confirmed problems yet";
  const tone = totalProblems > 0 ? "red" : erroredSources.length > 0 ? "amber" : "neutral";
  const sectionTone =
    tone === "red"
      ? "border-red-200 bg-red-50"
      : tone === "amber"
        ? "border-amber-200 bg-amber-50"
        : "border-slate-200 bg-slate-50";
  const headerTone =
    tone === "red"
      ? "border-red-200 text-red-800"
      : tone === "amber"
        ? "border-amber-200 text-amber-800"
        : "border-slate-200 text-slate-700";

  return (
    <section role="region" aria-label="Current problems" className={`rounded-md border shadow-sm ${sectionTone}`}>
      <header className={`flex items-center justify-between border-b px-4 py-2 text-sm font-semibold ${headerTone}`}>
        <span>{headerText}</span>
        <span className="flex items-center gap-3 text-xs font-normal">
          {pendingSources.length > 0 ? (
            <span className="text-slate-600">
              Checking {pendingSources.length} more source{pendingSources.length === 1 ? "" : "s"}…
            </span>
          ) : null}
          {erroredSources.length > 0 ? (
            <span className="text-amber-700" role="status">
              Could not re-check {erroredSources.join(", ")} — using last known state
            </span>
          ) : null}
        </span>
      </header>
      <ul className="divide-y divide-red-100">
        {actionNeeded.map((item) => (
          <ActionNeededRow key={item.root_layer} item={item} onOpen={() => onOpenOrchestrator(item.root_layer)} />
        ))}
        {secretMissing.map((item) => (
          <SecretMissingRow key={item.layer} item={item} />
        ))}
        {failingJobs.map((job) => (
          <li key={`job-${job.name}`} className="px-4 py-2 text-sm">
            <div className="flex items-start gap-2">
              <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500" />
              <div className="flex-1">
                <div className="font-medium text-red-800">{job.name} — last run failed</div>
                {job.last_finished_at !== null ? (
                  <div className="text-xs text-slate-600">Failed at {formatDateTime(job.last_finished_at)}</div>
                ) : null}
                <div className="text-xs text-slate-600">
                  Clears when the next run of {job.name} succeeds.
                </div>
              </div>
              <Link
                to={`/admin/jobs/${encodeURIComponent(job.name)}`}
                className="shrink-0 text-xs font-medium text-blue-700 hover:underline"
                aria-label={`View runs for ${job.name}`}
              >
                View runs →
              </Link>
            </div>
          </li>
        ))}
        {coverageNullRows > 0 ? (
          <li className="px-4 py-2 text-sm">
            <div className="flex items-start gap-2">
              <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-amber-500" />
              <div className="flex-1">
                <div className="font-medium text-amber-800">
                  {coverageNullRows} instrument{coverageNullRows === 1 ? "" : "s"} have a NULL filings_status
                </div>
                <div className="text-xs text-slate-600">The filings-status audit job has not covered these instruments yet.</div>
                <div className="text-xs text-slate-600">
                  Clears after the fundamentals/coverage audit tags these
                  instruments. If the count is not falling, the audit job
                  is stuck — check its last run.
                </div>
              </div>
            </div>
          </li>
        ) : null}
      </ul>
    </section>
  );
}


function ActionNeededRow({ item, onOpen }: { item: ActionNeededItem; onOpen: () => void }): JSX.Element {
  const [expanded, setExpanded] = useState(false);
  const fix = item.operator_fix;
  const fixAsLink = fix !== null && mentionsSettings(fix);
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500" />
        <div className="flex-1">
          <div className="font-medium text-red-800">
            {item.display_name} — {item.operator_message}
          </div>
          {fix !== null ? (
            <div className="text-xs text-slate-700">
              {fixAsLink ? (
                <Link to="/settings#providers" className="font-medium text-blue-700 hover:underline">
                  {fix}
                </Link>
              ) : (
                <span>{fix}</span>
              )}
            </div>
          ) : null}
          <div className="mt-1 text-xs text-slate-500">{item.consecutive_failures} consecutive failures</div>
          <div className="text-xs text-slate-600">
            Clears when the next run of {item.root_layer} succeeds.
          </div>
          {item.affected_downstream.length > 0 ? (
            <button
              type="button"
              onClick={() => setExpanded((v) => !v)}
              className="mt-1 text-xs text-red-700 hover:underline"
            >
              +{item.affected_downstream.length} layers waiting
            </button>
          ) : null}
          {expanded ? (
            <ul className="mt-1 list-disc pl-5 text-xs text-slate-600">
              {item.affected_downstream.map((name) => (
                <li key={name}>{name}</li>
              ))}
            </ul>
          ) : null}
        </div>
        <button
          type="button"
          onClick={onOpen}
          className="shrink-0 text-xs font-medium text-blue-700 hover:underline"
          aria-label={`Open orchestrator details for ${item.root_layer}`}
        >
          Open orchestrator details →
        </button>
      </div>
    </li>
  );
}


function SecretMissingRow({ item }: { item: SecretMissingItem }): JSX.Element {
  // Most secret_missing rows have a Settings → Providers fix. The
  // backend has a defensive fallback path ("Check layer secret
  // configuration") for layers with no declared secret_refs — apply
  // the same link heuristic ActionNeededRow uses so a generic fix
  // does not get a misleading Settings link.
  const fixAsLink = mentionsSettings(item.operator_fix);
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-amber-500" />
        <div className="flex-1">
          <div className="font-medium text-amber-800">{item.display_name} — credential needed</div>
          <div className="text-xs text-slate-700">
            {fixAsLink ? (
              <Link to="/settings#providers" className="font-medium text-blue-700 hover:underline">
                {item.operator_fix}
              </Link>
            ) : (
              <span>{item.operator_fix}</span>
            )}
          </div>
          <div className="text-xs text-slate-600">
            Clears when the credential is supplied in Settings → Providers.
          </div>
        </div>
      </div>
    </li>
  );
}
