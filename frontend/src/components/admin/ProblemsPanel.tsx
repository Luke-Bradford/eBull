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
  CredentialHealthSummary,
  ExitProtectionResponse,
  ProcessListResponse,
  SecretMissingItem,
  SyncLayersV2Response,
} from "@/api/types";
import { formatDateTime } from "@/lib/format";
import { steadyStateAttentionRows } from "@/lib/processHealth";


export interface ProblemsPanelProps {
  /** v2 payload. Null on first mount + while refetch is in flight. */
  readonly v2: SyncLayersV2Response | null;
  /**
   * Processes catalogue (#1959). Steady-state rows with
   * `health_verdict === "attention"` surface here as failing processes.
   * This replaced the legacy `/system/jobs` source, which omitted
   * `ingest_sweep` processes (e.g. `nport_sweep`) and so under-counted
   * problems relative to the Processes control-hub below.
   */
  readonly processes: ProcessListResponse | null;
  /** Coverage payload (unchanged from v1; null_rows still surface here). */
  readonly coverage: CoverageSummaryResponse | null;
  /**
   * Operator credential health (#979 / #974/E). When state==='rejected'
   * the panel surfaces a single "Credentials rejected" banner item
   * with a Settings link — the orchestrator gate already PREREQ_SKIPs
   * the affected layers, so without this banner the operator would
   * see no problems at all even though the system is gated.
   *
   * Optional + nullable so existing tests that pre-date #979 don't
   * have to thread a value through; the banner only renders when
   * state==='rejected'.
   */
  readonly credentialHealth?: CredentialHealthSummary | null;
  /**
   * Fixed-exit protection per engine-held position (#3284 item 4b).
   *
   * ⚠ A SAFETY-STATE INDICATOR under `safety-state-ui.md`: "a position is carrying
   * no working stop" is precisely a state whose disappearance could let the operator
   * make a wrong-state decision. So it is cached-and-ORed like the panel's other
   * sources and rendered from the last CONFIRMED snapshot, with a visible stale
   * marker when the live value is absent — a silent cache that looks live is worse
   * than no cache.
   *
   * ⚠ Deliberately NOT counted in `pendingSources` / `erroredSources`. Those drive
   * the "Checking N more sources…" header and the 3-source `allPending` short
   * circuit; widening them would change when the whole panel suppresses itself,
   * which is a behaviour change to every existing problem class. The row carries
   * its own staleness instead.
   *
   * Optional + nullable so tests predating #3284 need not thread a value through.
   */
  readonly exitProtection?: readonly ExitProtectionResponse[] | null;
  /** True when the /system/status fetch errored — marks a cached row stale. */
  readonly exitProtectionError?: boolean;
  readonly v2Error: boolean;
  readonly processesError: boolean;
  readonly coverageError: boolean;
  /** Called with the root layer name when the operator clicks drill-through. */
  readonly onOpenOrchestrator: (layerName: string) => void;
}


/** Synthetic ActionNeededItem injected when credentialHealth.state === 'rejected'.
 *
 * The orchestrator gate (#977) PREREQ_SKIPs credential-using layers
 * when the operator's aggregate health is REJECTED. Without this
 * synthetic banner item, action_needed would be empty and the
 * operator would see no problems despite the system being fully
 * gated. The banner gives them the actionable "go fix Settings".
 */
const CREDENTIAL_REJECTED_BANNER: ActionNeededItem = {
  root_layer: "_credential_health",
  display_name: "Credentials rejected by provider",
  category: "auth_expired",
  operator_message:
    "eToro rejected your credentials. The orchestrator has paused all credential-using layers until you save valid keys.",
  operator_fix: "Update the public key in Settings → Providers",
  self_heal: false,
  consecutive_failures: 0,
  affected_downstream: [],
  error_excerpt: null,
};


interface SourceCache {
  v2: SyncLayersV2Response | null;
  processes: ProcessListResponse | null;
  coverage: CoverageSummaryResponse | null;
  /** #3284 item 4b — last CONFIRMED exit-protection snapshot (see the prop docs). */
  exitProtection: readonly ExitProtectionResponse[] | null;
}


function mentionsSettings(text: string): boolean {
  // Match the canonical remedy phrasings emitted by the backend REMEDIES
  // table — e.g. "Set X in Settings → Providers", "Update the public key in
  // Settings". Narrow enough that a remedy mentioning settings/providers
  // in passing (e.g. "nothing to do with Settings — inspect the row
  // manually") stays plain text rather than becoming a misleading link.
  return /\bin\s+Settings\b/i.test(text) || /\bSettings\s*[→>]\s*Providers\b/i.test(text);
}


export function ProblemsPanel({
  v2,
  processes,
  coverage,
  credentialHealth,
  exitProtection,
  exitProtectionError = false,
  v2Error,
  processesError,
  coverageError,
  onOpenOrchestrator,
}: ProblemsPanelProps): JSX.Element | null {
  const [cache, setCache] = useState<SourceCache>({
    v2: null,
    processes: null,
    coverage: null,
    exitProtection: null,
  });

  useEffect(() => {
    if (v2 !== null) setCache((prev) => ({ ...prev, v2 }));
  }, [v2]);
  useEffect(() => {
    if (processes !== null) setCache((prev) => ({ ...prev, processes }));
  }, [processes]);
  useEffect(() => {
    if (coverage !== null) setCache((prev) => ({ ...prev, coverage }));
  }, [coverage]);
  // ⚠ Clear-on-positive (safety-state-ui.md): only a non-null response updates the
  // cache, so a refetch or an error leaves the last confirmed verdict standing. An
  // EMPTY array is a real answer ("no engine-held position is unprotected"), not an
  // absence, so it DOES clear — `undefined`/`null` are the only "we do not know" values.
  //
  // ⚠⚠ ...and so is an `error` SENTINEL, which is the subtler half. `check_exit_protection`
  // contains its own failures and returns `[{status: "error", ...}]` inside a successful
  // HTTP 200, so `exitProtectionError` stays FALSE on a failed probe. Caching that payload
  // would overwrite a confirmed list of unprotected position IDs with a row that names
  // none of them — a query hiccup silently erasing the safety alert it was supposed to
  // report. The sentinel is surfaced separately (`probeErrors` below) and never cached.
  useEffect(() => {
    if (exitProtection === null || exitProtection === undefined) return;
    if (exitProtection.some((entry) => entry.status === "error")) return;
    setCache((prev) => ({ ...prev, exitProtection }));
  }, [exitProtection]);

  const pendingSources: string[] = [];
  if (cache.v2 === null) pendingSources.push("layers");
  if (cache.processes === null) pendingSources.push("processes");
  if (cache.coverage === null) pendingSources.push("coverage");

  const erroredSources: string[] = [];
  if (v2Error && cache.v2 !== null) erroredSources.push("layers");
  if (processesError && cache.processes !== null) erroredSources.push("processes");
  if (coverageError && cache.coverage !== null) erroredSources.push("coverage");

  const allPending = pendingSources.length === 3;
  if (allPending) {
    return (
      <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-600 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-300">
        Checking for problems…
      </div>
    );
  }

  const baseActionNeeded = cache.v2?.action_needed ?? [];
  const secretMissing = cache.v2?.secret_missing ?? [];
  // #1959 — count failing PROCESSES from the same catalogue + predicate the
  // control-hub "N need attention" uses (steady-state rows with
  // `health_verdict === "attention"`; see `steadyStateAttentionRows`). This
  // replaced the legacy `/system/jobs` source, which omitted `ingest_sweep`
  // processes (e.g. `nport_sweep`) so the top banner under-counted vs the
  // Processes section below.
  // #1689 — `attention` is the COMPUTED verdict: a retrying (`self_healing`),
  // aged one-shot (`stale_manual`), kill-switch-disabled (`paused`, #1831),
  // `working`, or `current` row is NOT a problem, so a transient/reaped
  // failure does not raise a false red banner.
  const failingProcesses = steadyStateAttentionRows(cache.processes?.rows ?? []);
  const coverageNullRows = cache.coverage?.null_rows ?? 0;

  // #3284 item 4b — positions whose stop repair has refused for its whole class
  // budget. `unrepairable` is the only alerting status by design: `repairing` is the
  // expected transient and `never_checked` is a position opened seconds ago, and an
  // alarm that fires on either would ship with a documented "ignore this" attached.
  const unprotectedPositions = (cache.exitProtection ?? []).filter((entry) => entry.status === "unrepairable");
  // Read from the LIVE payload, never the cache: an unreadable probe is a fact about
  // the current response, and a cached one would keep claiming the check is broken
  // after it recovered.
  const probeErrors = (exitProtection ?? []).filter((entry) => entry.status === "error");
  // Stale when the cached rows are being rendered without a live confirmation —
  // absent response, transport error, or a contained probe failure.
  const exitProtectionStale =
    exitProtection === null || exitProtection === undefined || exitProtectionError || probeErrors.length > 0;

  // Inject the credential-rejected banner when the operator's aggregate
  // health is REJECTED. Backend already PREREQ_SKIPs affected layers
  // (#977) and AUTH_EXPIRED suppression hides stale rows post-recovery,
  // so without this synthetic item action_needed would be empty even
  // though the system is fully gated. Prepended so it's visually first.
  const showCredRejectedBanner = credentialHealth?.state === "rejected";
  const actionNeeded: ActionNeededItem[] = showCredRejectedBanner
    ? [CREDENTIAL_REJECTED_BANNER, ...baseActionNeeded]
    : baseActionNeeded;

  const totalProblems =
    actionNeeded.length +
    secretMissing.length +
    failingProcesses.length +
    unprotectedPositions.length +
    probeErrors.length +
    (coverageNullRows > 0 ? 1 : 0);

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
      ? "border-red-200 bg-red-50 dark:border-red-900/60 dark:bg-red-950/30"
      : tone === "amber"
        ? "border-amber-200 bg-amber-50 dark:border-amber-900/60 dark:bg-amber-950/30"
        : "border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-slate-900/40";
  const headerTone =
    tone === "red"
      ? "border-red-200 text-red-800 dark:border-red-900/60 dark:text-red-300"
      : tone === "amber"
        ? "border-amber-200 text-amber-800 dark:border-amber-900/60 dark:text-amber-300"
        : "border-slate-200 text-slate-700 dark:border-slate-800 dark:text-slate-200";

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
        {/* FIRST in the list deliberately: an engine-held position with no working
            stop is the operator's standing mandate broken, which outranks a failing
            ingest layer. */}
        {unprotectedPositions.map((entry) => (
          <UnprotectedPositionRow key={`exit-protection-${entry.ownership_id}`} entry={entry} stale={exitProtectionStale} />
        ))}
        {probeErrors.map((entry, index) => (
          <ExitProtectionUnreadableRow key={`exit-protection-error-${index}`} detail={entry.detail} />
        ))}
        {actionNeeded.map((item) => (
          <ActionNeededRow key={item.root_layer} item={item} onOpen={() => onOpenOrchestrator(item.root_layer)} />
        ))}
        {secretMissing.map((item) => (
          <SecretMissingRow key={item.layer} item={item} />
        ))}
        {failingProcesses.map((proc) => {
          const label = proc.display_name || proc.process_id;
          const failedAt = proc.last_run?.finished_at ?? null;
          return (
            <li key={`process-${proc.process_id}`} className="px-4 py-2 text-sm">
              <div className="flex items-start gap-2">
                <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500" />
                <div className="flex-1">
                  <div className="font-medium text-red-800">
                    {label} — {proc.verdict_reason || "needs attention"}
                  </div>
                  {failedAt !== null ? (
                    <div className="text-xs text-slate-600">Failed at {formatDateTime(failedAt)}</div>
                  ) : null}
                  <div className="text-xs text-slate-600">
                    Clears when the next run of {label} succeeds.
                  </div>
                </div>
                <Link
                  to={`/admin/processes/${encodeURIComponent(proc.process_id)}`}
                  className="shrink-0 text-xs font-medium text-blue-700 hover:underline"
                  aria-label={`View runs for ${label}`}
                >
                  View runs →
                </Link>
              </div>
            </li>
          );
        })}
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


/** One engine-held position whose stop repair has refused for its whole budget (#3284 item 4b).
 *
 * ⚠ The stale marker is NOT optional (`safety-state-ui.md`): when the row is rendered
 * from cache the operator must be told the indicator is real but unverified. A silent
 * cache that looks live is worse than no cache at all.
 */
function UnprotectedPositionRow({
  entry,
  stale,
}: {
  entry: ExitProtectionResponse;
  stale: boolean;
}): JSX.Element {
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500" />
        <div className="flex-1">
          {/* ⚠ "exit levels", not "no stop". An `unrepairable` verdict proves
              `_exit_intent.has_gap`, which is `stop_gap OR take_gap` — a position with a
              perfectly good stop and a missing TAKE-PROFIT reaches this row, and telling
              the operator its stop is gone would send them to check something that is
              fine. The streak table does not record WHICH half gapped (`sql/408` stores
              the refusal reason, not the intent), so the honest wording is the one that
              covers both. Carrying the gap type through would be a schema change. */}
          <div className="font-medium text-red-800 dark:text-red-300">
            Position {entry.broker_position_id}: the broker-side exit levels are not in place
            {stale ? (
              <span
                className="ml-2 text-[10px] font-normal uppercase text-red-600 dark:text-red-400"
                role="status"
              >
                (stale — refreshing)
              </span>
            ) : null}
          </div>
          <div className="text-xs text-slate-700 dark:text-slate-300">
            The stop/target repair has refused {entry.consecutive_refusals} consecutive{" "}
            {entry.consecutive_refusals === 1 ? "visit" : "visits"} against a budget of {entry.refusal_budget}
            {entry.last_refusal_reason !== null ? ` — ${entry.last_refusal_reason}` : ""}.
          </div>
          {entry.first_refused_at !== null ? (
            <div className="text-xs text-slate-600 dark:text-slate-400">
              Refusing since {formatDateTime(entry.first_refused_at)}
            </div>
          ) : null}
          {entry.last_checked_at !== null ? (
            <div className="text-xs text-slate-600 dark:text-slate-400">
              Last checked {formatDateTime(entry.last_checked_at)}
            </div>
          ) : null}
          <div className="text-xs text-slate-600 dark:text-slate-400">
            Clears when the five-minute strategy cycle lands the stop and target at the broker. If the count keeps
            rising, set them by hand on position {entry.broker_position_id} at the broker.
          </div>
        </div>
      </div>
    </li>
  );
}


/** The contained-failure sentinel from `check_exit_protection` (#3284 item 4b).
 *
 * ⚠ Its own row, never folded into the cached position list: the sentinel names no
 * position, so caching it would replace a confirmed list of unprotected IDs with a row
 * that identifies none of them. "The safety check is unreadable" and "this position is
 * unprotected" are different facts and are reported as such.
 */
function ExitProtectionUnreadableRow({ detail }: { detail: string | null }): JSX.Element {
  return (
    <li className="px-4 py-2 text-sm">
      <div className="flex items-start gap-2">
        <span aria-hidden className="mt-1 inline-block h-2 w-2 rounded-full bg-red-500" />
        <div className="flex-1">
          <div className="font-medium text-red-800 dark:text-red-300">
            Exit protection could not be checked
          </div>
          <div className="text-xs text-slate-600 dark:text-slate-400">{detail ?? "see server logs"}</div>
          <div className="text-xs text-slate-600 dark:text-slate-400">
            Any position rows above are the last confirmed verdict, not a live one.
          </div>
        </div>
      </div>
    </li>
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
          {item.error_excerpt !== null && item.error_excerpt !== undefined ? (
            <div
              className="mt-0.5 truncate font-mono text-xs text-red-700"
              title={item.error_excerpt}
              data-testid="problems-error-excerpt"
            >
              {item.error_excerpt}
            </div>
          ) : null}
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
