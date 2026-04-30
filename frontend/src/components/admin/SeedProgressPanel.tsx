/**
 * SEC ingest seed progress + per-CIK timing + operator pause toggle.
 *
 * Backed by three endpoints from PR #423:
 *   - GET /sync/ingest/seed_progress   — seeded / total, latest run, pause flag
 *   - GET /sync/ingest/cik_timing/latest — p50/p95 + top-5 slowest
 *   - POST /sync/ingest/{key}/enabled   — operator pause switch
 *
 * Polling cadence:
 *   - 10s while the latest run is 'running'
 *   - 60s otherwise (idle / success / failed)
 *
 * This panel is the #414 design-goal-G and #418-acceptance surface:
 * operator can see seed progress, runtime throughput regression, and
 * pause state at a glance without tailing logs.
 */

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  fetchCikTimingLatest,
  fetchSeedProgress,
  setIngestEnabled,
} from "@/api/sync";
import type {
  CikTimingSummaryResponse,
  SeedProgressResponse,
} from "@/api/sync";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { formatDateTime } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

const FUNDAMENTALS_INGEST_KEY = "fundamentals_ingest";

function formatSeconds(s: number | null): string {
  if (s === null || Number.isNaN(s)) return "—";
  if (s < 1) return `${(s * 1000).toFixed(0)} ms`;
  if (s < 60) return `${s.toFixed(2)} s`;
  const mins = Math.floor(s / 60);
  const rem = Math.round(s - mins * 60);
  return `${mins}m ${rem}s`;
}

function formatPct(seeded: number, total: number): string {
  if (total === 0) return "—";
  const pct = (seeded / total) * 100;
  return `${pct.toFixed(1)}%`;
}

export function SeedProgressPanel() {
  const seedState = useAsync(fetchSeedProgress, []);
  const timingState = useAsync(fetchCikTimingLatest, []);
  const [toggleBusy, setToggleBusy] = useState(false);
  const [toggleError, setToggleError] = useState<string | null>(null);

  const latestStatus = seedState.data?.latest_run?.status ?? null;
  const isRunning = latestStatus === "running";
  const pollInterval = isRunning ? 10_000 : 60_000;

  const refetchAll = useCallback(() => {
    seedState.refetch();
    timingState.refetch();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [seedState.refetch, timingState.refetch]);

  useEffect(() => {
    const id = window.setInterval(refetchAll, pollInterval);
    return () => window.clearInterval(id);
  }, [refetchAll, pollInterval]);

  const seed = seedState.data;
  const timing = timingState.data;

  const handleTogglePause = useCallback(async () => {
    if (seed === null) return;
    setToggleBusy(true);
    setToggleError(null);
    try {
      await setIngestEnabled(FUNDAMENTALS_INGEST_KEY, seed.ingest_paused);
      // Refetch both sibling states — the toggle does not change
      // timing data directly, but a subsequent run under the new
      // state will, and leaving the timing card stale for up to
      // 60 s of idle polling is operator-confusing.
      seedState.refetch();
      timingState.refetch();
    } catch (err) {
      setToggleError(err instanceof Error ? err.message : "Toggle failed");
    } finally {
      setToggleBusy(false);
    }
  }, [seed, seedState, timingState]);

  const toggleLabel = useMemo(() => {
    if (toggleBusy) return "Updating…";
    return seed?.ingest_paused ? "Resume ingest" : "Pause ingest";
  }, [toggleBusy, seed?.ingest_paused]);

  return (
    <Section
      title="SEC ingest progress"
      action={
        <button
          type="button"
          onClick={handleTogglePause}
          disabled={toggleBusy || seed === null}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
        >
          {toggleLabel}
        </button>
      }
    >
      {seedState.loading ? (
        <SectionSkeleton rows={3} />
      ) : seedState.error !== null || seed === null ? (
        <SectionError onRetry={seedState.refetch} />
      ) : (
        <div className="space-y-4">
          {toggleError !== null && (
            <div
              role="alert"
              className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700"
            >
              {toggleError}
            </div>
          )}

          {seed.ingest_paused && (
            <div
              role="status"
              className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800"
            >
              Ingest is paused — scheduled runs write <code>status='skipped'</code> until
              resumed.
            </div>
          )}

          {/* Per-source progress bars */}
          <div className="space-y-3">
            {seed.sources.map((src) => {
              const pct = src.total === 0 ? 0 : Math.min(100, (src.seeded / src.total) * 100);
              return (
                <div key={src.source}>
                  <div className="flex items-center justify-between text-xs text-slate-600">
                    <span className="font-medium text-slate-700">{src.key_description}</span>
                    <span>
                      {src.seeded.toLocaleString()} / {src.total.toLocaleString()} ·{" "}
                      {formatPct(src.seeded, src.total)}
                    </span>
                  </div>
                  <div className="mt-1 h-2 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800">
                    <div
                      className="h-full rounded-full bg-sky-500 transition-all"
                      style={{ width: `${pct}%` }}
                      role="progressbar"
                      aria-valuenow={Math.round(pct)}
                      aria-valuemin={0}
                      aria-valuemax={100}
                      aria-label={`${src.key_description} seed progress`}
                    />
                  </div>
                </div>
              );
            })}
          </div>

          {/* Latest run card */}
          {seed.latest_run !== null && (
            <LatestRunRow run={seed.latest_run} />
          )}

          {/* Timing percentiles */}
          <TimingSection timingState={timingState} data={timing} />
        </div>
      )}
    </Section>
  );
}

function LatestRunRow({
  run,
}: {
  run: NonNullable<SeedProgressResponse["latest_run"]>;
}) {
  const tone =
    run.status === "running"
      ? "text-sky-600"
      : run.status === "success"
        ? "text-emerald-600"
        : run.status === "partial"
          ? "text-amber-600"
          : "text-red-600";
  return (
    <div className="rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs dark:border-slate-800 dark:bg-slate-900/40">
      <div className="flex items-center justify-between">
        <span className="font-medium text-slate-700 dark:text-slate-200">Latest run #{run.ingestion_run_id}</span>
        <span className={`font-semibold ${tone}`}>{run.status}</span>
      </div>
      <div className="mt-1 grid grid-cols-2 gap-x-4 gap-y-1 text-slate-600 sm:grid-cols-4">
        <div>
          <span className="text-slate-500 dark:text-slate-400">Started </span>
          {formatDateTime(run.started_at)}
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400">Finished </span>
          {run.finished_at ? formatDateTime(run.finished_at) : "—"}
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400">Rows upserted </span>
          {run.rows_upserted.toLocaleString()}
        </div>
        <div>
          <span className="text-slate-500 dark:text-slate-400">Rows skipped </span>
          {run.rows_skipped.toLocaleString()}
        </div>
      </div>
    </div>
  );
}

function TimingSection({
  timingState,
  data,
}: {
  timingState: ReturnType<typeof useAsync<CikTimingSummaryResponse>>;
  data: CikTimingSummaryResponse | null;
}) {
  if (timingState.loading) return <SectionSkeleton rows={2} />;
  if (timingState.error !== null) {
    return <SectionError onRetry={timingState.refetch} />;
  }
  if (data === null || data.ingestion_run_id === null) {
    return (
      <p className="text-xs text-slate-500 dark:text-slate-400">
        Per-CIK timing will appear here after the next SEC ingest run.
      </p>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Per-CIK timing · run #{data.ingestion_run_id}
        </h3>
      </div>
      <table className="w-full text-xs">
        <thead className="text-left text-[11px] uppercase text-slate-500 dark:text-slate-400">
          <tr>
            <th className="py-1">Mode</th>
            <th>Count</th>
            <th>p50</th>
            <th>p95</th>
            <th>Max</th>
            <th>Facts upserted</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {data.modes.map((m) => (
            <tr key={m.mode}>
              <td className="py-1 font-medium text-slate-700">{m.mode}</td>
              <td>{m.count.toLocaleString()}</td>
              <td>{formatSeconds(m.p50_seconds)}</td>
              <td>{formatSeconds(m.p95_seconds)}</td>
              <td>{formatSeconds(m.max_seconds)}</td>
              <td>{m.facts_upserted_total.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {data.slowest.length > 0 && (
        <details className="mt-2 text-xs">
          <summary className="cursor-pointer text-slate-600">
            Slowest {data.slowest.length} CIKs
          </summary>
          <table className="mt-2 w-full">
            <thead className="text-left text-[11px] uppercase text-slate-500 dark:text-slate-400">
              <tr>
                <th className="py-1">CIK</th>
                <th>Mode</th>
                <th>Seconds</th>
                <th>Outcome</th>
                <th>Facts</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {data.slowest.map((s) => (
                <tr key={`${s.cik}-${s.mode}-${s.finished_at}`}>
                  <td className="py-1 font-mono text-slate-700">{s.cik}</td>
                  <td>{s.mode}</td>
                  <td>{formatSeconds(s.seconds)}</td>
                  <td>{s.outcome}</td>
                  <td>{s.facts_upserted.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}
    </div>
  );
}
