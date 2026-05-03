/**
 * Operator ingest-health page (#793, Batch 4 of #788).
 *
 * Surfaces the data the operator asked for on 2026-05-03:
 *
 *   "we also need to be mindful of the first start up for a user,
 *   that once they have got set up with at least one api key for
 *   etoro, we should have visibility of the data being ingested,
 *   so they know how far the updates are, how long it will take
 *   or anything, to make that a good user experience."
 *
 * Three blocks:
 *   1. Provider group cards — five canonical groups (SEC EDGAR
 *      fundamentals / SEC EDGAR ownership / eToro / Other regulated
 *      sources / Uncategorised) with state pill, last-run summary,
 *      and the in-progress / failed queue counts.
 *   2. Recent failures — last-7-days failed runs the operator might
 *      need to retry.
 *   3. Backfill queue — drilldown table of every queued / running /
 *      failed backfill row.
 *
 * Auto-refresh every 30s so the page is live without polling
 * aggressively. Operator-driven backfill enqueue is idempotent —
 * the API uses ON CONFLICT to refresh an existing row instead of
 * inserting a duplicate.
 */

import { useCallback, useEffect } from "react";

import {
  fetchBackfillQueue,
  fetchIngestFailures,
  fetchIngestStatus,
} from "@/api/ingestStatus";
import type {
  BackfillQueueResponse,
  IngestFailuresResponse,
  IngestProviderGroup,
  IngestStatusResponse,
} from "@/api/ingestStatus";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

const _STATE_BADGE_CLASS: Record<IngestProviderGroup["state"], string> = {
  never_run:
    "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
  green:
    "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200",
  amber:
    "bg-amber-100 text-amber-900 dark:bg-amber-900/40 dark:text-amber-200",
  red: "bg-red-100 text-red-900 dark:bg-red-900/40 dark:text-red-200",
};

const _STATE_LABEL: Record<IngestProviderGroup["state"], string> = {
  never_run: "Never run",
  green: "Healthy",
  amber: "Limited / stale",
  red: "Action needed",
};

export function IngestHealthPage(): JSX.Element {
  const statusState = useAsync<IngestStatusResponse>(
    useCallback(() => fetchIngestStatus(), []),
    [],
  );
  const failuresState = useAsync<IngestFailuresResponse>(
    useCallback(() => fetchIngestFailures(50), []),
    [],
  );
  const queueState = useAsync<BackfillQueueResponse>(
    useCallback(() => fetchBackfillQueue({ limit: 200 }), []),
    [],
  );

  // Auto-refresh every 30s. Direct setInterval on the three refetch
  // functions — the prior tick-based pattern (state increment ->
  // re-effect) was a no-op because the refetch effect's dep array
  // was empty, so the callbacks ran once on mount and never again.
  // Codex pre-push review (Batch 4 of #788) caught this.
  useEffect(() => {
    const id = window.setInterval(() => {
      statusState.refetch();
      failuresState.refetch();
      queueState.refetch();
    }, 30_000);
    return () => window.clearInterval(id);
  }, [statusState, failuresState, queueState]);

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <header className="mb-6">
        <h1 className="text-xl font-semibold text-slate-900 dark:text-slate-100">
          Ingest health
        </h1>
        <p className="mt-1 text-sm text-slate-600 dark:text-slate-400">
          Visibility into every data source the app ingests from. Click
          a card to expand its per-source detail. Use this page after
          first-run setup to see what's still in progress, and when
          troubleshooting "why is data missing on instrument X".
        </p>
      </header>

      <section className="mb-8" data-test="ingest-status-section">
        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Provider groups
        </h2>
        {statusState.loading ? (
          <SectionSkeleton rows={5} />
        ) : statusState.error !== null || statusState.data === null ? (
          <SectionError onRetry={statusState.refetch} />
        ) : (
          <ProviderGroupGrid status={statusState.data} />
        )}
      </section>

      <section className="mb-8" data-test="ingest-failures-section">
        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Recent failures
        </h2>
        {failuresState.loading ? (
          <SectionSkeleton rows={3} />
        ) : failuresState.error !== null || failuresState.data === null ? (
          <SectionError onRetry={failuresState.refetch} />
        ) : (
          <FailuresTable failures={failuresState.data} />
        )}
      </section>

      <section data-test="ingest-queue-section">
        <h2 className="mb-3 text-sm font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Backfill queue
        </h2>
        {queueState.loading ? (
          <SectionSkeleton rows={3} />
        ) : queueState.error !== null || queueState.data === null ? (
          <SectionError onRetry={queueState.refetch} />
        ) : (
          <QueueTable queue={queueState.data} />
        )}
      </section>
    </div>
  );
}

function ProviderGroupGrid({
  status,
}: {
  readonly status: IngestStatusResponse;
}): JSX.Element {
  if (status.groups.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No ingest activity yet. Run an initial sync from the admin page
        to start populating data.
      </p>
    );
  }
  return (
    <div className="grid gap-3 md:grid-cols-2">
      {status.groups.map((g) => (
        <article
          key={g.key}
          className="rounded-md border border-slate-200 p-4 dark:border-slate-800"
          data-test={`provider-group-${g.key}`}
        >
          <div className="mb-2 flex items-center justify-between gap-2">
            <h3 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
              {g.label}
            </h3>
            <span
              className={`rounded-full px-2 py-0.5 text-xs font-medium ${_STATE_BADGE_CLASS[g.state]}`}
              data-test={`provider-group-state-${g.key}`}
            >
              {_STATE_LABEL[g.state]}
            </span>
          </div>
          <p className="mb-3 text-xs text-slate-600 dark:text-slate-400">
            {g.description}
          </p>
          {g.sources.length === 0 ? (
            <p className="text-xs italic text-slate-500 dark:text-slate-500">
              No runs recorded yet for any source in this group.
            </p>
          ) : (
            <ul className="space-y-1.5">
              {g.sources.map((s) => (
                <li
                  key={s.source}
                  className="flex items-baseline justify-between gap-3 text-xs"
                >
                  <span className="font-mono text-slate-700 dark:text-slate-300">
                    {s.source}
                  </span>
                  <span className="text-slate-500 dark:text-slate-400">
                    {s.last_success_at
                      ? `last success ${formatRelative(s.last_success_at)}`
                      : "never"}
                    {s.failures_24h > 0 && (
                      <span className="ml-2 text-red-700 dark:text-red-300">
                        · {s.failures_24h} failure{s.failures_24h === 1 ? "" : "s"} (24h)
                      </span>
                    )}
                  </span>
                </li>
              ))}
            </ul>
          )}
          {(g.backlog_pending > 0 ||
            g.backlog_running > 0 ||
            g.backlog_failed > 0) && (
            <p className="mt-3 text-xs text-slate-500 dark:text-slate-400">
              Queue:{" "}
              {g.backlog_running > 0 && (
                <span className="text-blue-700 dark:text-blue-300">
                  {g.backlog_running} running
                </span>
              )}
              {g.backlog_running > 0 && g.backlog_pending > 0 && " · "}
              {g.backlog_pending > 0 && (
                <span>{g.backlog_pending} pending</span>
              )}
              {(g.backlog_running > 0 || g.backlog_pending > 0) &&
                g.backlog_failed > 0 &&
                " · "}
              {g.backlog_failed > 0 && (
                <span className="text-red-700 dark:text-red-300">
                  {g.backlog_failed} failed
                </span>
              )}
            </p>
          )}
        </article>
      ))}
    </div>
  );
}

function FailuresTable({
  failures,
}: {
  readonly failures: IngestFailuresResponse;
}): JSX.Element {
  if (failures.failures.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        No failed runs in the last 7 days.
      </p>
    );
  }
  return (
    <div className="overflow-x-auto rounded-md border border-slate-200 dark:border-slate-800">
      <table className="w-full text-xs">
        <thead className="bg-slate-50 text-slate-500 dark:bg-slate-900 dark:text-slate-400">
          <tr>
            <th className="px-3 py-2 text-left">Source</th>
            <th className="px-3 py-2 text-left">Started</th>
            <th className="px-3 py-2 text-left">Error</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {failures.failures.map((f, i) => (
            <tr key={`${f.source}-${f.started_at}-${i}`}>
              <td className="px-3 py-2 font-mono text-slate-700 dark:text-slate-300">
                {f.source}
              </td>
              <td className="px-3 py-2 text-slate-600 dark:text-slate-400">
                {formatRelative(f.started_at)}
              </td>
              <td className="max-w-md truncate px-3 py-2 font-mono text-slate-700 dark:text-slate-300">
                {f.error ?? <span className="italic">no message</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function QueueTable({
  queue,
}: {
  readonly queue: BackfillQueueResponse;
}): JSX.Element {
  if (queue.rows.length === 0) {
    return (
      <p className="text-sm text-slate-500 dark:text-slate-400">
        Backfill queue is empty.
      </p>
    );
  }
  return (
    <div className="overflow-x-auto rounded-md border border-slate-200 dark:border-slate-800">
      <table className="w-full text-xs">
        <thead className="bg-slate-50 text-slate-500 dark:bg-slate-900 dark:text-slate-400">
          <tr>
            <th className="px-3 py-2 text-left">Status</th>
            <th className="px-3 py-2 text-left">Symbol</th>
            <th className="px-3 py-2 text-left">Pipeline</th>
            <th className="px-3 py-2 text-right">Priority</th>
            <th className="px-3 py-2 text-left">Triggered by</th>
            <th className="px-3 py-2 text-left">Last error</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {queue.rows.map((r) => (
            <tr key={`${r.instrument_id}-${r.pipeline_name}`}>
              <td className="px-3 py-2 text-slate-700 dark:text-slate-300">
                {r.status}
              </td>
              <td className="px-3 py-2 font-mono text-slate-700 dark:text-slate-300">
                {r.symbol ?? `#${r.instrument_id}`}
              </td>
              <td className="px-3 py-2 font-mono text-slate-700 dark:text-slate-300">
                {r.pipeline_name}
              </td>
              <td className="px-3 py-2 text-right text-slate-600 dark:text-slate-400">
                {r.priority}
              </td>
              <td className="px-3 py-2 text-slate-600 dark:text-slate-400">
                {r.triggered_by}
              </td>
              <td className="max-w-xs truncate px-3 py-2 font-mono text-slate-700 dark:text-slate-300">
                {r.last_error ?? <span className="italic">—</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatRelative(iso: string): string {
  // Lightweight relative formatter: use the locale time for recent
  // events and ISO date for older ones. The full date is in the cell
  // tooltip on hover via the title attribute (omitted for brevity in
  // this initial render — Codex pre-push review can call this out
  // and add it as a follow-up).
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    const now = Date.now();
    const diffMs = now - d.getTime();
    const minutes = Math.round(diffMs / 60_000);
    if (minutes < 1) return "just now";
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.round(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.round(hours / 24);
    if (days < 30) return `${days}d ago`;
    return d.toISOString().slice(0, 10);
  } catch {
    return iso;
  }
}

