/**
 * Admin job detail — run history + drill-through (issue #415).
 *
 * Surfaces the most recent 50 rows of `job_runs` for a single job so
 * the operator can see *why* a red "last run failed" alert is on the
 * admin page. Failure rows expand to show `error_msg` verbatim; other
 * statuses are non-interactive (no clickable row that expands to
 * nothing).
 *
 * The `:name` path param is read straight from `useParams()` — React
 * Router 6 already URL-decodes path params, so a second decode would
 * corrupt names containing `%`. `fetchJobRuns` re-encodes internally
 * when building the query string.
 */

import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { fetchJobRuns } from "@/api/jobs";
import type { JobRunResponse } from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";
import { formatDateTime } from "@/lib/format";

const STATUS_TONE: Record<JobRunResponse["status"], string> = {
  success: "text-emerald-700",
  failure: "text-red-700",
  running: "text-sky-700",
  skipped: "text-slate-500",
};

export function AdminJobDetailPage() {
  const params = useParams<{ name: string }>();
  const name = params.name ?? "";

  const list = useAsync(
    useCallback(() => fetchJobRuns(name, 50), [name]),
    [name],
  );

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <div>
          <div className="text-xs text-slate-500">Admin / Jobs</div>
          <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">{name}</h1>
        </div>
        <Link to="/admin" className="text-xs text-blue-700 hover:underline">
          ← Back to Admin
        </Link>
      </div>

      <Section title="Recent runs">
        {list.loading ? (
          <SectionSkeleton rows={5} />
        ) : list.error !== null ? (
          <SectionError onRetry={list.refetch} />
        ) : list.data && list.data.items.length === 0 ? (
          <EmptyState />
        ) : list.data ? (
          <RunsTable rows={list.data.items} />
        ) : null}
      </Section>
    </div>
  );
}

function EmptyState() {
  // Intentionally neutral about "renamed" vs "no history" — the
  // endpoint returns 200 + empty items for both, and conflating them
  // honestly is less misleading than inventing a distinction. The
  // header already carries the back link, so this component does not
  // duplicate it.
  return (
    <div className="space-y-2 text-sm text-slate-600">
      <p>No recent runs for this job.</p>
      <p className="text-xs text-slate-500">
        If you arrived from an old bookmark, the job may have been renamed.
      </p>
    </div>
  );
}

function RunsTable({ rows }: { rows: JobRunResponse[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Started</th>
            <th className="py-2 pr-4">Finished</th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4">Duration</th>
            <th className="py-2 pr-4 text-right">Rows</th>
            {/* Sixth column carries the per-row expand button on
                failure rows. Declared here so the header and body row
                column counts match (5 vs 6 would skew alignment and
                flag the table as malformed by semantic-HTML linters). */}
            <th className="py-2 pr-2 text-right">
              <span className="sr-only">Actions</span>
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map((row) => (
            <RunRow key={row.run_id} row={row} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RunRow({ row }: { row: JobRunResponse }) {
  const [expanded, setExpanded] = useState(false);
  const expandable = row.status === "failure" && row.error_msg !== null;
  const duration = formatDuration(row.started_at, row.finished_at);
  return (
    <>
      <tr className="align-top" data-started-at={row.started_at}>
        <td className="py-2 pr-4 text-xs text-slate-600">
          {formatDateTime(row.started_at)}
        </td>
        <td className="py-2 pr-4 text-xs text-slate-600">
          {row.finished_at !== null ? formatDateTime(row.finished_at) : "—"}
        </td>
        <td className={`py-2 pr-4 text-xs font-medium ${STATUS_TONE[row.status]}`}>
          {row.status}
        </td>
        <td className="py-2 pr-4 text-xs text-slate-500">{duration}</td>
        <td className="py-2 pr-4 text-right text-xs text-slate-700">
          {row.row_count ?? "—"}
        </td>
        <td className="py-2 pr-2 text-right">
          {expandable ? (
            <button
              type="button"
              onClick={() => setExpanded((v) => !v)}
              aria-label={
                expanded
                  ? `Hide error for run ${row.run_id}`
                  : `Show error for run ${row.run_id}`
              }
              className="text-xs font-medium text-red-700 hover:underline"
            >
              {expanded ? "Hide" : "Show error"}
            </button>
          ) : null}
        </td>
      </tr>
      {expandable && expanded ? (
        <tr>
          <td colSpan={6} className="bg-slate-50 dark:bg-slate-900/40 px-4 py-3">
            <pre className="whitespace-pre-wrap text-xs text-slate-800 dark:text-slate-100">
              {row.error_msg}
            </pre>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function formatDuration(startedAt: string, finishedAt: string | null): string {
  if (finishedAt === null) return "—";
  const startMs = Date.parse(startedAt);
  const endMs = Date.parse(finishedAt);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return "—";
  const seconds = Math.max(0, Math.round((endMs - startMs) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const rem = seconds % 60;
  return `${mins}m ${rem}s`;
}
