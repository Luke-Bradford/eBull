/**
 * Drill-down view for instruments stuck in ``insufficient`` or
 * ``structurally_young`` filings states (#268 Chunk H).
 *
 * Read-only — no manual re-enqueue or status override surfaces here;
 * those are deferred. An operator can see which instruments are
 * painful (highest attempts used first) + earliest SEC filing date
 * so "structurally_young" rows can be sanity-checked against SEC
 * ground truth.
 */

import { Link } from "react-router-dom";

import { fetchCoverageInsufficient } from "@/api/coverage";
import type { InsufficientRow } from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";
import { formatDate, formatDateTime } from "@/lib/format";

const STATUS_TONE: Record<InsufficientRow["filings_status"], string> = {
  insufficient: "text-amber-700",
  structurally_young: "text-blue-700",
};

export function CoverageInsufficientPage() {
  const list = useAsync(fetchCoverageInsufficient, []);

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
          Coverage drill-down
        </h1>
        <Link
          to="/admin"
          className="text-xs text-blue-700 hover:underline"
        >
          ← Back to admin
        </Link>
      </div>

      <Section title="Stuck instruments">
        {list.loading ? (
          <SectionSkeleton rows={5} />
        ) : list.error !== null ? (
          <SectionError onRetry={list.refetch} />
        ) : list.data ? (
          <InsufficientTable rows={list.data.rows} />
        ) : null}
      </Section>
    </div>
  );
}

function InsufficientTable({ rows }: { rows: InsufficientRow[] }) {
  if (rows.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        No instruments currently stuck below the analysable bar.
      </p>
    );
  }
  return (
    <div className="overflow-x-auto">
      <p className="mb-3 text-xs text-slate-500">
        Ordered by attempts-used descending, then stalest first.
        ``insufficient`` rows may indicate a failed backfill;
        ``structurally_young`` rows will flip to analysable once the
        issuer ages past 18 months.
      </p>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Symbol</th>
            <th className="py-2 pr-4">CIK</th>
            <th className="py-2 pr-4">Status</th>
            <th className="py-2 pr-4 text-right">Attempts</th>
            <th className="py-2 pr-4">Last attempt</th>
            <th className="py-2 pr-4">Last reason</th>
            <th className="py-2 pr-4">Earliest SEC filing</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map((row) => (
            <tr key={row.instrument_id} className="align-top">
              <td className="py-2 pr-4">
                <div className="font-medium text-slate-700">{row.symbol}</div>
                <div className="text-xs text-slate-500">
                  {row.company_name ?? "—"}
                </div>
              </td>
              <td className="py-2 pr-4 font-mono text-xs text-slate-600">
                {row.cik ?? "—"}
              </td>
              <td className={`py-2 pr-4 text-xs font-medium ${STATUS_TONE[row.filings_status]}`}>
                {row.filings_status}
              </td>
              <td className="py-2 pr-4 text-right text-xs text-slate-700">
                {row.filings_backfill_attempts}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-500">
                {formatDateTime(row.filings_backfill_last_at)}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-500">
                {row.filings_backfill_reason ?? "—"}
              </td>
              <td className="py-2 pr-4 text-xs text-slate-500">
                {formatDate(row.earliest_sec_filing_date)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
