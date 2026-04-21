/**
 * AlertsStrip — guard-rejection alerts on the operator dashboard (#315 Phase 3).
 *
 * Sits between RollingPnlStrip and PortfolioValueChart. Hidden when empty;
 * silent on fetch error (matches the RollingPnlStrip pattern — a failing
 * /alerts must not blank the dashboard).
 *
 * Cursor is decision_id (not decision_time) — see spec
 * docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md for why.
 */
import { Link } from "react-router-dom";

import { dismissAllAlerts, fetchGuardRejections, markAlertsSeen } from "@/api/alerts";
import type { GuardRejection } from "@/api/types";
import { formatRelativeTime } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

function isUnseen(
  row: GuardRejection,
  lastSeen: number | null,
): boolean {
  return lastSeen === null || row.decision_id > lastSeen;
}

function RowView({
  row,
  lastSeen,
}: {
  row: GuardRejection;
  lastSeen: number | null;
}) {
  const unseen = isUnseen(row, lastSeen);
  const border = unseen
    ? "border-l-4 border-amber-400"
    : "border-l-4 border-slate-200";
  const content = (
    <div
      data-testid="alerts-row"
      role="listitem"
      className={`flex items-center gap-3 px-3 py-2 text-sm ${border} bg-white`}
    >
      <span className="w-16 font-semibold tabular-nums">{row.symbol ?? "—"}</span>
      <span className="w-12 text-xs uppercase text-slate-500">{row.action ?? "—"}</span>
      <span
        className="flex-1 truncate text-slate-700"
        title={row.explanation}
      >
        {row.explanation}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.decision_time)}
      </span>
    </div>
  );
  if (row.instrument_id !== null) {
    return (
      <Link to={`/instruments/${row.instrument_id}`} className="block hover:bg-slate-50">
        {content}
      </Link>
    );
  }
  return content;
}

export function AlertsStrip(): JSX.Element | null {
  const { data, error, refetch } = useAsync(fetchGuardRejections, []);

  if (error !== null || data === null) return null;
  if (data.rejections.length === 0) return null;

  const lastSeen = data.alerts_last_seen_decision_id;
  const normalAck =
    data.unseen_count > 0 && data.unseen_count <= data.rejections.length;
  const overflowAck = data.unseen_count > data.rejections.length;

  async function onMarkAllRead() {
    // rejections is non-empty here (strip is hidden otherwise),
    // and is ordered decision_id DESC on the server so index 0 is MAX.
    const seenThroughDecisionId = data!.rejections[0]!.decision_id;
    try {
      await markAlertsSeen(seenThroughDecisionId);
    } catch (err) {
      // Silent-on-error matches the rest of the strip; log for debugging.
      // Server ack is idempotent (GREATEST monotonic) so a future retry is safe.
      console.error("[AlertsStrip] markAlertsSeen failed", err);
    }
    refetch();
  }

  async function onDismissAll() {
    const hiddenCount = data!.unseen_count - data!.rejections.length;
    const msg = `Dismiss all ${data!.unseen_count} unseen rejections? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
    if (!window.confirm(msg)) return;
    try {
      await dismissAllAlerts();
    } catch (err) {
      console.error("[AlertsStrip] dismissAllAlerts failed", err);
    }
    refetch();
  }

  return (
    <section aria-labelledby="alerts-strip-heading" className="rounded-md border border-slate-200 bg-white shadow-sm">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <div className="flex items-center gap-2">
          <h2 id="alerts-strip-heading" className="text-sm font-semibold text-slate-700">Guard rejections</h2>
          {data.unseen_count > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              {data.unseen_count} new
            </span>
          ) : null}
        </div>
        {normalAck ? (
          <button
            type="button"
            onClick={onMarkAllRead}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            Mark all read
          </button>
        ) : null}
        {overflowAck ? (
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onDismissAll}
              className="rounded border border-amber-300 bg-amber-50 px-2 py-1 text-xs font-medium text-amber-800 hover:bg-amber-100"
            >
              Dismiss all ({data.unseen_count}) as acknowledged
            </button>
            <Link
              to="/recommendations"
              className="text-xs text-slate-500 underline hover:text-slate-700"
            >
              Triage at /recommendations
            </Link>
          </div>
        ) : null}
      </header>
      <div
        tabIndex={0}
        role="list"
        aria-labelledby="alerts-strip-heading"
        className="max-h-96 overflow-y-auto divide-y divide-slate-100"
      >
        {data.rejections.map((row) => (
          <RowView key={row.decision_id} row={row} lastSeen={lastSeen} />
        ))}
      </div>
    </section>
  );
}
