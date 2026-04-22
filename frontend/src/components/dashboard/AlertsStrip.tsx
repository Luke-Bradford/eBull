/**
 * AlertsStrip — unified dashboard alert feed (#399).
 *
 * Renders three independent alert streams in a single timestamp-sorted list:
 *
 *   1. Guard rejections (#394)
 *   2. Position alerts — SL/TP/thesis breach episodes (#401)
 *   3. Coverage status drops from 'analysable' (#402)
 *
 * Each feed keeps its own BIGSERIAL cursor column on `operators`. Partial
 * failure is tolerated: one feed GET erroring does not hide the others; one
 * POST failing does not block the siblings (Promise.allSettled).
 *
 * Overflow math is per-feed — each backend query caps at LIMIT 500 so
 * global `totalUnseen > merged.length` would conflate feed-A hidden rows
 * with feed-B seen padding. See spec
 * docs/superpowers/specs/2026-04-22-alerts-strip-unified.md for rationale.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import {
  dismissAllAlerts,
  dismissAllCoverageStatusDrops,
  dismissAllPositionAlerts,
  fetchCoverageStatusDrops,
  fetchGuardRejections,
  fetchPositionAlerts,
  markAlertsSeen,
  markCoverageStatusDropsSeen,
  markPositionAlertsSeen,
} from "@/api/alerts";
import type {
  CoverageStatusDrop,
  CoverageStatusDropsResponse,
  GuardRejection,
  GuardRejectionsResponse,
  PositionAlert,
  PositionAlertsResponse,
} from "@/api/types";
import { formatRelativeTime } from "@/lib/format";

type FeedState<T> =
  | { status: "loading" }
  | { status: "ok"; data: T }
  | { status: "err" };

type AlertRow =
  | { kind: "guard"; ts: string; sortKey: number; row: GuardRejection }
  | { kind: "position"; ts: string; sortKey: number; row: PositionAlert }
  | { kind: "coverage"; ts: string; sortKey: number; row: CoverageStatusDrop };

type Cursors = {
  guard: number | null;
  position: number | null;
  coverage: number | null;
};

function buildRows(
  guard: FeedState<GuardRejectionsResponse>,
  position: FeedState<PositionAlertsResponse>,
  coverage: FeedState<CoverageStatusDropsResponse>,
): AlertRow[] {
  const rows: AlertRow[] = [];
  if (guard.status === "ok") {
    for (const r of guard.data.rejections) {
      rows.push({
        kind: "guard",
        ts: r.decision_time,
        sortKey: Date.parse(r.decision_time),
        row: r,
      });
    }
  }
  if (position.status === "ok") {
    for (const r of position.data.alerts) {
      rows.push({
        kind: "position",
        ts: r.opened_at,
        sortKey: Date.parse(r.opened_at),
        row: r,
      });
    }
  }
  if (coverage.status === "ok") {
    for (const r of coverage.data.drops) {
      rows.push({
        kind: "coverage",
        ts: r.changed_at,
        sortKey: Date.parse(r.changed_at),
        row: r,
      });
    }
  }
  rows.sort((a, b) => b.sortKey - a.sortKey);
  return rows;
}

function isUnseen(r: AlertRow, c: Cursors): boolean {
  switch (r.kind) {
    case "guard":
      return c.guard === null || r.row.decision_id > c.guard;
    case "position":
      return c.position === null || r.row.alert_id > c.position;
    case "coverage":
      return c.coverage === null || r.row.event_id > c.coverage;
  }
}

function KindPill({ kind }: { kind: AlertRow["kind"] }) {
  const style = {
    guard: "bg-amber-100 text-amber-800",
    position: "bg-red-100 text-red-800",
    coverage: "bg-slate-100 text-slate-700",
  }[kind];
  const label = { guard: "GUARD", position: "POSITION", coverage: "COVERAGE" }[kind];
  return (
    <span
      className={`w-20 rounded px-1.5 py-0.5 text-center text-[10px] font-semibold uppercase ${style}`}
    >
      {label}
    </span>
  );
}

function RowShell({
  kind,
  unseen,
  instrumentId,
  children,
}: {
  kind: AlertRow["kind"];
  unseen: boolean;
  instrumentId: number | null;
  children: React.ReactNode;
}) {
  const border = unseen
    ? "border-l-4 border-amber-400"
    : "border-l-4 border-slate-200";
  const content = (
    <div
      data-testid="alerts-row"
      role="listitem"
      className={`flex items-center gap-3 px-3 py-2 text-sm ${border} bg-white`}
    >
      <KindPill kind={kind} />
      {children}
    </div>
  );
  if (instrumentId !== null) {
    return (
      <Link to={`/instruments/${instrumentId}`} className="block hover:bg-slate-50">
        {content}
      </Link>
    );
  }
  return content;
}

function GuardRow({ row, unseen }: { row: GuardRejection; unseen: boolean }) {
  return (
    <RowShell kind="guard" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol ?? "—"}</span>
      <span className="w-16 text-xs uppercase text-slate-500">{row.action ?? "—"}</span>
      <span className="flex-1 truncate text-slate-700" title={row.explanation}>
        {row.explanation}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.decision_time)}
      </span>
    </RowShell>
  );
}

function PositionRow({ row, unseen }: { row: PositionAlert; unseen: boolean }) {
  const alertLabel = {
    sl_breach: "SL",
    tp_breach: "TP",
    thesis_break: "THESIS",
  }[row.alert_type];
  return (
    <RowShell kind="position" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol}</span>
      <span className="w-16 text-xs uppercase text-slate-500">{alertLabel}</span>
      <span className="flex-1 truncate text-slate-700" title={row.detail}>
        {row.detail}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.opened_at)}
      </span>
    </RowShell>
  );
}

function CoverageRow({ row, unseen }: { row: CoverageStatusDrop; unseen: boolean }) {
  const transition = `${row.old_status} → ${row.new_status ?? "—"}`;
  return (
    <RowShell kind="coverage" unseen={unseen} instrumentId={row.instrument_id}>
      <span className="w-16 font-semibold tabular-nums">{row.symbol}</span>
      <span className="flex-1 truncate text-slate-700" title={transition}>
        {transition}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.changed_at)}
      </span>
    </RowShell>
  );
}

function RowView({ row, cursors }: { row: AlertRow; cursors: Cursors }) {
  const unseen = isUnseen(row, cursors);
  switch (row.kind) {
    case "guard":
      return <GuardRow row={row.row} unseen={unseen} />;
    case "position":
      return <PositionRow row={row.row} unseen={unseen} />;
    case "coverage":
      return <CoverageRow row={row.row} unseen={unseen} />;
  }
}

function rowId(row: AlertRow): number {
  switch (row.kind) {
    case "guard":
      return row.row.decision_id;
    case "position":
      return row.row.alert_id;
    case "coverage":
      return row.row.event_id;
  }
}

export function AlertsStrip(): JSX.Element | null {
  const [guard, setGuard] = useState<FeedState<GuardRejectionsResponse>>({
    status: "loading",
  });
  const [position, setPosition] = useState<FeedState<PositionAlertsResponse>>({
    status: "loading",
  });
  const [coverage, setCoverage] = useState<FeedState<CoverageStatusDropsResponse>>({
    status: "loading",
  });
  const [refetchKey, setRefetchKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setGuard({ status: "loading" });
    setPosition({ status: "loading" });
    setCoverage({ status: "loading" });

    fetchGuardRejections()
      .then((d) => {
        if (!cancelled) setGuard({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchGuardRejections failed", err);
          setGuard({ status: "err" });
        }
      });
    fetchPositionAlerts()
      .then((d) => {
        if (!cancelled) setPosition({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchPositionAlerts failed", err);
          setPosition({ status: "err" });
        }
      });
    fetchCoverageStatusDrops()
      .then((d) => {
        if (!cancelled) setCoverage({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchCoverageStatusDrops failed", err);
          setCoverage({ status: "err" });
        }
      });

    return () => {
      cancelled = true;
    };
  }, [refetchKey]);

  const refetch = () => setRefetchKey((k) => k + 1);

  if (
    guard.status === "loading" ||
    position.status === "loading" ||
    coverage.status === "loading"
  ) {
    return null;
  }
  if (
    guard.status === "err" &&
    position.status === "err" &&
    coverage.status === "err"
  ) {
    return null;
  }

  const merged = buildRows(guard, position, coverage);

  if (merged.length === 0) {
    return null;
  }

  const unseenGuard = guard.status === "ok" ? guard.data.unseen_count : 0;
  const unseenPosition = position.status === "ok" ? position.data.unseen_count : 0;
  const unseenCoverage = coverage.status === "ok" ? coverage.data.unseen_count : 0;

  const renderedGuard = guard.status === "ok" ? guard.data.rejections.length : 0;
  const renderedPosition = position.status === "ok" ? position.data.alerts.length : 0;
  const renderedCoverage = coverage.status === "ok" ? coverage.data.drops.length : 0;

  const totalUnseen = unseenGuard + unseenPosition + unseenCoverage;

  const anyOverflow =
    unseenGuard > renderedGuard ||
    unseenPosition > renderedPosition ||
    unseenCoverage > renderedCoverage;

  const overflowAck = anyOverflow;
  const normalAck = totalUnseen > 0 && !anyOverflow;

  const cursors: Cursors = {
    guard: guard.status === "ok" ? guard.data.alerts_last_seen_decision_id : null,
    position:
      position.status === "ok"
        ? position.data.alerts_last_seen_position_alert_id
        : null,
    coverage:
      coverage.status === "ok"
        ? coverage.data.alerts_last_seen_coverage_event_id
        : null,
  };

  async function onMarkAllRead() {
    const guardMax = Math.max(
      0,
      ...merged
        .filter((r): r is Extract<AlertRow, { kind: "guard" }> => r.kind === "guard")
        .map((r) => r.row.decision_id),
    );
    const positionMax = Math.max(
      0,
      ...merged
        .filter((r): r is Extract<AlertRow, { kind: "position" }> => r.kind === "position")
        .map((r) => r.row.alert_id),
    );
    const coverageMax = Math.max(
      0,
      ...merged
        .filter((r): r is Extract<AlertRow, { kind: "coverage" }> => r.kind === "coverage")
        .map((r) => r.row.event_id),
    );

    const promises: Promise<void>[] = [];
    if (guardMax > 0) promises.push(markAlertsSeen(guardMax));
    if (positionMax > 0) promises.push(markPositionAlertsSeen(positionMax));
    if (coverageMax > 0) promises.push(markCoverageStatusDropsSeen(coverageMax));

    const results = await Promise.allSettled(promises);
    for (const r of results) {
      if (r.status === "rejected") {
        console.error("[AlertsStrip] mark-all-read partial failure", r.reason);
      }
    }
    refetch();
  }

  async function onDismissAll() {
    const hiddenCount =
      Math.max(0, unseenGuard - renderedGuard) +
      Math.max(0, unseenPosition - renderedPosition) +
      Math.max(0, unseenCoverage - renderedCoverage);
    const msg = `Dismiss all ${totalUnseen} unseen alerts? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
    if (!window.confirm(msg)) return;

    const promises: Promise<void>[] = [];
    if (guard.status === "ok") promises.push(dismissAllAlerts());
    if (position.status === "ok") promises.push(dismissAllPositionAlerts());
    if (coverage.status === "ok") promises.push(dismissAllCoverageStatusDrops());

    const results = await Promise.allSettled(promises);
    for (const r of results) {
      if (r.status === "rejected") {
        console.error("[AlertsStrip] dismiss-all partial failure", r.reason);
      }
    }
    refetch();
  }

  return (
    <section
      aria-labelledby="alerts-strip-heading"
      className="rounded-md border border-slate-200 bg-white shadow-sm"
    >
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <div className="flex items-center gap-2">
          <h2
            id="alerts-strip-heading"
            className="text-sm font-semibold text-slate-700"
          >
            Alerts
          </h2>
          {totalUnseen > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              {totalUnseen} new
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
              Dismiss all ({totalUnseen}) as acknowledged
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
        {merged.map((row) => (
          <RowView key={`${row.kind}-${rowId(row)}`} row={row} cursors={cursors} />
        ))}
      </div>
    </section>
  );
}
