/**
 * AlertsStrip — unified dashboard alert feed (#399), grouped + severity-tiered (#1898).
 *
 * Renders three independent alert streams collapsed to root-cause groups and ordered by
 * severity tier (actionable → informational → housekeeping), so a shared root cause
 * (e.g. kill-switch active) shows as ONE card with the affected symbols instead of N×M
 * near-identical rows. Grouping lives in ./alertModel (pure, unit-tested).
 *
 *   1. Guard rejections (#394) — grouped by leading rule code (informational).
 *   2. Position alerts — SL/TP/thesis breach episodes (#401) — per-instrument (actionable).
 *   3. Coverage status drops from 'analysable' (#402) — grouped by transition (housekeeping).
 *
 * Seen/unseen + overflow accounting is UNCHANGED and operates on the RAW feed arrays by
 * BIGSERIAL id (clocks can skew): each feed keeps its own cursor column on `operators`,
 * the header pill counts backend `unseen_count`, and mark-read/dismiss use per-feed max ids.
 * Partial failure is tolerated (Promise.allSettled). See spec
 * docs/specs/ui/2026-07-04-alerts-strip-grouping.md and the original
 * docs/superpowers/specs/2026-04-22-alerts-strip-unified.md.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import {
  dismissAllAlerts,
  dismissAllCoverageStatusDrops,
  dismissAllPositionAlerts,
  dismissAllRankMoves,
  fetchCoverageStatusDrops,
  fetchGuardRejections,
  fetchPositionAlerts,
  fetchRankMoves,
  fetchThesisChanges,
  fetchThesisStaleness,
  markAlertsSeen,
  markCoverageStatusDropsSeen,
  markPositionAlertsSeen,
  markRankMovesSeen,
  markThesisChangesSeen,
} from "@/api/alerts";
import type {
  CoverageStatusDropsResponse,
  GuardRejectionsResponse,
  PositionAlert,
  PositionAlertsResponse,
  RankMovesResponse,
  ThesisChangesResponse,
  ThesisStalenessResponse,
} from "@/api/types";
import { formatRelativeTime } from "@/lib/format";

import {
  buildAlertModel,
  type AlertItem,
  type Cursors,
  type GuardGroupItem,
  type CoverageGroupItem,
  type RankMoveItem,
  type ThesisChangeItem,
  type ThesisStaleItem,
  type Tier,
} from "./alertModel";

type FeedState<T> =
  | { status: "loading" }
  | { status: "ok"; data: T }
  | { status: "err" };

const TIER_LABEL: Record<Tier, string> = {
  actionable: "ACTION",
  informational: "GUARD",
  housekeeping: "COVERAGE",
};

const TIER_PILL: Record<Tier, string> = {
  actionable: "bg-red-100 dark:bg-red-900/40 text-red-800 dark:text-red-200",
  informational:
    "bg-amber-100 dark:bg-amber-900/40 text-amber-800 dark:text-amber-200",
  housekeeping: "bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300",
};

function tierBorder(tier: Tier, unseen: boolean): string {
  if (unseen) {
    return {
      actionable: "border-l-4 border-red-400",
      informational: "border-l-4 border-amber-400",
      housekeeping: "border-l-4 border-slate-300 dark:border-slate-600",
    }[tier];
  }
  return "border-l-4 border-slate-200 dark:border-slate-800";
}

// `label` overrides the tier-derived pill text. The pill doubles as a
// kind/category badge (ACTION/GUARD/COVERAGE), so a feed that shares a tier
// with another kind (rank moves are informational, like guard) must supply its
// own label to stay legible.
function TierPill({ tier, label }: { tier: Tier; label?: string }) {
  return (
    <span
      className={`w-20 shrink-0 rounded px-1.5 py-0.5 text-center text-[10px] font-semibold uppercase ${TIER_PILL[tier]}`}
    >
      {label ?? TIER_LABEL[tier]}
    </span>
  );
}

function CardShell({
  tier,
  unseen,
  label,
  children,
}: {
  tier: Tier;
  unseen: boolean;
  label?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      data-testid="alerts-row"
      role="listitem"
      className={`flex items-start gap-3 px-3 py-2 text-sm ${tierBorder(tier, unseen)} bg-white dark:bg-slate-900`}
    >
      <TierPill tier={tier} label={label} />
      {children}
    </div>
  );
}

function SymbolSummary({ symbols, count }: { symbols: string[]; count: number }) {
  if (symbols.length === 0) {
    return (
      <span className="text-xs text-slate-500 dark:text-slate-400">
        {count} {count === 1 ? "occurrence" : "occurrences"}
      </span>
    );
  }
  const noun = symbols.length === 1 ? "symbol" : "symbols";
  return (
    <span className="text-xs text-slate-600 dark:text-slate-300">
      <span className="text-slate-400 dark:text-slate-500">
        {symbols.length} {noun}:{" "}
      </span>
      {symbols.join(", ")}
    </span>
  );
}

function GuardGroupCard({ item }: { item: GuardGroupItem }) {
  return (
    <CardShell tier={item.tier} unseen={item.unseen}>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <div className="flex items-baseline gap-2">
          <span className="font-semibold text-slate-800 dark:text-slate-100">
            {item.label}
          </span>
          {item.count > 1 ? (
            <span className="rounded-full bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 text-[10px] font-medium text-slate-600 dark:text-slate-300 tabular-nums">
              ×{item.count}
            </span>
          ) : null}
        </div>
        <span className="text-xs text-slate-500 dark:text-slate-400">
          {item.consequence}
        </span>
        <SymbolSummary symbols={item.symbols} count={item.count} />
      </div>
      <div className="flex shrink-0 flex-col items-end gap-1">
        <span className="text-xs text-slate-400 dark:text-slate-500">
          {formatRelativeTime(item.latestTs)}
        </span>
        <Link
          to={item.action.to}
          className="text-xs font-medium text-sky-600 dark:text-sky-400 underline hover:text-sky-800 dark:hover:text-sky-300"
        >
          {item.action.label}
        </Link>
      </div>
    </CardShell>
  );
}

function PositionCard({
  row,
  unseen,
}: {
  row: PositionAlert;
  unseen: boolean;
}) {
  const alertLabel = {
    sl_breach: "SL",
    tp_breach: "TP",
    thesis_break: "THESIS",
  }[row.alert_type];
  return (
    <Link
      to={`/instruments/${row.instrument_id}`}
      className="block hover:bg-slate-50 dark:hover:bg-slate-800/40"
    >
      <CardShell tier="actionable" unseen={unseen}>
        <div className="flex min-w-0 flex-1 items-baseline gap-2">
          <span className="w-16 shrink-0 font-semibold tabular-nums">
            {row.symbol}
          </span>
          <span className="w-16 shrink-0 text-xs uppercase text-slate-500 dark:text-slate-400">
            {alertLabel}
          </span>
          <span className="flex-1 truncate text-slate-700 dark:text-slate-200" title={row.detail}>
            {row.detail}
          </span>
        </div>
        <span className="shrink-0 text-xs text-slate-400 dark:text-slate-500">
          {formatRelativeTime(row.opened_at)}
        </span>
      </CardShell>
    </Link>
  );
}

function CoverageGroupCard({ item }: { item: CoverageGroupItem }) {
  return (
    <CardShell tier={item.tier} unseen={item.unseen}>
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <div className="flex items-baseline gap-2">
          <span className="font-semibold text-slate-800 dark:text-slate-100">
            Coverage {item.transition}
          </span>
          {item.count > 1 ? (
            <span className="rounded-full bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 text-[10px] font-medium text-slate-600 dark:text-slate-300 tabular-nums">
              ×{item.count}
            </span>
          ) : null}
        </div>
        <SymbolSummary symbols={item.symbols} count={item.count} />
      </div>
      <span className="shrink-0 text-xs text-slate-400 dark:text-slate-500">
        {formatRelativeTime(item.latestTs)}
      </span>
    </CardShell>
  );
}

function RankMoveCard({ item }: { item: RankMoveItem }) {
  // rankDelta: positive = moved UP the board (better), negative = down.
  const up = item.rankDelta > 0;
  const magnitude = Math.abs(item.rankDelta);
  return (
    <Link
      to={`/instruments/${item.instrumentId}`}
      className="block hover:bg-slate-50 dark:hover:bg-slate-800/40"
    >
      <CardShell tier={item.tier} unseen={item.unseen} label="RANK">
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <div className="flex items-baseline gap-2">
            <span className="font-semibold text-slate-800 dark:text-slate-100">
              {item.symbol}
            </span>
            <span
              className={`tabular-nums font-medium ${
                up
                  ? "text-emerald-600 dark:text-emerald-400"
                  : "text-red-600 dark:text-red-400"
              }`}
            >
              {up ? "▲" : "▼"}
              {magnitude}
            </span>
            {item.count > 1 ? (
              <span className="rounded-full bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 text-[10px] font-medium text-slate-600 dark:text-slate-300 tabular-nums">
                ×{item.count}
              </span>
            ) : null}
          </div>
          <span className="text-xs text-slate-500 dark:text-slate-400 tabular-nums">
            {up ? "Up" : "Down"} {magnitude} to rank #{item.rank}
          </span>
        </div>
        <span className="shrink-0 text-xs text-slate-400 dark:text-slate-500">
          {formatRelativeTime(item.latestTs)}
        </span>
      </CardShell>
    </Link>
  );
}

function ThesisChangeCard({ item }: { item: ThesisChangeItem }) {
  // Material re-thesis (#2013): deterministic summary from thesis_diff —
  // stance/type changes, targets added/removed, moves ≥5%.
  return (
    <Link
      to={`/instruments/${item.instrumentId}`}
      className="block hover:bg-slate-50 dark:hover:bg-slate-800/40"
    >
      <CardShell tier={item.tier} unseen={item.unseen} label="RE-THESIS">
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <div className="flex items-baseline gap-2">
            <span className="font-semibold text-slate-800 dark:text-slate-100">
              {item.symbol}
            </span>
            {item.count > 1 ? (
              <span className="rounded-full bg-slate-100 dark:bg-slate-800 px-1.5 py-0.5 text-[10px] font-medium text-slate-600 dark:text-slate-300 tabular-nums">
                ×{item.count}
              </span>
            ) : null}
          </div>
          <span
            className="truncate text-xs text-slate-600 dark:text-slate-300"
            title={item.summary}
          >
            {item.summary}
          </span>
        </div>
        <span className="shrink-0 text-xs text-slate-400 dark:text-slate-500">
          {formatRelativeTime(item.latestTs)}
        </span>
      </CardShell>
    </Link>
  );
}

function ThesisStaleCard({ item }: { item: ThesisStaleItem }) {
  // Standing condition (#1902): no unseen highlight, no dismiss — the card
  // clears when the theses regenerate. Links to the pre-filtered library.
  const noun = item.count === 1 ? "instrument has" : "instruments have";
  return (
    <CardShell tier={item.tier} unseen={false} label="THESIS">
      <div className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="font-semibold text-slate-800 dark:text-slate-100">
          {item.count} held {noun} a stale thesis
        </span>
        <SymbolSummary symbols={item.symbols} count={item.count} />
      </div>
      <div className="flex shrink-0 flex-col items-end gap-1">
        <Link
          to="/theses?held=true&stale=true"
          className="text-xs font-medium text-sky-600 dark:text-sky-400 underline hover:text-sky-800 dark:hover:text-sky-300"
        >
          Review in Theses
        </Link>
      </div>
    </CardShell>
  );
}

function ItemView({ item }: { item: AlertItem }) {
  switch (item.kind) {
    case "guardGroup":
      return <GuardGroupCard item={item} />;
    case "position":
      return <PositionCard row={item.row} unseen={item.unseen} />;
    case "coverageGroup":
      return <CoverageGroupCard item={item} />;
    case "rankMove":
      return <RankMoveCard item={item} />;
    case "thesisChange":
      return <ThesisChangeCard item={item} />;
    case "thesisStale":
      return <ThesisStaleCard item={item} />;
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
  const [rank, setRank] = useState<FeedState<RankMovesResponse>>({
    status: "loading",
  });
  const [thesisStale, setThesisStale] = useState<
    FeedState<ThesisStalenessResponse>
  >({
    status: "loading",
  });
  const [thesisChange, setThesisChange] = useState<
    FeedState<ThesisChangesResponse>
  >({
    status: "loading",
  });
  const [refetchKey, setRefetchKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setGuard({ status: "loading" });
    setPosition({ status: "loading" });
    setCoverage({ status: "loading" });
    setRank({ status: "loading" });
    setThesisStale({ status: "loading" });
    setThesisChange({ status: "loading" });

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
    fetchRankMoves()
      .then((d) => {
        if (!cancelled) setRank({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchRankMoves failed", err);
          setRank({ status: "err" });
        }
      });
    fetchThesisStaleness()
      .then((d) => {
        if (!cancelled) setThesisStale({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchThesisStaleness failed", err);
          setThesisStale({ status: "err" });
        }
      });
    fetchThesisChanges()
      .then((d) => {
        if (!cancelled) setThesisChange({ status: "ok", data: d });
      })
      .catch((err) => {
        if (!cancelled) {
          console.error("[AlertsStrip] fetchThesisChanges failed", err);
          setThesisChange({ status: "err" });
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
    coverage.status === "loading" ||
    rank.status === "loading" ||
    thesisStale.status === "loading" ||
    thesisChange.status === "loading"
  ) {
    return null;
  }
  if (
    guard.status === "err" &&
    position.status === "err" &&
    coverage.status === "err" &&
    rank.status === "err" &&
    thesisStale.status === "err" &&
    thesisChange.status === "err"
  ) {
    return null;
  }

  // Raw feed arrays — the source of truth for BOTH the grouped body and the
  // seen/unseen + overflow accounting (grouping never feeds the counts).
  const rejections = guard.status === "ok" ? guard.data.rejections : [];
  const positionAlerts = position.status === "ok" ? position.data.alerts : [];
  const drops = coverage.status === "ok" ? coverage.data.drops : [];
  const moves = rank.status === "ok" ? rank.data.moves : [];
  // Standing-condition snapshot (#1902): no cursor, so it participates in
  // the grouped body only — never in unseen/overflow/dismiss accounting.
  const staleTheses =
    thesisStale.status === "ok" ? thesisStale.data.items : [];
  const thesisChanges =
    thesisChange.status === "ok" ? thesisChange.data.changes : [];

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
    rank: rank.status === "ok" ? rank.data.alerts_last_seen_rank_event_id : null,
    thesisChange:
      thesisChange.status === "ok"
        ? thesisChange.data.alerts_last_seen_thesis_change_id
        : null,
  };

  const items = buildAlertModel(
    rejections,
    positionAlerts,
    drops,
    moves,
    cursors,
    staleTheses,
    thesisChanges,
  );

  if (items.length === 0) {
    return null;
  }

  const unseenGuard = guard.status === "ok" ? guard.data.unseen_count : 0;
  const unseenPosition = position.status === "ok" ? position.data.unseen_count : 0;
  const unseenCoverage = coverage.status === "ok" ? coverage.data.unseen_count : 0;
  const unseenRank = rank.status === "ok" ? rank.data.unseen_count : 0;
  const unseenThesisChange =
    thesisChange.status === "ok" ? thesisChange.data.unseen_count : 0;

  // Overflow math compares backend unseen counts to RAW fetched row counts (each feed caps
  // at LIMIT 500; thesis changes at 50) — NOT to the grouped card count.
  const renderedGuard = rejections.length;
  const renderedPosition = positionAlerts.length;
  const renderedCoverage = drops.length;
  const renderedRank = moves.length;
  const renderedThesisChange = thesisChanges.length;

  const totalUnseen =
    unseenGuard + unseenPosition + unseenCoverage + unseenRank + unseenThesisChange;

  const anyOverflow =
    unseenGuard > renderedGuard ||
    unseenPosition > renderedPosition ||
    unseenCoverage > renderedCoverage ||
    unseenRank > renderedRank ||
    unseenThesisChange > renderedThesisChange;

  const overflowAck = anyOverflow;
  const normalAck = totalUnseen > 0 && !anyOverflow;

  async function onMarkAllRead() {
    // Advance each cursor to the max id in its RAW feed array (BIGSERIAL id, not timestamp).
    const guardMax = Math.max(0, ...rejections.map((r) => r.decision_id));
    const positionMax = Math.max(0, ...positionAlerts.map((r) => r.alert_id));
    const coverageMax = Math.max(0, ...drops.map((r) => r.event_id));
    const rankMax = Math.max(0, ...moves.map((r) => r.score_id));
    const thesisChangeMax = Math.max(0, ...thesisChanges.map((r) => r.thesis_id));

    const promises: Promise<void>[] = [];
    if (guardMax > 0) promises.push(markAlertsSeen(guardMax));
    if (positionMax > 0) promises.push(markPositionAlertsSeen(positionMax));
    if (coverageMax > 0) promises.push(markCoverageStatusDropsSeen(coverageMax));
    if (rankMax > 0) promises.push(markRankMovesSeen(rankMax));
    if (thesisChangeMax > 0) promises.push(markThesisChangesSeen(thesisChangeMax));

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
      Math.max(0, unseenCoverage - renderedCoverage) +
      Math.max(0, unseenRank - renderedRank) +
      Math.max(0, unseenThesisChange - renderedThesisChange);
    const msg = `Dismiss all ${totalUnseen} unseen alerts? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
    if (!window.confirm(msg)) return;

    const promises: Promise<void>[] = [];
    if (guard.status === "ok") promises.push(dismissAllAlerts());
    if (position.status === "ok") promises.push(dismissAllPositionAlerts());
    if (coverage.status === "ok") promises.push(dismissAllCoverageStatusDrops());
    if (rank.status === "ok") promises.push(dismissAllRankMoves());
    // No dismiss-all endpoint (#2013): the DESC list always contains the
    // newest material change, so seen-through-the-max-listed-id clears all.
    {
      const thesisChangeMax = Math.max(0, ...thesisChanges.map((r) => r.thesis_id));
      if (thesisChangeMax > 0) promises.push(markThesisChangesSeen(thesisChangeMax));
    }

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
      className="border-t border-slate-200 dark:border-slate-800 pt-3"
    >
      <header className="flex items-baseline justify-between gap-2">
        <div className="flex items-baseline gap-2">
          <h2
            id="alerts-strip-heading"
            className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-700"
          >
            Alerts
          </h2>
          {totalUnseen > 0 ? (
            <span className="rounded-full bg-amber-100 dark:bg-amber-900/40 px-2 py-0.5 text-xs font-medium text-amber-700 dark:text-amber-300">
              {totalUnseen} new
            </span>
          ) : null}
        </div>
        {normalAck ? (
          <button
            type="button"
            onClick={onMarkAllRead}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
          >
            Mark all read
          </button>
        ) : null}
        {overflowAck ? (
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onDismissAll}
              className="rounded border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/40 px-2 py-1 text-xs font-medium text-amber-800 dark:text-amber-200 hover:bg-amber-100"
            >
              Dismiss all ({totalUnseen}) as acknowledged
            </button>
            <Link
              to="/recommendations"
              className="text-xs text-slate-500 dark:text-slate-400 underline hover:text-slate-700"
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
        className="max-h-96 overflow-y-auto divide-y divide-slate-100 dark:divide-slate-800"
      >
        {items.map((item) => (
          <ItemView key={item.id} item={item} />
        ))}
      </div>
    </section>
  );
}
