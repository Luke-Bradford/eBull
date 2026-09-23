import { useMemo, useState } from "react";

import { postKillSwitch } from "@/api/config";
import {
  closeStrategyOwnedPosition,
  fetchCoreSleeve,
  fetchStrategyOrderActivity,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
} from "@/api/strategies";
import type {
  CoreSleeveResponse,
  StrategyOrderActivityResponse,
  StrategyOverviewResponse,
  StrategyOwnedPosition,
  StrategyPendingEntry,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { LiveQuoteProvider } from "@/components/quotes/LiveQuoteProvider";
import { EmptyState } from "@/components/states/EmptyState";
import { OpenStrategyPositions, StrategyCloseModal } from "@/components/strategies/StrategyPositions";
import {
  AccountEvidence,
  BenchmarkRefusals,
  BlockerRow,
  EmptyPnlChart,
  PnlChart,
} from "@/components/strategies/StrategyPortfolioPanels";
import { Badge } from "@/components/ui/Badge";
import { Modal } from "@/components/ui/Modal";
import { formatDate, formatDateTime, formatMoney, formatNumber, formatPct, formatUnsignedPct } from "@/lib/format";
import { aggregate, namedList, positionsOutsideStrategyPnl, potWealthSummary } from "@/lib/strategyAggregate";
import { money, number } from "@/lib/strategyFormat";
import { strategyPortfolioStatus } from "@/lib/strategyPortfolioStatus";
import { useAsync } from "@/lib/useAsync";

function toneOf(value: number | null): "muted" | "positive" | "negative" {
  if (value === null || value === 0) return "muted";
  return value > 0 ? "positive" : "negative";
}

/**
 * The engine's working orders, as far as the app can see them (#3334).
 *
 * ⚠ Derived, not a broker order book: a close the engine submitted and has not
 * yet reconciled (`trade_status = closing`), the core sleeve's unresolved order
 * (`pending_order_id`), and alpha entries still `planned` or `submitted`
 * (`/strategies/order-activity`). The endpoint leaves the core arm out of
 * `pending_entries` precisely so the core order is listed once, here.
 */
/** ⚠ A `planned` trade with an order row may already be at the broker (the
 *  executor commits the order before the call), so only a trade with NO order
 *  row is "not yet sent" (Codex ckpt-2 on #3334 slice B). */
function entryState(entry: StrategyPendingEntry): string {
  if (entry.trade_status === "reconcile_required") {
    return entry.order_id === null ? "Outcome unknown, reconciling" : `Order #${entry.order_id} outcome unknown, reconciling`;
  }
  if (entry.order_id === null) return "Planned, not yet sent";
  return `Order #${entry.order_id} sent, awaiting fill`;
}

function EngineOrders({
  positions,
  coreSleeve,
  coreSleeveFailed,
  onRetryCore,
  pendingEntries,
  pendingTruncated,
  activityFailed,
  onRetryActivity,
  positionsFailed,
}: {
  /** `null` while the positions read is unresolved or failed — NOT "no closes". */
  positions: readonly StrategyOwnedPosition[] | null;
  /** `null` while the core-sleeve read is unresolved or failed — NOT "no order". */
  coreSleeve: CoreSleeveResponse | null;
  coreSleeveFailed: boolean;
  onRetryCore: () => void;
  /** `null` while the activity read is unresolved or failed — NOT "no entries". */
  pendingEntries: readonly StrategyPendingEntry[] | null;
  pendingTruncated: boolean;
  activityFailed: boolean;
  onRetryActivity: () => void;
  /** The positions error renders once, on Holdings; here it only blocks "empty". */
  positionsFailed: boolean;
}) {
  const closing = (positions ?? []).filter((position) => position.trade_status === "closing");
  const corePending = coreSleeve?.pending_order_id ?? null;
  const entries = pendingEntries ?? [];
  // ⚠ Unknown is not empty (Codex ckpt-2 on #3334): until ALL three reads resolve,
  // "No engine order is working" would be a claim about orders nobody has looked for.
  const unknown = coreSleeve === null || pendingEntries === null || positions === null;
  const failed = coreSleeveFailed || activityFailed || positionsFailed;
  const nothing = closing.length === 0 && corePending === null && entries.length === 0;
  return (
    <section aria-labelledby="engine-orders" className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900">
      <h2 id="engine-orders" className="text-sm font-semibold">Engine orders</h2>
      {coreSleeveFailed ? (
        <div className="mt-2">
          <SectionError onRetry={onRetryCore} />
        </div>
      ) : null}
      {activityFailed ? (
        <div className="mt-2">
          <SectionError onRetry={onRetryActivity} />
        </div>
      ) : null}
      {unknown && !failed ? <p className="mt-2 text-sm text-slate-500">Checking for working orders…</p> : null}
      {nothing ? (
        unknown ? null : (
          <p className="mt-2 text-sm text-slate-500">No engine order is working. Entries, closes and core-sleeve orders appear here until the broker confirms them.</p>
        )
      ) : (
        <ul className="mt-2 divide-y divide-slate-200 text-sm dark:divide-slate-800">
          {corePending !== null ? (
            <li className="flex flex-wrap justify-between gap-2 py-2">
              <span>Core sleeve order #{corePending}{coreSleeve?.selected_symbol ? ` · ${coreSleeve.selected_symbol}` : ""}</span>
              <span className="text-xs text-amber-700 dark:text-amber-300">Awaiting broker reconciliation</span>
            </li>
          ) : null}
          {entries.map((entry) => (
            <li key={`entry:${entry.strategy_trade_id}`} className="flex flex-wrap justify-between gap-2 py-2">
              <span>
                Buy {entry.symbol} · {entry.strategy_title}
                {entry.funded_amount !== null ? <span className="text-slate-500"> · {money(entry.funded_amount)}</span> : null}
              </span>
              <span className="text-xs text-slate-500">
                {entryState(entry)}
              </span>
            </li>
          ))}
          {closing.map((position) => (
            <li key={`${position.strategy_trade_id}:${position.broker_position_id}`} className="flex flex-wrap justify-between gap-2 py-2">
              <span>Close {position.symbol} · {position.strategy_title}</span>
              <span className="text-xs text-slate-500">Submitted, awaiting fill</span>
            </li>
          ))}
        </ul>
      )}
      {pendingTruncated ? (
        <p className="mt-2 text-xs text-amber-700 dark:text-amber-300">More engine entries are working than this list shows.</p>
      ) : null}
    </section>
  );
}

/**
 * Recent broker-confirmed fills on engine-owned positions (#3334), newest first.
 *
 * Money is the pot's USD (`money_currency`); the fill price is the instrument's
 * own currency, labelled as such. P&L and fees exist on closes only — an open
 * shows a dash rather than a zero it never had.
 */
function RecentFills({
  activity,
  loading,
  failed,
  onRetry,
}: {
  activity: StrategyOrderActivityResponse | null;
  loading: boolean;
  failed: boolean;
  onRetry: () => void;
}) {
  return (
    <section aria-labelledby="recent-fills" className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900">
      <h2 id="recent-fills" className="text-sm font-semibold">Recent fills</h2>
      {failed ? (
        <div className="mt-2">
          <SectionError onRetry={onRetry} />
        </div>
      ) : loading || activity === null ? (
        <SectionSkeleton rows={2} />
      ) : activity.fills.length === 0 ? (
        <p className="mt-2 text-sm text-slate-500">No fill yet. Opens and closes the broker confirms appear here.</p>
      ) : (
        <div className="mt-2 overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-slate-500">
              <tr>
                <th className="py-1 pr-3 font-medium">When</th>
                <th className="py-1 pr-3 font-medium">Fill</th>
                <th className="py-1 pr-3 font-medium">Strategy</th>
                <th className="py-1 pr-3 text-right font-medium">Units</th>
                <th className="py-1 pr-3 text-right font-medium">Price</th>
                <th className="py-1 text-right font-medium">Realised P&L</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-200 dark:divide-slate-800">
              {activity.fills.map((fill) => {
                const pnl = number(fill.realised_pnl);
                return (
                  <tr key={fill.event_id}>
                    <td className="py-1.5 pr-3 whitespace-nowrap">{formatDateTime(fill.executed_at)}</td>
                    <td className="py-1.5 pr-3">
                      {fill.event_kind === "open" ? "Open" : "Close"} {fill.symbol}
                    </td>
                    <td className="py-1.5 pr-3 text-slate-600 dark:text-slate-300">{fill.strategy_title}</td>
                    <td className="py-1.5 pr-3 text-right tabular-nums">{formatNumber(number(fill.units), 4)}</td>
                    <td className="py-1.5 pr-3 text-right tabular-nums">
                      {fill.price_currency ? formatMoney(number(fill.price), fill.price_currency) : formatNumber(number(fill.price), 2)}
                    </td>
                    <td
                      className={`py-1.5 text-right tabular-nums ${
                        pnl === null || pnl === 0 ? "" : pnl > 0 ? "text-emerald-700 dark:text-emerald-400" : "text-rose-700 dark:text-rose-400"
                      }`}
                    >
                      {fill.event_kind === "close" ? formatMoney(pnl, activity.money_currency) : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/**
 * Portfolio lens of `/strategies` — READ-ONLY apart from closing positions (#3334).
 *
 * Operator feedback 2026-09-23: the page opened on configuration and made them
 * scroll past a form to see results. Inverted pyramid now: status and the
 * summary strip, then what is held, then what is working, then performance.
 * Every write that configures the pot lives on the Setup lens; closing stays
 * here, separated to the right and always confirmed, because it acts on what
 * this lens shows.
 *
 * ⚠ The kill-switch Clear stays here too. It is not configuration: it is the
 * control attached to a blocker (the `BlockerRow` rule — a blocker the operator
 * can clear carries its own control), and a halted pot is exactly what this lens
 * must lead with.
 */
export function StrategyPortfolioLens() {
  const overview = useAsync(fetchStrategyOverview, []);
  const coreSleeve = useAsync(fetchCoreSleeve, [], { preserveOnRefetch: true });
  /** ⚠ `preserveOnRefetch` so the positions count's honest `—` (#3222) is the
   *  FIRST load only; a close refetches this list. */
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, [], { preserveOnRefetch: true });
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);
  const activity = useAsync(fetchStrategyOrderActivity, [], { preserveOnRefetch: true });
  const [closeFor, setCloseFor] = useState<StrategyOwnedPosition | null>(null);
  const [confirmCloseAll, setConfirmCloseAll] = useState(false);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const exitTimingById = useMemo(
    () => new Map((overview.data?.strategies ?? []).map((strategy) => [strategy.strategy_id, strategy.exit_timing])),
    [overview.data],
  );

  if (overview.loading) return <SectionSkeleton rows={6} />;
  if (overview.error || !overview.data) return <SectionError onRetry={overview.refetch} />;

  const data: StrategyOverviewResponse = overview.data;
  const status = strategyPortfolioStatus(data, coreSleeve.data ?? null);
  const summary = aggregate(data);
  const pool = data.paper_pool;
  const positions = ownedPositions.data?.positions ?? [];
  /** What the `Strategy total P&L` tile cannot account for. Derived from the
   *  page's own position list so the caveat and the table can never disagree.
   *  Empty while the fetch is unresolved: the caveat asserts an exclusion, and
   *  asserting one before the list is known would be a claim about positions
   *  nobody has read yet. */
  const outsideStrategyPnl = positionsOutsideStrategyPnl(positions);
  const excludedTitles = namedList(outsideStrategyPnl.map((position) => position.strategy_title));
  /** ⚠ Only the currencies this total genuinely CANNOT take, i.e. those that
   *  differ from the pool's (review WARNING on PR #3226). */
  const unconvertibleCurrencies = namedList(
    outsideStrategyPnl.map((position) => position.currency).filter((code) => code !== pool.currency),
  );
  /** ⚠ Bulk close operates on CLOSABLE positions only. A row whose trade is
   *  already `closing` disables its own Close button, and resubmitting it makes
   *  the endpoint reject — which, because the loop stops on first failure,
   *  would strand every genuinely open position behind it (Codex ckpt-2). */
  /** Valued holdings reported in a currency other than the pot's (#3336).
   *  Unvalued rows render only dashes, so their placeholder label is skipped. */
  const holdingCurrencies = namedList(
    positions
      .filter((position) => position.valuation_available && position.currency !== pool.currency)
      .map((position) => position.currency),
  );
  const closable = positions.filter((position) => position.trade_status !== "closing");
  const killActive = data.entry_block.global_kill_active;
  const wealth = potWealthSummary(pnlHistory.data?.points ?? []);
  // #3334 item 3: the server dates the time-weighted return. It is inception
  // only when inception is inside the history window, so it is always labelled.
  const returnSince = pnlHistory.data?.return_since ?? null;
  const sp500Refusal = data.benchmark_refusals.find((refusal) => refusal.benchmark === "sp500_total_return");

  async function clearKillSwitch() {
    setBusy(true);
    setActionError(null);
    try {
      await postKillSwitch({
        active: false,
        reason: "Operator cleared the automated-pot kill switch from the portfolio panel",
        activated_by: "operator",
      });
      await overview.refetch();
    } catch (error) {
      setActionError(error instanceof ApiError ? error.message : "The kill switch could not be cleared.");
    } finally {
      setBusy(false);
    }
  }

  /** Sequential, not parallel: each close is a broker order, and a partial
   *  failure must leave the rest closed rather than racing an unknown number
   *  of submissions. Stops on the first failure and reports what remains. */
  async function closeAll() {
    setBusy(true);
    setActionError(null);
    let closed = 0;
    try {
      for (const position of closable) {
        await closeStrategyOwnedPosition(position.strategy_trade_id, position.broker_position_id);
        closed += 1;
      }
    } catch (error) {
      const detail = error instanceof ApiError ? error.message : "A close request failed.";
      setActionError(`${detail} ${closed} of ${closable.length} closed; the rest are still open.`);
    } finally {
      setBusy(false);
      setConfirmCloseAll(false);
      void ownedPositions.refetch();
      void overview.refetch();
      void pnlHistory.refetch();
      void activity.refetch();
    }
  }

  return (
    <div className="space-y-8">
      <section aria-labelledby="pot-state">
        <h2 id="pot-state" className="flex items-center gap-2 text-lg font-semibold">
          {status.headline}
          {/* ⚠ Four states, not two (#3222), and the verdict names its own
              badge rather than the component deriving one from `trading`:
              `checking` is "not yet known" and `settling` is a recovery that
              is neither live nor halted. */}
          <Badge tone={status.tone}>{status.badge}</Badge>
        </h2>

        <div className="mt-4 grid grid-cols-2 gap-x-6 sm:grid-cols-3 lg:grid-cols-5">
          {/* #3334: the strip reads ONE series — the end-of-day mark-to-market
              NAV (`/strategies/wealth-history`) — so pot value, P&L and the
              day's change can never disagree with each other or the chart.
              The positions table below is live; this is the last close, and
              every hint says which close. */}
          <StatTile
            label="Pot value"
            value={pnlHistory.error ? "—" : formatMoney(wealth?.potValue ?? null, pool.currency)}
            hint={wealth ? `${formatDate(wealth.date)} close · ${pool.capital_mode === "compound" ? "moving" : "fixed"} budget ${formatMoney(number(pool.capital_limit), pool.currency)}` : pnlHistory.error ? "History unavailable" : "No close recorded yet"}
          />
          <StatTile
            label="P&L since start"
            value={pnlHistory.error ? "—" : formatMoney(wealth?.totalPnl ?? null, pool.currency)}
            hint={
              wealth
                ? wealth.cumulativeReturn !== null && returnSince
                  ? `${formatPct(wealth.cumulativeReturn)} time-weighted since ${formatDate(returnSince)}`
                  : "Realised + open · no return % yet"
                : "—"
            }
            tone={toneOf(wealth?.totalPnl ?? null)}
            toneHint
          />
          <StatTile
            label="Last day"
            value={pnlHistory.error ? "—" : formatMoney(wealth?.dayPnl ?? null, pool.currency)}
            hint={(() => {
              // The server's period return can span a bridged incomplete close,
              // where the adjacent-close money change is unknown — show it anyway.
              const pct =
                wealth?.periodReturn != null && wealth.periodStart
                  ? `${formatPct(wealth.periodReturn)} since ${formatDate(wealth.periodStart)} close`
                  : null;
              if (wealth?.dayPnl != null) {
                return `${formatDate(wealth.date)} vs prior close, net of funding${pct ? ` · ${pct}` : ""}`;
              }
              return pct ?? "Needs two closes";
            })()}
            tone={toneOf(wealth?.dayPnl ?? null)}
            toneHint
          />
          {/* No number, and the tile says why by the refusal's own label
              (#2602 item 5): a tracking ETF's price is not the index's total
              return, and substituting one is the mislabelling that refusal
              exists to prevent. The full reasons render under the chart. */}
          <StatTile
            label="vs S&P 500"
            value="—"
            hint={sp500Refusal ? "No licensed benchmark" : "Not reported"}
          />
          {/* Budget, not broker cash: `remaining_capital` is the pot's
              assignment minus committed authority and never reads the
              broker's cash balance (Codex ckpt-2 on #3334). */}
          <StatTile
            label="Budget available"
            value={formatMoney(number(pool.capital_observation_complete === false ? null : pool.remaining_capital), pool.currency)}
            hint={pool.capital_observation_complete === false ? "Checked at each order" : "Budget less commitments"}
          />
        </div>

        {actionError ? (
          <p role="alert" className="mt-3 text-sm text-rose-700 dark:text-rose-300">
            {actionError}
          </p>
        ) : null}

        {status.blockers.length ? (
          <div className="mt-5" aria-label="Blocking conditions">
            {killActive ? (
              <BlockerRow
                tone="risk"
                label={`Kill switch on — ${data.entry_block.global_kill_reason ?? "no reason recorded"}`}
                action={
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => void clearKillSwitch()}
                    className="min-h-11 rounded-md border border-slate-300 px-3 text-sm font-medium hover:bg-slate-50 disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
                  >
                    Clear
                  </button>
                }
              />
            ) : null}
            {/* Only what the Setup lens does NOT already show. Capital,
                mandate and the on/off switch are fields there. What stays is
                what no field can fix: evidence a strategy has not earned yet. */}
            {status.blockers
              .filter((blocker) => blocker.key === "no_approved_strategies")
              .map((blocker) => (
                <BlockerRow
                  key={blocker.key}
                  tone="warn"
                  label={blocker.detail ? `${blocker.label} — ${blocker.detail}` : blocker.label}
                />
              ))}
          </div>
        ) : null}
      </section>

      <section aria-labelledby="pot-holdings" className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 id="pot-holdings" className="text-sm font-semibold">
            Holdings{" "}
            <span className="font-normal text-slate-500">
              · {formatNumber(ownedPositions.data ? positions.length : null, 0)} open
            </span>
          </h2>
          {/* Destructive, so set apart on the right and always confirmed. */}
          {closable.length > 0 ? (
            <button
              type="button"
              disabled={busy}
              onClick={() => setConfirmCloseAll(true)}
              className="min-h-11 rounded-md border border-rose-300 px-3 text-sm font-medium text-rose-700 hover:bg-rose-50 disabled:opacity-50 dark:border-rose-800 dark:text-rose-300 dark:hover:bg-rose-950/40"
            >
              Close all {closable.length} positions
            </button>
          ) : null}
        </div>
        {ownedPositions.loading ? <SectionSkeleton rows={3} /> : null}
        {ownedPositions.error ? <SectionError onRetry={ownedPositions.refetch} /> : null}
        {/* #3336: holdings arrive in the operator's display currency (converted in
            `app/api/portfolio.py`), the pot above in its USD contract currency.
            Nothing on this page converts between them, so say which is which. */}
        {holdingCurrencies.length > 0 ? (
          <p className="text-xs text-slate-500">
            Holdings are in {holdingCurrencies.join(" / ")}, your display currency. The pot figures above are in{" "}
            {pool.currency}, the pot&apos;s own currency, so the two are not directly comparable.
          </p>
        ) : null}
        {ownedPositions.data && positions.length > 0 ? (
          <LiveQuoteProvider instrumentIds={ownedPositions.data.live_quote_instrument_ids}>
            <OpenStrategyPositions positions={positions} exitTimingById={exitTimingById} onClose={setCloseFor} />
          </LiveQuoteProvider>
        ) : null}
        {ownedPositions.data && positions.length === 0 ? (
          <EmptyState title="Nothing held" description="Positions opened by an approved strategy appear here." />
        ) : null}
        {/* Mounted independently of the positions read (Codex ckpt-2): an
            unrelated broker-position failure must not hide pending entries. */}
        <EngineOrders
          positions={ownedPositions.data ? positions : null}
          positionsFailed={ownedPositions.error !== null && !ownedPositions.data}
          coreSleeve={coreSleeve.data ?? null}
          coreSleeveFailed={coreSleeve.error !== null && !coreSleeve.data}
          onRetryCore={coreSleeve.refetch}
          pendingEntries={activity.data?.pending_entries ?? null}
          pendingTruncated={activity.data?.pending_entries_truncated ?? false}
          activityFailed={activity.error !== null && !activity.data}
          onRetryActivity={activity.refetch}
        />
        <RecentFills
          activity={activity.data ?? null}
          loading={activity.loading}
          failed={activity.error !== null && !activity.data}
          onRetry={activity.refetch}
        />
      </section>

      <section aria-labelledby="pot-performance">
        <h2 id="pot-performance" className="text-sm font-semibold">
          Portfolio performance
        </h2>
        {pnlHistory.loading ? (
          <div className="flex h-52 items-center justify-center text-xs text-slate-500">Loading…</div>
        ) : pnlHistory.error ? (
          <SectionError onRetry={pnlHistory.refetch} />
        ) : pnlHistory.data?.points.length ? (
          <PnlChart history={pnlHistory.data.points} />
        ) : (
          <EmptyPnlChart />
        )}
        {/* #2602 item 5. Sourced from `overview`, not `pnlHistory` — see the
            component. A benchmark that is absent must say so by name in every
            branch above, including the error one. */}
        <BenchmarkRefusals refusals={data.benchmark_refusals} />
        {/* The strategies' own trade record. Automated positions only;
            research backtests are excluded by `aggregate`. */}
        <div className="mt-5 grid grid-cols-2 gap-x-6 sm:grid-cols-3">
          {/* "Strategy", not pot-wide: this sums `pnl.total_pnl` over
              `overview.strategies` only, and the core sleeve is in no
              strategy's block (#3222 residual 2). The caveat below names what
              it leaves out. */}
          <StatTile size="md" label="Strategy total P&L" value={formatMoney(summary.totalPnl, pool.currency)} hint="Realised + open" />
          <StatTile size="md" label="Average / trade" value={formatPct(summary.averageReturn)} hint="Completed outcomes" />
          {/* UNSIGNED beside a SIGNED expectancy on purpose (#3032): a win rate
              is a composition, not a return. */}
          <StatTile size="md" label="Win rate" value={formatUnsignedPct(summary.successRate)} hint={`${formatNumber(summary.resolved, 0)} completed`} />
        </div>
        {outsideStrategyPnl.length > 0 ? (
          <p className="mt-2 text-xs text-amber-700 dark:text-amber-300">
            ⚠ Strategy figures exclude {formatNumber(outsideStrategyPnl.length, 0)}{" "}
            {outsideStrategyPnl.length === 1 ? "position" : "positions"} held outside the
            strategies ({excludedTitles.join(", ")}).
            {unconvertibleCurrencies.length > 0
              ? ` Reported in ${unconvertibleCurrencies.join(" / ")}, which this ${pool.currency} total cannot convert.`
              : ""}
          </p>
        ) : null}
        {/* Whether our P&L agrees with the broker's own equity — the reason to
            trust the numbers above, not commentary about them. */}
        <AccountEvidence overview={data} />
      </section>

      <StrategyCloseModal
        position={closeFor}
        onRequestClose={() => setCloseFor(null)}
        onAccepted={() => {
          setCloseFor(null);
          void ownedPositions.refetch();
          void overview.refetch();
          void pnlHistory.refetch();
          void activity.refetch();
        }}
      />

      <Modal isOpen={confirmCloseAll} onRequestClose={() => setConfirmCloseAll(false)} labelledBy="close-all-title">
          <h2 id="close-all-title" className="text-base font-semibold">
            Close all {closable.length} automated positions?
          </h2>
          <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
            Each is submitted to the broker one at a time. Manual positions are not touched. This
            does not stop the strategies — turn automation off on the Setup lens if you also want new entries to stop.
          </p>
          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => setConfirmCloseAll(false)}
              className="min-h-11 rounded-md border border-slate-300 px-3 text-sm dark:border-slate-700"
            >
              Cancel
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => void closeAll()}
              className="min-h-11 rounded-md bg-rose-600 px-3 text-sm font-medium text-white hover:bg-rose-700 disabled:opacity-50"
            >
              {busy ? "Closing…" : `Close ${closable.length}`}
            </button>
          </div>
      </Modal>
    </div>
  );
}
