import { Link } from "react-router-dom";

import {
  fetchCoreSleeve,
  fetchStrategyOverview,
  fetchStrategyOwnedPositions,
  fetchStrategyPnlHistory,
} from "@/api/strategies";
import type { StrategyOwnedPosition } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { StatTile } from "@/components/dashboard/StatTile";
import { BenchmarkRefusals, BlockerRow } from "@/components/strategies/StrategyPortfolioPanels";
import { Badge } from "@/components/ui/Badge";
import { formatDate, formatMoney, formatPct } from "@/lib/format";
import { investNarrative } from "@/lib/investSummary";
import { namedList, potWealthSummary } from "@/lib/strategyAggregate";
import { number } from "@/lib/strategyFormat";
import { strategyPortfolioStatus } from "@/lib/strategyPortfolioStatus";
import { useAsync } from "@/lib/useAsync";

function toneOf(value: number | null): "muted" | "positive" | "negative" {
  if (value === null || value === 0) return "muted";
  return value > 0 ? "positive" : "negative";
}

function HoldingRow({ position }: { position: StrategyOwnedPosition }) {
  const pnl = number(position.unrealised_pnl);
  return (
    <li className="flex flex-wrap items-baseline justify-between gap-2 py-2 text-sm">
      <span>
        <span className="font-semibold">{position.symbol}</span>
        <span className="text-slate-500"> · {position.strategy_title}</span>
      </span>
      <span className="tabular-nums">
        {position.valuation_available ? formatMoney(number(position.current_value), position.currency) : "Not valued"}
        {position.valuation_available && pnl !== null ? (
          <span
            className={
              pnl === 0
                ? "text-slate-500"
                : pnl > 0
                  ? "text-emerald-700 dark:text-emerald-400"
                  : "text-red-700 dark:text-red-400"
            }
          >
            {" "}
            ({formatMoney(pnl, position.currency)})
          </span>
        ) : null}
      </span>
    </li>
  );
}

/**
 * `/invest` (#3423) — the one page the operator needs: how much the engine is
 * trusted with, where that money is, how it is doing, and what happens next.
 *
 * Operator north star (2026-09-26): "put money in and then take my hands off
 * the wheel". Everything detailed stays on `/strategies` (linked as Advanced);
 * this page READS the same four endpoints the Portfolio lens reads and adds no
 * machinery. Slice 1 is read-only — the amount, risk and on/off controls land
 * in a later slice, and until then the Setup lens is linked for them.
 *
 * ⚠ The first-class message while it is true is "no strategy has passed yet —
 * your money is in the index sleeve", and it is said only when the positions
 * read shows a sleeve holding (`investNarrative`).
 */
export function InvestPage() {
  const overview = useAsync(fetchStrategyOverview, []);
  const coreSleeve = useAsync(fetchCoreSleeve, []);
  const ownedPositions = useAsync(fetchStrategyOwnedPositions, []);
  const pnlHistory = useAsync(fetchStrategyPnlHistory, []);

  const header = (
    <header>
      <h1 className="text-xl font-semibold">Invest</h1>
      <p className="mt-1 text-sm text-slate-500">
        What the engine is doing with your money, and how it is doing.
      </p>
    </header>
  );

  if (overview.loading) {
    return (
      <div className="space-y-6 pt-6">
        {header}
        <SectionSkeleton rows={6} />
      </div>
    );
  }
  if (overview.error || !overview.data) {
    return (
      <div className="space-y-6 pt-6">
        {header}
        <SectionError onRetry={overview.refetch} />
      </div>
    );
  }

  const data = overview.data;
  const pool = data.paper_pool;
  const core = coreSleeve.data ?? null;
  const positions = ownedPositions.data?.positions ?? null;
  const status = strategyPortfolioStatus(data, core);
  const narrative = investNarrative(data, core, positions, {
    core: coreSleeve.error !== null && !coreSleeve.data,
    positions: ownedPositions.error !== null && !ownedPositions.data,
  });
  const wealth = potWealthSummary(pnlHistory.data?.points ?? []);
  const returnSince = pnlHistory.data?.return_since ?? null;
  const trackerRefusal = data.benchmark_refusals.find((refusal) => refusal.benchmark === "sp500_total_return");
  // #3336: holdings arrive in the display currency, the pot in USD. Nothing on
  // this page converts between them, so say which is which.
  const holdingCurrencies = namedList(
    (positions ?? [])
      .filter((position) => position.valuation_available && position.currency !== pool.currency)
      .map((position) => position.currency),
  );

  return (
    <div className="space-y-8 pt-6">
      {header}

      <section aria-labelledby="invest-status" className="border border-slate-200 bg-white p-5 dark:border-slate-800 dark:bg-slate-900">
        <h2 id="invest-status" className="flex items-center gap-2 text-lg font-semibold">
          {status.headline}
          <Badge tone={status.tone}>{status.badge}</Badge>
        </h2>
        <p className="mt-3 text-base text-slate-800 dark:text-slate-100">{narrative.whereMoney}</p>
        {/* A refusal outranks everything: the operator must see why nothing
            can be bought before any number. Setup gaps are listed too — on
            this page they are the reason the money is idle. */}
        {status.blockers.length > 0 ? (
          <div className="mt-4" aria-label="What is stopping the engine">
            {status.blockers.map((blocker) => (
              <BlockerRow
                key={`${blocker.key}:${blocker.label}`}
                tone={blocker.key === "global_kill" ? "risk" : "warn"}
                label={blocker.detail ? `${blocker.label} — ${blocker.detail}` : blocker.label}
              />
            ))}
          </div>
        ) : null}
      </section>

      <section aria-labelledby="invest-money">
        <h2 id="invest-money" className="sr-only">Money</h2>
        <div className="grid grid-cols-2 gap-x-6 lg:grid-cols-4">
          <StatTile
            label="Amount assigned"
            value={pool.configured ? formatMoney(number(pool.capital_limit), pool.currency) : "None"}
            hint={pool.configured ? `${pool.capital_mode === "compound" ? "Moving" : "Fixed"} budget${pool.enabled ? "" : " · paused"}` : "Nothing assigned yet"}
          />
          <StatTile
            label="Value now"
            value={pnlHistory.error ? "—" : formatMoney(wealth?.potValue ?? null, pool.currency)}
            hint={wealth ? `At the ${formatDate(wealth.date)} close` : pnlHistory.error ? "History unavailable" : "No close recorded yet"}
          />
          <StatTile
            label="Gain / loss"
            value={pnlHistory.error ? "—" : formatMoney(wealth?.totalPnl ?? null, pool.currency)}
            hint={
              pnlHistory.error
                ? "History unavailable"
                : wealth?.cumulativeReturn != null && returnSince
                  ? `${formatPct(wealth.cumulativeReturn)} since ${formatDate(returnSince)}`
                  : "No return % yet"
            }
            tone={toneOf(wealth?.totalPnl ?? null)}
            toneHint
          />
          {/* No number, by the refusal's own label (#2602 item 5): a tracker
              ETF's price is not the index's total return. */}
          <StatTile label="vs tracker" value="—" hint={trackerRefusal ? "No licensed benchmark yet" : "Not reported"} />
        </div>
      </section>

      <section aria-labelledby="invest-holdings" className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900">
        <h2 id="invest-holdings" className="text-sm font-semibold">What it holds</h2>
        {ownedPositions.loading ? <SectionSkeleton rows={2} /> : null}
        {ownedPositions.error ? <SectionError onRetry={ownedPositions.refetch} /> : null}
        {positions !== null && positions.length === 0 ? (
          <p className="mt-2 text-sm text-slate-500">Nothing is held yet.</p>
        ) : null}
        {positions !== null && positions.length > 0 ? (
          <ul className="mt-1 divide-y divide-slate-200 dark:divide-slate-800">
            {positions.map((position) => (
              <HoldingRow key={`${position.strategy_trade_id}:${position.broker_position_id}`} position={position} />
            ))}
          </ul>
        ) : null}
        {holdingCurrencies.length > 0 ? (
          <p className="mt-2 text-xs text-slate-500">
            Holdings are in {holdingCurrencies.join(" / ")}, your display currency; the figures above are in {pool.currency}.
          </p>
        ) : null}
      </section>

      <section aria-labelledby="invest-next" className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900">
        <h2 id="invest-next" className="text-sm font-semibold">What happens next</h2>
        {coreSleeve.error ? <SectionError onRetry={coreSleeve.refetch} /> : null}
        <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700 dark:text-slate-200">
          {narrative.next.map((line) => (
            <li key={line}>{line}</li>
          ))}
        </ul>
        <BenchmarkRefusals refusals={data.benchmark_refusals} />
      </section>

      <nav aria-label="Advanced" className="flex flex-wrap gap-x-6 gap-y-2 text-sm">
        <Link to="/strategies?view=setup" className="text-blue-700 hover:underline dark:text-blue-400">
          Change amount, risk or on/off
        </Link>
        <Link to="/strategies" className="text-blue-700 hover:underline dark:text-blue-400">
          Advanced: full portfolio and strategy evidence
        </Link>
      </nav>
    </div>
  );
}
