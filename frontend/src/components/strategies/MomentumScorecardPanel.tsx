import { fetchMomentumScorecard } from "@/api/strategies";
import type { MomentumPeriodScore, MomentumScorecardResponse } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Badge } from "@/components/ui/Badge";
import { formatDate, formatPct } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "Sep 2026" from a formation month's first day; fixed labels, not locale-dependent. */
function monthLabel(iso: string): string {
  const [year, month] = iso.split("-");
  return `${MONTHS[Number(month) - 1] ?? month} ${year}`;
}

/** "72%" (the share of random baskets the book beat), or a dash when the control is not scored. */
function percentileText(p: number | null): string {
  return p === null ? "—" : `${Math.round(p * 100)}%`;
}

function PeriodRow({ p }: { p: MomentumPeriodScore }) {
  const notes = [
    p.names_stale > 0 ? `${p.names_stale} stale` : null,
    p.universe_source === "reconstructed" ? "control universe rebuilt" : null,
    p.universe_source === null ? "control universe not recorded yet" : null,
  ].filter((n): n is string => n !== null);
  return (
    <tr className="border-t border-slate-200 dark:border-slate-800">
      <td className="py-1.5 pr-3">{monthLabel(p.formation_month)}</td>
      <td className="py-1.5 pr-3 text-slate-500">
        {formatDate(p.start)} → {formatDate(p.end)}
      </td>
      <td className="py-1.5 pr-3 text-right font-medium">{formatPct(p.book_return)}</td>
      <td className="py-1.5 pr-3 text-right">{formatPct(p.spy_return)}</td>
      <td className="py-1.5 pr-3 text-right">
        {formatPct(p.control_median)}
        {p.control_p05 !== null && p.control_p95 !== null ? (
          <span className="ml-1 text-xs text-slate-500">
            ({formatPct(p.control_p05)} to {formatPct(p.control_p95)})
          </span>
        ) : null}
      </td>
      <td className="py-1.5 pr-3 text-right">{percentileText(p.book_percentile)}</td>
      <td className="py-1.5 text-xs text-slate-500">
        {p.names_priced} priced{notes.length > 0 ? ` · ${notes.join(" · ")}` : ""}
      </td>
    </tr>
  );
}

function Body({ card }: { card: MomentumScorecardResponse }) {
  const first = card.periods[0];
  // Only the newest formation can be pending (an older one ends at its successor's start); show the newest.
  const pending = card.pending.at(-1);
  if (first === undefined) {
    return (
      <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
        {pending !== undefined
          ? `The ${monthLabel(pending)} book is formed; its first score appears after the next close.`
          : "No book formed yet. The momentum job forms each month's book on the first NYSE session of the month."}
      </p>
    );
  }
  return (
    <>
      {card.book_cumulative !== null ? (
        <p className="mt-2 text-sm text-slate-700 dark:text-slate-200">
          Since {formatDate(first.start)}: book{" "}
          <span className="font-semibold tabular-nums">{formatPct(card.book_cumulative)}</span>, SPY{" "}
          <span className="tabular-nums">{formatPct(card.spy_cumulative)}</span>, random 30-name baskets{" "}
          <span className="tabular-nums">{formatPct(card.control_cumulative_median)}</span> (median of{" "}
          {card.control_draws}). The book beat {percentileText(card.book_cumulative_percentile)} of the baskets.
        </p>
      ) : null}
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-sm tabular-nums" aria-label="Momentum book by formation">
          <thead className="text-left text-xs uppercase text-slate-500">
            <tr>
              <th className="pb-1 pr-3 font-semibold">Book</th>
              <th className="pb-1 pr-3 font-semibold">Held</th>
              <th className="pb-1 pr-3 text-right font-semibold">Return</th>
              <th className="pb-1 pr-3 text-right font-semibold">SPY</th>
              <th className="pb-1 pr-3 text-right font-semibold">Random 30 (5th to 95th)</th>
              <th className="pb-1 pr-3 text-right font-semibold">Beat</th>
              <th className="pb-1 font-semibold" />
            </tr>
          </thead>
          <tbody>
            {card.periods.map((p) => (
              <PeriodRow key={p.formation_month} p={p} />
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-xs text-slate-500">
        The rule's book, valued close to close before costs, like SPY and the baskets. Executed fills and spreads
        are in the positions list.
      </p>
    </>
  );
}

/**
 * #3822: the momentum book's rule against SPY and random 30-name baskets drawn
 * from the same universe — the live test the evidence note says decides it.
 */
export function MomentumScorecardPanel() {
  const card = useAsync(fetchMomentumScorecard, []);
  const data = card.data;
  const book = data?.book_cumulative ?? null;
  const spy = data?.spy_cumulative ?? null;
  const badge =
    book === null || spy === null ? null : book > spy ? (
      <Badge tone="ok">Ahead of SPY</Badge>
    ) : book < spy ? (
      <Badge tone="warn">Behind SPY</Badge>
    ) : (
      <Badge tone="neutral">Level with SPY</Badge>
    );
  return (
    <section
      aria-labelledby="momentum-scorecard"
      className="border border-slate-200 bg-white px-5 py-4 dark:border-slate-800 dark:bg-slate-900"
    >
      <h2 id="momentum-scorecard" className="flex items-center gap-2 text-sm font-semibold">
        Momentum book vs SPY
        {badge}
      </h2>
      {card.loading ? <SectionSkeleton rows={3} /> : null}
      {card.error ? <SectionError onRetry={card.refetch} /> : null}
      {data ? <Body card={data} /> : null}
    </section>
  );
}
