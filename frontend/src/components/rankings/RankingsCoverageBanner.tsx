import type { RankingsCoverage } from "@/api/types";
import { formatNumber } from "@/lib/format";

/**
 * "Ranked N of M — coverage" denominator for the Rankings header (#1918).
 *
 * The list only ever shows the scored subset of a ~12.6k tradable universe;
 * without an explicit denominator the ~8.7k absent instruments read as a bug.
 * This states the count and, on disclosure, the MECE why-not-ranked breakdown.
 *
 * Degrades to null while loading or on error — the header keeps working; the
 * denominator is additive context, never a blocker (parent passes
 * `coverage={null}` in those states).
 */
export function RankingsCoverageBanner({ coverage }: { coverage: RankingsCoverage | null }) {
  if (!coverage) return null;

  const { ranked, universe, not_ranked } = coverage;
  const hasBreakdown = not_ranked.length > 0;

  return (
    <div className="text-xs text-slate-500 dark:text-slate-400">
      <details className="group">
        <summary className="flex cursor-pointer list-none items-center gap-1.5 select-none">
          <span className="tabular-nums text-slate-600 dark:text-slate-300">
            Ranked <span className="font-semibold">{formatNumber(ranked, 0)}</span> of{" "}
            <span className="font-semibold">{formatNumber(universe, 0)}</span>
          </span>
          {hasBreakdown && (
            <span className="text-slate-400 group-open:text-slate-600 dark:group-open:text-slate-200">
              <span className="underline decoration-dotted underline-offset-2">why</span>
              <span className="ml-0.5 inline-block transition-transform group-open:rotate-90">›</span>
            </span>
          )}
        </summary>
        {hasBreakdown && (
          <ul className="mt-2 space-y-1 border-l border-slate-200 pl-3 dark:border-slate-800">
            {not_ranked.map((b) => (
              <li key={b.reason} className="flex items-baseline justify-between gap-4">
                <span className="text-slate-600 dark:text-slate-300">{b.label}</span>
                <span className="tabular-nums text-slate-500 dark:text-slate-400">
                  {formatNumber(b.count, 0)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </details>
    </div>
  );
}
