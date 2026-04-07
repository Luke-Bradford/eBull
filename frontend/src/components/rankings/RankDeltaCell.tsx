/**
 * Rank delta visual cell.
 *
 * The backend convention (app/services/scoring.py): rank_delta is the
 * change in rank vs the most recent prior run for the same model_version.
 * A positive delta means the instrument moved DOWN the table (rank number
 * went up), which is a worsening signal; a negative delta means it moved
 * UP (improved). Color follows that meaning, not the raw sign of the
 * number.
 *
 * Color uses the operator-ui-conventions palette only:
 *   - emerald  improved (delta < 0)
 *   - red      worsened (delta > 0)
 *   - slate    unchanged or unknown (delta == 0 or null)
 *
 * The arrow glyph and a screen-reader label are always present so the
 * signal does not rely on color alone.
 */
export function RankDeltaCell({ delta }: { delta: number | null }) {
  if (delta === null) {
    return (
      <span className="text-slate-400">
        <span aria-hidden="true">—</span>
        <span className="sr-only">no prior rank</span>
      </span>
    );
  }
  if (delta === 0) {
    return (
      <span className="text-slate-500">
        <span aria-hidden="true">▬ 0</span>
        <span className="sr-only">unchanged</span>
      </span>
    );
  }
  const improved = delta < 0;
  const magnitude = Math.abs(delta);
  return (
    <span className={improved ? "text-emerald-600" : "text-red-600"}>
      <span aria-hidden="true">
        {improved ? "▲" : "▼"} {magnitude}
      </span>
      <span className="sr-only">
        {improved ? `improved by ${magnitude}` : `worsened by ${magnitude}`}
      </span>
    </span>
  );
}
