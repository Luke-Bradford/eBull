/**
 * Rank delta visual cell.
 *
 * The backend convention (app/services/scoring.py::compute_rankings):
 * rank_delta = prior_rank − current_rank, vs the most recent prior run of
 * the same model_version. A POSITIVE delta means the instrument moved UP the
 * table (rank number fell), which is an improvement; a negative delta means
 * it moved down. (#3389: this cell used to read the sign the other way round;
 * `reporting.py`'s risers = delta > 0 was right; the full-population check
 * of stored rows is recorded on the #3389 slice (b) PR.)
 *
 * Color uses the operator-ui-conventions palette only:
 *   - emerald  improved (delta > 0)
 *   - red      worsened (delta < 0)
 *   - slate    unchanged or unknown (delta == 0 or null)
 *
 * The arrow glyph and a screen-reader label are always present so the
 * signal does not rely on color alone.
 */
import type { JSX } from "react";

export function RankDeltaCell({ delta }: { delta: number | null }): JSX.Element {
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
  const improved = delta > 0;
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
