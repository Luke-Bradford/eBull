/**
 * Shared chart formatters (#601).
 *
 * Single source of truth for tick / hover / volume formatting used by
 * both `PriceChart` (instrument-page overview) and
 * `ChartWorkspaceCanvas` (workspace). Keeping these here prevents the
 * two surfaces drifting — a previous patch had `formatHoverLabel`
 * copy-pasted into both, and a typo in one would silently diverge the
 * UI without any test catching it.
 *
 * All helpers operate on UTC epoch seconds (the unified time format
 * produced by `lib/chartData.ts` for both daily and intraday bars).
 */

const _MONTH_ABBR = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
] as const;

function _padDateLocal(d: Date): string {
  // Browser-local YYYY-MM-DD. ISO `toISOString()` would force UTC.
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Hover label rendered in the **browser's local timezone** so a UK
 * operator on BST sees `21:30` for a 20:30 UTC bar — matching the
 * convention TradingView and Robinhood use when the user is in a
 * different zone from the exchange. The chart's epoch-second time
 * value is universal; only the rendered label localises.
 *
 *   - Daily/weekly/monthly: `YYYY-MM-DD` (local date)
 *   - Intraday: `YYYY-MM-DD HH:MM` (local time, no zone suffix —
 *     the chart's controls/range carry the calendar context).
 */
export function formatHoverLabel(epochSeconds: number, intraday: boolean): string {
  const d = new Date(epochSeconds * 1000);
  const date = _padDateLocal(d);
  if (!intraday) return date;
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  return `${date} ${hh}:${mm}`;
}

/**
 * Adaptive tick-mark formatter for the time scale. lightweight-charts
 * passes a `tickMarkType` discriminator with each tick:
 *   0 = Year, 1 = Month, 2 = DayOfMonth, 3 = Time, 4 = TimeWithSeconds
 * We use that to render TradingView-style adaptive labels — `HH:MM`
 * within a day, `Apr 27` at the day boundary, `Apr` at month, year at
 * year. Single formatter handles both daily and intraday modes
 * because the library only emits the higher-resolution discriminators
 * when the chart is actually intraday.
 *
 * **Local timezone**: getters use the browser's local zone (e.g. BST
 * for a UK operator). The chart's underlying time values are still
 * UTC epoch seconds; only the displayed labels localise.
 */
export function tickFormatter(time: number, tickMarkType: number): string {
  const d = new Date(time * 1000);
  switch (tickMarkType) {
    case 0:
      return String(d.getFullYear());
    case 1:
      return _MONTH_ABBR[d.getMonth()] ?? "";
    case 2:
      return `${_MONTH_ABBR[d.getMonth()]} ${d.getDate()}`;
    case 3:
    case 4:
    default: {
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${hh}:${mm}`;
    }
  }
}

/**
 * Volume in TradingView-style abbreviated form: `14.36K`, `1.20M`,
 * `1.20B`. Kept separate from the price formatter because volumes
 * have a different scale and the abbreviation is what an operator
 * expects to see — full integer would dominate the status-line strip.
 */
export function humanizeVolume(v: number): string {
  if (!Number.isFinite(v) || v === 0) return "0";
  const abs = Math.abs(v);
  if (abs >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(v / 1e3).toFixed(2)}K`;
  return v.toLocaleString();
}
