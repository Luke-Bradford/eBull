/**
 * Display formatters for money, percentages, and dates.
 *
 * The backend already serialises numbers as plain JSON numbers and dates as
 * ISO 8601 strings, so these helpers only handle presentation. The display
 * currency is provided by DisplayCurrencyContext and passed to formatMoney
 * by each call site; it defaults to GBP when the context is unavailable.
 */

/**
 * Cached Intl.NumberFormat instances keyed by currency code.
 * Avoids creating a new formatter on every call.
 */
const formatters: Record<string, Intl.NumberFormat> = {};
function getFormatter(currency: string): Intl.NumberFormat {
  if (!formatters[currency]) {
    formatters[currency] = new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    });
  }
  return formatters[currency];
}

const PCT = new Intl.NumberFormat("en-GB", {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  signDisplay: "exceptZero",
});

const DATE = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

// UTC timezone pinned: backend date-only strings (``YYYY-MM-DD``)
// parse as midnight UTC in JS. Without ``timeZone: "UTC"`` here,
// Intl renders them in local TZ — a London operator sees "15 Jun
// 2024" correctly, but a New York operator (UTC-5) sees "14 Jun
// 2024" for the same input. Pin UTC so the calendar date is
// stable regardless of viewer timezone.
const DATE_ONLY = new Intl.DateTimeFormat("en-GB", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  timeZone: "UTC",
});

export function formatMoney(
  value: number | null | undefined,
  currency = "GBP",
): string {
  if (value === null || value === undefined) return "—";
  return getFormatter(currency).format(value);
}

/** Format a fraction (0.0123 → "+1.23%"). Pass `null` for "—". */
export function formatPct(fraction: number | null | undefined): string {
  if (fraction === null || fraction === undefined) return "—";
  return PCT.format(fraction);
}

/** Compact "day month" close date (e.g. "12 Jun") for as-of stamps (#1924).
 *  Formatted in UTC: `new Date("YYYY-MM-DD")` is UTC midnight, so a local-TZ
 *  format would render the prior day west of UTC and shift the close date.
 *  Returns null on an absent/unparseable date. */
export function formatCloseDate(iso: string | null | undefined): string | null {
  if (iso === null || iso === undefined) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    timeZone: "UTC",
  });
}

const PCT_UNSIGNED = new Intl.NumberFormat("en-GB", {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

/** Unsigned fraction percent (0.5238 → "52.38%") for weights / exposure
 *  shares, where formatPct's exceptZero sign would misread ("+52.38%"
 *  weight). Returns are signed (formatPct); compositions are not. */
export function formatUnsignedPct(fraction: number | null | undefined): string {
  if (fraction === null || fraction === undefined) return "—";
  return PCT_UNSIGNED.format(fraction);
}

export function formatNumber(
  value: number | null | undefined,
  fractionDigits = 4,
): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  });
}

/** Currency-symbol prefix for abbreviated magnitudes:
 *  `formatBigMoney(2_138_850_000, "USD") → "US$2.14B"` (en-GB locale). Keeps the currency
 *  context that bare `formatBigNumber` drops, without the full-precision
 *  noise of `formatMoney` on billion-scale figures (#1978 review). */
export function formatBigMoney(n: number | null, currency = "GBP"): string {
  if (n === null) return "—";
  const sym =
    getFormatter(currency)
      .formatToParts(0)
      .find((p) => p.type === "currency")?.value ?? currency;
  return `${sym}${formatBigNumber(n)}`;
}

/** Abbreviated large magnitudes for financial-statement values:
 *  `416161000000 → "416.16B"`. Canonical home of the helper that
 *  previously lived privately in FundamentalsPane (#554). */
export function formatBigNumber(n: number | null): string {
  if (n === null) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(2)}K`;
  return n.toFixed(0);
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return DATE.format(d);
}

const TIME = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
});

/** Time-only (HH:MM, en-GB 24h) for "checked HH:MM"-style freshness labels
 *  (#1513). Accepts a Date (client poll-completion time) or an ISO string. */
export function formatTime(value: Date | string | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return TIME.format(d);
}

/** Format a YYYY-MM-DD date-only value (pydantic `date` serialisation).
 *  Unlike formatDateTime, does NOT render hours/minutes. */
export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return DATE_ONLY.format(d);
}

/** Compute unrealized P&L percentage from raw cost and PnL values. */
export function pnlPct(unrealized: number, costBasis: number): number | null {
  if (costBasis === 0) return null;
  return unrealized / costBasis;
}

/** Relative-time formatter for strip rows. Uses local system clock.
 *  <60s → "just now", <1h → "Nm ago", <1d → "Nh ago", <7d → "Nd ago",
 *  else → formatDate fallback. */
export function formatRelativeTime(iso: string | null | undefined): string {
  if (iso === null || iso === undefined || iso === "") return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";
  const deltaS = Math.floor((Date.now() - then) / 1000);
  if (deltaS < 60) return "just now";
  if (deltaS < 3600) return `${Math.floor(deltaS / 60)}m ago`;
  if (deltaS < 86400) return `${Math.floor(deltaS / 3600)}h ago`;
  if (deltaS < 604800) return `${Math.floor(deltaS / 86400)}d ago`;
  return formatDate(iso);
}

/** #1409 P5 — server-computed rows/sec for a running bootstrap stage.
 *  null (not measurable) → "—"; ≥1000 abbreviated with `k`. */
export function formatRate(rowsPerSec: number | null | undefined): string {
  if (rowsPerSec === null || rowsPerSec === undefined) return "—";
  if (rowsPerSec >= 1000) return `${(rowsPerSec / 1000).toFixed(1)}k rows/s`;
  return `${rowsPerSec.toFixed(1)} rows/s`;
}

/** #1409 P5 — projected seconds-to-target for a running stage.
 *  null (unknown target / already met) → "—"; <60s → "<1m"; the `~`
 *  prefix marks it an estimate. */
export function formatEta(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 60) return "<1m";
  const totalMin = Math.floor(seconds / 60);
  if (totalMin < 60) return `~${totalMin}m`;
  const hours = Math.floor(totalMin / 60);
  const mins = totalMin % 60;
  return `~${hours}h ${mins}m`;
}

/** #1409 P5 — render a server-computed heartbeat age (seconds since the
 *  stage last wrote progress) as "updated Ns/Nm/Nh ago". null → "—".
 *  Takes a number (not an ISO string) so it stays on the DB clock the
 *  server measured against — no client-skew re-derivation. */
export function formatHeartbeatAge(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "—";
  const s = Math.max(0, Math.floor(seconds));
  if (s < 60) return `updated ${s}s ago`;
  if (s < 3600) return `updated ${Math.floor(s / 60)}m ago`;
  return `updated ${Math.floor(s / 3600)}h ago`;
}
