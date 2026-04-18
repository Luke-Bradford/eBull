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

export function formatNumber(value: number | null | undefined, fractionDigits = 4): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  });
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return DATE.format(d);
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
