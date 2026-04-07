/**
 * Display formatters for money, percentages, and dates.
 *
 * The backend already serialises numbers as plain JSON numbers and dates as
 * ISO 8601 strings, so these helpers only handle presentation. Currency is
 * intentionally hard-coded to GBP for v1 — eToro's reporting currency for
 * this operator is GBP and there is no multi-currency requirement yet.
 * Revisit when a non-GBP account is added.
 */

const GBP = new Intl.NumberFormat("en-GB", {
  style: "currency",
  currency: "GBP",
  maximumFractionDigits: 2,
});

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

export function formatMoney(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return GBP.format(value);
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

/** Compute unrealized P&L percentage from raw cost and PnL values. */
export function pnlPct(unrealized: number, costBasis: number): number | null {
  if (costBasis === 0) return null;
  return unrealized / costBasis;
}
