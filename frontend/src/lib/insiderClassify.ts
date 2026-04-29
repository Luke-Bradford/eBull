/**
 * Form 4 transaction classifier (#588).
 *
 * Frontend mirror of the SQL classification used in
 * `app/services/insider_transactions.py::get_insider_summary`. The
 * authoritative signal is `acquired_disposed_code` (SEC's explicit
 * A / D flag); when absent we fall back to `txn_code`.
 *
 * `txn_code` mappings come from SEC Form 4 reference codes:
 *   Acquired:  P (open-market buy), A (grant), M (option exercise),
 *              X (in-the-money exercise), C (conversion), V (filed
 *              voluntarily), J (other acquisition)
 *   Disposed:  S (open-market sale), D (disposition), F (tax
 *              withholding), G (gift)
 *
 * Duplicated from the backend SQL because the API ships rows
 * unclassified (the response carries every structured field; the
 * classification is a render-time concern). If a third consumer
 * appears, fold the rule into a backend-side derived column.
 */

export type Direction = "acquired" | "disposed" | "unknown";

const ACQ_CODES = new Set(["P", "A", "M", "X", "C", "V", "J"]);
const DISP_CODES = new Set(["S", "D", "F", "G"]);

export function directionOf(
  acquiredDisposedCode: string | null,
  txnCode: string,
): Direction {
  if (acquiredDisposedCode === "A") return "acquired";
  if (acquiredDisposedCode === "D") return "disposed";
  if (ACQ_CODES.has(txnCode)) return "acquired";
  if (DISP_CODES.has(txnCode)) return "disposed";
  return "unknown";
}

/** Signed share count for a transaction. Acquired → +shares, disposed
 *  → -shares, unknown direction or null shares → 0. */
export function signedShares(
  shares: string | null,
  acquiredDisposedCode: string | null,
  txnCode: string,
): number {
  if (shares === null) return 0;
  const n = Number(shares);
  if (!Number.isFinite(n)) return 0;
  const dir = directionOf(acquiredDisposedCode, txnCode);
  if (dir === "acquired") return n;
  if (dir === "disposed") return -n;
  return 0;
}

/** Notional value (shares × price) when both present, else 0. */
export function notionalValue(
  shares: string | null,
  price: string | null,
): number {
  if (shares === null || price === null) return 0;
  const s = Number(shares);
  const p = Number(price);
  if (!Number.isFinite(s) || !Number.isFinite(p)) return 0;
  return s * p;
}

/** UTC midnight epoch-ms for the start of today. Backend windows
 *  use calendar dates (`CURRENT_DATE - INTERVAL '90 days'`), so the
 *  frontend lens must anchor at midnight rather than `Date.now()` to
 *  avoid dropping transactions filed earlier on the cutoff date. */
export function startOfTodayUtcMs(): number {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

/** UTC epoch-ms for the start of the calendar month `monthsBack`
 *  months before the current month. `monthsBack=0` returns the first
 *  of this month; `monthsBack=23` returns the first of the month 23
 *  months ago, giving a 24-month inclusive window. */
export function startOfMonthUtcMs(monthsBack: number): number {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - monthsBack, 1);
}

/** Inclusive cutoff for a "last N days" window. Mirrors the backend
 *  SQL `txn_date >= CURRENT_DATE - INTERVAL 'N days'` semantics:
 *  today minus N midnight, so a transaction filed on the boundary
 *  date is included regardless of clock time. */
export function startOfNDaysAgoUtcMs(days: number): number {
  return startOfTodayUtcMs() - days * 86_400_000;
}
