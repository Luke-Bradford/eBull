/**
 * Per-category freshness classifier for the ownership card (#767).
 *
 * Different ownership categories have wildly different expected
 * cadences:
 *
 *   * Institutions / ETFs (13F-HR): quarterly snapshot + 45-day
 *     filing window. A ``period_of_report`` 90-135 days old is
 *     normal. Older than 270 days = stale (filer dropped coverage
 *     or the ingest pipeline is wedged).
 *   * Insiders (Form 4): 2-business-day filing on every transaction.
 *     ``latest_txn_date`` reflects the last *insider event*, not the
 *     last data refresh — so a long quiet period is real-world
 *     signal, not stale data. Aging > 30d, stale > 90d.
 *   * Treasury (XBRL 10-Q): quarterly balance-sheet snapshot.
 *     Aging > 100d, stale > 200d.
 *
 * Lumping these into one card-level "as of" timestamp lets a 75%-stale
 * card read as fresh as a 5%-stale one. Per-category chips give the
 * operator a glanceable read on which slice is the bottleneck.
 *
 * Thresholds are deliberately rough — the chip strip is for
 * orientation, not alerting. Tighter SLO + paging belongs in #13 ops
 * monitor.
 */

import type { CategoryKey } from "@/components/instrument/ownershipRings";

export type FreshnessLevel = "fresh" | "aging" | "stale" | "unknown";

interface CadenceThresholds {
  /** Days before the chip flips from ``fresh`` to ``aging`` (amber). */
  readonly aging_days: number;
  /** Days before the chip flips from ``aging`` to ``stale`` (red). */
  readonly stale_days: number;
}

/** Per-category cadence thresholds. Tuned to the upstream filing
 *  cadence + filing window so a freshly-arrived 13F doesn't read
 *  amber the day after it lands. */
export const CADENCE: Record<CategoryKey, CadenceThresholds> = {
  institutions: { aging_days: 135, stale_days: 270 },
  etfs: { aging_days: 135, stale_days: 270 },
  insiders: { aging_days: 30, stale_days: 90 },
  treasury: { aging_days: 100, stale_days: 200 },
  // Blockholders (13D / 13G #766) are event-driven, not periodic —
  // a reporter only re-files when their position changes materially
  // (>=1% delta) or on the annual 13G refresh. Long quiet periods
  // are normal, but a 9-month-old block on a small-cap is usually
  // either a stale ingest or a position that quietly evaporated.
  // Aging > 180d, stale > 365d.
  blockholders: { aging_days: 180, stale_days: 365 },
};

/**
 * Classify a category's freshness given its ``as_of_date`` and a
 * reference ``today`` (operator's clock). Pure: no ``Date.now()`` —
 * the caller passes ``today`` so tests can pin time without patching
 * globals.
 *
 * Returns ``"unknown"`` when the as_of_date is missing or unparsable
 * — the chip renders neutrally without an age label.
 */
export function classifyFreshness(
  category_key: CategoryKey,
  as_of_date: string | null,
  today: Date,
): FreshnessLevel {
  if (as_of_date === null) return "unknown";
  const parsed = parseIsoDate(as_of_date);
  if (parsed === null) return "unknown";
  const age = ageInDays(parsed, today);
  // Negative ages can happen if the upstream date is in the future
  // (clock drift, test fixtures). Treat as fresh — operator-side this
  // is "we just got the data", not "it's stale". The chip still shows
  // the absolute as_of_date so anomalies surface visually.
  if (age < 0) return "fresh";
  const cadence = CADENCE[category_key];
  if (age >= cadence.stale_days) return "stale";
  if (age >= cadence.aging_days) return "aging";
  return "fresh";
}

/**
 * Render-friendly age label. Returns ``"2d"`` / ``"45d"`` / ``"3mo"`` /
 * ``"1y"`` based on the magnitude. ``null`` when no date.
 *
 * Granularity policy:
 *   * < 60 days   → ``Nd`` (days are the natural unit; chips need to
 *     distinguish 2d vs 45d).
 *   * < 18 months → ``Nmo`` (operator scans for "1mo" vs "8mo" without
 *     mental arithmetic).
 *   * else        → ``Ny``.
 */
export function formatAge(as_of_date: string | null, today: Date): string | null {
  if (as_of_date === null) return null;
  const parsed = parseIsoDate(as_of_date);
  if (parsed === null) return null;
  const age = ageInDays(parsed, today);
  if (age < 0) return "0d"; // future date — clock drift, render as "today"
  if (age < 60) return `${age}d`;
  const months = Math.round(age / 30);
  if (months < 18) return `${months}mo`;
  const years = Math.round(age / 365);
  return `${years}y`;
}

function parseIsoDate(text: string): Date | null {
  // Accept ``YYYY-MM-DD`` and full ISO-8601 timestamps. The reader
  // endpoints today emit plain dates, but the type is ``string`` so a
  // future change to timestamps shouldn't silently break the chip.
  //
  // Time-zone caveat: a bare ``YYYY-MM-DD`` is parsed as UTC midnight
  // while ``today = new Date()`` resolves to the local clock, so
  // ``ageInDays`` can drift ±1 day for an as_of_date observed near
  // local midnight. Harmless at the freshness cadences used here
  // (30-270 days) — the chip would only mis-classify within ~24h of a
  // hard threshold boundary, which the operator-facing copy does not
  // depend on. If we ever surface absolute hour-precision freshness,
  // align both sides on UTC explicitly.
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return null;
  return date;
}

function ageInDays(as_of: Date, today: Date): number {
  const ms_per_day = 24 * 60 * 60 * 1000;
  return Math.floor((today.getTime() - as_of.getTime()) / ms_per_day);
}
