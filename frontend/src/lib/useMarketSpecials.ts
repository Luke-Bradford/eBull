/**
 * useMarketSpecials — fetch the NYSE special-day sets (#609) for the years a
 * chart's bars span, so `classifySession` can shade closed days + half-day
 * afternoons.
 *
 * Only fetches for US-equity profiles (`us_equity` / `us_equity_rth`);
 * `continuous` / `foreign_equity` have no holiday concept and get an empty
 * set. Years are derived from **NY-local** bar dates (matching the endpoint's
 * date semantics), not browser-local / UTC, so a bar near a Jan-1 / Dec-31
 * boundary resolves its specials.
 *
 * Degrades gracefully: a failed fetch leaves the corresponding year absent,
 * so `classifySession` falls back to weekday-only behaviour for it — the
 * chart never blocks on the calendar (loading-error-empty-states skill).
 *
 * The fetched calendar is deterministic + slow-changing, so successful
 * years are memoised process-wide to avoid refetching as the operator
 * switches ranges / instruments.
 */
import { useEffect, useState } from "react";

import { fetchUsMarketCalendar } from "@/api/marketCalendar";
import type { SessionProfile } from "@/api/types";
import { type MarketSpecials, nyDateString } from "@/lib/chartFormatters";

// Process-wide cache of successfully-fetched years. Immutable per year.
const _yearCache = new Map<number, { fullClosures: string[]; halfDays: string[] }>();

const _EMPTY: MarketSpecials = { fullClosures: new Set(), halfDays: new Set() };

function _usProfile(profile: SessionProfile): boolean {
  return profile === "us_equity" || profile === "us_equity_rth";
}

function _yearsSpanned(bars: ReadonlyArray<{ readonly time: number }>): number[] {
  const years = new Set<number>();
  for (const b of bars) {
    // NY-local year — the first 4 chars of the `YYYY-MM-DD` NY date string.
    years.add(Number(nyDateString(b.time).slice(0, 4)));
  }
  return [...years];
}

export function useMarketSpecials(
  profile: SessionProfile,
  bars: ReadonlyArray<{ readonly time: number }>,
): MarketSpecials {
  const needsCalendar = _usProfile(profile);
  const years = needsCalendar ? _yearsSpanned(bars) : [];
  // Stable dependency key so the effect only re-runs when the set of years
  // (or the profile's calendar-need) actually changes — not on every bar
  // array identity churn.
  const yearsKey = years.slice().sort((a, b) => a - b).join(",");

  const [specials, setSpecials] = useState<MarketSpecials>(_EMPTY);

  useEffect(() => {
    if (!needsCalendar || years.length === 0) {
      setSpecials(_EMPTY);
      return;
    }
    let cancelled = false;

    const build = (): MarketSpecials => {
      const fullClosures = new Set<string>();
      const halfDays = new Set<string>();
      for (const y of years) {
        const entry = _yearCache.get(y);
        if (entry === undefined) continue;
        for (const d of entry.fullClosures) fullClosures.add(d);
        for (const d of entry.halfDays) halfDays.add(d);
      }
      return { fullClosures, halfDays };
    };

    const missing = years.filter((y) => !_yearCache.has(y));
    if (missing.length === 0) {
      setSpecials(build());
      return;
    }

    void Promise.allSettled(missing.map((y) => fetchUsMarketCalendar(y))).then((results) => {
      if (cancelled) return;
      results.forEach((r) => {
        if (r.status === "fulfilled") {
          _yearCache.set(r.value.year, {
            fullClosures: r.value.full_closures,
            halfDays: r.value.half_days,
          });
        }
        // Rejected years stay absent → weekday-only fallback for them.
      });
      setSpecials(build());
    });

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- yearsKey encodes `years`
  }, [needsCalendar, yearsKey]);

  return specials;
}
