/**
 * /calendar — calendar-of-events (#1754 Phase B).
 *
 * Portfolio/watchlist-wide market status for the week ahead + the real
 * upcoming corporate events we ingest (ex-dividends). US market status is
 * NYSE-precise; foreign equities degrade to weekday/weekend (holidays not
 * modelled); continuous (crypto/FX/…) is not modelled. The live "now" session
 * is computed client-side via `classifySession` (no server duplication).
 *
 * Forward earnings + filing dates are intentionally absent — we ingest no
 * forward earnings calendar or filing-due dates (stated on-page, not faked).
 */
import { useCallback, useMemo, useState } from "react";

import { fetchCalendarEvents } from "@/api/calendar";
import type { CalendarEvents, CalendarScope, MarketDayType, SessionProfile } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { classifySession, type SessionKind } from "@/lib/chartFormatters";
import { useAsync } from "@/lib/useAsync";
import { useMarketSpecials } from "@/lib/useMarketSpecials";

const SCOPES: ReadonlyArray<{ key: CalendarScope; label: string }> = [
  { key: "portfolio", label: "Portfolio" },
  { key: "watchlist", label: "Watchlist" },
  { key: "all", label: "All" },
];

const DAY_TYPE_STYLE: Record<MarketDayType, string> = {
  open: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300",
  half_day: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300",
  closed: "bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400",
  not_modelled: "bg-slate-50 text-slate-400 dark:bg-slate-800/50 dark:text-slate-500",
};

const DAY_TYPE_LABEL: Record<MarketDayType, string> = {
  open: "Open",
  half_day: "Half day",
  closed: "Closed",
  not_modelled: "—",
};

const SESSION_LABEL: Record<SessionKind, string> = {
  rth: "Open · regular hours",
  pre: "Pre-market",
  ah: "After-hours",
  closed: "Closed",
};

function weekdayShort(isoDate: string): string {
  // Parse as UTC noon to avoid TZ date-shift on the label.
  return new Date(`${isoDate}T12:00:00Z`).toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function nowSession(
  profile: SessionProfile,
  epochSeconds: number,
  specials: ReturnType<typeof useMarketSpecials>,
): SessionKind {
  return classifySession(profile, epochSeconds, specials);
}

export function CalendarPage(): JSX.Element {
  const [scope, setScope] = useState<CalendarScope>("portfolio");
  const events = useAsync<CalendarEvents>(
    useCallback(() => fetchCalendarEvents(scope), [scope]),
    [scope],
  );

  // "Now" frozen at mount — the live-session badge is a load-time snapshot
  // (refresh re-reads it). Memoised so the array reference is render-stable:
  // passing a fresh literal to a hook each render is a refetch hazard even
  // though useMarketSpecials currently keys on a derived years-string, not the
  // array identity. One specials fetch (current year) reused for every
  // profile's badge — the hook only fetches for US profiles; foreign/continuous
  // ignore specials in classifySession.
  const nowBar = useMemo(() => [{ time: Math.floor(Date.now() / 1000) }], []);
  const specials = useMarketSpecials("us_equity", nowBar);

  return (
    <div className="mx-auto max-w-screen-lg space-y-4 p-4">
      <header className="border-b border-slate-200 pb-3 dark:border-slate-800">
        <h1 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Calendar</h1>
        <p className="mt-1 text-xs text-slate-500">
          Market status for the week ahead + upcoming ex-dividends across your{" "}
          {scope === "all" ? "portfolio & watchlist" : scope}. US markets are NYSE-precise;
          foreign exchanges show weekday/weekend only (holidays not modelled).
        </p>
        <div className="mt-2 flex gap-1">
          {SCOPES.map((s) => (
            <button
              key={s.key}
              type="button"
              onClick={() => setScope(s.key)}
              className={`rounded px-2 py-1 text-xs ${
                scope === s.key
                  ? "bg-sky-600 text-white"
                  : "bg-slate-100 text-slate-600 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-300"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </header>

      {events.loading ? (
        <SectionSkeleton rows={5} />
      ) : events.error !== null ? (
        <SectionError onRetry={events.refetch} />
      ) : events.data === null || events.data.market_status.length === 0 ? (
        <EmptyState
          title="Nothing to show"
          description={
            scope === "watchlist"
              ? "Your watchlist is empty — add instruments to see their market calendar."
              : "No holdings in scope yet."
          }
        />
      ) : (
        <>
          <Section title="Market status — this week">
            <div className="space-y-3">
              {events.data.market_status.map((row) => (
                <div key={row.profile}>
                  <div className="mb-1 flex items-baseline gap-2">
                    <span className="text-sm font-medium text-slate-700 dark:text-slate-200">
                      {row.label}
                    </span>
                    <span className="text-[11px] text-slate-500">
                      {SESSION_LABEL[nowSession(row.profile, nowBar[0]!.time, specials)]} now
                    </span>
                    {!row.holidays_modelled && (
                      <span className="text-[10px] text-slate-400">· holidays not modelled</span>
                    )}
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {row.week.map((d) => (
                      <div
                        key={d.date}
                        className={`rounded px-2 py-1 text-[11px] tabular-nums ${DAY_TYPE_STYLE[d.day_type]}`}
                        title={`${d.date}: ${DAY_TYPE_LABEL[d.day_type]}`}
                      >
                        <span className="block font-medium">{weekdayShort(d.date)}</span>
                        <span className="block">{DAY_TYPE_LABEL[d.day_type]}</span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </Section>

          <Section title="Upcoming ex-dividends">
            {events.data.ex_dividends.length === 0 ? (
              <p className="px-2 py-4 text-xs text-slate-500">
                No upcoming ex-dividends for instruments in scope.
              </p>
            ) : (
              <ul className="divide-y divide-slate-100 text-sm dark:divide-slate-800">
                {events.data.ex_dividends.map((d) => (
                  <li key={`${d.instrument_id}-${d.ex_date}`} className="flex justify-between py-1.5">
                    <span className="font-medium text-slate-700 dark:text-slate-200">{d.symbol}</span>
                    <span className="tabular-nums text-slate-500">
                      ex {d.ex_date}
                      {d.pay_date !== null ? ` · pay ${d.pay_date}` : ""}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </Section>

          <p className="px-1 text-[10px] text-slate-400">
            Earnings &amp; filing dates are not yet shown — eBull does not ingest forward earnings
            calendars or filing-due dates.
          </p>
        </>
      )}
    </div>
  );
}
