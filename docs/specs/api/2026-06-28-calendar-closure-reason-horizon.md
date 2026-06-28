# Calendar: closure reason + selectable horizon (#1766)

Issue #1766 (epic #585, predecessor #1754). Smallest-high-value slice of the
"calendar too thin" finding: **(2) surface WHY a day is non-open** + **(1) let
the operator widen the window** beyond a fixed week. Items 3/4/5 (foreign
holiday depth, futures sessions, forward earnings/filings) stay out — no data
source today (stated on-page already; #1754 premise unchanged).

## Source rule

NYSE published holidays + early-closings
(https://www.nyse.com/markets/hours-calendars), already the governing source in
`app/services/market_calendar.py`. Holiday **names** come from the same
`pandas.tseries.holiday` rules that derive the closure dates —
`AbstractHolidayCalendar.holidays(..., return_name=True)` returns the name per
observed date (verified on dev: 2026-07-03 → "Independence Day", i.e. the
observed Jul-4 closure). No new source, no new dependency, no first-principles
inference: the names are a byproduct of the existing closure derivation.

Premise check (dev): the service already computes `full_closures` + `half_days`;
the API just was not carrying a reason. Confirmed — the only gap is plumbing.

## Behaviour

### Reason (item 2)

`app/services/market_calendar.py`:
- `MarketYear` gains `reasons: Mapping[date, str]` — observed closure/half-day
  date → human name. Built alongside the existing sets:
  - scheduled full closures → pandas `return_name=True` name.
  - extraordinary closures → name from a new `_EXTRAORDINARY_CLOSURE_NAMES`
    dict (the dates are already enumerated + commented; lift the comment text).
  - half days → name set at derivation (`"Friday after Thanksgiving"`,
    `"Christmas Eve"`, `"Independence Day eve"`). Closure-wins precedence is
    already applied to the set; the reasons map only carries the surviving days.
- New pure helper `us_market_reason(d: date) -> str | None`:
  - weekend → `"Weekend"`.
  - date in `reasons` → its name.
  - else `None` (regular open day).

### API (`app/api/calendar.py`)

- `MarketStatusDay` gains `reason: str | None`.
- US profiles (`us_equity`, `us_equity_rth`): `reason = us_market_reason(d)`.
- `foreign_equity`: `reason = "Weekend"` on Sat/Sun, else `None` (holidays not
  modelled — unchanged; only the weekend gets a label).
- `continuous` / unknown: `None`.
- New query param `days: int = Query(default=7, ge=1, le=28)` replaces the fixed
  `_WEEK_DAYS`. Window = `[today_ny + i for i in range(days)]`. Default 7 keeps
  the current view; FE offers 1/2/4-week presets. Ex-dividends stay unbounded
  forward (all upcoming) — out of scope to bound, and "all upcoming" is more
  useful than a window cap.

### Frontend (`frontend/src/pages/CalendarPage.tsx`, `api/calendar.ts`, `api/types.ts`)

- `MarketStatusDay` gains `reason: string | null`.
- `fetchCalendarEvents(scope, days)` adds the `days` param.
- Horizon control: three buttons `1 week / 2 weeks / 4 weeks` → `days` 7/14/28,
  state-driven like the scope toggle. Default 1 week.
- Each non-open tile shows its reason: closure/half-day tiles render the reason
  under the status label (small, truncated) and in the `title` tooltip. Open
  tiles unchanged. Weekend tiles get the "Weekend" reason too (de-clutters the
  blank closed look the issue called out).

## Out of scope (no source / tracked elsewhere)

Foreign-exchange holiday set, futures session model, forward earnings + filing
dates. The on-page note already states earnings/filings are not ingested; leave
it.

## Tests (pure)

- `tests/test_market_calendar.py`: `us_market_reason` — a scheduled holiday name
  ("Independence Day" for 2026-07-03), an extraordinary closure name, a half-day
  name ("Christmas Eve" 2026-12-24), a weekend ("Weekend"), a plain weekday
  (`None`). Observed-shift case: 2026-07-03 reads "Independence Day", not None.
- `tests/test_market_calendar_status.py`: `_day_type` unchanged; add reason
  assertions via a small `_day_reason`-style check at the API layer if one
  exists, else assert `us_market_reason` (kept pure, no DB).
- FE: extend `CalendarPage.test.tsx` — reason renders on a closed tile; horizon
  buttons change the requested `days`.

## Verification

FE+API read-path only. No schema, no backfill, no daemon restart. Dev-verify:
`GET /calendar/events?scope=all&days=28` returns `reason` populated on Jul-3
(and any in-window holiday) + 28 day tiles; load `/calendar`, confirm the
horizon buttons widen the grid and closed tiles show the reason.
