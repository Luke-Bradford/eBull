# #1754 — exchange-calendar Phase B (events page) + Phase C (coverage-gap shading)

Parent #585. Follow-up to #609 Phase A (PR1753: in-house NYSE calendar + `session_profile` + `classifySession` intraday shading).

## Premise check (dev — the handoff's "already ingested" is partly false)
The issue says Phase B surfaces "earnings, ex-dividend, 10-K/10-Q dates (already ingested)". Verified on dev:
- **Forward earnings dates: do not exist.** No earnings/calendar table; `financial_periods` is historical (period_end in the past). We ingest no forward earnings calendar.
- **Future filings: 0.** `filing_events.filing_date` is always past; no due/expected filing dates.
- **Future ex-dividends: 4 universe-wide** (`dividend_events.ex_date >= today`, the partial `idx_dividend_events_ex_date_future`). Real but sparse; **0** in the current portfolio (6 holdings, all `us_equity`; watchlist empty).

So the always-correct, always-populated value is **market status** (is each held/watched market open / closed / half-day today + this week). Upcoming corporate events = the **real** future ex-dividends only. Forward earnings/filing dates are **out of scope** (no source) → a separate forward-events-ingestion ticket if ever needed.

## Deps decision (the issue's "decide deps here")
Portfolio + watchlist are **100% US today** (6 holdings, 0 foreign; universe has ~4,700 foreign tradables but none held/watched). Adding `pandas_market_calendars` (+6 transitive deps) or hand-rolling LSE/Xetra/Euronext closure sets (annual re-verify burden per exchange) for **zero** foreign holdings is the speculative dependency the project forbids ("don't add libraries casually; keep dependencies justified and minimal") and contradicts the Phase A decision. **Decision: no new deps.** US market status is precise via the in-house `market_calendar`; a foreign-listed holding shows **open (weekday) / closed (weekend)** with an "exchange holidays not modelled" caveat (Phase A's settled posture). Foreign holiday precision becomes a real-need-triggered follow-up the day a foreign holding is added.

## Phase B — market-status + upcoming-events page
Route `/calendar` (portfolio-wide, not per-instrument). Backend `GET /calendar/events?scope=portfolio|watchlist|all`:
- **Market status** — for the distinct `session_profile`s across the scope's instruments, today's status + this week (next 7 days): `open` / `closed` / `half_day` (US via `market_calendar.us_market_specials`; `foreign_equity` → weekday open/weekend closed + `holidays_modelled=false`; `continuous` → always open). Computed server-side in America/New_York for US, UTC weekday for foreign.
- **Upcoming ex-dividends** — `dividend_events.ex_date >= today` for the scope's instruments, ordered by ex_date, with symbol + pay_date. (Real data; honestly sparse/empty.)
- Pydantic `CalendarEvents { scope, market_status: [{profile, label, today, week: [{date,status}]}], ex_dividends: [{symbol, ex_date, pay_date}] }`.
- Scope resolution: `portfolio` = `positions`, `watchlist` = `watchlist` (operator-scoped), `all` = union. Reuse the session-token operator id.

FE: `CalendarPage` (`/calendar`), `useAsync` over `fetchCalendarEvents`, loading/error/empty states, a market-status strip + an upcoming ex-dividends list. Honest empty states ("No upcoming ex-dividends for your holdings"). Link from the dashboard/nav.

**No forward earnings/filings section** — stated on-page as "earnings & filing dates: not yet ingested" rather than a fake/empty widget.

## Phase C — intraday coverage-gap shading
FE-only on the intraday price chart. A gap = consecutive bars `bar[i].time - bar[i-1].time > interval × _GAP_FACTOR` (factor ~1.5 to tolerate a single missing bar; ignore the expected overnight/weekend gaps — only flag WITHIN a session via `classifySession` both ends in the same session-day's RTH/extended window, OR simpler: flag intrasession gaps, not cross-session). Draw a dashed vertical line (lightweight-charts overlay, mirror `SessionBands`) at each gap. Pure detector `detectCoverageGaps(bars, intervalSeconds, profile, specials)` in `chartFormatters`-style lib, table-tested; the chart renders the lines. Caption notes sparse intraday coverage.

## Source rule / settled
In-house NYSE calendar (`market_calendar`, Phase A) is the US source of truth; ⚠ NYSE half-day set needs annual re-verify (carried from #609). `dividend_events.ex_date` is the settled ex-dividend source. `session_profile` (instruments API) drives foreign vs US vs continuous.

## Tests
- Pure: `detectCoverageGaps` (intrasession gap flagged; overnight/weekend NOT flagged; exact-interval clean) table-test.
- Pure/db: market-status builder (US holiday/half-day/open; foreign weekend; continuous) — prefer a pure status fn over `(profile, date, specials)` table-tested.
- FE: CalendarPage render-test (loading/empty/populated); chart gap-line render-test (mirror SessionBands test pattern).

## DoD
Smoke `/calendar?scope=portfolio` on dev (6 US holdings → US market status this week + likely empty ex-div list, honest). Phase C: load an intraday chart with a known sparse window, confirm a dashed line renders at the gap. No daemon restart (API + FE only).
