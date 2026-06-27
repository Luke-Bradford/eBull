# #609 Phase A — exchange-aware intraday session shading

Status: spec (Phase A only; B/C re-filed per operator 2026-06-27)
Parent: #585 chart decision-support cluster · predecessor #602 · supersedes the #602/#607 hardcoded-NYSE stopgap.

## Problem

`frontend/src/lib/chartFormatters.ts::classifyUsSession(epoch)` hardcodes NYSE/Nasdaq
wall-clock bands (PM 04:00–09:30, RTH 09:30–16:00, AH 16:00–20:00 ET, weekend→closed)
and applies them to **every** instrument. Wrong for:

1. Non-US listings (LSE/Euronext/Xetra) — get fictitious ET bands.
2. 24/5 fx / commodity / index / crypto — get a PM/AH/overnight-closed concept they don't have.
3. US **half-days** (early close 13:00 ET) — RTH band runs to 16:00, AH mislabelled.
4. US **full holidays** — a mid-week closure renders as normal RTH.

## Falsified premises (full-population, dev DB, 2026-06-27)

The issue says "refactor keyed by `instruments.exchange`." Verified against the real data:

- `instruments.exchange` **is populated** but holds eToro's opaque numeric `exchangeID`
  as text (`"4"`,`"5"`,`"33"`…), **not** an exchange name/MIC. `pandas_market_calendars`
  keys on names → cannot key off this column directly. (Premise half-true → reshaped.)
- The operator-curated **`exchanges`** table (`app/services/universe.py` joins
  `instruments.exchange = exchanges.exchange_id`) already carries the rescue data:
  `description` (Nasdaq/NYSE/LSE/Euronext Paris/Xetra/Tokyo…), `country`, and
  **`asset_class`** ∈ {us_equity, eu_equity, uk_equity, asia_equity, mena_equity,
  commodity, fx, index, crypto, unknown} (CHECK-constrained, migration sql/067).
  `asset_class` segments bugs (1)+(2) with **no per-exchange window math**.
- US instrument population by exchangeID: `4`=Nasdaq(3701), `5`=NYSE(2803),
  `33`="Regular Trading Hours - RTH"(562, symbols `AAPL.RTH`), `19`=OTC(84), `20`=CBOE(51).
  Exchange 33 = eToro's RTH-only operational-duplicate (settled-decisions §494/§534) →
  US calendar but **no PM/AH bands**.
- `InstrumentDetail` (`app/api/instruments.py:400`) serves `exchange`+`country` but
  **not** `asset_class` → FE cannot branch session logic today. Backend work needed.
- `eToro WS / metadata exposes no trading-hours field` (issue premise) — confirmed; also
  no per-tick volume (separate #608, closed). Calendar must come from us.

## Source rule

The authoritative session schedule is the **NYSE published trading calendar** (full
closures + half-day early closes + observed-day shifting). Phase A scopes the *correct*
calendar to US equities (the charted-intraday-dominant population); non-US equities get a
closed/open-only model (eToro emits no extended-hours bars for them), and
fx/commodity/index/crypto get a continuous model (no bands).

Authority: **NYSE published holidays + early-closings** (nyse.com/markets/hours-calendars)
and the ICE/NYSE trading-calendar rules; cite these inline in `market_calendar.py`, not
first principles.

NYSE full closures: New Year's Day (nearest_workday), MLK Day, Washington's Birthday,
**Good Friday**, Memorial Day, **Juneteenth — `start_date=date(2022,1,1)`** (first NYSE
observance 2022; a plain `nearest_workday` rule without `start_date` wrongly closes
2021-06-18 — Codex-verified), Independence Day (nearest_workday), Labor Day, Thanksgiving,
Christmas (nearest_workday). **Good Friday is the tell that
`sec_calendar.is_us_federal_holiday` cannot be reused** — it's a market holiday, not a
federal one (and federal Columbus/Veterans are NYSE-open). Distinct calendar required.

NYSE early closes (13:00 ET): day after Thanksgiving; Christmas Eve (Dec 24) when a
weekday; July 3 when a weekday **and not itself a full closure**. **`half_days` is computed
as the rule set MINUS `full_closures` — closure always wins.** Worked case: 2026-07-04 is a
Saturday → Independence Day observed Friday 2026-07-03 (a `full_closure`), so 2026-07-03 is
**not** a half-day. (Codex-verified `pandas` run: 2026 closures include `2026-07-03`.) NYSE
publishes early-close days years ahead; the rule set is stable.

Year-boundary straddle (reused from `sec_calendar`): query the holiday calendar over
`[Dec 15 (y-1) .. Jan 15 (y+1)]` and keep stamps whose **observed** date lands in `y`, so a
Jan-1-observed-on-Dec-31 closure is attributed correctly.

Zero new dependency: compose the calendar from `pandas.tseries.holiday` primitives
(`AbstractHolidayCalendar`, `Holiday`, `GoodFriday`, `USMartinLutherKingJr`,
`USPresidentsDay`, `USMemorialDay`, `USLaborDay`, `USThanksgivingDay`, `nearest_workday`),
exactly as `app/providers/implementations/sec_calendar.py` composes the federal one.
`pandas` is already a top-level dep (`pyproject.toml:28`). Operator-chosen over
`pandas_market_calendars` (6 transitive packages) 2026-06-27.

## Design

Authoritative calendar logic lives in **Python** (pytest-testable, single source of
truth); the FE consumes a served schedule and only paints bands. No observed-shift /
holiday rules duplicated in TypeScript.

### Backend

1. `app/services/market_calendar.py` — new.
   - `_NyseHolidayCalendar(AbstractHolidayCalendar)` with the rules above; per-year
     cache like `sec_calendar._holidays_for_year` (straddle year boundary for observed
     New-Year shifts).
   - `us_market_specials(year: int) -> MarketYear` returning `full_closures: frozenset[date]`
     and `half_days: frozenset[date]` (early-close 13:00 ET).
   - Pure, no DB, no network.
2. `GET /market-calendar/us/{year}` (new router) →
   `{ "year": 2026, "full_closures": ["2026-01-01", …], "half_days": ["2026-11-27", …] }`.
   - **Dates are `America/New_York` calendar dates** (the exchange-local civil date), stated
     in the response/docstring. FE derives the year(s) to fetch from **NY-local** bar dates,
     never browser-local or UTC, so a bar near a Jan-1/Dec-31 boundary resolves its specials.
   - Auth: `require_session_or_service_token` (same dependency as `/sse/quotes`).
   - Bounds: `year` ∈ [2000, 2100]; outside → 400. (Holiday primitives are valid far beyond
     our data range; the bound just rejects garbage.)
   - Cache: `Cache-Control: public, max-age=86400` — the set is deterministic + slow-changing;
     a day of staleness can never matter (NYSE publishes years ahead).
   - Instrument-independent + cacheable; FE fetches per visible year (≤2 calls/chart),
     client-caches.
3. `InstrumentDetail` (+ list row): add `session_profile: str`. Derived in SQL with explicit
   precedence (exchange-33 first — its RTH-only nature is **not** encoded in `asset_class`,
   which is `us_equity`; it needs the exchange-id check), via
   `LEFT JOIN exchanges e ON e.exchange_id = i.exchange`:

   ```sql
   CASE
     WHEN i.exchange = '33'              THEN 'us_equity_rth'  -- eToro RTH duplicate, no PM/AH
     WHEN e.asset_class = 'us_equity'    THEN 'us_equity'
     WHEN e.asset_class IN ('eu_equity','uk_equity','asia_equity','mena_equity')
                                          THEN 'foreign_equity'
     WHEN e.asset_class IN ('commodity','fx','index','crypto')
                                          THEN 'continuous'
     ELSE 'us_equity'                    -- 'unknown' / NULL / any unrecognized value:
   END                                   -- preserve today's behaviour (charted set is
                                         -- us-equity-dominant); never error on a new enum.
   ```

   The `session_profile` Literal type is the **single source of truth** for the four values;
   FE mirrors it. Total + default-bearing — an `asset_class` value added to the CHECK later
   falls through to `'us_equity'`, never crashes.

### Frontend

1. `classifyUsSession(epoch)` → `classifySession(profile, epoch, specials?)` (`specials`
   optional, default empty → behaves as weekday-only). NY-tz `_nyParts` unchanged.
   - `continuous` → always `"rth"` (no tint; PM/AH filters below become no-ops).
   - `foreign_equity` → weekend→`"closed"`, else `"rth"` (no PM/AH).
   - `us_equity` / `us_equity_rth` → existing ET logic, then override by NY-local date:
     - date ∈ `full_closures` → `"closed"` all day.
     - date ∈ `half_days` → `"rth"` for 09:30–13:00; **`"ah"` for 13:00–17:00; ≥17:00 →
       `"closed"`** (bounded — 13:00 early close per NYSE; 17:00 extended-session end. The
       prior unbounded `≥13:00→ah` wrongly tinted the overnight, Codex-flagged).
     - `us_equity_rth` → no PM/AH at all: 04:00–09:30 & 16:00–20:00 (and half-day ≥13:00) →
       `"closed"`; 09:30–16:00 (half-day 09:30–13:00) → `"rth"`.
   - `specials` = fetched `{full_closures, half_days}` (`Set<string>` of NY-local
     `YYYY-MM-DD`) for the visible years.
2. **All four call sites migrate** (`classifyUsSession` has no other callers — grep-verified):
   - `SessionBands.tsx` — shading; takes `profile` + `specials`. Run-grouping loop unchanged.
   - `ChartWorkspaceCanvas.tsx:477` — PM/AH visibility filter; pass `profile` (+ `specials`).
     Non-US profiles return `"rth"` → filter is a correct no-op (no PM/AH to hide).
   - `useLiveLastBar.ts:268` — live-tick session gate; pass `profile`. Same no-op property.
   - `PriceChart.tsx` — owns `instrument.session_profile`; fetches `/market-calendar/us/{year}`
     for the visible range's NY-local year(s), **us_* profiles only** (skip continuous/foreign),
     and threads `profile` + `specials` to the three consumers.
   `specials` is only material to `SessionBands` + the half-day-afternoon edge of the PM/AH
   filters; passing it everywhere is cheap but the filters may pass empty — the normal-day
   PM/AH toggle (the 99% case) needs only `profile`.
3. Loading/empty: if the calendar fetch fails, fall back to `specials` empty (current
   weekday-only behaviour) — never block the chart (`loading-error-empty-states` skill).

## Full-population verification (pre-merge)

- `us_market_specials(2026)` full_closures == NYSE 2026 published list (10 dates: Jan 1,
  Jan 19, Feb 16, Apr 3 Good Friday, May 25, Jun 19, **Jul 3** Independence-observed, Sep 7,
  Nov 26, Dec 25 — Codex-verified pandas run) and half_days == **{Nov 27, Dec 24}** (Jul 3
  is a closure, not a half-day → closure-wins). Cross-check 2025 + 2027 against nyse.com.
- Juneteenth guard: `us_market_specials(2021)` does **not** contain Jun 18/19 (pre-2022).
- Good Friday present in market but absent from `is_us_federal_holiday` (asserts the two
  calendars genuinely differ).
- Observed shift: New Year 2028-01-01 = Sat → Jan-going; July 4 2026 = Sat → observed Jul 3
  (and Jul 3 then is NOT also a half-day — closure wins).
- Dev DB: every distinct `instruments.exchange` resolves to a `session_profile` (no NULL
  crash); spot-check AAPL→us_equity, AAPL.RTH→us_equity_rth, an LSE sym→foreign_equity, a
  commodity/fx→continuous.

## Tests

- `tests/test_market_calendar.py` (pure, fast tier): the verification matrix above +
  observed-shift + half-day + Good-Friday-≠-federal cases.
- `frontend` `chartFormatters.test.ts`: `classifySession` table tests per profile ×
  {pre, rth, ah, half-day-afternoon, full-holiday, weekend, continuous}.
- One API test for `/market-calendar/us/{year}` shape + the `session_profile` join.

## Out of scope (Phase A)

- Phase B events-calendar page, Phase C coverage-gap dashed-line — re-filed.
- Non-US half-day/holiday precision (foreign equities get open/closed only). `pmc` revisited
  in Phase B if we chart non-US intraday.
- Per-region RTH *windows* for foreign equities (no PM/AH is already correct for eToro's feed).

## Definition of done

Lint/format/pyright + FE typecheck/test:unit/dark:check green; backend verify on dev
(`curl /market-calendar/us/2026` + `/instruments/AAPL` shows `session_profile`); FE verify
on :5173 (AAPL half-day Nov 27 shades correctly, a commodity shows no bands). Read-only +
FE/API → no jobs-daemon restart. PR records the verification figures.
