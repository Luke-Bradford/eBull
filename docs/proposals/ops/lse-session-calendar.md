# LSE trading-session calendar (#2312 clauses 1-3, minimum unblocking scope)

Status: proposed, 2026-09-16. Refs #2312, #2834, #2603, #2833.

## Why now

#2834 ARM A (tilt-ETF sleeve) is **unexecutable, not merely expensive** (#2312,
2026-09-16): all 12 US-listed tilt ETFs are CFD-only, so the sleeve cannot hold a
US listing at any cost; and all three ARM A candidates (`R1VL.L`, `IUMO.L`,
`IUQA.L`) are exchange `7` = LSE, `asset_class = 'uk_equity'`, which
`decide_core_preflight` refuses `core_unsupported_market_session` because
`SESSION_SUPPORTED_ASSET_CLASSES` is `{"us_equity"}`.

## ⚠⚠ What this does NOT do: it does not widen the allow-list

The obvious shape of this change — build the calendar, add `uk_equity` to the
allow-list — is **wrong, and Codex checkpoint 1 found why.** Verified in the repo:

`strategy_core_preflight._PREFLIGHT_SQL` reads halt state as

```sql
EXISTS (SELECT 1 FROM strategy_market_halts mh
         WHERE mh.source = 'nasdaq_trader_rss' AND mh.symbol = … AND mh.resumed_at IS NULL)
  AS is_halted,
(SELECT fetched_at FROM strategy_halt_feed_state WHERE source = 'nasdaq_trader_rss')
  AS halt_feed_at
```

Both halves are hardcoded to a **US** feed. For a `.L` instrument, `is_halted`
would be FALSE because the feed carries no such identity, and `halt_feed_at` would
be fresh because the US feed is polling. So the `core_instrument_halted` and
`core_halt_feed_stale` refusals **fail open for LSE names** — a UK name would be
admitted on the strength of evidence that cannot see it.

#2312's own park comment already listed the three pieces it owns: *"calendar,
LSE-clock session gate, LSE halt coverage"*. Halt coverage is the third and is
unbuilt. Widening the allow-list with one of the three landed is precisely the
*"weakening a gate that reads as progress"* failure the ticket warns about.

**So the admission set becomes DERIVED rather than hand-listed:**

```
SESSION_SUPPORTED_ASSET_CLASSES = {venues with a calendar} ∩ {venues with halt coverage}
```

Today that evaluates to `{"us_equity"}` — byte-identical behaviour, every existing
test green — and `uk_equity` moves from *"no trading-session calendar in this
repo"* to *"has a calendar; no halt feed covers this venue"*. The refusal is
unchanged; the **reason** stops being false and starts naming the real remaining
blocker. Admission later costs one map entry and no judgement call.

⚠ `CORE_PREFLIGHT_POLICY_VERSION = "core-preflight-v2"` names *"the `us_equity`
session allow-list"* among the things it freezes. The admission set does not move,
so no bump is required — and the derivation makes a future move impossible to do
silently.

## Scope

**LSE holidays, for the LSE-family venues.** `uk_equity` is exactly four
exchanges, all LSE-family, all `country = 'GB'`:

```sql
select exchange_id, description, country, asset_class from exchanges where asset_class = 'uk_equity';
 7 LSE | 42 LSE_AIM | 43 LSE AIM Auction | 44 LSE Auction   (all GB)
```

⚠ `exchanges.currency` is deliberately NOT cited as evidence here: it is a VENUE
lookup, so every `.L` row reads GBP regardless of the instrument's actual
denomination (`sql/366:37-50`). Country + description carry the claim; currency
carries nothing.

`asset_class` is an honest key for the UK and **not** for `eu_equity` — 13 distinct
venues (Madrid, Borsa Italiana, SIX, Oslo, Stockholm, Copenhagen, Helsinki,
Lisbon, Brussels, Amsterdam, Xetra ETFs, FRA, Euronext Paris) with different
calendars and, per the LSEG calendar cited below, different early-close times
(12:00 / 12:30 / 13:00). The `.DE` fallback mirrors stay refused; that is correct,
not an omission.

Out of scope, and named rather than implied: `exchanges` schema columns (clause 1's
`timezone`/`session_open`/`session_close`), the other 30+ venues, LSE halt
coverage, and anything inferring hours from observed bars (clause 2 forbids it).

## Source rule

**Bank holidays.** LSE recognises the public and bank holidays of **England &
Wales** (londonstockexchange.com/equities-trading/business-days). The authoritative
dated list is GOV.UK `https://www.gov.uk/bank-holidays.json`, division
`england-and-wales`.

⚠ `strategy_core_selection.earliest_possible_verdict_at`'s docstring already leans
on this rule in-repo (its 2026-09-01 vs hand-derived 2026-09-02 gap *is* the
31 August LSE bank holiday). The rule was identified there; this spec encodes it.

**Hours.** London Stock Exchange, *Consultation on market structure and trading
hours* (Market Notice N18/19 attachment 1,
`docs.londonstockexchange.com/…/n1819_attach1.pdf`), published SETS timetable,
verbatim row — `SETS 07:50 08:00 16:30 16:35 08:45` (opening-auction call /
continuous start / closing-auction call / end / duration) — and option E,
*"Maintain the current time of 08:00 - 16:30 London time"*.

Corroborated on a current document: LSEG **Turquoise Trading Calendar 2025**
(`docs.londonstockexchange.com/…/lse-turquoise-calendar-2025.pdf`), whose header
states *"Continuous trading … 08:00 / Market Close … 16:30"* in **London times**,
and whose per-venue **London Stock Exchange** column gives an *"Early Closing
Time"* of **12:30**.

⚠ Stated rather than papered over: the *governing* artefact for timings is the
**Millennium Exchange & TRADEcho Business Parameters** document, which MIT201
Issue 15.8 §4.4 delegates to and which is not publicly downloadable. The two
sources above are LSE-published and dated 2019 and 2025 respectively; MIT201 15.8
(effective 2026-01-19) still describes the same session sequence. That is the
strongest citation available without a customer login, and the module records it.

**Half days.** LSE Service Announcement 001/17122014, verbatim: *"SETS,
International Order Book (IOB), Order Book for Retail Bonds (ORB), SETSqx &
International Board closing auctions will all commence at 12:30GMT"* on 24 & 31
December — corroborated for 2025 by the LSEG calendar above, which marks
`Wed 24-Dec-25` and `Wed 31-Dec-25` as Early Close for the LSE column with the
12:30 time.

⚠ A dated announcement is not a standing rule. The module therefore treats the
December half-days as **rule-derived and annually re-verifiable**, and the
supported-horizon mechanism below is what stops a stale rule answering silently.

**The submission window** is continuous trading, `08:00 <= t < 16:30`
Europe/London (open inclusive, close exclusive), mirroring the NYSE gate's
`09:30 <= t < 16:00`. On a half day the close is `12:30`.

⚠ The 16:30-16:35 closing auction and the CPX session after it are deliberately
EXCLUDED: an auction is not a continuous-trading submission, and refusing there is
the safe direction.

⚠⚠ This is a **scheduled-window** claim, not a trading-state guarantee. Volatility
auctions, price-monitoring extensions, EDSP auctions and suspensions can occur
inside the window (MIT201 §4.4, §7.2). The gate that would catch those is halt
coverage — which is exactly what this change does NOT claim to have for the UK,
and why the allow-list stays closed.

⚠ The `08:00-16:30` window is the **SETS** timetable. AIM and SETSqx instruments
can run different market models (periodic auctions, quote-driven). Bank holidays
are venue-wide and apply to all four LSE-family exchanges; the HOURS half is
verified for SETS only, and admitting exchange 42/43/44 would need its own
parameter check. Recorded, not assumed away — and inert today because nothing is
admitted.

## Full-population verification

Scheduled rules composed from `pandas.tseries.holiday` primitives (no new
dependency — the same 2026-06-27 operator decision the NYSE module cites):

| holiday | rule |
| --- | --- |
| New Year's Day | Jan 1, `weekend_to_monday` |
| Good Friday | `GoodFriday` |
| Easter Monday | `EasterMonday` |
| Early May | first Monday in May |
| Spring | last Monday in May |
| Summer | last Monday in August |
| Christmas Day | Dec 25, `next_monday` |
| Boxing Day | Dec 26, `next_monday_or_tuesday` |

⚠⚠ **UK substitutes always move FORWARD. NYSE's `nearest_workday` moves a Saturday
holiday BACK onto the Friday.** Copying the NYSE observance would be wrong in both
December and January. `next_monday_or_tuesday` is pandas' rule for exactly the
"second of two adjacent holidays" case Boxing Day is.

Checked against the **full** GOV.UK England & Wales population, 2019-01-01 to
2028-12-26 (83 dates), not a sample:

- **78 of 83 derived exactly.**
- **5 transcribed additions** (no rule derives a royal or commemorative day):
  `2020-05-08` (Early May moved for VE Day 75), `2022-06-02` (Spring moved),
  `2022-06-03` (Platinum Jubilee), `2022-09-19` (State Funeral of Queen
  Elizabeth II), `2023-05-08` (Coronation of King Charles III).
- **2 transcribed suppressions**: `2020-05-04` and `2022-05-30` — the rule-derived
  dates those two holidays were **moved from**.

⚠ The suppression half is load-bearing and is what an additions-only model gets
wrong: a *moved* holiday is not an *extra* holiday, and modelling it as one leaves
two ordinary trading Mondays wrongly closed.

⚠ **Dates are not the whole population — NAMES are checked too**, because the
date-set can be right while the labels are wrong. `next_monday` /
`next_monday_or_tuesday` assign 2022-12-26 to *Christmas Day* and 2022-12-27 to
*Boxing Day*; GOV.UK has them the other way round (Boxing Day falls on the Monday
and is not moved; Christmas Day is the substitute on the Tuesday). Set equality
hides that, so the December pair carries an explicit reason override and the test
asserts the full `date -> name` mapping.

⚠ What the full-population check does NOT cover, stated so it is not read as
total: LSE half-days (corroborated for 2025 only, from the LSEG calendar), any
extraordinary LSE-specific closure that is not a bank holiday, and the session
boundaries themselves. Those rest on the cited documents, not on a population.

**Supported horizon.** GOV.UK publishes a rolling window; the verified range is
2019-2028. Outside it the calendar **raises** rather than answering, and the
venue-session helper maps that to a REFUSAL. A submission gate must fail closed on
an unverified year, and a raise is loud where a silently-wrong `open` is not. The
frozen fixture is therefore self-expiring: when the horizon is reached the calendar
stops answering and the fixture must be refreshed — this is the answer to "a frozen
fixture goes green while going stale".

The test ships a frozen copy of the GOV.UK JSON and asserts
`(derived ∪ additions) − suppressions == gov.uk` **by date and by name** over the
whole range, so the verification is deterministic, offline, and re-runs on every
push.

## Design

Additive. **`app/services/market_calendar.py` is not edited at all** — not even its
docstring — because `strategy_mt1_books.BOOK_RULE_VERSION` is
`… + MARKET_CALENDAR_RULE_VERSION`, which hashes that file's bytes. A docstring
tidy-up there would move MT-1 book identities. (Codex ckpt-1 finding 18; verified.)

1. **`app/services/lse_market_calendar.py`** (new) — mirrors the NYSE module's
   shape: `RULE_SET_ID` / `RULE_SET_VERSION` (code-hash), `lse_market_specials`,
   `lse_market_status`, `lse_market_reason`, `LSE_SESSION_OPEN` / `LSE_SESSION_CLOSE`
   / `LSE_HALF_DAY_CLOSE`, `_EXTRAORDINARY_CLOSURE_NAMES` (additions),
   `_RULE_SUPPRESSIONS` (moved-from dates) and `_REASON_OVERRIDES` (the 2022
   December pair). Reuses `MarketYear` from `market_calendar` rather than
   redefining it — importing does not change that module's hash; editing would.

   Import-time assertions, so a bad override cannot ship: suppressions must be
   disjoint from additions, and every suppression must actually be produced by the
   scheduled rules (a suppression that matches nothing is a typo, and silently
   doing nothing is the worst outcome). Closure-wins over half-days, and the
   `reasons` map is built AFTER suppression so a reopened Monday carries no
   holiday reason; `reasons` is total over `full_closures ∪ half_days` by
   construction.

2. **`app/services/market_session_support.py`** — gains the venue resolution the
   allow-list was already implying, plus the halt-coverage intersection described
   above. `venue_session_is_open(asset_class, now)` and
   `venue_market_status(asset_class, d)`; both require an aware `datetime` and
   raise on naive input rather than silently treating it as UTC.

   ⚠ The module stops being import-free. It now imports the two calendar modules,
   which are themselves leaves (stdlib + pandas only), so the
   `strategy_core_selection -> strategy_core_preflight -> strategy_core_mandate ->
   strategy_core_selection` cycle it exists to dodge is unaffected.

3. **`app/services/strategy_core_preflight.py`** — `_session_is_open(now)` becomes
   `_session_is_open(now, asset_class=…)`, dispatching through the resolver, and
   the `core_market_session_closed` detail reports the **venue-local** instant
   instead of always New York. For `us_equity` both are byte-identical to today;
   the existing tests are the equivalence evidence. Ordering is unchanged and
   matters: `session_support_reason` is checked FIRST, so the session test only
   ever runs for an admitted venue.

4. **`strategy_core_selection.py`** — no code change; it calls
   `session_support_reason`, whose result is unchanged.

## Deliberately NOT fixed here (named, so they are not read as covered)

Codex ckpt-1 surfaced these; all are pre-existing, none is made worse by this
change, and folding them in would turn a bounded ticket into an audit:

- LSE halt coverage (the blocker above) — #2312 owns it.
- `price_window_verdict._CALENDARED_ASSET_CLASS = "us_equity"`: UK non-session
  bars still bypass the non-session-bar clause.
- Candle freshness, `ops_monitor` and `sync_orchestrator` content predicates all
  use `latest_completed_us_session` globally.
- `/calendar` + `frontend/src/lib/chartFormatters.ts` classify UK instruments as
  `foreign_equity` on New York dates.
- `account_reconciliation_ledger` counts NYSE sessions account-wide.
- Preflight checks the session, then writes and commits before submitting; no
  re-check immediately before the broker call. Pre-existing, and sharper for LSE's
  earlier close — but unreachable while `uk_equity` is not admitted.

## Acceptance

1. Full-population holiday test green over 2019-2028 against the frozen GOV.UK
   fixture, **by date and by name**.
2. `lse_market_status`: `closed` on 2026-08-31 (Summer BH); `half_day` on
   2026-12-24 and 2026-12-31; `open` on 2020-05-04 and 2022-05-30 (the
   suppressions); raises outside 2019-2028.
3. `venue_session_is_open("uk_equity", …)` is TRUE at 15:59 London and FALSE at
   16:30 London, in both GMT and BST — the summer window is 07:00-15:30 UTC and
   the winter window 08:00-16:30 UTC, and a test asserts both so London is never
   treated as fixed GMT.
4. `SESSION_SUPPORTED_ASSET_CLASSES == {"us_equity"}` still, and
   `session_support_reason("uk_equity")` now cites the halt feed, not the calendar.
5. Every existing NYSE and preflight test unchanged and green — the equivalence
   evidence. `market_calendar.py` has a zero-byte diff.

## Security

No security surface: published reference data and pure-function predicates. No
broker mutation, no credential path, **no change to reachable trading authority** —
the admission set is byte-identical before and after, which is the whole point of
deriving it rather than editing it.
