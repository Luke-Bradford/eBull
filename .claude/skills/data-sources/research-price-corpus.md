# Research price corpus — vendor landscape and the survivorship acceptance test

## When to use

Before evaluating, adopting or arguing about any historical price data source for
backtesting. Also read it before repeating the phrase "we can just download free market
data" — the landscape below was measured on 2026-08-05 (#2284) and the answer is not the
obvious one.

This covers the **research corpus** (deep history, backtesting). It does not cover the
**execution venue** — eToro, `price_daily`, `daily_candle_refresh` — which is
`.claude/skills/market-data/SKILL.md`. Keeping those two roles separate is the settled
model; see `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §0.

## The one-line answer

**Every free source is Yahoo, or downstream of Yahoo, and Yahoo has no delisting concept.**
Cost is not the binding constraint on a research corpus — survivorship is, and no free
source has it.

## Measured landscape (2026-08-05, #2284)

⚠ **The cohort was rebuilt properly in #2282 stage 2c (2026-08-05) and is now committed at
`tests/fixtures/form25_2023_cohort.csv` — 259 issuers, not 382.** The 382 below was
measured before a **security-class** filter existed, so its denominator mixed warrants
(155), funds (111), units (62), preferred (56), notes (10) and rights (3) in with the
common stock. Those are not companies, and four of those classes have no eToro instrument
type at all, so they can never contribute survivorship bias to our backtests. See
`sec-edgar.md` §2.6 trap 6.
**The vendor hit RATES below are still directionally valid** — every free source returned
0 — but re-run any new vendor against the committed 259-name file, not against 382.

Each row was tested against the same cohort: **all 382 US common-equity delistings of 2023**,
built from EDGAR Form 25/25-NSE per `sec-edgar.md` §2.6. "Serves" means the source returns a
series that **stops at the delisting** rather than being absent or silently continuing on a
successor/reuse of the ticker.

| source | serves the delisted cohort | notes |
| --- | ---: | --- |
| yfinance / Yahoo | **0 / 382** | 87.4% absent. ToS §2.4(i) bars automated collection **for any purpose** |
| HF `paperswithbacktest/Stocks-Daily-Price` | **0 / 382** | 92.4% absent. **29/29 identical first-bar dates vs Yahoo — it is a Yahoo scrape** |
| HF `defeatbeta/yahoo-finance-data` | — | named for its source |
| Microsoft `qlib` US bundle | — | `scripts/data_collector/yahoo/collector.py` |
| `lse-data` (London Strategic Edge) | **0 / 382** (5.2% present, none stopping) | catalog is **live instruments only** — 0 of 3,982 stock series end >180d ago. 57.9% of our us_equity |
| Nasdaq's own free API | — | `AAPL` returns rows; **`UMPQ` → `"Symbol not exists"`** |
| stockanalysis.com | — | **redirects delisted tickers to the acquirer** (`UMPQ`→`COLB`, `LHCG`→`UNH`) |
| Alpha Vantage | — | 25 req/day free ⇒ **507 days** for one pass over 12,684 instruments |
| Marketstack | — | 100 req/mo, **1 year** history, non-commercial |
| Stooq | — | proof-of-work JS challenge on CSV, bulk and the `.pl` mirror |
| **FirstRateData** | **308 / 382 (80.6%)** tagged `-DELISTED` | one-off bundle. 8,069 delisted of 16,255. Three adjustment bases |
| **HistoricalData.net** | untested; claims 50,000+ delisted | **one-off, from $299**, "no API, no subscription, no lock-in" |

## Two techniques worth reusing

### 1. Fingerprint a "free archive" against Yahoo before trusting it

A dataset card that names no upstream source is not an independent source. Pull the
**first-bar date per symbol** from the archive and from Yahoo for the same names and compare
exactly. On the HF archive this was **29 of 29 identical**, including Yahoo's own artefacts
(`ATCX` 2026-01-09, `SAFE` 1989-11-16 — iStar's history under Safehold's ticker). Real
independence shows up as disagreement on the pathological cases; identity to the day does not
happen by coincidence.

### 2. Size the API against the corpus before evaluating anything else

Bulk history is a **transfer problem, not an access problem**. Compute
`instruments × history ÷ rate limit` first and most candidates disqualify themselves before
coverage or licence matter:

| | one pass over 12,684 instruments |
| --- | ---: |
| a 525 MB Parquet archive | **~30 seconds** |
| Yahoo, measured | ~3 hours, ~15% of probes 429'd |
| Alpha Vantage free (25/day) | **507 days** |
| Marketstack free (100/mo) | **127 months** |

APIs are built for incremental top-up; archives are built for history. That is the
architecture: **buy or download the back history once, top up from `daily_candle_refresh`.**

## The acceptance test — run it before adopting any source

Do not accept a vendor's survivorship claim. The cohort build is in `sec-edgar.md` §2.6
(EDGAR full-index → `<ruleProvision>` filter → cover-page `dei:TradingSymbol`). For each of
the 382 names ask three things, because "the symbol resolves" is not one of them:

1. **Is the name served at all?**
2. **Does the series terminate at the suspension date?** (Not the filing date — see §2.6
   trap 5; a Form 25 carries three distinct dates.)
3. **Is it the right company?** A series that *spans* the delisting is the successor entity,
   an OTC continuation, or a later occupant of the ticker. Ten of Yahoo's 48 hits **begin
   after** the delisting — `SI` (Silvergate, failed 2023) returns a series starting
   2025-07-31.

A source that keeps `X` and `X-DELISTED` as separate series (FirstRateData) can answer (3).
One keyed on the live ticker (Yahoo, LSE, every free archive) structurally cannot.

**Known cohort bias, state it when using it.** On the rebuilt common-equity cohort
(#2282 2c) ticker resolution succeeds for **259 of 308 issuers (84.1%)**; the failures are
closed-end funds (they file N-CSR, not cover-page XBRL) plus foreign private issuers.

⚠ **Do NOT repeat the earlier "unbiased on the failure-vs-acquisition axis" claim.**
Measured on the common-equity cohort, the unresolved side is **71%** (a)(3) acquisitions
(n=49) against **65%** on the resolved side (n=259) — a +6.6-point skew toward losing
acquisitions, z = 0.92. Neither established as biased nor demonstrated to be unbiased; the
cohort cannot rule it out. Recompute with
`build_2282_form25_register --census` rather than quoting these figures. The earlier claim was computed on the
pre-security-filter denominator. This is the axis survivorship turns on, so state the
uncertainty rather than the reassurance.

## What free data can and cannot support

**Can** — deep history on *currently listed* names, which is a real gain over eToro's ~4-year
cap: price-structure primitives (#2279), single-name strategy development and debugging.

**Cannot** — validation. Every result on a survivor-only universe is biased, and the bias is
largest exactly where it hurts most: **mean-reversion on oversold names**, because the
missing names are disproportionately the ones that got oversold and kept going. #2260
(RSI<30 → 76.8%) is that shape, which is why it cannot be answered on free data.

Survivorship's importance is **strategy-shape-dependent** — smallest for trend-following on
liquid large caps (which are acquired rather than dying), fatal for cross-sectional momentum
(which additionally needs point-in-time index membership that none of these sources sell).

Two guards are mandatory whenever free data is used, because it fails silently:

- **Label it.** A metric computed on a survivor-only universe is marked as such in the
  signal ledger, not in a comment.
- **Guard ticker reuse.** Require the series' first bar to precede the instrument's known
  listing date, and truncate at any Form 25 suspension date.

## Coverage limit that no option on the table fixes

Every survivorship-free source found is **US-only**. Our tradable universe is 12,684 of
which **4,749 are non-US**, and Form 25 is US-only too. So a bought corpus validates
strategies on ~57% of what we can actually trade. That is a scope decision for the strategy
catalogue (§4), not something to discover later.

## Identifier constraint — ours, not the vendor's

`external_identifiers` covers **6,052 of 12,684 tradable (47.7%)**, all SEC/OpenFIGI-sourced
and therefore US-centric. **No ISIN and no SEDOL anywhere in the schema.** For ~6,600 mostly
non-US instruments the only join key to any vendor is symbol + exchange. Rule-based symbol
generation resolves **12,420 of 12,684 (97.9%)**; the 264 that do not are CME dated futures,
commodity CFDs and index CFDs — no equities.
