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

## What we actually hold, and how to re-measure it

```sql
select count(*) from research_price_daily;    -- 75,972,649 bars   (2026-08-22)
select count(*) from research_price_series;   -- 30,591 series
select min(bar_date), max(bar_date) from research_price_daily;  -- 1962-01-02 .. 2026-08-21
```

⚠ **Run the queries; do not cite the numbers.** They are written down beside their own
SQL precisely because they go stale — and they did: five skill files carried
**25.9M bars / 7,727 series** until 2026-08-22, roughly a THIRD of the truth, and one of
them had derived a percentage from the stale denominator (the pre-decimalisation share in
`quant/strategy-evidence.md` §2.11a, which moved 20.3% → 16.2% when re-measured rather
than re-denominated). A figure written by hand goes stale silently in the place a reader
trusts most.

⚠ #2841 named THIS file as the one holding the stale figure. It did not hold it at all —
the copies were in `quant/strategy-evidence.md` (×3), `quant/data-capability.md` (×3),
`data-sources/market-structure.md` and `data-sources/etoro-api.md`. When correcting a
duplicated statistic, `grep` for the VALUE across the whole skill tree; the ticket telling
you where it lives is a starting point, not the inventory.

## The one-line answer

**Free FEEDS are Yahoo or downstream of it and have no delisting concept — but free
ARCHIVES do, because a scrape taken while a name was live keeps it forever.** Three
GitHub archives serve 258 of the 259-name Form 25 cohort (99.6%), split-adjusted, with
series terminating at the delisting. Survivorship is not a cost problem; it is a
**capture-date** problem, and the question to ask a source is *when was this frozen*, not
*what does it cost*.

⚠ **Superseded 2026-08-07 (#2346).** This section previously read *"no free source has
it"*, on the strength of the ten sources measured in #2284 — every one a live API or a
Yahoo-derived dataset. The sweep was sound and its conclusion did not generalise: it never
asked who had *stored a copy*. If you are about to conclude that some class of data cannot
be obtained, check whether you have searched for the live source or for its archives.

## Free archives that DO serve the delisted cohort (2026-08-07, #2346)

Measured on the committed fixture `tests/fixtures/form25_2023_cohort.csv` (259 names), not
a sample. All three are public GitHub repos — no key, no account, no payment.

| archive | tickers | cohort served | notes |
| --- | ---: | ---: | --- |
| `Stonks/tickers` | 9,805 (`nasdaq/` 6,661 + `nyse/` 3,144) | **247 / 259** after Q-strip | split-adjusted; AAPL 10,797 bars to 1980-12-12; frozen **2023-10-10**; volume on some files only; **no licence** |
| `icyDenev/Intrader` | 22,879 (`Data/Day/`) | **+11** (the AMEX residual) | runs later (2023-11-29); headerless OHLCV |
| `Deamoner/ultimate-…-training-dataset` | 8,188 (`full_history/`) | partial | carries `volume` **and** `adjclose`; frozen 2019-04-18; date-DESCENDING |

**258 / 259 (99.6%) served by at least one, with the series terminating at the delisting.**
Only `MNKT` is absent everywhere. For contrast, every source in the #2284 sweep scored 0.

⚠ **Mirror before use, do not fetch live.** None carries a licence and none owes us uptime.
Local copies: `var/research_corpus/mirrors/` (gitignored, ~8.1 GB, 40,872 CSVs). Consume
them; do not redistribute — the prices are facts, but a substantial extraction of someone's
compilation is protected by UK database right even where the facts are not.

### ⚠ The bankruptcy suffix — a symbol rule, not a lookup trick

A Form 25 cover page carries the **post**-bankruptcy ticker (`BBBYQ`, `YELLQ`, `SRNEQ` —
the `Q` is appended when an issuer files Chapter 11 and moves to OTC). **Every price
archive keys the pre-bankruptcy one.** Resolving a Form 25 symbol against any archive must
try the `Q`-stripped form: it recovered 13 of the 25 names that first read as absent.

This is a data-treatment decision, not convenience. Skipping it loses precisely the
**bankruptcies** and keeps the acquisitions — biasing the corpus along the one axis
survivorship-free data exists to protect, and the same axis §"Known cohort bias" says the
cohort cannot rule out.

### Venue coverage is a directory, not a vendor claim

`Stonks/tickers` ships `nasdaq/` and `nyse/` and nothing else, so the entire NYSE American
(AMEX) residual — `CCF`, `EMAN`, `IMH`, `NAVB`, `PLM`, `TKAT`, `TMBR`, `UFAB`, `WLMS`,
`WTT` — read as twelve missing names when it was one missing directory. Check the archive's
*layout* before concluding a name is unserved.

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
  listing date. ⚠ The second half as originally written — "truncate at any Form 25
  suspension date" — was tried in #2297 and does not work; see the block immediately
  below before implementing anything against it.

⚠⚠ **The second guard is now WIRED (#2297) and the wiring FALSIFIED it. Truncating at the
Form 25 suspension date cannot be done correctly, and the reason is structural, not a
coverage gap that more data fixes.** Run
`uv run python -m scripts.ingest_2282_research_archive --link-delistings` for live figures.

The asymmetry, measured on the full 2023 register — this is the whole finding:

```sql
select coalesce(rule_provision,'(c)/absent') as provision,
       count(*), count(suspension_date)
  from sec_form25_common_equity_delistings group by 1;
--   (a)(3)  212  62
--   (b)     105   0
```

- **`(b)` is the provision where truncation is unambiguously right** — exchange-initiated
  delisting for non-compliance, the ticker died. It states a suspension date on **0 of 105**
  cohort rows, because that sentence lives in the EX-99 rule-provision exhibit and exchanges
  attach a stub (sec-edgar.md §2.6 trap 5).
- **Every date the cohort supplies is `(a)(3)`** — merger, holdco reorganisation,
  redomiciliation, where the same economic entity commonly keeps trading under the same
  ticker and a continuous series is CORRECT.

So the truncation set is empty by construction, and a provision-blind truncation fires only
where it is wrong. Concretely, the two corpus series that carry a date are `LIN` (Linde plc,
8,572 bars, 1992→2026) and `AMRX` (Amneal, 2,051 bars) — **both `is_tradable = true` in our
universe today**. Truncating either deletes ~3 years of correct history from a live name.
Linde's own EX-99 (accession `0000876661-23-000160`, fetched direct from EDGAR) states all
three dates: removal-effective March 13, operation-of-law March 01, *"suspended from trading
on March 02, 2023"* — and LIN trades now.

**What #2297 therefore shipped:** the date and its `delisting_provision` are STORED
(sql/253, CHECK-tied so a reader of the date cannot lose the provision), and nothing
truncates — at ingest or at read time. `link_form25_delistings` in
`app/services/research_corpus_ingest.py`.

**#2721 extended this to the EVIDENCE model (sql/353):** `delisting_source` +
`delisting_provision` + `delisting_filed_date` now persist WITHOUT a suspension date —
`(b)` states one on 0 of 105 cohort rows, so the date-paired schema held the
exchange-failure class at zero coverage by construction. `delisting_date` is still never
back-filled from `filed_date`, a bare date is still unrepresentable, and a DATED
'sec_form25' row still must carry its provision. Measured on the 2023 register alone the
re-link moved terminating-Intrader coverage 37 → 249 series, `(b)` 0 → 89. What a held
position REALISES at a termination is `app/services/series_termination.py` (pure,
versioned, UNWIRED until the `BACKTEST_UNIVERSE` parameterisation wires it): linked
`(b)`/`(a)(4)` → last close × (1 − 0.55) (Shumway JF 1997 Table V −30% NYSE/AMEX;
Shumway & Warther JF 1999 −55% Nasdaq; adverse anchor, venue unknown), `(a)(3)` → last
close, unlinked → two-armed bounds.

**What is actually detectable, and it is the OTHER half of the guard.** "First bar precedes
the known listing date" was thought unimplementable because the schema holds no listing date.
A proxy needs neither a listing date nor a threshold: a series whose first bar postdates a
Form 25 on the same symbol cannot be the series of the security that filing removed. Four
today — `ALPS` (filed 2023-07-27, first bar 2025-10-31), `ATCX` (2023-04-19 → 2026-01-09),
`USX` (2023-07-03 → 2026-06-03), `DBD` (2023-06-20 → 2023-08-14). ⚠ Since #2721 the
linkage REFUSES to write evidence onto these (`classify_form25_match` →
`identity_unverified`): the filing removed a security whose price history the series
demonstrably is not, and under the evidence model an unrefused write would mark a live,
running series as delisted. `DBD` is Diebold Nixdorf relisting after Chapter 11, the SAME
company — which is exactly why the verdict is "identity unverified" (censused), never
"different company" (asserted).

⚠ **No corpus series is currently known to be welded.** `REED` and `SRAX` span their `(b)`
filings with no gap at all (last bar the day before, first bar the day of) — both moved to
OTC, which is continuation of the same company, not contamination. `terminating` is **0**:
nothing in the archive ends at a delisting, which is the same structural fact as the 0/259
acceptance result below.

⚠ **The overlap is 15 series, not 16.** The earlier figure counted JOIN ROWS: `AESI` has two
cohort filings (a 25-NSE and its 25-NSE/A, same day), so a naive join double-counts it —
§2.6 trap 1's shape one level up. Aggregate the cohort per symbol before joining.

⚠ Note also that only **62 of 317** cohort rows carry a `suspension_date` at all (395 of
1,282 across the whole register). The other 255 must stay NULL; back-filling with `filed_date`
is a different event and mistruncates. Reproduce with
`select count(*), count(suspension_date) from sec_form25_register`.

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

## ⚠ Correction to the acceptance test — "does the series terminate?" is the WRONG question for (a)(3)

Added 2026-08-05 (#2282 2c), after running the test for the first time against the
committed cohort. **Item 2 as originally written is not universally right, and applying it
flatly produces a false negative on the most common delisting provision.**

Measured: of the 259-name cohort, 15 symbols appear in the HF corpus at all. None
terminates at the delisting. But splitting them by shape:

| shape | n | provisions |
|---|---:|---|
| series **spans** the filing | 12 | mostly `(a)(3)` |
| series **starts after** the filing | 4 | `(b)` ×2, `(a)(3)` ×2 |

The spanning set includes `LIN`, `BG`, `LBTYA`, `HUT`, `NWE`, `AESI` — all `(a)(3)`. And
`(a)(3)` is *"instruments now evidence OTHER securities by operation of law"*: holdco
reorganisations and redomiciliations, where **the same economic entity keeps trading under
the same ticker**. For those, a continuous price series is the CORRECT answer, not
contamination. A backtest holding the name through a redomiciliation did not experience a
delisting.

So the test must be **stratified by provision**, not applied flat:

- **`(b)` — exchange-initiated failure.** The series SHOULD terminate at/near the
  suspension. A series continuing past it is an OTC continuation or a later occupant, and
  must be distinguishable.
- **`(a)(3)` — merger / reorg / redomiciliation.** Terminate-or-continue is
  *case-dependent*: the ticker may die (acquired for cash) or continue (holdco reorg).
  Continuation is not evidence of a defect.
- **`(a)(4)`, `(c)`** — as `(b)` for `(a)(4)`; `(c)` (voluntary withdrawal) usually means
  a move to OTC, so continuation under the same ticker is expected.

⚠ **Do not attribute individual names without checking.** `DBD` (Diebold Nixdorf, `(b)`,
filed 2023-06-20) has a series starting 2023-08-14 — consistent with a Chapter 11
reorganisation and relisting, i.e. the same company, not a later occupant. That reading is
*plausible and unverified*; the point is that "starts after the filing" does not by itself
establish ticker reuse either. Only **`(b)` with no corporate-action explanation** is
unambiguous evidence of a wrong series.

**What the 0/259 result does establish**, and it is enough: no name in the cohort is served
as a *delisted* series — nothing terminates, and the archive keys on the live ticker, so it
structurally cannot distinguish `X` from `X-DELISTED`. The free path remains unusable for
survivorship. The correction is to the *diagnosis*, not to the verdict.

## ⚠ Why the #2284 sweep missed all three (2026-08-07)

Worth keeping, because the sweep was competent and still produced a wrong general
conclusion. It enumerated **who sells or serves price data** — ten vendors and dataset
hosts — and every one is either a live API or a Yahoo redistribution, so every one scored
zero and the pattern looked structural.

What it never asked: **who stored a copy while the names were still trading.** A 2019 or
2023 scrape of a live feed contains exactly the delisted history the live feed can no
longer give you, and it sits in ordinary code repositories rather than in anything
resembling a data vendor.

The search that worked was `filename:<TICKER>.csv` for a name known to be delisted, which
is a search over *artefacts* rather than over *sources*. Generalises past prices: when a
sweep over providers returns uniform zero, the next question is not "which provider did I
miss" but "who has a frozen copy".

## Exchange test issues are not outliers — exclude them by identity

Added 2026-08-23 (#2912). The Intrader corpus contains official exchange test
securities. `ZBZZT` moved from 14 to 199,999.99 in March 2019 and destroyed a
published-factor correlation, but filtering that return by magnitude would be
post-result tuning. Nasdaq's Symbol Directory carries the structured source rule:
`Test Issue=Y` means a test security.

Before treating any extreme corpus return as a market observation, intersect the
vendor symbols with the official Nasdaq-listed and other-listed directory rows whose
test flag is `Y`. Exclude those identities before alive/terminated classification,
count the exclusion in the universe reconciliation, and move the hashed universe-rule
version. Freeze the response SHA-256 and normalized symbol set when historical
reproducibility matters; the live directory is not itself a point-in-time archive.

Do not replace this with a `Z*` symbol rule or a return cap. Legitimate issuers can
begin with Z, while exchange test traffic can use other prefixes (`ATEST`, `CTEST`,
`MTEST`, `NTEST`). The source identity, not the observed price path, is the rule.
