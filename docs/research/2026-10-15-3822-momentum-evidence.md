# #3822 — why the engine's first demo strategy is a momentum book (2026-10-15)

Operator direction (2026-10-15, #2437): prove a strategy in the demo account with data; pivot off dead ends. This note
records the quick tests that chose the strategy. Every figure is printed by a script in `scripts/research_3822/`, run
from the repo root with `PYTHONPATH=. uv run python <script>` against the dev DB.

## Dead ends (measured the same day)

| idea | script | result |
|---|---|---|
| Trend filter on the US market (Faber 10-month SMA), with and without leverage | `index_vol_trend.py` (French daily, snapshot 60) and `spy_trend_2026.py` (SPY 1993-2026) | Halves drawdowns, but trails SPY since 2022: 4.41%/yr against 12.38% (2022-01..2026-10-14) |
| Volatility targeting on the market | `index_vol_trend.py` | Roughly the market's return at lower volatility; leverage gains vanish at cash + 6% financing |
| Turn-of-month effect | `turn_of_month.py` | t = 8.8 for 1927-1986, t = 1.3 since 2006; no overlay gain since 2006 |
| Merger arbitrage (tender offers and merger votes, all SEC filers) | `merger_arb_extract.py`, `merger_arb_sim.py` | Tender offers about zero net (median 0.00%); the merger-vote portfolio returned 6.89%/yr against SPY's 13.50%, beta 0.47 |
| Real strategy funds against SPY | `strategy_etfs_vs_spy.py` | Merger arb (MNA) 2.24%/yr for 2016-2024 against SPY's 14.64%; managed futures (DBMF, KMLM) trailed by 8.7 to 9.2 points a year for 2021-2024; value, low-vol and small caps lost to SPY in every window |

## The one survivor: momentum

**Real funds.** Momentum ETFs beat SPY in every window measured:
- SPMO: +2.05 points a year for 2016-2024, and +2.72 for 2021-2024.
- XMMO: +0.72 for 2006-2024, and +3.45 for 2016-2024.

**Our book.** `momentum_backtest.py` runs on the survivorship-free Intrader archive, 2014-10..2024-09:
- **Universe:** the top 500 operating companies (#3361 linkage) by trailing dollar volume, close at least $5.
- **Signal and book:** risk-adjusted 12-1 momentum (11 monthly returns compounded, divided by their standard deviation), top 30 equal-weighted, monthly.

| | CAGR | max DD |
|---|---|---|
| Book, 0.1% per side | **16.97%** | 22.0% |
| Book, 0.2% per side | 15.87% | 22.7% |
| SPY | 13.19% | |
| Random 30 from the same universe (20 seeds) | median 7.40%, range 1.95%..10.64% | |

- **Control:** the book beats all 20 random baskets.
- **Plain momentum fails:** without risk adjustment, top 30 returns 11.58% at 0.2% per side.
- **Regime dependence:** every size lags SPY for 2014-2019 (top 30: 8.73% against 11.70%) and leads for 2019-2024 (26.64% against 14.82%). Top 40 and top 50 roughly match SPY over the decade.
- **Holding band:** a 1.5N or 2N band lowers the return to 14.2%..15.0%, so the book has no band.
- **Costs:** measured eToro spreads are about 0.03% (AAPL) and 0.17% (median of the quoted top-500 names), so 0.1-0.2% per side is conservative.

## Sector cap: tested, not adopted

The hypothetical 2026 book lost 23.5% in the June formation, with 27 of 30 names down in an AI-hardware and semis
sell-off. So the risk skill's rule was tested: cap any sector at about 2× its benchmark weight
(`portfolio-construction-and-risk.md`).

`momentum_sector_cap.py` applies it to the same book. Its first run takes `--extract`, which reads the panel.
- **Sector:** the FF-12 group of the point-in-time SIC from the #3609 factor panel. A name without a SIC is its own
  bucket.
- **Benchmark weight:** the group's market-cap share of the book's own 500-name universe. #3740 step 3 uses the same
  kind of same-universe reference, because no SPY constituent weights are held.
- **Construction:** a group holds at most floor(2 × share × 30) names, filled down the ranking. This is the strict
  reading of a weight ceiling on 30 equal weights, and it is a choice: a group under a 1/60 share gets no names.

About a fifth of universe names carry no usable panel market cap (median 78.8%, minimum 74.4% covered). A name with no
panel row cannot fund its own cap, so the clean comparison runs both arms on covered names only (`--covered-only`):

| covered names, 0.1% per side | 2014-2024 | max DD | 2014-2019 | 2019-2024 |
|---|---|---|---|---|
| Book | 13.72% | 21.9% | 8.00% | 20.28% |
| Book with the sector cap | 13.51% | 25.3% | 8.83% | 18.83% |
| SPY | 13.19% | 23.6% | 11.70% | 14.82% |

- **Binding:** the cap binds in 116 of 119 formations and displaces 622 holding slots.
- **Not a concentration control here:** the largest group's name count barely moves, from a mean of 9.5 to 9.2 (maximum
  20 to 15). Most displaced names come from small-share groups.
- **Result:** the cap raises the maximum drawdown by 3.4 points and costs 0.21 points a year. The worst month is about
  the same (−14.6% against −14.8%). The gaps are the same at 0.2% per side.
- **The 2026 theme crosses groups:** SNDK, WDC and MU (SIC 3572, 3674) are Business Equipment; GLW and LRCX (3357,
  3559) are Manufacturing (current SIC, `instrument_sec_profile`). An FF-12 cap cannot target that theme.
- **No 2026 replay:** the dev DB holds no market cap for eToro instruments, so the cap reference cannot be built.
- **Full universe:** the cap costs 3.2 points a year (13.76% against 16.97%). Dropping uncovered names alone cuts the
  uncapped book from 16.97% to 13.72%, so the cap's own effect and the coverage gap are confounded there.
- **Shared treatment:** both arms inherit `momentum_backtest.py`'s cash-out, where a held name with no next-month bar
  earns 0%. This hits 2 of 3,570 held name-months in each covered arm.
- **Decision:** the book stays uncapped. A theme limit for the live book needs a theme-level grouping, not FF-12.

## Out of sample, 2024-10..2026-08 (added 2026-10-16)

The backtest stops at 2024-09 because the Intrader archive does. `momentum_oos.py` runs the same rule on the PWB
`Stocks-Daily-Price@2026-09-09` snapshot for the 23 holding months after it. Its `run()` reproduces the backtest's
16.97% on Intrader, so the ranking, turnover and cost code match `momentum_backtest.py`; the PWB data path differs as
listed under "Limits".

- **Universe:** PWB series linked to an instrument the live book could hold today (tradable, canonical, US stock; the
  `momentum_book_job._CANDIDATES_SQL` predicate). **Survivor-only:** a name delisted before 2026-09 is absent, and the
  predicate is today's, not each formation's. The live 2026-09 formation's 500-name universe has a PWB series for 498
  names.
- **Returns:** total return from `adj_close`. SPY is Yahoo's monthly adjclose (public, no account); its closes match
  eToro's SPY month-end closes within 0.8% over 24 months. September 2026 is not scored (last PWB bar 2026-09-09).

| 2024-10..2026-08 | cumulative | per year | max DD |
|---|---|---|---|
| Book, 0.1% per side | +77.8% | +35.0% | 21.7% |
| Book, 0.2% per side | +74.6% | +33.8% | 21.7% |
| SPY | +36.4% | +17.6% | 7.6% |
| Random 30, same universe (50 seeds) | median +28.0%, range −2.7%..+65.2% | | |

| year | months | book | SPY | random-30 median |
|---|---|---|---|---|
| 2024 | 3 | +2.1% | +2.5% | −0.8% |
| 2025 | 12 | +33.1% | +17.7% | +15.6% |
| 2026 | 8 | +30.8% | +13.1% | +11.9% |

- **Control:** the book's cumulative return beats all 50 random-30 paths (each re-drawn monthly from the same universe).
  Both share the survivor-only universe.
- **Cross-dataset gap:** the same PWB universe over the backtest decade returns 19.88% a year against Intrader's
  survivorship-free 16.97% (random-30 median: 7.76% against 6.94%). The 2.9-point gap mixes survivorship, universe
  composition and return construction, so it is not a survivorship estimate and is not subtracted from the result.
- **Cross-source:** for the six holding months shared with the eToro-close replay posted on #3822 on 2026-10-15
  (March..August 2026), the two agree within 2.3 points in every month:

  | 2026 | Mar | Apr | May | Jun | Jul | Aug | compounded |
  |---|---|---|---|---|---|---|---|
  | PWB, total return | −12.4% | +29.7% | +13.6% | +0.5% | −21.7% | +0.7% | +2.3% |
  | eToro replay, price | −10.1% | +28.4% | +14.4% | +1.2% | −23.5% | −0.3% | +1.9% |

  The replay's +1.1% total adds September and starts after January and February 2026 (+13.5% and +12.7% here).
- **Volatility:** the book's months range from −21.7% to +29.7%, and its drawdown is about three times SPY's.
- **Stop rule** (#2437, 2026-10-16): stop if the stretch lags SPY by more than the backtest's worst lag over a window
  of the same length. The backtest's worst 23-month lag is 24.8 points (from 2015-08). Out of sample the book leads
  SPY by 41.4 points. The rule would fire only if the true book returned below +11.6% cumulative (5.9% a year)
  against the +77.8% measured, so a bias of about 66 points would be needed to reverse it. **Verdict: this
  survivor-only replay does not trigger the stop rule; the book keeps trading.**

**Limits.**
- The universe predicate is applied as of today, and series are de-duplicated by full-history bar count.
- Returns are PWB `adj_close` ratios; the backtest builds them from close, split factor and cash dividends.
- The $5 filter reads PWB's split-adjusted close, not the as-traded close.
- PWB bars carry no price quarantine, unlike the live candidate SQL.
- Two held name-months of 690 have no holding-month return and earn 0%: CDTX (last bar 2026-01-08, held February)
  and TERN (last bar 2026-05-07, held June). The live rule's 7-day staleness check (#3832) would have excluded both.
- The stop rule's threshold is the worst of 97 overlapping backtest windows, a sample maximum and not a confidence
  bound.

### With the dead names #3843 prices (added 2026-10-17)

`momentum_oos_dead.py` adds the 325 Form 25 events that #3843 covers to the same replay. 165 come from PWB, with total
return; 160 come from eToro `price_daily`, with price return.

Reproduce (dev DB, 2026-10-17 01:10Z):
`PYTHONPATH=. uv run python scripts/research_3822/momentum_oos_dead.py`

How the dead names are treated:
- **Termination** follows `series_termination.py` at the series' last bar. `(a)(3)` realises the last close; `(b)`
  realises 45% of it. The terminal month's return is rebuilt from levels, so a collapse to zero is charged rather than
  filtered out.
- **One series per instrument.** 14 names have a survivor PWB series whose capture ends before the delisting
  (`after_capture`). That series cannot show the termination, so the covering eToro series replaces it.
- **Formation.** A name is not a formation candidate in the month its series ends. This is a research convention, not a
  source rule, so a sensitivity row keeps the month eligible.
- **"Holdable"** means the name's instrument passes today's candidate predicate, ignoring `is_tradable`: US equity,
  stock type, canonical. That is a current-metadata proxy for whether eToro listed it at the time. 294 of the 325
  names pass.
- The survivor-only row already contains 133 of the covering PWB series, because they are still linked to a tradable
  instrument. It has no termination treatment.

| 2024-10..2026-08, 0.1% per side | book cumulative | per year | max DD | random-30 median (50 seeds) | book beats |
|---|---|---|---|---|---|
| Survivor-only (above) | +77.8% | +35.0% | 21.7% | +28.0% | 50/50 |
| Plus holdable dead names | +76.1% | +34.4% | 21.8% | +31.5% | 50/50 |
| Same, terminal month eligible | +76.3% | +34.4% | 21.8% | +29.0% | 50/50 |
| Plus all 325 covered | +76.1% | +34.4% | 21.8% | +31.5% | 50/50 |

- **Dead names held.** The book held dead names in 5 name-months, all `(a)(3)`: CYBR twice, CDTX and TERN in their
  final months, and EA, whose last bar is 2026-08-04. It held none of the 24 covered `(b)` failures.
- **CDTX and TERN** were the survivor-only row's two 0% cash months. They drop out once a name whose series has ended
  stops being a formation candidate.
- **The 31 names that fail the holdable predicate** leave the summary figures unchanged; the two arms print the same
  results.
- **Stop rule:** in every row the book leads SPY by at least 39.7 points, against a 24.8-point threshold. **The rule
  does not fire for these replays.**
- **What stays unmeasured.** The 299 uncovered events are still absent: 119 `(a)(3)`, 103 `(b)`, 1 `(a)(4)` and 76
  with no symbol. The rule would fire only if the book's true cumulative return were below +11.6%. No figure here
  bounds what the uncovered names would have contributed: their histories could change scores, the liquidity cut and
  holdings. The mix of price-return eToro series and total-return PWB series is also not separated from the treatment
  effect.
- **Unpriced terminations.** GRTSQ's and KAVL's terminal months have no positive prior-month level, so their
  termination is unpriced. In the rows that exclude the terminal month, neither the book nor any of the 50 random
  draws held a dead name in a month with no return.

## Tournament: three challengers, none adopted (added 2026-10-16)

#2437 item 2 asked for three price-only quick tests against the book. A challenger goes to demo only if it beats SPY
and the book out of sample. `momentum_tournament.py` runs each one through `momentum_oos.run()`: same universe, top
30 equal weight, monthly, 0.1% per side. Only the ranking changes, and the script reproduces the book's 16.97% and
+77.8% first. Every challenger held 30 names in every formation.

- **52-week high** (George & Hwang 2004): month-end close divided by the highest daily close of the past 12 months,
  on a split-adjusted price-only level. A missing or filtered month breaks the comparison, so that name is skipped.
  Ties at the high are broken by the book's signal.
- **Industry momentum** (Moskowitz & Grinblatt 1999):
  - **Industry return:** an equal-weighted portfolio of the industry's universe names, rebalanced monthly and
    compounded over the last 6 months (m-5..m, no skip month). M&G weight by value; no market cap is held for the
    out-of-sample names.
  - **Selection:** the top 3 FF-12 industries, each with at least 5 names, supply 30 names by the book's signal.
  - **Classification:** FF-12 is French's committed `Siccodes12`; M&G's own 20-industry table is not held. SIC is
    point in time from the #3609 panel in the backtest, which classifies a median 85.8% of universe names. The panel
    ends at 2024-07, so the 2024-08 formation reuses it. Out of sample, SIC is current (`instrument_sec_profile`,
    99.6%), which is look-ahead on classification.
- **Residual momentum** (Blitz, Huij & Martens 2011):
  - **Model:** 36 monthly excess returns (m-35..m) regressed on market, SMB and HML.
  - **Score:** the sum of residuals over m-11..m-1, divided by their standard deviation.
  - **Factors:** French's five-factor monthly file, pinned to snapshot 39 (to 2026-08). Its SMB is the five-factor
    construction, not the three-factor file's. The latest vintage is used for every month, so factor revisions are
    not point in time.

| 0.1% per side | backtest 2014-11..2024-09, per year | max DD | 2014-2019 | 2020-2024 | out of sample 2024-10..2026-08, cumulative | max DD | turnover per month |
|---|---|---|---|---|---|---|---|
| **Book** | **16.97%** | 22.0% | 8.73% | 26.64% | **+77.8%** | 21.7% | 78% |
| 52-week high | 5.92% | 21.1% | 1.01% | 11.54% | +9.4% | 19.0% | 160–168% |
| Industry momentum | 15.25% | 24.0% | 8.48% | 23.10% | +1.5% | 27.1% | 97–111% |
| Residual momentum | 12.09% | 32.1% | 7.49% | 17.33% | +47.3% | 12.3% | 95–96% |
| SPY | 13.19% | 23.6% | 11.70% | 14.82% | +36.4% | 7.6% | |
| Random 30, median of 20 | 6.94% | | | | +29.5% | | |

- **Turnover** is the mean of the sum of absolute weight changes per rebalance, excluding the initial buy. Full
  replacement is 200%.
- **Cash-out** is the inherited convention (a held name with no return earns 0%). In the backtest it covers 28, 89,
  1 and 23 of 3,570 held name-months (book, 52-week high, industry, residual), and in the out-of-sample stretch 2, 5,
  0 and 2 of 690.

- **None is adopted.** All three trail the book in both windows.
- **Industry momentum** beats SPY in the backtest (15.25% against 13.19%, 20 of 20 random paths), but out of sample
  it returns +1.5%, below SPY and every random path.
  - It is fragile to construction. A first version scored an industry by the mean of its names' compounded 6-month
    returns. That favours industries holding one or two extreme winners, and it returned +62.3% out of sample (14.99%
    in the backtest; that version is not in the committed script).
  - Codex checkpoint 1 flagged that version as unlike M&G's industry portfolio, and the faithful construction above
    replaced it. Neither version beats the book.
- **Residual momentum** trails SPY in the backtest (12.09% against 13.19%). Out of sample it beats SPY (+47.3%) with
  the smallest drawdown of the momentum variants (12.3%), but it trails the book.
- **52-week high** is a dead end in this form: below the random median in both windows.
  - Each month, between 0 and 137 universe names close at their 12-month closing high in the backtest (median 17),
    and between 0 and 56 out of sample (median 28).
  - The 30th name sits at 0.90–1.00 of its high in the backtest, and 0.96–1.00 out of sample. So the book is mostly
    "names at their high", rotated almost completely each month.
  - G&H's evidence is on 6-month holds and decile-wide portfolios, not a monthly top 6%. This test covers only the
    book's construction.
- **One configuration each:** apart from the industry-return correction above, each challenger has one configuration
  fixed before its first run. Variants (industry counts, holding periods, blends) were not searched, so the result is
  not "the best version of each idea".

**Expected, honestly:** a modest and uncertain premium over SPY, with drawdowns at least as deep as SPY's and long
stretches of lagging. The demo book is scored live against SPY and against a random-30 control, and the stop rule
above applies.

## Live rule

`app/services/momentum_book.py`; the live data is eToro `price_daily` (split-adjusted, no dividends, so price momentum).
- **Universe:** tradable canonical US stocks.
- **Formation:** monthly, from the completed month t, traded on the first session after month t.
- **Book:** top 30. Names that leave the book are sold.
