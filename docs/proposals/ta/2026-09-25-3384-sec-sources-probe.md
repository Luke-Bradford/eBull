# #3384 slice 2a — admissibility probe: SEC fails-to-deliver and SEC MIDAS

Follows slice 1 (`2026-09-25-3384-hunt-admissibility-census.md`). Neither source is held. Nothing is ingested
and no outcome return is computed. Slice 2b (Cboe, FRED/ALFRED, COT, Wikipedia) comes separately.

**Reproduce:** `SEC_UA="<Name> <email>" PYTHONPATH=. uv run python -m scripts.probe_3384_sec_sources --out
<path.json> --cache <dir>`. Committed output: `2026-09-25-3384-sec-sources-probe.json`. Every figure below is
read from it. The file lists are the sources' own download pages as of the run (`generated_at`).

## Method, and what it can and cannot show
- **Inventory:** every `.zip` linked from the source's page, parsed to its period.
- **Publication clock:** HTTP `Last-Modified` minus period end, per file. That is an **upper bound** on the
  first-release lag, because a re-post moves it later and never earlier. A date shared by ≥ 5 files is
  treated as a bulk re-post and excluded from the lag quantiles.
- **Identity:** one file per year, the first date in it. The source's tickers on that date are matched to
  Intrader series whose stored bounds contain the date, with separators unified. This is a **symbol**
  match: reuse and renames are not resolved, and the match rate is what an unresolved ticker route
  would deliver.

## SEC fails-to-deliver
Source rule (the SEC page, quoted in the probe's inventory): the files give *"the date, CUSIP numbers, ticker
symbols, issuer name, price, and total number of fails-to-deliver (i.e., the balance level outstanding)"* from
NSCC's CNS system. **"Data prior to September 16, 2008 include only securities with a balance of total
fails-to-deliver of at least 10,000 shares"**. From then on, every security with a balance is included. *"The
first half of a given month is available at the end of the month. The second half … at about the 15th of the
next month."*

- **Files:** 434 (all HTTP 200). Quarterly FOIA files 2004Q1 → 2009Q2, then half-monthly (`a` = 1st–15th,
  `b` = 16th–month end) from 2009-07. Two periods (201910a, 202308b) also have a `_0` re-post file.
- **Clock:** 278 files share Last-Modified 2020-12-19, covering periods 2004-03 → 2020-11. 18 files return the
  epoch date (1970-01-01), covering periods 2008-09 → 2018-04. **So no per-file release date is observable
  for any period before 2020-12.** The other 138 files (periods from 2020-12): lag p50 15 days, p90 18. The
  outliers
  are `202511b` (67 days) and the `202308b` re-post (987). That matches the page's stated schedule; the
  schedule is the only rule for earlier years.
- **Identity:** each row carries a CUSIP and a ticker. Tickers on the sampled dates:

| sample | tickers | matched to a spanning Intrader series | admitted series with a bar | … of which in FTD |
| --- | ---: | ---: | ---: | ---: |
| 2004q2 (2004-04-01) | 2,826 | 596 | 3,324 | 446 |
| 2008q2 (2008-05-01) | 3,094 | 1,007 | 4,428 | 768 |
| 200907a (2009-07-01) | 6,774 | 3,061 | 4,606 | 2,379 |
| 201306a (2013-06-03) | 5,603 | 3,632 | 5,885 | 2,726 |
| 201806a (2018-06-01) | 4,953 | 3,529 | 6,986 | 2,514 |
| 202206a (2022-06-01) | 5,976 | 4,477 | 8,301 | 2,741 |
| 202406a (2024-06-03) | 5,331 | 3,917 | 5,086 | 1,673 |

(all 21 years in the JSON). The page says the data "include fails-to-deliver in equity securities"; the
archive holds a subset of those, so unmatched tickers are expected, but their composition is not measured here.
Absence from a file is not "zero fails": it means no balance, or before 2008-09-16 a balance below 10,000
shares.

## SEC MIDAS — metrics by individual security and exchange
- **Files:** 57 quarterly zips, 2012Q1 → 2026Q2. Each is a zip of a zip of monthly CSVs (sampled files 76–187 MB).
  Columns: `Date, Security, Ticker, Exchange, McapRank, TurnRank, VolatilityRank, PriceRank, Cancels, Trades,
  LitTrades, OddLots, Hidden, TradesForHidden, OrderVol('000), TradeVol('000), LitVol('000), OddLotVol('000),
  HiddenVol('000), TradeVolForHidden('000)`. **Ticker only: no CUSIP, no CIK.** Some quarters write `Date` as a
  float (`20160601.0`).
- **Clock:** 29 files share 2020-12-19 (periods 2012-03 → 2020-09) and 5 return the epoch date. Periods
  2020-Q4 onward (23 files): lag p50 32 days, p90 198. The 2025 quarters came out 163–289 days after quarter
  end, so **release timing is not a constant**. An availability rule needs each quarter's real release date,
  and no such date is observable before 2020-Q4.
- **Identity:** tickers matched to a spanning Intrader series: 3,887 / 4,949 (2012-04-02), 5,016 / 5,282
  (2016-06-01), 5,357 / 5,741 (2020-04-01), 6,644 / 7,340 (2024-04-01). Admitted series with a bar that appear
  in MIDAS: 3,091 / 5,464, 3,846 / 7,137, 3,686 / 6,578, 3,511 / 5,291.
- `McapRank` is, by its name, a market-cap rank (its definition is not read here). If so, it is the only size
  axis in any source probed so far, and it exists only from 2012.

## Inputs this gives #3385 / the FTD and MIDAS ingests
1. **FTD availability before 2020-12 cannot be measured.** The only rule is the SEC's stated schedule
   (period end + ~15 days); the measured lags since 2020-12 agree (p50 15, p90 18, one 67-day outlier).
   An availability lag is a construction choice to freeze, and it must be at least as late as the measured
   tail.
2. **FTD before 2008-09-16 is a different population** (≥ 10,000-share balances only), so a pre-/post-2008-09
   series is not one variable. Discovery (to 2008-12-31) is censored except for its last 3½ months.
3. **FTD has a CUSIP; we hold no dated CUSIP↔series map** (slice 1, Finding 4). A ticker-only join reaches
   12–17% of admitted-with-bar series in the 2004–2008 samples and 32–54% in 2009–2024, as symbol matches with reuse
   unresolved.
4. **MIDAS stays validation-only** (2012+), as the programme doc already says. Its release timing varies by
   hundreds of days and cannot be observed before 2020-Q4.
